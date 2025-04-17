#!/usr/bin/env python3
"""
Example demonstrating the Headers Exchange pattern using RabbitMQ.

Headers exchanges route messages based on header attributes instead of routing keys.
This allows for more complex routing scenarios based on message metadata.
"""
import os
import sys
import time
import logging
import threading
import pika
import uuid
from typing import Dict, Any

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dmq.connection_manager import BrokerConnectionManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def message_handler(channel, method, properties, body):
    """Handler for received messages in headers exchange
    
    Args:
        channel: The channel object
        method: Contains delivery information
        properties: Message properties
        body: The message body
    """
    consumer_tag = method.consumer_tag
    queue_name = method.routing_key
    headers = properties.headers or {}
    
    try:
        # Decode the message body
        message = body.decode('utf-8')
        logger.info(f"Consumer {consumer_tag} on queue {queue_name} received message: {message}")
        logger.info(f"Message headers: {headers}")
        
        # Simulate processing
        time.sleep(0.2)
        
        # Acknowledge the message
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        # Negative acknowledgment (reject and requeue)
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

def setup_consumer(exchange_name, queue_name, binding_headers, match_type="all"):
    """Set up a consumer for a headers exchange with a dedicated connection manager
    
    Args:
        exchange_name: Name of the headers exchange
        queue_name: Name of the queue to bind
        binding_headers: Headers to match for routing
        match_type: "all" to match all headers, "any" to match any header
    """
    # Create dedicated connection manager for this consumer
    consumer_broker_manager = BrokerConnectionManager()
    consumer_broker_manager.register_broker(1, "localhost", 5672)
    consumer_broker_manager.register_broker(2, "localhost", 5682)
    consumer_broker_manager.register_broker(3, "localhost", 5692)
    
    if len(consumer_broker_manager.load_balancer.connections) == 0:
        logger.error(f"Failed to connect to RabbitMQ cluster for consumer {queue_name}")
        return None
    
    # Get a connection first, then a channel
    connection = consumer_broker_manager.acquire_connection(strategy="least_active")
    if not connection:
        logger.error(f"Failed to acquire connection for consumer {queue_name}")
        consumer_broker_manager.close()
        return None
        
    channel = connection.get_channel()
    if not channel:
        logger.error(f"Failed to acquire channel for consumer {queue_name}")
        consumer_broker_manager.close()
        return None
    
    try:
        # Declare the exchange
        channel.exchange_declare(
            exchange=exchange_name,
            exchange_type='headers',
            durable=True
        )
        
        # Declare the queue
        channel.queue_declare(
            queue=queue_name, 
            durable=True
        )
        
        # Add the special x-match header
        binding_args = binding_headers.copy()
        binding_args["x-match"] = match_type
        
        # Bind the queue to the exchange with the headers arguments
        channel.queue_bind(
            queue=queue_name,
            exchange=exchange_name,
            routing_key='',  # Ignored for headers exchanges
            arguments=binding_args
        )
        
        # Set QoS (prefetch count)
        channel.basic_qos(prefetch_count=1)
        
        # Start consuming
        consumer_tag = channel.basic_consume(
            queue=queue_name,
            on_message_callback=message_handler,
            auto_ack=False
        )
        
        logger.info(f"Set up consumer on queue: {queue_name}")
        logger.info(f"Queue will match messages with headers: {binding_headers} ({match_type})")
        return channel, consumer_tag, queue_name, consumer_broker_manager
    except Exception as e:
        logger.error(f"Failed to setup consumer for {queue_name}: {e}")
        consumer_broker_manager.close()
        return None

def run_example():
    """Run the headers exchange example"""
    # Create a headers exchange
    exchange_name = "order_processing"
    
    # Set up consumers with different header binding patterns
    consumers = []
    
    # Queue for high priority orders
    high_priority_consumer = setup_consumer(
        exchange_name,
        "high_priority_orders",
        {"priority": "high"},
        "all"  # Must match all specified headers
    )
    if high_priority_consumer:
        consumers.append(high_priority_consumer)
    
    # Queue for orders from specific regions
    region_consumer = setup_consumer(
        exchange_name,
        "us_eu_orders",
        {"region": "us", "region": "eu"},
        "any"  # Match any of the specified headers
    )
    if region_consumer:
        consumers.append(region_consumer)
    
    # Queue for premium customer orders
    premium_consumer = setup_consumer(
        exchange_name,
        "premium_customer_orders",
        {"customer_type": "premium", "loyalty_level": "gold"},
        "all"
    )
    if premium_consumer:
        consumers.append(premium_consumer)
    
    # Set up a dedicated publisher connection manager
    publisher_broker_manager = BrokerConnectionManager()
    publisher_broker_manager.register_broker(1, "localhost", 5672)
    publisher_broker_manager.register_broker(2, "localhost", 5682)
    publisher_broker_manager.register_broker(3, "localhost", 5692)
    
    if len(publisher_broker_manager.load_balancer.connections) == 0:
        logger.error("Failed to connect to RabbitMQ cluster for publisher. Is the DMQ cluster running?")
        # Clean up any already created consumers
        for _, _, _, consumer_broker_manager in consumers:
            consumer_broker_manager.close()
        return
    
    # Get a connection first, then a channel
    publisher_connection = publisher_broker_manager.acquire_connection(strategy="least_active")
    if not publisher_connection:
        logger.error("Failed to acquire connection for publisher")
        # Clean up any already created consumers
        for _, _, _, consumer_broker_manager in consumers:
            consumer_broker_manager.close()
        publisher_broker_manager.close()
        return
        
    publisher_channel = publisher_connection.get_channel()
    if not publisher_channel:
        logger.error("Failed to acquire channel for publisher")
        # Clean up any already created consumers
        for _, _, _, consumer_broker_manager in consumers:
            consumer_broker_manager.close()
        publisher_broker_manager.close()
        return
    
    try:
        # Declare the exchange
        publisher_channel.exchange_declare(
            exchange=exchange_name,
            exchange_type='headers',
            durable=True
        )
        
        # Start consumer threads
        consumer_threads = []
        stop_event = threading.Event()
        
        for channel, consumer_tag, queue_name, _ in consumers:
            # Define thread function to process events until stop event is set
            def consumer_thread_func(ch=channel, stop=stop_event):
                try:
                    while not stop.is_set():
                        # Process events for a short interval, then check stop_event again
                        ch.connection.process_data_events(time_limit=1)
                except Exception as e:
                    logger.error(f"Error in consumer thread: {e}")
            
            thread = threading.Thread(target=consumer_thread_func)
            thread.daemon = True
            thread.start()
            consumer_threads.append(thread)
            logger.info(f"Started consumer thread for queue: {queue_name}")
        
        # Wait for consumers to initialize
        time.sleep(1)
        
        # Define some sample order types
        order_types = [
            {
                "description": "High priority order from regular customer in US",
                "headers": {
                    "priority": "high",
                    "region": "us",
                    "customer_type": "regular"
                }
            },
            {
                "description": "Normal priority order from premium customer in EU",
                "headers": {
                    "priority": "normal",
                    "region": "eu",
                    "customer_type": "premium",
                    "loyalty_level": "gold"
                }
            },
            {
                "description": "High priority order from premium customer in Asia",
                "headers": {
                    "priority": "high",
                    "region": "asia",
                    "customer_type": "premium",
                    "loyalty_level": "gold"
                }
            },
            {
                "description": "Normal priority order from regular customer in US",
                "headers": {
                    "priority": "normal",
                    "region": "us",
                    "customer_type": "regular"
                }
            }
        ]
        
        # Publish orders with different headers
        for i, order_type in enumerate(order_types):
            # Create an order message
            order_id = str(uuid.uuid4())
            message = f"Order #{i+1} ({order_id}): {order_type['description']}"
            
            # Add the headers for routing
            properties = pika.BasicProperties(
                content_type='text/plain',
                delivery_mode=2,  # Persistent
                headers=order_type['headers'],
                message_id=order_id
            )
            
            # Publish the message
            publisher_channel.basic_publish(
                exchange=exchange_name,
                routing_key='',  # Ignored for headers exchanges
                body=message.encode('utf-8'),
                properties=properties
            )
            
            logger.info(f"Published order with headers: {order_type['headers']}")
            time.sleep(1)  # Delay between messages
        
        # Wait for messages to be processed
        logger.info("Waiting for orders to be processed...")
        time.sleep(5)
        
        # Display summary
        logger.info("\n== Headers Exchange Summary ==")
        logger.info("- Headers exchanges route based on message attributes")
        logger.info("- You can match 'all' headers (AND logic) or 'any' header (OR logic)")
        logger.info("- This is useful for complex routing scenarios not expressible with routing keys")
        logger.info("- Each consumer has its own dedicated connection to avoid race conditions")
        
    except KeyboardInterrupt:
        logger.info("Example interrupted by user")
    finally:
        # Signal threads to stop
        stop_event.set()
        
        # Allow threads time to finish
        time.sleep(1)
        
        # Stop consuming and clean up
        logger.info("Cleaning up resources...")
        
        for channel, _, queue_name, consumer_broker_manager in consumers:
            try:
                channel.stop_consuming()
                logger.info(f"Stopped consuming on queue: {queue_name}")
                channel.close()
                consumer_broker_manager.close()
            except Exception as e:
                logger.error(f"Error cleaning up consumer: {e}")
                pass
        
        try:
            publisher_channel.close()
            publisher_broker_manager.close()
            logger.info("Closed publisher connection")
        except Exception as e:
            logger.error(f"Error closing publisher connection: {e}")
            
        logger.info("Headers exchange example finished")

if __name__ == "__main__":
    run_example()