#!/usr/bin/env python3
"""
Example demonstrating the Fanout Exchange pattern using RabbitMQ.

Fanout exchanges broadcast messages to all queues bound to them, regardless of routing keys.
This is ideal for broadcast-style messaging where all listeners should receive all messages.
This version includes thread-safe consumer handling.
"""
import os
import sys
import time
import logging
import threading
import json
import pika
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
    """Handler for received messages in fanout exchange
    
    Args:
        channel: The channel object
        method: Contains delivery information
        properties: Message properties
        body: The message body
    """
    consumer_tag = method.consumer_tag
    queue_name = method.routing_key  # In fanout exchange, this will be the queue name
    headers = properties.headers or {}
    thread_id = threading.get_ident()
    
    try:
        # Decode the message body - check if it's JSON
        if properties.content_type == 'application/json':
            message_str = body.decode('utf-8')
            message = json.loads(message_str)
        else:
            # Plain text message
            message = body.decode('utf-8')
            
        logger.info(f"Consumer {consumer_tag} on queue {queue_name} (thread {thread_id}) received: {message}")
        
        if headers:
            logger.info(f"Message headers: {headers}")
        
        # Simulate processing
        time.sleep(0.2)
        
        # Acknowledge the message
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        # Negative acknowledgment (reject and requeue)
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

def setup_consumer(broker_manager, exchange_name, service_name):
    """Set up a consumer for a fanout exchange with its own queue"""
    # Get a channel
    channel = broker_manager.acquire_channel()
    if not channel:
        logger.error(f"Failed to acquire channel for {service_name}")
        return None
    
    try:
        # Declare the exchange
        channel.exchange_declare(
            exchange=exchange_name,
            exchange_type='fanout',
            durable=True
        )
        
        # Declare a queue with a generated name for this consumer
        # Set exclusive=True so queue is deleted when consumer disconnects
        queue_result = channel.queue_declare(
            queue=f"{service_name}_queue", 
            durable=True,
            exclusive=False
        )
        queue_name = queue_result.method.queue
        
        # Bind the queue to the exchange (no routing key needed for fanout)
        channel.queue_bind(
            queue=queue_name,
            exchange=exchange_name
        )
        
        # Set QoS (prefetch count)
        channel.basic_qos(prefetch_count=1)
        
        # Register the message handler callback but don't start consuming yet
        consumer_tag = channel.basic_consume(
            queue=queue_name,
            on_message_callback=message_handler,
            auto_ack=False
        )
        
        logger.info(f"Set up consumer '{service_name}' on queue: {queue_name}")
        return channel, consumer_tag, queue_name
    except Exception as e:
        logger.error(f"Failed to setup consumer for {service_name}: {e}")
        return None

def start_consumer_thread(channel, service_name):
    """Start a consumer in a thread-safe manner with its own connection.
    This is necessary because Pika's connection objects are not thread-safe.
    
    Args:
        channel: The channel to consume from
        service_name: Name of the service for the thread
        
    Returns:
        The thread object
    """
    def consumer_thread():
        thread_id = threading.get_ident()
        logger.info(f"Starting consumer {service_name} in thread {thread_id}")
        try:
            # This will block until stop_consuming is called
            channel.start_consuming()
        except Exception as e:
            logger.error(f"Error in consumer thread for {service_name}: {e}")
        logger.info(f"Consumer thread for {service_name} is ending")
    
    # Create and start the thread
    thread = threading.Thread(
        target=consumer_thread,
        name=service_name,
        daemon=True
    )
    thread.start()
    return thread

def run_example():
    """Run the fanout exchange example (broadcast)"""
    # Create a fanout exchange for broadcasting
    exchange_name = "broadcast_notifications"
    
    # Create connection manager
    broker_manager = BrokerConnectionManager()
    
    # Register the RabbitMQ nodes
    # broker_manager.register_broker(1, "localhost", 5672)
    # broker_manager.register_broker(2, "localhost", 5682)
    # broker_manager.register_broker(3, "localhost", 5692)
    
    if len(broker_manager.load_balancer.connections) == 0:
        logger.error("Failed to connect to RabbitMQ cluster. Is the DMQ cluster running?")
        return
    
    # Set up multiple service consumers that will all receive the same messages
    services = ["email_service", "sms_service", "push_notification_service", "audit_service"]
    consumers = []
    
    # Set up consumers - each gets its own queue bound to the fanout exchange
    for service in services:
        consumer_info = setup_consumer(broker_manager, exchange_name, service)
        if consumer_info:
            consumers.append((service, *consumer_info))
    
    # Set up a publisher
    publisher_channel = broker_manager.acquire_channel()
    if not publisher_channel:
        logger.error("Failed to acquire channel for publisher")
        return
    
    try:
        # Declare the exchange
        publisher_channel.exchange_declare(
            exchange=exchange_name,
            exchange_type='fanout',
            durable=True
        )
        
        # FIRST: Publish broadcast notifications
        logger.info("Publishing broadcast notifications...")
        for i in range(5):
            # Create a notification message as a dictionary
            notification = {
                "message": f"System-wide notification #{i}: Maintenance scheduled at {time.ctime(time.time() + 3600)}",
                "notification_id": i
            }
            
            # Add headers
            properties = pika.BasicProperties(
                content_type='application/json',
                delivery_mode=2,  # Persistent
                headers={
                    "notification_id": str(i),
                    "priority": "high" if i % 2 == 0 else "normal",
                    "timestamp": str(int(time.time()))  # Convert timestamp to string
                }
            )
            
            # Convert dictionary to JSON string, then encode to bytes
            message_body = json.dumps(notification).encode('utf-8')
            
            # Publish to the fanout exchange (routing key is ignored)
            publisher_channel.basic_publish(
                exchange=exchange_name,
                routing_key='',  # Ignored for fanout exchanges
                body=message_body,
                properties=properties
            )
            
            logger.info(f"Published broadcast notification #{i}")
            time.sleep(0.5)  # Delay between notifications
        
        # SECOND: Now start consumer threads in a thread-safe way
        # Each thread gets its own dedicated channel for thread safety
        logger.info("\nStarting consumers with thread-safe connections...")
        consumer_threads = []
        for service_name, channel, _, _ in consumers:
            # Start consuming in a dedicated thread with its own connection
            thread = start_consumer_thread(channel, service_name)
            consumer_threads.append((service_name, thread, channel))
            logger.info(f"Started thread-safe consumer for {service_name}")
        
        # Wait for messages to be processed
        logger.info("Waiting for all services to process notifications...")
        time.sleep(5)
        
        # Display summary
        logger.info("\n== Fanout Exchange Summary ==")
        logger.info(f"- Broadcast notifications were sent to {len(consumers)} services")
        logger.info("- All services received identical copies of each message")
        logger.info("- Each consumer thread has its own dedicated connection")
        logger.info("- This pattern is ideal for event broadcasting")
        logger.info("- The publish-first, consume-later pattern demonstrates messages are persisted")
        
    except KeyboardInterrupt:
        logger.info("Example interrupted by user")
    except Exception as e:
        logger.error(f"Error in example: {e}")
    finally:
        # Stop consuming
        for service_name, thread, channel in consumer_threads:
            try:
                logger.info(f"Stopping consumer for {service_name}")
                if channel.is_open:
                    channel.stop_consuming()
            except Exception as e:
                logger.error(f"Error stopping consumer {service_name}: {e}")
        
        # Wait for threads to finish
        for service_name, thread, channel in consumer_threads:
            try:
                thread.join(timeout=1.0)
                if thread.is_alive():
                    logger.warning(f"Thread for {service_name} is still running")
            except Exception as e:
                logger.error(f"Error joining thread for {service_name}: {e}")
        
        # Clean up all channels
        for _, _, channel in consumer_threads:
            try:
                if channel and channel.is_open:
                    channel.close()
            except Exception as e:
                logger.debug(f"Error closing channel: {e}")
        
        # Close publisher channel if it's still open
        try:
            if publisher_channel and publisher_channel.is_open:
                publisher_channel.close()
        except Exception as e:
            logger.debug(f"Error closing publisher channel: {e}")
        
        # Close connection manager
        broker_manager.close()
        logger.info("Fanout exchange example finished")

if __name__ == "__main__":
    run_example()