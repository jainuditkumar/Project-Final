#!/usr/bin/env python3
"""
Example demonstrating the usage of the FanoutProducer and FanoutConsumer classes.

This example shows a broadcast messaging pattern using RabbitMQ's fanout exchange,
where one message is received by multiple consumers with their own queues.
This example demonstrates thread-safety with Pika connections.
"""
import os
import sys
import time
import random
import logging
import threading
import pika
from typing import Dict, Any, List

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dmq.messaging import FanoutProducer, FanoutConsumer
from dmq.connection_manager import BrokerConnectionManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def message_handler(message: Any, headers: Dict[str, Any] = None) -> bool:
    """Handler for received messages
    
    Args:
        message: The message body (already decoded from JSON if applicable)
        headers: Optional message headers
        
    Returns:
        True to acknowledge, False to reject
    """
    # Get the consumer name from the current thread
    consumer_name = threading.current_thread().name
    thread_id = threading.get_ident()
    
    logger.info(f"Consumer {consumer_name} (thread {thread_id}) received message: {message}")
    if headers:
        logger.info(f"Message headers: {headers}")
    
    # Simulate processing work
    time.sleep(0.2)
    
    # Return True to acknowledge the message
    return True

def run_example():
    """Run the fanout exchange example using the FanoutProducer and FanoutConsumer classes"""
    # Create a fanout exchange name
    exchange_name = f"fanout_exchange_{int(time.time())}"
    logger.info(f"Using fanout exchange: {exchange_name}")
    
    # Create a connection manager to connect to the RabbitMQ cluster
    broker_manager = BrokerConnectionManager()
    
    # Register the RabbitMQ nodes from the cluster with correct ports
    broker_manager.register_broker(1, "localhost", 5672)  # Primary node
    broker_manager.register_broker(2, "localhost", 5682)  # Node 2
    broker_manager.register_broker(3, "localhost", 5692)  # Node 3
    
    if len(broker_manager.load_balancer.connections) > 0:
        logger.info(f"Successfully connected to RabbitMQ cluster with {len(broker_manager.load_balancer.connections)} nodes")
    else:
        logger.error("Failed to connect to RabbitMQ cluster. Is the DMQ cluster running?")
        return
    
    # Create a fanout producer
    producer = FanoutProducer(exchange_name=exchange_name, connection_manager=broker_manager)
    
    # Declare the exchange
    if not producer.declare_exchange(durable=True):
        logger.error("Failed to declare fanout exchange")
        return
    
    # Create multiple consumers with different queue names
    consumers = []
    consumer_count = 3
    
    for i in range(consumer_count):
        # Create a unique queue name for each consumer
        service_name = f"service_{i+1}"
        queue_name = f"fanout_queue_{service_name}"
        
        # Create a consumer
        consumer = FanoutConsumer(
            exchange_name=exchange_name,
            queue_name=queue_name,
            connection_manager=broker_manager,
            auto_ack=False,  # Use manual acknowledgments
            prefetch_count=1  # Only get one message at a time
        )
        
        # Set up the consumer (declare exchange, queue, and bind them)
        if not consumer.setup(durable=True):
            logger.error(f"Failed to set up consumer {i+1}")
            continue
        
        # Add to our list of consumers
        consumers.append((service_name, consumer))
    
    try:
        # First, let's publish several broadcast messages
        logger.info("Publishing broadcast messages...")
        for i in range(5):
            # Create a message payload
            message = {
                "broadcast_id": i,
                "content": f"Broadcast message {i}",
                "timestamp": time.time(),
                "importance": random.choice(["high", "medium", "low"])
            }
            
            # Add some custom headers
            headers = {
                "source": "fanout_producer_consumer_example",
                "message_type": "broadcast",
                "sent_at": time.strftime("%Y-%m-%d %H:%M:%S")
            }
            
            # Create properties with the headers
            properties = pika.BasicProperties(
                content_type='application/json',
                delivery_mode=2,  # Persistent
                headers=headers
            )
            
            # Publish the message to the fanout exchange
            if producer.publish(
                message=message,
                properties=properties
            ):
                logger.info(f"Published broadcast message {i}")
            else:
                logger.error(f"Failed to publish broadcast message {i}")
            
            # Small delay between messages
            time.sleep(0.5)
        
        # Now start all consumers using thread-safe implementation
        logger.info("\nStarting consumers with thread-safe connections...")
        consumer_threads = []
        for service_name, consumer in consumers:
            # Start the consumer in a background thread with a dedicated thread name
            thread = consumer.start_consuming_thread(
                message_handler,
                thread_name=service_name  # Use the thread_name parameter to identify the thread
            )
            consumer_threads.append(thread)
            logger.info(f"Started thread-safe consumer for {service_name}")
        
        # Wait a bit to allow all consumers to process messages
        logger.info("Waiting for all consumers to process messages...")
        time.sleep(5)
        
        # Display summary
        logger.info("\n=== Fanout Exchange Summary ===")
        logger.info(f"Published 5 messages to fanout exchange: {exchange_name}")
        logger.info(f"Messages were received by {consumer_count} different consumers")
        logger.info("Each consumer received a copy of all messages")
        logger.info("Each consumer thread has its own dedicated Pika connection")
        logger.info("This demonstrates both the broadcast pattern and thread-safe connection handling")
        
    except KeyboardInterrupt:
        logger.info("Example interrupted by user")
    except Exception as e:
        logger.error(f"Error in example: {e}", exc_info=True)
    finally:
        # Clean up
        logger.info("Shutting down consumers...")
        for _, consumer in consumers:
            # This will clean up both the main consumer and any thread consumers
            consumer.close()
        
        producer.close()
        broker_manager.close()
        logger.info("Example finished")

if __name__ == "__main__":
    run_example()