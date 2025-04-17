#!/usr/bin/env python3
"""
Example demonstrating the usage of the DirectProducer and DirectConsumer classes.
This example shows a simple point-to-point messaging pattern using RabbitMQ's default exchange.
"""
import os
import sys
import time
import random
import logging
import threading
import pika
from typing import Dict, Any

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dmq.messaging import DirectProducer, DirectConsumer
from dmq.connection_manager import BrokerConnectionManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        # logging.FileHandler(os.path.join(os.path.dirname(__file__), '../logs/direct_exchange_example.log'))
    ]
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
    logger.info(f"Received message: {message}")
    if headers:
        logger.info(f"Message headers: {headers}")
    
    # Simulate processing work
    time.sleep(0.2)
    
    # Return True to acknowledge the message
    return True

def run_example():
    """Run the direct exchange example"""
    # Create a unique queue name for this example
    queue_name = f"direct_queue_{int(time.time())}"
    logger.info(f"Using queue: {queue_name}")
    
    # Create a connection manager to connect to the RabbitMQ cluster
    broker_manager = BrokerConnectionManager()
    
    # Register the RabbitMQ nodes from the cluster with correct ports
    # Alternative configuration using localhost with mapped ports
    # broker_manager.register_broker(1, "localhost", 5672)  # Primary node
    # broker_manager.register_broker(2, "localhost", 5682)  # Node 2
    # broker_manager.register_broker(3, "localhost", 5692)  # Node 3
    # broker_manager.register_broker(4, "localhost", 5702)  # Node 4
    
    if len(broker_manager.load_balancer.connections) > 0:
        logger.info(f"Successfully connected to RabbitMQ cluster with {len(broker_manager.load_balancer.connections)} nodes")
    else:
        logger.error("Failed to connect to RabbitMQ cluster. Is the DMQ cluster running?")
        return
    
    # Create a producer
    producer = DirectProducer(connection_manager=broker_manager)
    
    # Declare the queue before creating consumer
    channel = broker_manager.acquire_channel()
    if channel:
        try:
            channel.queue_declare(queue=queue_name, durable=True)
            logger.info(f"Declared queue: {queue_name}")
        except Exception as e:
            logger.error(f"Failed to declare queue: {e}")
            return
    
    # Create a consumer
    
    try:
        # Allow consumer some time to initialize
        time.sleep(1)
        
        # Send 10 messages
        for i in range(10):
            # Create a message payload
            message = {
                "message_id": i,
                "content": f"Test message {i}",
                "timestamp": time.time(),
                "random_value": random.randint(1, 100)
            }
            
            # Add some custom headers
            headers = {
                "source": "direct_exchange_example",
                "priority": "normal",
                "created_at": time.strftime("%Y-%m-%d %H:%M:%S")
            }
            
            # Create properties with the headers
            properties = pika.BasicProperties(
                content_type='application/json',
                delivery_mode=2,  # Persistent
                headers=headers
            )
            
            # Publish the message
            if producer.publish(
                routing_key=queue_name,  # For the default exchange, the routing key is the queue name
                message=message,
                properties=properties
            ):
                logger.info(f"Published message {i}")
            else:
                logger.error(f"Failed to publish message {i}")
            
            # Small delay between messages
            time.sleep(0.5)
            
            consumer = DirectConsumer(
            queue_name=queue_name,
            connection_manager=broker_manager,
            auto_ack=False  # Use manual acknowledgement for demonstration
            )
            
        # Start the consumer in a background thread
        consumer_thread = consumer.start_consuming_thread(message_handler)
        logger.info("Consumer started")
        
        # Wait a bit to let consumer process all messages
        logger.info("Waiting for consumer to finish processing...")
        time.sleep(3)
        
    except KeyboardInterrupt:
        logger.info("Example interrupted by user")
    except Exception as e:
        logger.error(f"Error in example: {e}", exc_info=True)
    finally:
        # Clean up
        logger.info("Shutting down consumer...")
        consumer.stop_consuming()
        consumer.close()
        producer.close()
        broker_manager.close()
        logger.info("Example finished")

if __name__ == "__main__":
    run_example()