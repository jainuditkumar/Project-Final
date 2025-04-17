#!/usr/bin/env python3
"""
Example demonstrating the Topic Exchange pattern using RabbitMQ.

Topic exchanges route messages to queues based on wildcard matches between the routing key
and the queue binding pattern. This allows for flexible routing based on message categories.
"""
import os
import sys
import time
import random
import logging
import threading
from typing import Dict, Any

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dmq.connection_manager import BrokerConnectionManager
from dmq.messaging import TopicProducer, TopicConsumer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def error_message_handler(message, headers, routing_key):
    """Handler for received error messages
    
    Args:
        message: The message body (automatically decoded)
        headers: Message headers
        routing_key: The routing key that matched the binding pattern
    """
    logger.info(f"ERROR CONSUMER received message: {message}")
    logger.info(f"Routing key: {routing_key}")
    logger.info(f"Headers: {headers}")
    
    # Simulate processing
    time.sleep(0.2)
    
    # Return True to acknowledge (if not using auto_ack)
    return True

def kern_message_handler(message, headers, routing_key):
    """Handler for received kern messages
    
    Args:
        message: The message body (automatically decoded)
        headers: Message headers
        routing_key: The routing key that matched the binding pattern
    """
    logger.info(f"KERN CONSUMER received message: {message}")
    logger.info(f"Routing key: {routing_key}")
    logger.info(f"Headers: {headers}")
    
    # Simulate processing
    time.sleep(0.2)
    
    # Return True to acknowledge (if not using auto_ack)
    return True

def all_message_handler(message, headers, routing_key):
    """Handler for all messages
    
    Args:
        message: The message body (automatically decoded)
        headers: Message headers
        routing_key: The routing key that matched the binding pattern
    """
    logger.info(f"ALL LOGS CONSUMER received message: {message}")
    logger.info(f"Routing key: {routing_key}")
    logger.info(f"Headers: {headers}")
    
    # Simulate processing
    time.sleep(0.2)
    
    # Return True to acknowledge (if not using auto_ack)
    return True

def run_example():
    """Run the topic exchange example using the new TopicProducer and TopicConsumer classes"""
    # Create a unique exchange name
    exchange_name = "logs_topics"
    
    # Create connection manager
    broker_manager = BrokerConnectionManager()
    
    # Register the RabbitMQ nodes
    broker_manager.register_broker(1, "localhost", 5672)
    broker_manager.register_broker(2, "localhost", 5682)
    broker_manager.register_broker(3, "localhost", 5692)
    
    if len(broker_manager.load_balancer.connections) == 0:
        logger.error("Failed to connect to RabbitMQ cluster. Is the DMQ cluster running?")
        return
    
    # Create a producer
    producer = TopicProducer(exchange_name=exchange_name, connection_manager=broker_manager)
    
    # Declare the exchange
    producer.declare_exchange(durable=True)
    
    # Create consumers with different binding patterns
    consumers = []
    
    # Consumer for all error logs (*.*.error)
    error_consumer = TopicConsumer(
        exchange_name=exchange_name,
        binding_pattern="*.*.error",
        queue_name="error_logs",
        connection_manager=broker_manager,
        auto_ack=False
    )
    error_consumer.setup(durable=True)
    consumers.append(error_consumer)
    
    # Consumer for all logs from kern facility (kern.#)
    kern_consumer = TopicConsumer(
        exchange_name=exchange_name,
        binding_pattern="kern.#",
        queue_name="kern_logs",
        connection_manager=broker_manager,
        auto_ack=False
    )
    kern_consumer.setup(durable=True)
    consumers.append(kern_consumer)
    
    # Consumer for all logs (#)
    all_consumer = TopicConsumer(
        exchange_name=exchange_name,
        binding_pattern="#",
        queue_name="all_logs",
        connection_manager=broker_manager,
        auto_ack=False
    )
    all_consumer.setup(durable=True)
    consumers.append(all_consumer)
    
    try:
        # Start consumers in background threads
        error_thread = error_consumer.start_consuming_thread(
            error_message_handler, 
            thread_name="error-consumer"
        )
        
        kern_thread = kern_consumer.start_consuming_thread(
            kern_message_handler,
            thread_name="kern-consumer"
        )
        
        all_thread = all_consumer.start_consuming_thread(
            all_message_handler,
            thread_name="all-consumer"
        )
        
        # Define some log types for our example
        facilities = ["kern", "user", "mail", "daemon", "auth"]
        hosts = ["host1", "host2", "host3"]
        severities = ["info", "warning", "error", "critical"]
        
        # Publish some messages with different routing keys
        for i in range(20):
            # Generate a random routing key in the format facility.host.severity
            facility = random.choice(facilities)
            host = random.choice(hosts)
            severity = random.choice(severities)
            routing_key = f"{facility}.{host}.{severity}"
            
            # Create a message
            message = f"Log message #{i} from {facility} on {host} with severity {severity}"
            
            # Create message headers
            headers = {
                "timestamp": time.time(),
                "message_id": str(i)
            }
            
            # Publish the message
            producer.publish(
                routing_key=routing_key,
                message=message,
                properties={
                    "headers": headers,
                    "content_type": "text/plain",
                    "delivery_mode": 2  # Persistent
                }
            )
            
            logger.info(f"Published message with routing key: {routing_key}")
            time.sleep(0.5)  # Small delay between messages
        
        # Wait for messages to be processed
        logger.info("Waiting for messages to be processed...")
        time.sleep(5)
        
    except KeyboardInterrupt:
        logger.info("Example interrupted by user")
    finally:
        # Clean up
        logger.info("Cleaning up...")
        
        # Stop all consumers
        for consumer in consumers:
            consumer.close()
        
        # Close the producer
        producer.close()
        
        logger.info("Topic exchange example finished")

if __name__ == "__main__":
    run_example()