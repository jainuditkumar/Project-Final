import os
import sys
import time
import logging
import threading
from typing import Dict, Any
import pika

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))



from dmq.messaging import DirectProducer, DirectConsumer
from dmq.connection_manager import BrokerConnectionManager

#!/usr/bin/env python3
"""
Example demonstrating Message Persistence using RabbitMQ.

Message persistence ensures that messages survive broker restarts by storing them on disk.
This example shows how to create durable queues and publish persistent messages,
demonstrating that messages sent while consumers are offline will be delivered when
consumers reconnect later.
"""

# Need to import pika for the BasicProperties

# Add the project root to the Python path
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def message_handler(message: Any, headers: Dict[str, Any] = None) -> bool:
    """Handle received persistent messages
    
    Args:
        message: The message body (already decoded from JSON if applicable)
        headers: Optional message headers
        
    Returns:
        True to acknowledge, False to reject
    """
    consumer_id = threading.current_thread().name
    logger.info(f"{consumer_id} received message: {message}")
    logger.info(f"Message headers: {headers}")
    return True  # Acknowledge the message

def run_example():
    """Run the message persistence example with producer and consumer"""
    # Use a queue name that indicates persistence
    queue_name = "persistent_queue"
    
    # Create separate connection managers for producer and consumer
    producer_broker_manager = BrokerConnectionManager()
    consumer_broker_manager = BrokerConnectionManager()
    
    # # Register RabbitMQ nodes to both connection managers
    # producer_broker_manager.register_broker(1, "localhost", 5672)
    # consumer_broker_manager.register_broker(1, "localhost", 5672)
    
    # Create a producer to send persistent messages with its own connection manager
    producer = DirectProducer(connection_manager=producer_broker_manager)
    
    # Declare a durable queue (survives broker restarts) with its own connection manager
    consumer = DirectConsumer(
        queue_name=queue_name,
        connection_manager=consumer_broker_manager,
        auto_ack=False,  # Manual acknowledgment is important for persistence
    )
    
    # Declare the queue with durable=True for persistence
    if not consumer.declare_queue(durable=True):
        logger.error("Failed to declare durable queue")
        return
    
    logger.info(f"Declared durable queue: {queue_name}")
    
    try:
        # Phase 1: Send persistent messages with no active consumers
        logger.info("Phase 1: Sending persistent messages (no consumers active)")
        for i in range(5):
            message = {
                "id": i,
                "content": f"Persistent Message {i}",
                "timestamp": time.time()
            }
            
            # Create message properties for persistence
            properties = pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,  # 2 means persistent
                headers={"persistence_example": "true", "phase": "first_batch"}
            )
            
            # Publish with persistent delivery mode
            if producer.publish(
                routing_key=queue_name,
                message=message,
                properties=properties
            ):
                logger.info(f"Published persistent message {i}")
            else:
                logger.error(f"Failed to publish message {i}")
            
            time.sleep(0.5)
        
        # Phase 2: Start a consumer that will receive the previously sent messages
        logger.info("Phase 2: Starting consumer to receive previously sent messages")
        consumer_thread = consumer.start_consuming_thread(
            message_handler,
            thread_name="PersistenceConsumer-1"
        )
        
        # Wait for consumer to process the messages
        time.sleep(5)
        
        # Phase 3: Send more messages with consumer already active
        logger.info("Phase 3: Sending more messages with consumer active")
        for i in range(5, 10):
            message = {
                "id": i,
                "content": f"Persistent Message {i}",
                "timestamp": time.time()
            }
            
            properties = pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,  # 2 means persistent
                headers={"persistence_example": "true", "phase": "second_batch"}
            )
            
            if producer.publish(
                routing_key=queue_name,
                message=message,
                properties=properties
            ):
                logger.info(f"Published persistent message {i}")
            else:
                logger.error(f"Failed to publish message {i}")
            
            time.sleep(0.5)
        
        # Wait for all messages to be processed
        logger.info("Waiting for all messages to be processed...")
        time.sleep(5)
        
    except KeyboardInterrupt:
        logger.info("Example interrupted by user")
    finally:
        # Clean up resources
        logger.info("Cleaning up resources...")
        consumer.close()
        producer.close()
        
        # Close the broker connection managers
        if hasattr(producer_broker_manager, 'close'):
            producer_broker_manager.close()
        if hasattr(consumer_broker_manager, 'close'):
            consumer_broker_manager.close()
            
        logger.info("Message persistence example finished")

if __name__ == "__main__":
    run_example()