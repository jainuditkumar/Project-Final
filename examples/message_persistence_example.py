#!/usr/bin/env python3
"""
Example demonstrating Message Persistence and Recovery in RabbitMQ.

This example shows how messages can persist across broker restarts and consumer
reconnections, ensuring that no messages are lost during system failures.
"""
import os
import sys
import time
import uuid
import logging
import threading
from typing import Dict, Any, Optional, Union

# Need to import pika for the BasicProperties
import pika

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dmq.connection_manager import BrokerConnectionManager
from dmq.messaging import DirectProducer, DirectConsumer
from dmq.dmq import DMQCluster

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def consumer_handler(message: Any, headers: Dict[str, Any] = None) -> bool:
    """Consumer task handler that processes persistent messages
    
    Args:
        message: The message body (already decoded from JSON if applicable)
        headers: Optional message headers
        
    Returns:
        True to acknowledge, False to reject
    """
    consumer_id = threading.current_thread().name
    message_id = message.get('id', 'unknown')
    
    logger.info(f"{consumer_id}: Received message #{message_id}: {message}")
    
    # Process the message - simulate some work
    processing_time = 0.5
    logger.info(f"{consumer_id}: Processing message #{message_id} for {processing_time:.1f} seconds")
    time.sleep(processing_time)
    
    logger.info(f"{consumer_id}: Successfully processed message #{message_id}")
    return True  # Acknowledge the message

def run_example():
    """Run the message persistence example"""
    # Create a durable queue for persistent messages
    queue_name = "persistent_queue"
    
    # Create a connection manager for the producer
    producer_broker_manager = BrokerConnectionManager()
    
    # Register the RabbitMQ nodes
    producer_broker_manager.register_broker(1, "localhost", 5672)
    producer_broker_manager.register_broker(2, "localhost", 5682)
    producer_broker_manager.register_broker(3, "localhost", 5692)
    
    if len(producer_broker_manager.load_balancer.connections) == 0:
        logger.error("Failed to connect to RabbitMQ cluster. Is the DMQ cluster running?")
        return
    
    try:
        # Create a producer to send persistent messages
        producer = DirectProducer(connection_manager=producer_broker_manager)
        
        # Create a temporary consumer for queue declaration
        queue_consumer = DirectConsumer(
            queue_name=queue_name,
            connection_manager=producer_broker_manager,
            auto_ack=False
        )
        
        # Declare a durable queue
        if not queue_consumer.declare_queue(
            durable=True,  # Survive broker restarts
            arguments={"x-ha-policy": "all"}  # Replicate across all nodes
        ):
            logger.error("Failed to declare queue")
            return
            
        logger.info(f"Declared durable queue: {queue_name}")
        
        # Close the temporary consumer
        queue_consumer.close()
        
        # First, publish several persistent messages
        logger.info("\n=== Publishing persistent messages ===")
        messages_sent = 0
        
        for i in range(10):
            # Create a message
            message = {
                "id": i,
                "content": f"Persistent message {i} - {uuid.uuid4()}",
                "timestamp": time.time()
            }
            
            # Create message properties with delivery_mode=2 for persistence
            properties = pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,  # Persistent message
                headers={
                    "source": "persistence_example",
                    "message_id": str(i)
                }
            )
            
            # Publish with persistent properties
            if producer.publish(
                routing_key=queue_name,
                message=message,
                properties=properties
            ):
                messages_sent += 1
                logger.info(f"Published persistent message {i}")
            else:
                logger.error(f"Failed to publish message {i}")
                
            time.sleep(0.2)  # Small delay between publishing messages
        
        logger.info(f"Published {messages_sent} persistent messages")

        
        # Create first consumer connection manager
        first_consumer_manager = BrokerConnectionManager()
        first_consumer_manager.register_broker(1, "localhost", 5672)
        first_consumer_manager.register_broker(2, "localhost", 5682)  # Simulate node failure
        first_consumer_manager.register_broker(3, "localhost", 5692)  # Skip node 2 which is down
        
        # Create a consumer that consumes half the messages
        logger.info("\n=== Starting first consumer (first batch of messages) ===")
        first_consumer = DirectConsumer(
            queue_name=queue_name,
            connection_manager=first_consumer_manager,
            auto_ack=False  # Manual acknowledgment
        )
        
        # Set prefetch count to only get half the messages
        first_consumer.channel.basic_qos(prefetch_count=5)
        
        # Start first consumer
        first_consumer_thread = first_consumer.start_consuming_thread(
            consumer_handler,
            thread_name="FirstConsumer"
        )
        
        # Let the first consumer process messages
        logger.info("\n=== Consuming first batch of messages ===")
        time.sleep(5)
        
        # Stop the first consumer
        first_consumer.stop_consuming()
        first_consumer.close()
        logger.info("First consumer stopped")
        
        # Now restart the stopped node
        logger.info(f"\n=== Restarting node {node_to_stop} ===")
        dmq_cluster.start_node(node_to_stop)
        logger.info(f"Node {node_to_stop} restarted")
        time.sleep(3)  # Give it time to rejoin the cluster
        
        # Create second consumer connection manager
        second_consumer_manager = BrokerConnectionManager()
        second_consumer_manager.register_broker(1, "localhost", 5672)
        second_consumer_manager.register_broker(2, "localhost", 5682)  # Using all nodes including restarted one
        second_consumer_manager.register_broker(3, "localhost", 5692)
        
        if len(second_consumer_manager.load_balancer.connections) == 0:
            logger.error("Failed to connect second consumer to RabbitMQ cluster")
            return
        
        # Create a new consumer for the remaining messages
        logger.info("\n=== Starting second consumer for remaining messages ===")
        second_consumer = DirectConsumer(
            queue_name=queue_name,
            connection_manager=second_consumer_manager,
            auto_ack=False  # Manual acknowledgment
        )
        
        # Start second consumer
        second_consumer_thread = second_consumer.start_consuming_thread(
            consumer_handler,
            thread_name="SecondConsumer"
        )
        
        # Process remaining messages
        logger.info("\n=== Consuming remaining messages ===")
        time.sleep(5)
        
        # Stop the second consumer
        second_consumer.stop_consuming()
        second_consumer.close()
        logger.info("Second consumer stopped")
        
        # Check if there are any messages left in the queue
        logger.info("\n=== Checking for remaining messages ===")
        check_consumer = DirectConsumer(
            queue_name=queue_name,
            connection_manager=producer_broker_manager,
            auto_ack=False
        )
        
        queue_info = check_consumer.channel.queue_declare(
            queue=queue_name, 
            durable=True, 
            passive=True
        )
        check_consumer.close()
        
        remaining_messages = queue_info.method.message_count
        
        logger.info(f"\n=== Persistence Test Results ===")
        logger.info(f"Messages remaining in queue: {remaining_messages}")
        if remaining_messages == 0:
            logger.info("Success! All messages were processed successfully despite node failure.")
        else:
            logger.info(f"There are still {remaining_messages} messages to be processed.")
        
        logger.info("\n=== Key Persistence Features Demonstrated ===")
        logger.info("1. Message persistence (delivery_mode=2) - Messages saved to disk")
        logger.info("2. Durable queues - Queue survived broker restart")
        logger.info("3. HA policy - Messages replicated across cluster nodes")
        logger.info("4. Manual acknowledgment - Ensures messages aren't lost if consumer fails")
        logger.info("5. Separate connection managers - Producer and consumer using different connection pools")
        
    except KeyboardInterrupt:
        logger.info("Example interrupted by user")
    except Exception as e:
        logger.error(f"Error in persistence example: {e}", exc_info=True)
    finally:
        # Clean up
        logger.info("\n=== Cleaning up ===")
        try:
            # Make sure the stopped node is running again
            if not dmq_cluster.is_node_running(node_to_stop):
                dmq_cluster.start_node(node_to_stop)
                
            # Close all resources
            if 'producer' in locals():
                producer.close()
                
            if 'first_consumer' in locals() and hasattr(first_consumer, 'close'):
                first_consumer.close()
                
            if 'second_consumer' in locals() and hasattr(second_consumer, 'close'):
                second_consumer.close()
                
            # Close connection managers
            if 'producer_broker_manager' in locals() and hasattr(producer_broker_manager, 'close'):
                producer_broker_manager.close()
                
            if 'first_consumer_manager' in locals() and hasattr(first_consumer_manager, 'close'):
                first_consumer_manager.close()
                
            if 'second_consumer_manager' in locals() and hasattr(second_consumer_manager, 'close'):
                second_consumer_manager.close()
                
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
        
        logger.info("Message persistence example finished")

if __name__ == "__main__":
    run_example()