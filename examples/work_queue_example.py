#!/usr/bin/env python3
"""
Example demonstrating a Work Queue pattern using RabbitMQ.

Work queues distribute tasks among multiple workers, ensuring tasks are only processed once.
This example simulates a task distribution system where each task takes a variable amount of time,
and multiple workers process tasks in parallel.
"""
import os
import sys
import time
import random
import logging
import threading
from typing import Dict, Any, Union

# Need to import pika for the BasicProperties
import pika

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dmq.messaging import DirectProducer, DirectConsumer
from dmq.connection_manager import BrokerConnectionManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def worker_handler(message: Any, headers: Dict[str, Any] = None) -> bool:
    """Worker task handler that processes messages with variable processing time
    
    Args:
        message: The message body (already decoded from JSON if applicable)
        headers: Optional message headers
        
    Returns:
        True to acknowledge, False to reject
    """
    worker_id = threading.current_thread().name
    logger.info(f"Worker {worker_id} received task: {message}")
    
    # Extract task complexity (determines processing time)
    complexity = message.get("complexity", 1)
    
    # Simulate processing work with variable duration based on complexity
    processing_time = complexity * 0.5
    logger.info(f"Worker {worker_id} processing task {message['task_id']} " 
                f"(complexity: {complexity}) for {processing_time:.1f} seconds")
    time.sleep(processing_time)
    
    logger.info(f"Worker {worker_id} completed task {message['task_id']}")
    return True  # Acknowledge the message

def run_example():
    """Run the work queue example with producer and consumer"""
    # Create a work queue
    queue_name = "work_queue"
    
    # Create a connection manager for queue declaration and producer
    broker_manager = BrokerConnectionManager()
    
    # Register the RabbitMQ nodes 
    broker_manager.register_broker(1, "localhost", 5672)
    broker_manager.register_broker(2, "localhost", 5682)
    broker_manager.register_broker(3, "localhost", 5692)
    
    if len(broker_manager.load_balancer.connections) == 0:
        logger.error("Failed to connect to RabbitMQ cluster. Is the DMQ cluster running?")
        return
    
    # Create a producer to send tasks
    producer = DirectProducer(connection_manager=broker_manager)
    
    # Create a consumer for queue declaration only
    consumer = DirectConsumer(
        queue_name=queue_name,
        connection_manager=broker_manager,
        auto_ack=False,  # Manual acknowledgment
    )
    
    # Declare the queue with durable=True for persistence
    if not consumer.declare_queue(durable=True):
        logger.error("Failed to declare queue")
        return
    
    # Create multiple consumers/workers (3 in this example)
    workers = []
    for i in range(3):
        # Create separate connection manager for each worker
        worker_broker_manager = BrokerConnectionManager()
        worker_broker_manager.register_broker(1, "localhost", 5672)
        worker_broker_manager.register_broker(2, "localhost", 5682)
        worker_broker_manager.register_broker(3, "localhost", 5692)
        
        if len(worker_broker_manager.load_balancer.connections) == 0:
            logger.error(f"Failed to connect Worker-{i+1} to RabbitMQ cluster")
            continue
            
        worker = DirectConsumer(
            queue_name=queue_name,
            connection_manager=worker_broker_manager,  # Use dedicated connection manager
            auto_ack=False,  # Manual acknowledgment
        )
        worker_thread = worker.start_consuming_thread(
            worker_handler, 
            thread_name=f"Worker-{i+1}"
        )
        workers.append((worker, worker_thread, worker_broker_manager))  # Store manager for cleanup
        logger.info(f"Started Worker-{i+1}")
    
    try:
        # Allow workers to initialize
        time.sleep(1)
        
        # Send tasks with varying complexity
        for i in range(10):
            # Create a task with random complexity (1-5)
            complexity = random.randint(1, 5)
            task = {
                "task_id": i,
                "description": f"Task {i}",
                "complexity": complexity,
                "timestamp": time.time()
            }
            
            # Create message properties with headers using pika.BasicProperties
            properties = pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,  # Persistent message
                headers={"task_type": "processing"}
            )
            
            # Publish the task
            if producer.publish(
                routing_key=queue_name,
                message=task,
                properties=properties
            ):
                logger.info(f"Published task {i} with complexity {complexity}")
            else:
                logger.error(f"Failed to publish task {i}")
            
            time.sleep(0.2)  # Small delay between publishing tasks
        
        # Wait for tasks to be processed
        logger.info("Waiting for workers to process all tasks...")
        time.sleep(15)  # Wait longer to ensure all tasks are processed
        
    except KeyboardInterrupt:
        logger.info("Example interrupted by user")
    finally:
        # Clean up
        logger.info("Shutting down workers...")
        
        # Clean up workers
        for worker, _, worker_broker_manager in workers:
            worker.close()
            # Close the worker's connection manager too
            if hasattr(worker_broker_manager, 'close'):
                worker_broker_manager.close()
        
        consumer.close()
        producer.close()
        # Close the main broker connection manager
        if hasattr(broker_manager, 'close'):
            broker_manager.close()
            
        logger.info("Work queue example finished")

if __name__ == "__main__":
    run_example()