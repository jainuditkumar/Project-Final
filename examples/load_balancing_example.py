#!/usr/bin/env python3
"""
Example demonstrating Load Balancing with Multiple Consumers in RabbitMQ.

This example shows how RabbitMQ distributes messages across multiple consumers,
and how client-side connection load balancing works with the BrokerConnectionManager.
"""
import os
import sys
import time
import logging
import threading
import pika
import random
import json
from typing import Dict, Any, List
import concurrent.futures
import queue

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dmq.connection_manager import BrokerConnectionManager
from dmq.dmq import DMQCluster

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Shared metrics for tracking load balancing
metrics = {
    "tasks_per_worker": {},
    "tasks_per_node": {},
    "total_tasks_processed": 0,
    "avg_processing_time": 0,
    "lock": threading.Lock()
}

def consumer_handler(channel, method, properties, body, worker_id, node_id):
    """Handler for received messages
    
    Args:
        channel: The channel object
        method: Contains delivery information
        properties: Message properties
        body: The message body
        worker_id: ID of the worker processing this message
        node_id: ID of the RabbitMQ node this consumer is connected to
    """
    start_time = time.time()
    delivery_tag = method.delivery_tag
    
    try:
        # Decode the message body
        message = json.loads(body.decode('utf-8'))
        task_id = message.get("task_id", "unknown")
        
        logger.info(f"Worker {worker_id} on node {node_id} received task {task_id}")
        
        # Simulate processing work
        # More complex tasks take longer to process
        complexity = message.get("complexity", 1)
        processing_time = 0.2 + (complexity * 0.1)
        time.sleep(processing_time)
        
        # Update metrics
        with metrics["lock"]:
            # Update tasks per worker
            if worker_id not in metrics["tasks_per_worker"]:
                metrics["tasks_per_worker"][worker_id] = 0
            metrics["tasks_per_worker"][worker_id] += 1
            
            # Update tasks per node
            if node_id not in metrics["tasks_per_node"]:
                metrics["tasks_per_node"][node_id] = 0
            metrics["tasks_per_node"][node_id] += 1
            
            # Update total tasks and average time
            metrics["total_tasks_processed"] += 1
            
            # Update average processing time with rolling average
            current_avg = metrics["avg_processing_time"]
            n = metrics["total_tasks_processed"]
            metrics["avg_processing_time"] = current_avg + ((processing_time - current_avg) / n)
        
        logger.info(f"Worker {worker_id} on node {node_id} completed task {task_id} in {processing_time:.2f}s")
        
        # Acknowledge the message
        channel.basic_ack(delivery_tag=delivery_tag)
        
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        # Negative acknowledgment with requeue=True
        channel.basic_nack(delivery_tag=delivery_tag, requeue=True)

def create_consumer(worker_id, queue_name, prefetch_count=1):
    """Create a consumer with a specific worker ID and dedicated connection manager"""
    # Create dedicated connection manager for this consumer
    worker_broker_manager = BrokerConnectionManager()
    # worker_broker_manager.register_broker(1, "localhost", 5672)
    # worker_broker_manager.register_broker(2, "localhost", 5682)
    # worker_broker_manager.register_broker(3, "localhost", 5692)
    
    if len(worker_broker_manager.load_balancer.connections) == 0:
        logger.error(f"Worker {worker_id} failed to connect to RabbitMQ cluster")
        return None
    
    # Get a channel with load balancing
    connection = worker_broker_manager.acquire_connection(strategy="least_active")
    if not connection:
        logger.error(f"Worker {worker_id} failed to get a connection")
        worker_broker_manager.close()
        return None
    
    node_id = connection.broker_id
    channel = connection.get_channel()
    
    if not channel:
        logger.error(f"Worker {worker_id} failed to get a channel on node {node_id}")
        worker_broker_manager.close()
        return None
    
    try:
        # Set QoS (prefetch count)
        channel.basic_qos(prefetch_count=prefetch_count)
        
        # Bind a callback that includes the worker and node IDs
        callback = lambda ch, method, properties, body: consumer_handler(
            ch, method, properties, body, worker_id, node_id
        )
        
        # Start consuming
        channel.basic_consume(
            queue=queue_name,
            on_message_callback=callback,
            auto_ack=False
        )
        
        logger.info(f"Created consumer worker {worker_id} connected to node {node_id}")
        return (channel, worker_id, node_id, worker_broker_manager)
    except Exception as e:
        logger.error(f"Error setting up consumer {worker_id}: {e}")
        worker_broker_manager.close()
        return None

def consumer_thread(channel, worker_id, node_id, stop_event):
    """Thread function for running a consumer"""
    try:
        logger.info(f"Worker {worker_id} on node {node_id} started consuming")
        
        # Run until stop event is set
        while not stop_event.is_set():
            # Process events for a short interval, then check stop_event again
            channel.connection.process_data_events(time_limit=1)
            
        logger.info(f"Worker {worker_id} on node {node_id} stopped consuming")
    except Exception as e:
        logger.error(f"Error in consumer thread {worker_id}: {e}")

def produce_tasks(queue_name, num_tasks):
    """Produce a batch of tasks to the queue using dedicated connection manager"""
    # Create dedicated connection manager for producer
    producer_broker_manager = BrokerConnectionManager()
    # producer_broker_manager.register_broker(1, "localhost", 5672)
    # producer_broker_manager.register_broker(2, "localhost", 5682)
    # producer_broker_manager.register_broker(3, "localhost", 5692)
    
    if len(producer_broker_manager.load_balancer.connections) == 0:
        logger.error("Producer failed to connect to RabbitMQ cluster")
        return 0
    
    # Get a connection first, then a channel
    connection = producer_broker_manager.acquire_connection(strategy="least_active")
    if not connection:
        logger.error("Failed to acquire connection for producer")
        producer_broker_manager.close()
        return 0
        
    channel = connection.get_channel()
    if not channel:
        logger.error("Failed to acquire channel for producer")
        producer_broker_manager.close()
        return 0
    
    tasks_published = 0
    
    try:
        # Publish tasks
        for i in range(num_tasks):
            # Create a task with random complexity (1-10)
            complexity = random.randint(1, 10)
            priority = random.randint(1, 5)  # 1 (lowest) to 5 (highest)
            
            task = {
                "task_id": f"{int(time.time())}-{i}",
                "description": f"Task {i}",
                "complexity": complexity,
                "priority": priority,
                "timestamp": str(int(time.time()))
            }
            
            # Create message properties
            properties = pika.BasicProperties(
                content_type='application/json',
                delivery_mode=2,  # Persistent
                priority=priority
            )
            
            # Publish to the queue
            channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=json.dumps(task).encode('utf-8'),
                properties=properties
            )
            
            tasks_published += 1
            
        logger.info(f"Published {tasks_published} tasks")
        return tasks_published
    except Exception as e:
        logger.error(f"Error producing tasks: {e}")
        return tasks_published
    finally:
        try:
            channel.close()
        except:
            pass
        producer_broker_manager.close()

def print_load_balance_metrics():
    """Print the current load balancing metrics"""
    with metrics["lock"]:
        # Calculate standard deviation of tasks per worker to measure balance
        worker_loads = list(metrics["tasks_per_worker"].values())
        if worker_loads:
            avg_worker_load = sum(worker_loads) / len(worker_loads)
            std_dev = (sum((x - avg_worker_load) ** 2 for x in worker_loads) / len(worker_loads)) ** 0.5
        else:
            avg_worker_load = 0
            std_dev = 0
        
        # Calculate node distribution
        node_loads = list(metrics["tasks_per_node"].values())
        if node_loads:
            avg_node_load = sum(node_loads) / len(node_loads)
            node_std_dev = (sum((x - avg_node_load) ** 2 for x in node_loads) / len(node_loads)) ** 0.5
        else:
            avg_node_load = 0
            node_std_dev = 0
        
        logger.info("\n=== Load Balancing Metrics ===")
        logger.info(f"Total tasks processed: {metrics['total_tasks_processed']}")
        logger.info(f"Avg processing time: {metrics['avg_processing_time']:.3f}s")
        logger.info(f"Tasks per worker: {metrics['tasks_per_worker']}")
        logger.info(f"Worker load balance (lower std dev is better): {std_dev:.2f}")
        logger.info(f"Tasks per node: {metrics['tasks_per_node']}")
        logger.info(f"Node load balance (lower std dev is better): {node_std_dev:.2f}")

def run_example():
    """Run the load balancing with multiple consumers example"""
    # Create a queue for tasks
    queue_name = "balanced_tasks"
    
    # Create a separate connection manager for setup
    setup_broker_manager = BrokerConnectionManager()
    # setup_broker_manager.register_broker(1, "localhost", 5672)
    # setup_broker_manager.register_broker(2, "localhost", 5682)
    # setup_broker_manager.register_broker(3, "localhost", 5692)
    
    if len(setup_broker_manager.load_balancer.connections) == 0:
        logger.error("Failed to connect to RabbitMQ cluster. Is the DMQ cluster running?")
        return
    
    # Get a connection first, then a channel for setup
    connection = setup_broker_manager.acquire_connection(strategy="least_active")
    if not connection:
        logger.error("Failed to acquire connection for setup")
        setup_broker_manager.close()
        return
        
    setup_channel = connection.get_channel()
    if not setup_channel:
        logger.error("Failed to acquire channel for setup")
        setup_broker_manager.close()
        return
    
    try:
        # Declare the task queue with priority support (max priority 10)
        setup_channel.queue_declare(
            queue=queue_name,
            durable=True,
            arguments={"x-max-priority": 10}
        )
        logger.info(f"Declared queue: {queue_name} with priority support")
        
        # Create multiple consumers across nodes
        num_consumers = 10  # Create 10 consumer workers
        consumers = []
        
        for i in range(1, num_consumers + 1):
            consumer = create_consumer(
                worker_id=i,
                queue_name=queue_name,
                prefetch_count=3  # Each consumer processes up to 3 messages at once
            )
            
            if consumer:
                consumers.append(consumer)
        
        logger.info(f"Created {len(consumers)} consumers")
        
        # Create a stop event for graceful shutdown
        stop_event = threading.Event()
        
        # Start consumer threads
        consumer_threads = []
        for channel, worker_id, node_id, _ in consumers:
            thread = threading.Thread(
                target=consumer_thread,
                args=(channel, worker_id, node_id, stop_event)
            )
            thread.daemon = True
            thread.start()
            consumer_threads.append(thread)
        
        # Wait for consumers to initialize
        time.sleep(1)
        
        # Produce tasks in several batches to simulate continuous workload
        total_tasks = 100
        batch_size = 20
        
        for batch in range(0, total_tasks, batch_size):
            # Produce a batch of tasks with separate connection manager
            tasks_published = produce_tasks(
                queue_name,
                min(batch_size, total_tasks - batch)
            )
            
            # Wait a bit to let processing happen
            time.sleep(2)
            
            # Print metrics after each batch
            logger.info(f"\n--- Metrics after batch {(batch // batch_size) + 1} ---")
            print_load_balance_metrics()
        
        # Wait until most tasks are processed
        logger.info("\nWaiting for tasks to complete...")
        target_completion = total_tasks * 0.95  # 95% of tasks
        
        max_wait = 30  # Maximum wait time in seconds
        start_wait = time.time()
        
        while (metrics["total_tasks_processed"] < target_completion and 
               time.time() - start_wait < max_wait):
            time.sleep(1)
        
        # Final metrics
        logger.info("\n=== Final Load Balancing Results ===")
        print_load_balance_metrics()
        
        # Print some conclusions
        logger.info("\n=== Load Balancing Features Demonstrated ===")
        logger.info("1. Connection-level load balancing across RabbitMQ nodes")
        logger.info("2. Consumer-level load balancing (RabbitMQ's fair dispatch)")
        logger.info("3. Prefetch limits to prevent worker overload")
        logger.info("4. Priority queues for processing important messages first")
        logger.info("5. Multiple consumers across nodes for scalability")
        logger.info("6. Separate connection managers for producers and consumers")
        
    except KeyboardInterrupt:
        logger.info("Example interrupted by user")
    except Exception as e:
        logger.error(f"Error in example: {e}", exc_info=True)
    finally:
        # Signal threads to stop
        stop_event.set()
        
        # Clean up
        logger.info("Cleaning up...")
        time.sleep(1)  # Give threads time to finish
        
        # Clean up all connection managers and consumers
        for channel, _, _, worker_broker_manager in consumers:
            try:
                channel.stop_consuming()
                channel.close()
            except:
                pass
            worker_broker_manager.close()
                
        # Close setup connection manager
        setup_broker_manager.close()

if __name__ == "__main__":
    run_example()