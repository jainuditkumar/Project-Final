#!/usr/bin/env python3
"""
Example demonstrating Acknowledge & Retry mechanisms in RabbitMQ.

This example shows how to handle message processing failures using acknowledgments,
negative acknowledgments (nacks), and dead letter exchanges for robust error handling.
"""
import os
import sys
import time
import logging
import threading
import pika
import random
import json
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

def process_message(channel, method, properties, body):
    """Main queue message handler that sometimes fails
    
    Args:
        channel: The channel object
        method: Contains delivery information
        properties: Message properties
        body: The message body
    """
    delivery_tag = method.delivery_tag
    
    try:
        # Decode the message body
        message = json.loads(body.decode('utf-8'))
        logger.info(f"Processing message #{delivery_tag}: {message}")
        
        # Get retry count from headers
        headers = properties.headers or {}
        retry_count = headers.get('x-retry-count', 0)
        
        # For demonstration, randomly succeed or fail based on a success rate
        # The success rate improves with each retry
        success_rate = min(0.3 + (retry_count * 0.2), 0.9)  # 30% initially, +20% per retry, max 90%
        
        if random.random() < success_rate:
            # Successful processing
            logger.info(f"Successfully processed message #{delivery_tag}")
            
            # Acknowledge the message
            channel.basic_ack(delivery_tag=delivery_tag)
            logger.info(f"Acknowledged message #{delivery_tag}")
        else:
            # Simulate processing failure
            logger.warning(f"Failed to process message #{delivery_tag} (retry: {retry_count})")
            
            if retry_count >= 3:
                # Max retries reached, send to dead letter queue
                logger.warning(f"Max retries reached for message #{delivery_tag}, sending to dead letter queue")
                
                # Acknowledge the original message as we're moving it to DLQ
                channel.basic_ack(delivery_tag=delivery_tag)
                
                # Republish to the dead letter exchange
                dlx_properties = pika.BasicProperties(
                    content_type='application/json',
                    delivery_mode=2,  # Persistent
                    headers={
                        'x-original-exchange': properties.headers.get('x-original-exchange', ''),
                        'x-original-routing-key': properties.headers.get('x-original-routing-key', ''),
                        'x-death-reason': 'max-retries-exceeded',
                        'x-retry-count': retry_count
                    }
                )
                
                channel.basic_publish(
                    exchange='dead.letter.exchange',
                    routing_key='dead.letter.queue',
                    body=body,
                    properties=dlx_properties
                )
                logger.info(f"Message #{delivery_tag} moved to dead letter queue")
            else:
                # Reject and requeue with a backoff delay
                logger.info(f"Rejecting message #{delivery_tag} for retry")
                
                # Use negative acknowledgment with requeue=False (we'll republish with backoff)
                channel.basic_nack(delivery_tag=delivery_tag, requeue=False)
                
                # Calculate backoff (exponential backoff strategy)
                backoff_seconds = 2 ** retry_count
                logger.info(f"Will retry message after {backoff_seconds} seconds")
                
                # Publish to the retry exchange with a TTL for the backoff
                retry_properties = pika.BasicProperties(
                    content_type='application/json',
                    delivery_mode=2,  # Persistent
                    headers={
                        'x-original-exchange': properties.headers.get('x-original-exchange', ''),
                        'x-original-routing-key': properties.headers.get('x-original-routing-key', method.routing_key),
                        'x-retry-count': retry_count + 1
                    },
                    expiration=str(backoff_seconds * 1000)  # TTL in milliseconds
                )
                
                # Publish to the retry exchange
                channel.basic_publish(
                    exchange='retry.exchange',
                    routing_key='retry.queue',
                    body=body,
                    properties=retry_properties
                )
                logger.info(f"Message #{delivery_tag} scheduled for retry with backoff")
                
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        # Negative acknowledgment with requeue=True on unexpected errors
        channel.basic_nack(delivery_tag=delivery_tag, requeue=True)

def process_retry_queue(channel, method, properties, body):
    """Handler for the retry queue when TTL expires
    
    When a message's TTL expires in the retry queue, it's sent to the main queue again
    """
    delivery_tag = method.delivery_tag
    
    try:
        logger.info(f"Retry delay completed for message, returning to main queue")
        
        # Get the original routing key
        headers = properties.headers or {}
        original_routing_key = headers.get('x-original-routing-key', 'task_queue')
        retry_count = headers.get('x-retry-count', 1)
        
        # Publish back to the main queue with the retry count
        main_properties = pika.BasicProperties(
            content_type=properties.content_type,
            delivery_mode=2,  # Persistent
            headers={
                'x-retry-count': retry_count,
                'x-original-routing-key': original_routing_key,
                'x-original-exchange': headers.get('x-original-exchange', '')
            }
        )
        
        channel.basic_publish(
            exchange='',  # Default exchange
            routing_key=original_routing_key,
            body=body,
            properties=main_properties
        )
        
        # Acknowledge the message in the retry queue
        channel.basic_ack(delivery_tag=delivery_tag)
        logger.info(f"Message returned to main queue for retry #{retry_count}")
        
    except Exception as e:
        logger.error(f"Error in retry queue handler: {e}")
        # Negative acknowledgment with requeue=True
        channel.basic_nack(delivery_tag=delivery_tag, requeue=True)

def process_dead_letter(channel, method, properties, body):
    """Handler for the dead letter queue
    
    In a real system, this would log failed messages, notify admins, or
    persist them for later analysis and manual reprocessing.
    """
    delivery_tag = method.delivery_tag
    
    try:
        # Decode the message body
        message = json.loads(body.decode('utf-8'))
        logger.info(f"Dead letter queue received message: {message}")
        
        # Get the headers
        headers = properties.headers or {}
        original_routing_key = headers.get('x-original-routing-key', 'unknown')
        retry_count = headers.get('x-retry-count', 0)
        death_reason = headers.get('x-death-reason', 'unknown')
        
        logger.warning(f"Message failed after {retry_count} retries. Original queue: {original_routing_key}")
        logger.warning(f"Failure reason: {death_reason}")
        
        # In a real system, you would:
        # 1. Log the failed message details
        # 2. Alert operations/support team
        # 3. Store in a database for later analysis
        # 4. Provide a way to manually retry
        
        # For this example, we'll just acknowledge it
        channel.basic_ack(delivery_tag=delivery_tag)
        logger.info(f"Dead letter message acknowledged")
        
    except Exception as e:
        logger.error(f"Error in dead letter handler: {e}")
        # Acknowledge anyway since we can't do much else
        channel.basic_ack(delivery_tag=delivery_tag)

def setup_retry_mechanism(channel):
    """Set up the retry and dead letter exchanges and queues"""
    # Declare the dead letter exchange and queue
    channel.exchange_declare(
        exchange='dead.letter.exchange',
        exchange_type='direct',
        durable=True
    )
    
    channel.queue_declare(
        queue='dead.letter.queue',
        durable=True
    )
    
    channel.queue_bind(
        queue='dead.letter.queue',
        exchange='dead.letter.exchange',
        routing_key='dead.letter.queue'
    )
    
    # Declare the retry exchange and queue
    channel.exchange_declare(
        exchange='retry.exchange',
        exchange_type='direct',
        durable=True
    )
    
    # Set up retry queue that will send messages back to original queue after TTL expires
    retry_queue_args = {
        'x-dead-letter-exchange': '',  # Default exchange for expired messages
        'x-dead-letter-routing-key': 'task_queue'  # Send back to main queue
    }
    
    channel.queue_declare(
        queue='retry.queue',
        durable=True,
        arguments=retry_queue_args
    )
    
    channel.queue_bind(
        queue='retry.queue',
        exchange='retry.exchange',
        routing_key='retry.queue'
    )
    
    logger.info("Retry mechanism set up successfully")

def run_example():
    """Run the acknowledge and retry example"""
    # Create a connection manager for setup and initial configuration
    setup_broker_manager = BrokerConnectionManager()
    
    # Register the RabbitMQ nodes
    # setup_broker_manager.register_broker(1, "localhost", 5672)
    # setup_broker_manager.register_broker(2, "localhost", 5682)
    # setup_broker_manager.register_broker(3, "localhost", 5692)
    
    if len(setup_broker_manager.load_balancer.connections) == 0:
        logger.error("Failed to connect to RabbitMQ cluster. Is the DMQ cluster running?")
        return
    
    # Get a connection first, then a channel for setup
    setup_connection = setup_broker_manager.acquire_connection(strategy="least_active")
    if not setup_connection:
        logger.error("Failed to acquire connection for setup")
        setup_broker_manager.close()
        return
        
    setup_channel = setup_connection.get_channel()
    if not setup_channel:
        logger.error("Failed to acquire channel for setup")
        setup_broker_manager.close()
        return
    
    try:
        # Declare the main task queue
        setup_channel.queue_declare(
            queue='task_queue',
            durable=True
        )
        
        # Setup retry mechanism
        setup_retry_mechanism(setup_channel)
        
        # Create separate connection managers for each consumer to avoid race conditions
        connections_and_channels = []
        
        # Create a main consumer with dedicated connection manager
        main_consumer_cm = BrokerConnectionManager()
        # main_consumer_cm.register_broker(1, "localhost", 5672)
        # main_consumer_cm.register_broker(2, "localhost", 5682)
        # main_consumer_cm.register_broker(3, "localhost", 5692)
        
        if len(main_consumer_cm.load_balancer.connections) == 0:
            logger.error("Failed to connect to RabbitMQ cluster for main consumer")
            setup_broker_manager.close()
            return
            
        main_consumer_connection = main_consumer_cm.acquire_connection(strategy="least_active")
        if not main_consumer_connection:
            logger.error("Failed to acquire connection for main consumer")
            main_consumer_cm.close()
            setup_broker_manager.close()
            return
            
        main_consumer_channel = main_consumer_connection.get_channel()
        if not main_consumer_channel:
            logger.error("Failed to acquire channel for main consumer")
            main_consumer_cm.close()
            setup_broker_manager.close()
            return
        
        # Set QoS for main queue
        main_consumer_channel.basic_qos(prefetch_count=1)
        
        # Start consuming from main queue
        main_consumer_channel.basic_consume(
            queue='task_queue',
            on_message_callback=process_message,
            auto_ack=False  # Manual acknowledgment
        )
        connections_and_channels.append((main_consumer_channel, main_consumer_cm, "Main Queue Consumer"))
        
        # Create a retry consumer with dedicated connection manager
        retry_consumer_cm = BrokerConnectionManager()
        # retry_consumer_cm.register_broker(1, "localhost", 5672)
        # retry_consumer_cm.register_broker(2, "localhost", 5682)
        # retry_consumer_cm.register_broker(3, "localhost", 5692)
        
        if len(retry_consumer_cm.load_balancer.connections) == 0:
            logger.error("Failed to connect to RabbitMQ cluster for retry consumer")
            main_consumer_cm.close()
            setup_broker_manager.close()
            return
            
        retry_consumer_connection = retry_consumer_cm.acquire_connection(strategy="least_active")
        if not retry_consumer_connection:
            logger.error("Failed to acquire connection for retry consumer")
            retry_consumer_cm.close()
            main_consumer_cm.close()
            setup_broker_manager.close()
            return
            
        retry_consumer_channel = retry_consumer_connection.get_channel()
        if not retry_consumer_channel:
            logger.error("Failed to acquire channel for retry consumer")
            retry_consumer_cm.close()
            main_consumer_cm.close()
            setup_broker_manager.close()
            return
        
        # Start consuming from retry queue
        retry_consumer_channel.basic_consume(
            queue='retry.queue',
            on_message_callback=process_retry_queue,
            auto_ack=False  # Manual acknowledgment
        )
        connections_and_channels.append((retry_consumer_channel, retry_consumer_cm, "Retry Queue Consumer"))
        
        # Create a dead letter consumer with dedicated connection manager
        dlq_consumer_cm = BrokerConnectionManager()
        # dlq_consumer_cm.register_broker(1, "localhost", 5672)
        # dlq_consumer_cm.register_broker(2, "localhost", 5682)
        # dlq_consumer_cm.register_broker(3, "localhost", 5692)
        
        if len(dlq_consumer_cm.load_balancer.connections) == 0:
            logger.error("Failed to connect to RabbitMQ cluster for DLQ consumer")
            retry_consumer_cm.close()
            main_consumer_cm.close()
            setup_broker_manager.close()
            return
            
        dlq_consumer_connection = dlq_consumer_cm.acquire_connection(strategy="least_active")
        if not dlq_consumer_connection:
            logger.error("Failed to acquire connection for DLQ consumer")
            dlq_consumer_cm.close()
            retry_consumer_cm.close()
            main_consumer_cm.close()
            setup_broker_manager.close()
            return
            
        dlq_consumer_channel = dlq_consumer_connection.get_channel()
        if not dlq_consumer_channel:
            logger.error("Failed to acquire channel for DLQ consumer")
            dlq_consumer_cm.close()
            retry_consumer_cm.close()
            main_consumer_cm.close()
            setup_broker_manager.close()
            return
        
        # Start consuming from dead letter queue
        dlq_consumer_channel.basic_consume(
            queue='dead.letter.queue',
            on_message_callback=process_dead_letter,
            auto_ack=False  # Manual acknowledgment
        )
        connections_and_channels.append((dlq_consumer_channel, dlq_consumer_cm, "Dead Letter Queue Consumer"))
        
        # Start consumer threads
        consumer_threads = []
        
        main_thread = threading.Thread(
            target=main_consumer_channel.start_consuming,
            name="main-consumer-thread"
        )
        main_thread.daemon = True
        main_thread.start()
        consumer_threads.append(main_thread)
        
        retry_thread = threading.Thread(
            target=retry_consumer_channel.start_consuming,
            name="retry-consumer-thread"
        )
        retry_thread.daemon = True
        retry_thread.start()
        consumer_threads.append(retry_thread)
        
        dlq_thread = threading.Thread(
            target=dlq_consumer_channel.start_consuming,
            name="dlq-consumer-thread"
        )
        dlq_thread.daemon = True
        dlq_thread.start()
        consumer_threads.append(dlq_thread)
        
        logger.info("Started all consumers with dedicated connection managers")
        
        # Wait for consumers to initialize
        time.sleep(1)
        
        # Create a publisher with dedicated connection manager
        publisher_cm = BrokerConnectionManager()
        # publisher_cm.register_broker(1, "localhost", 5672)
        # publisher_cm.register_broker(2, "localhost", 5682)
        # publisher_cm.register_broker(3, "localhost", 5692)
        
        if len(publisher_cm.load_balancer.connections) == 0:
            logger.error("Failed to connect to RabbitMQ cluster for publisher")
            # Clean up all connection managers
            for _, cm, name in connections_and_channels:
                logger.info(f"Closing {name} connection manager")
                cm.close()
            setup_broker_manager.close()
            return
            
        publisher_connection = publisher_cm.acquire_connection(strategy="least_active")
        if not publisher_connection:
            logger.error("Failed to acquire connection for publisher")
            publisher_cm.close()
            # Clean up all connection managers
            for _, cm, name in connections_and_channels:
                logger.info(f"Closing {name} connection manager")
                cm.close()
            setup_broker_manager.close()
            return
            
        publisher_channel = publisher_connection.get_channel()
        if not publisher_channel:
            logger.error("Failed to acquire channel for publisher")
            publisher_cm.close()
            # Clean up all connection managers
            for _, cm, name in connections_and_channels:
                logger.info(f"Closing {name} connection manager")
                cm.close()
            setup_broker_manager.close()
            return
        
        # Add the publisher to our list of connections for cleanup
        connections_and_channels.append((publisher_channel, publisher_cm, "Publisher"))
        
        # Publish sample messages
        logger.info("\n=== Publishing sample tasks ===")
        for i in range(10):
            # Create a task message
            task = {
                "task_id": i,
                "name": f"Task {i}",
                "complexity": random.randint(1, 10),
                "timestamp": time.time()
            }
            
            # Create message properties
            properties = pika.BasicProperties(
                content_type='application/json',
                delivery_mode=2,  # Persistent
                headers={
                    'x-retry-count': 0,
                    'x-original-routing-key': 'task_queue',
                    'x-original-exchange': ''
                }
            )
            
            # Publish to the main queue
            publisher_channel.basic_publish(
                exchange='',  # Default exchange
                routing_key='task_queue',
                body=json.dumps(task).encode('utf-8'),
                properties=properties
            )
            
            logger.info(f"Published task {i}")
            time.sleep(0.5)  # Small delay between tasks
        
        # Wait for processing to complete
        logger.info("\n=== Waiting for message processing to complete ===")
        logger.info("This may take some time due to retry delays")
        time.sleep(30)  # Allow time for retries with backoff
        
        # Display summary
        logger.info("\n=== Retry Mechanism Summary ===")
        logger.info("1. Messages are first tried in the main queue")
        logger.info("2. Failed messages are sent to retry queue with exponential backoff")
        logger.info("3. After max retries, messages go to dead letter queue")
        logger.info("4. This pattern ensures no message is lost and each gets multiple processing attempts")
        logger.info("5. Each consumer uses its own dedicated connection manager to avoid race conditions")
        
    except KeyboardInterrupt:
        logger.info("Example interrupted by user")
    except Exception as e:
        logger.error(f"Error in example: {e}", exc_info=True)
    finally:
        # Clean up all connection managers
        logger.info("Cleaning up...")
        
        try:
            # Stop all consumers
            for channel, _, name in connections_and_channels:
                try:
                    logger.info(f"Stopping {name}")
                    channel.stop_consuming()
                except Exception as e:
                    logger.error(f"Error stopping {name}: {e}")
        except:
            pass
            
        # Close all connection managers
        for _, cm, name in connections_and_channels:
            try:
                logger.info(f"Closing {name} connection manager")
                cm.close()
            except Exception as e:
                logger.error(f"Error closing {name} connection manager: {e}")
        
        # Close setup connection manager
        setup_broker_manager.close()
        logger.info("Acknowledge and retry example finished")

if __name__ == "__main__":
    run_example()