#!/usr/bin/env python3
"""
Basic implementation of direct exchange producer and consumer classes.
"""
import json
import uuid
import time
import logging
import threading
from typing import Dict, List, Optional, Any, Callable, Union

import pika

from dmq.connection_manager import BrokerConnectionManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DirectProducer:
    """A simple producer for publishing messages to a direct exchange"""
    
    def __init__(self, connection_manager=None, config_file=None):
        """Initialize the producer
        
        Args:
            connection_manager: Optional connection manager to use
            config_file: Path to config file with broker information
        """
        # Setup connection manager
        if connection_manager:
            self.connection_manager = connection_manager
            self._owns_connection = False
        else:
            self.connection_manager = BrokerConnectionManager()
            if config_file:
                # TODO: Implement config file loading
                pass
            self._owns_connection = True
    
    def publish(self, routing_key: str, message: Union[dict, str, bytes], 
                exchange: str = "", 
                properties=None,
                broker_id: Optional[int] = None) -> bool:
        """Publish a message to a direct exchange
        
        Args:
            routing_key: The routing key (queue name for default exchange)
            message: Message content (dict, string, or bytes)
            exchange: Exchange name (empty string for default exchange)
            properties: Optional message properties
            broker_id: Optional specific broker to use
            
        Returns:
            True if successful, False otherwise
        """
        # Get channel using connection manager
        channel = self.connection_manager.acquire_channel(broker_id)
        if not channel:
            logger.error("Failed to acquire channel for publishing")
            return False
        
        try:
            # Prepare message body
            if isinstance(message, dict):
                message_body = json.dumps(message).encode('utf-8')
            elif isinstance(message, str):
                message_body = message.encode('utf-8')
            else:
                message_body = message
            
            # Use default properties if none provided
            if properties is None:
                properties = pika.BasicProperties(
                    content_type='application/json' if isinstance(message, dict) else 'text/plain',
                    delivery_mode=2,  # Persistent
                    message_id=str(uuid.uuid4())
                )
            
            # Publish the message
            channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=message_body,
                properties=properties
            )
            
            logger.info(f"Published message to {exchange or 'default exchange'} with routing key {routing_key}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to publish message: {e}")
            return False
    
    def close(self):
        """Release connections if we own them"""
        if self._owns_connection:
            self.connection_manager.release_all_connections()


class FanoutProducer:
    """A producer for publishing messages to a fanout exchange"""
    
    def __init__(self, exchange_name: str, connection_manager=None, config_file=None):
        """Initialize the fanout producer
        
        Args:
            exchange_name: Name of the fanout exchange to publish to
            connection_manager: Optional connection manager to use
            config_file: Path to config file with broker information
        """
        # Setup connection manager
        if connection_manager:
            self.connection_manager = connection_manager
            self._owns_connection = False
        else:
            self.connection_manager = BrokerConnectionManager()
            if config_file:
                # TODO: Implement config file loading
                pass
            self._owns_connection = True
        
        self.exchange_name = exchange_name
        
    def declare_exchange(self, durable=True, broker_id=None) -> bool:
        """Declare the fanout exchange
        
        Args:
            durable: Whether the exchange should survive broker restarts
            broker_id: Optional specific broker to use
            
        Returns:
            True if successful, False otherwise
        """
        # Get channel using connection manager
        channel = self.connection_manager.acquire_channel(broker_id)
        if not channel:
            logger.error("Failed to acquire channel for declaring exchange")
            return False
        
        try:
            # Declare the exchange as fanout type
            channel.exchange_declare(
                exchange=self.exchange_name,
                exchange_type='fanout',
                durable=durable
            )
            logger.info(f"Declared fanout exchange: {self.exchange_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to declare exchange: {e}")
            return False
    
    def publish(self, message: Union[dict, str, bytes], 
                properties=None,
                broker_id: Optional[int] = None) -> bool:
        """Publish a message to the fanout exchange
        
        Args:
            message: Message content (dict, string, or bytes)
            properties: Optional message properties
            broker_id: Optional specific broker to use
            
        Returns:
            True if successful, False otherwise
        """
        # Get channel using connection manager
        channel = self.connection_manager.acquire_channel(broker_id)
        if not channel:
            logger.error("Failed to acquire channel for publishing")
            return False
        
        try:
            # Prepare message body
            if isinstance(message, dict):
                message_body = json.dumps(message).encode('utf-8')
            elif isinstance(message, str):
                message_body = message.encode('utf-8')
            else:
                message_body = message
            
            # Use default properties if none provided
            if properties is None:
                properties = pika.BasicProperties(
                    content_type='application/json' if isinstance(message, dict) else 'text/plain',
                    delivery_mode=2,  # Persistent
                    message_id=str(uuid.uuid4())
                )
            
            # Publish the message to the fanout exchange
            # For fanout exchanges, the routing key is ignored
            channel.basic_publish(
                exchange=self.exchange_name,
                routing_key='',  # Ignored for fanout exchanges
                body=message_body,
                properties=properties
            )
            
            logger.info(f"Published message to fanout exchange {self.exchange_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to publish message: {e}")
            return False
    
    def close(self):
        """Release connections if we own them"""
        if self._owns_connection:
            self.connection_manager.release_all_connections()


class DirectConsumer:
    """A simple consumer for consuming messages from a direct exchange"""
    
    def __init__(self, queue_name: str, connection_manager=None, config_file=None, auto_ack=True):
        """Initialize the consumer
        
        Args:
            queue_name: Name of the queue to consume from
            connection_manager: Optional connection manager to use
            config_file: Path to config file with broker information
            auto_ack: Whether to automatically acknowledge messages
        """
        # Setup connection manager
        if connection_manager:
            self.connection_manager = connection_manager
            self._owns_connection = False
        else:
            self.connection_manager = BrokerConnectionManager()
            if config_file:
                # TODO: Implement config file loading
                pass
            self._owns_connection = True
        
        self.queue_name = queue_name
        self.auto_ack = auto_ack
        self._running = False
        self._channel = None
        self._consumer_tag = None
    
    def declare_queue(self, durable=True, broker_id=None) -> bool:
        """Declare the queue to consume from
        
        Args:
            durable: Whether the queue should survive broker restarts
            broker_id: Optional specific broker to use
            
        Returns:
            True if successful, False otherwise
        """
        # Get channel using connection manager
        channel = self.connection_manager.acquire_channel(broker_id)
        if not channel:
            logger.error("Failed to acquire channel for declaring queue")
            return False
        
        try:
            # Declare the queue
            channel.queue_declare(
                queue=self.queue_name,
                durable=durable
            )
            logger.info(f"Declared queue: {self.queue_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to declare queue: {e}")
            return False
    
    def bind_queue(self, exchange: str, routing_key: str, broker_id=None) -> bool:
        """Bind the queue to an exchange
        
        Args:
            exchange: Name of the exchange to bind to
            routing_key: Routing key to use for the binding
            broker_id: Optional specific broker to use
            
        Returns:
            True if successful, False otherwise
        """
        # Get channel using connection manager
        channel = self.connection_manager.acquire_channel(broker_id)
        if not channel:
            logger.error("Failed to acquire channel for binding queue")
            return False
        
        try:
            # Bind the queue to the exchange
            channel.queue_bind(
                queue=self.queue_name,
                exchange=exchange,
                routing_key=routing_key
            )
            logger.info(f"Bound queue {self.queue_name} to exchange {exchange} with routing key {routing_key}")
            return True
        except Exception as e:
            logger.error(f"Failed to bind queue: {e}")
            return False
    
    def start_consuming(self, callback: Callable, broker_id=None):
        """Start consuming messages from the queue
        
        Args:
            callback: Function to call when a message is received
            broker_id: Optional specific broker to use
        """
        if self._running:
            logger.warning("Consumer is already running")
            return
        
        # Get thread ID to determine if we're on the main thread or a new thread
        current_thread_id = threading.get_ident()
        
        # Get channel using connection manager
        self._channel = self.connection_manager.acquire_channel(broker_id)
        if not self._channel:
            logger.error("Failed to acquire channel for consuming")
            return
        
        try:
            # Set prefetch to 1 so we only get one message at a time
            self._channel.basic_qos(prefetch_count=1)
            
            # Define the callback wrapper
            def callback_wrapper(ch, method, properties, body):
                try:
                    # Convert JSON body if applicable
                    if properties.content_type == 'application/json':
                        try:
                            decoded_body = json.loads(body)
                        except json.JSONDecodeError:
                            decoded_body = body.decode('utf-8')
                    else:
                        decoded_body = body.decode('utf-8')
                    
                    # Call the user callback
                    result = callback(decoded_body, properties.headers)
                    
                    # Handle acknowledgment if not auto_ack
                    if not self.auto_ack:
                        if result:
                            ch.basic_ack(delivery_tag=method.delivery_tag)
                        else:
                            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                            
                except Exception as e:
                    logger.error(f"Error in message handler: {e}")
                    # Negative acknowledge on error
                    if not self.auto_ack:
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            
            # Start consuming
            self._consumer_tag = self._channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=callback_wrapper,
                auto_ack=self.auto_ack
            )
            
            self._running = True
            thread_info = f"thread {current_thread_id}"
            logger.info(f"Started consuming from queue: {self.queue_name} on {thread_info}")
            
            # Start IO loop
            self._channel.start_consuming()
            
        except Exception as e:
            logger.error(f"Error starting consumer: {e}")
            self._running = False
    
    def start_consuming_thread(self, callback: Callable, thread_name=None, broker_id=None) -> threading.Thread:
        """Start consuming in a background thread with its own connection
        
        Args:
            callback: Function to call when a message is received
            thread_name: Optional name for the thread
            broker_id: Optional specific broker to use
            
        Returns:
            The thread object
        """
        # Create a new consumer instance for the thread to ensure thread safety
        # This is critical because Pika connections are not thread-safe
        thread_consumer = DirectConsumer(
            queue_name=self.queue_name,
            connection_manager=self.connection_manager,
            auto_ack=self.auto_ack
        )
        
        # Create a wrapper function that will be run in the thread
        def thread_consume():
            try:
                # Each thread gets its own channel and connection
                thread_consumer.start_consuming(callback, broker_id)
            except Exception as e:
                logger.error(f"Error in consumer thread: {e}")
            finally:
                # Make sure to properly close the thread's consumer
                thread_consumer.stop_consuming()
        
        # Create and start the thread
        thread = threading.Thread(
            target=thread_consume,
            name=thread_name,
            daemon=True
        )
        thread.start()
        
        # Store the thread consumer for later cleanup
        self._thread_consumers = getattr(self, '_thread_consumers', [])
        self._thread_consumers.append(thread_consumer)
        
        return thread
    
    def stop_consuming(self):
        """Stop consuming messages"""
        if not self._running:
            return
            
        try:
            if self._channel and self._consumer_tag:
                self._channel.basic_cancel(self._consumer_tag)
                self._channel.stop_consuming()
            
            self._running = False
            logger.info("Stopped consuming messages")
        except Exception as e:
            logger.error(f"Error stopping consumer: {e}")
    
    def close(self):
        """Release resources"""
        self.stop_consuming()
        
        # Clean up any thread consumers
        thread_consumers = getattr(self, '_thread_consumers', [])
        for consumer in thread_consumers:
            try:
                consumer.stop_consuming()
            except:
                pass
        
        if self._owns_connection:
            self.connection_manager.release_all_connections()


class FanoutConsumer:
    """A consumer for consuming messages from a fanout exchange"""
    
    def __init__(self, exchange_name: str, queue_name: Optional[str] = None, 
                 connection_manager=None, config_file=None, auto_ack=True,
                 exclusive=False, prefetch_count=1):
        """Initialize the fanout consumer
        
        Args:
            exchange_name: Name of the fanout exchange to consume from
            queue_name: Optional queue name (generated if not provided)
            connection_manager: Optional connection manager to use
            config_file: Path to config file with broker information
            auto_ack: Whether to automatically acknowledge messages
            exclusive: Whether the queue should be exclusive to this connection
            prefetch_count: Number of messages to prefetch
        """
        # Setup connection manager
        if connection_manager:
            self.connection_manager = connection_manager
            self._owns_connection = False
        else:
            self.connection_manager = BrokerConnectionManager()
            if config_file:
                # TODO: Implement config file loading
                pass
            self._owns_connection = True
        
        self.exchange_name = exchange_name
        self.queue_name = queue_name or f"fanout-{exchange_name}-{uuid.uuid4().hex[:8]}"
        self.auto_ack = auto_ack
        self.exclusive = exclusive
        self.prefetch_count = prefetch_count
        self._running = False
        self._channel = None
        self._consumer_tag = None
    
    def setup(self, durable=True, broker_id=None) -> bool:
        """Set up the exchange, queue, and binding
        
        Args:
            durable: Whether the exchange and queue should survive broker restarts
            broker_id: Optional specific broker to use
            
        Returns:
            True if successful, False otherwise
        """
        # Get channel using connection manager
        channel = self.connection_manager.acquire_channel(broker_id)
        if not channel:
            logger.error("Failed to acquire channel for setup")
            return False
        
        try:
            # Declare the exchange as fanout type
            channel.exchange_declare(
                exchange=self.exchange_name,
                exchange_type='fanout',
                durable=durable
            )
            
            # Declare the queue
            result = channel.queue_declare(
                queue=self.queue_name,
                durable=durable,
                exclusive=self.exclusive
            )
            # Store the generated queue name if one wasn't specified
            self.queue_name = result.method.queue
            
            # Bind the queue to the exchange
            channel.queue_bind(
                queue=self.queue_name,
                exchange=self.exchange_name
            )
            
            logger.info(f"Set up fanout consumer with exchange: {self.exchange_name}, queue: {self.queue_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to set up fanout consumer: {e}")
            return False
    
    def start_consuming(self, callback: Callable, broker_id=None):
        """Start consuming messages from the fanout exchange
        
        Args:
            callback: Function to call when a message is received
            broker_id: Optional specific broker to use
        """
        if self._running:
            logger.warning("Consumer is already running")
            return
        
        # Get thread ID to determine if we're on the main thread or a new thread
        current_thread_id = threading.get_ident()
        
        # Get channel using connection manager
        self._channel = self.connection_manager.acquire_channel(broker_id)
        if not self._channel:
            logger.error("Failed to acquire channel for consuming")
            return
        
        try:
            # Set prefetch
            self._channel.basic_qos(prefetch_count=self.prefetch_count)
            
            # Define the callback wrapper
            def callback_wrapper(ch, method, properties, body):
                try:
                    # Convert JSON body if applicable
                    if properties.content_type == 'application/json':
                        try:
                            decoded_body = json.loads(body)
                        except json.JSONDecodeError:
                            decoded_body = body.decode('utf-8')
                    else:
                        decoded_body = body.decode('utf-8')
                    
                    # Call the user callback
                    result = callback(decoded_body, properties.headers)
                    
                    # Handle acknowledgment if not auto_ack
                    if not self.auto_ack:
                        if result:
                            ch.basic_ack(delivery_tag=method.delivery_tag)
                        else:
                            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                            
                except Exception as e:
                    logger.error(f"Error in message handler: {e}")
                    # Negative acknowledge on error
                    if not self.auto_ack:
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            
            # Start consuming
            self._consumer_tag = self._channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=callback_wrapper,
                auto_ack=self.auto_ack
            )
            
            self._running = True
            thread_info = f"thread {current_thread_id}"
            logger.info(f"Started consuming from queue: {self.queue_name} bound to fanout exchange: {self.exchange_name} on {thread_info}")
            
            # Start IO loop
            self._channel.start_consuming()
            
        except Exception as e:
            logger.error(f"Error starting consumer: {e}")
            self._running = False
    
    def start_consuming_thread(self, callback: Callable, thread_name=None, broker_id=None) -> threading.Thread:
        """Start consuming in a background thread with its own connection
        
        Args:
            callback: Function to call when a message is received
            thread_name: Optional name for the thread
            broker_id: Optional specific broker to use
            
        Returns:
            The thread object
        """
        # Create a new consumer instance for the thread to ensure thread safety
        # This is critical because Pika connections are not thread-safe
        thread_consumer = FanoutConsumer(
            exchange_name=self.exchange_name,
            queue_name=self.queue_name,
            connection_manager=self.connection_manager,
            auto_ack=self.auto_ack,
            exclusive=self.exclusive,
            prefetch_count=self.prefetch_count
        )
        
        # Make sure the thread consumer is set up
        thread_consumer.setup(durable=True)
        
        # Create a wrapper function that will be run in the thread
        def thread_consume():
            try:
                # Each thread gets its own channel and connection
                thread_consumer.start_consuming(callback, broker_id)
            except Exception as e:
                logger.error(f"Error in consumer thread: {e}")
            finally:
                # Make sure to properly close the thread's consumer
                thread_consumer.stop_consuming()
        
        # Create and start the thread
        thread = threading.Thread(
            target=thread_consume,
            name=thread_name,
            daemon=True
        )
        thread.start()
        
        # Store the thread consumer for later cleanup
        self._thread_consumers = getattr(self, '_thread_consumers', [])
        self._thread_consumers.append(thread_consumer)
        
        return thread
    
    def stop_consuming(self):
        """Stop consuming messages"""
        if not self._running:
            return
            
        try:
            if self._channel and self._consumer_tag:
                self._channel.basic_cancel(self._consumer_tag)
                self._channel.stop_consuming()
            
            self._running = False
            logger.info("Stopped consuming messages")
        except Exception as e:
            logger.error(f"Error stopping consumer: {e}")
    
    def close(self):
        """Release resources"""
        self.stop_consuming()
        
        # Clean up any thread consumers
        thread_consumers = getattr(self, '_thread_consumers', [])
        for consumer in thread_consumers:
            try:
                consumer.stop_consuming()
            except:
                pass
        
        if self._owns_connection:
            self.connection_manager.release_all_connections()


class TopicProducer:
    """A producer for publishing messages to a topic exchange
    
    Topic exchanges route messages to queues based on routing key patterns.
    Routing keys are typically dot-separated strings like "system.logs.error"
    """
    
    def __init__(self, exchange_name: str, connection_manager=None, config_file=None):
        """Initialize the topic producer
        
        Args:
            exchange_name: Name of the topic exchange to publish to
            connection_manager: Optional connection manager to use
            config_file: Path to config file with broker information
        """
        # Setup connection manager
        if connection_manager:
            self.connection_manager = connection_manager
            self._owns_connection = False
        else:
            self.connection_manager = BrokerConnectionManager()
            if config_file:
                # TODO: Implement config file loading
                pass
            self._owns_connection = True
        
        self.exchange_name = exchange_name
        
    def declare_exchange(self, durable=True, broker_id=None) -> bool:
        """Declare the topic exchange
        
        Args:
            durable: Whether the exchange should survive broker restarts
            broker_id: Optional specific broker to use
            
        Returns:
            True if successful, False otherwise
        """
        # Get channel using connection manager
        channel = self.connection_manager.acquire_channel(broker_id)
        if not channel:
            logger.error("Failed to acquire channel for declaring exchange")
            return False
        
        try:
            # Declare the exchange as topic type
            channel.exchange_declare(
                exchange=self.exchange_name,
                exchange_type='topic',
                durable=durable
            )
            logger.info(f"Declared topic exchange: {self.exchange_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to declare exchange: {e}")
            return False
    
    def publish(self, routing_key: str, message: Union[dict, str, bytes], 
                properties=None,
                broker_id: Optional[int] = None) -> bool:
        """Publish a message to the topic exchange with a specific routing key
        
        Args:
            routing_key: The topic pattern for routing the message (e.g. "logs.error")
            message: Message content (dict, string, or bytes)
            properties: Optional message properties
            broker_id: Optional specific broker to use
            
        Returns:
            True if successful, False otherwise
        """
        # Get channel using connection manager
        channel = self.connection_manager.acquire_channel(broker_id)
        if not channel:
            logger.error("Failed to acquire channel for publishing")
            return False
        
        try:
            # Prepare message body
            if isinstance(message, dict):
                message_body = json.dumps(message).encode('utf-8')
            elif isinstance(message, str):
                message_body = message.encode('utf-8')
            else:
                message_body = message
            
            # Use default properties if none provided
            if properties is None:
                properties = pika.BasicProperties(
                    content_type='application/json' if isinstance(message, dict) else 'text/plain',
                    delivery_mode=2,  # Persistent
                    message_id=str(uuid.uuid4())
                )
            
            # Publish the message to the topic exchange with the routing key
            channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=routing_key,
                body=message_body,
                properties=properties
            )
            
            logger.info(f"Published message to topic exchange {self.exchange_name} with routing key {routing_key}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to publish message: {e}")
            return False
    
    def close(self):
        """Release connections if we own them"""
        if self._owns_connection:
            self.connection_manager.release_all_connections()


class TopicConsumer:
    """A consumer for consuming messages from a topic exchange based on routing patterns
    
    Topic exchanges allow consumers to subscribe to messages matching specific patterns:
    * (star) can substitute for exactly one word
    # (hash) can substitute for zero or more words
    
    Examples:
    "*.error" matches "system.error" but not "system.critical.error"
    "system.#" matches "system.error" and "system.critical.error"
    """
    
    def __init__(self, exchange_name: str, binding_pattern: str, queue_name: Optional[str] = None, 
                 connection_manager=None, config_file=None, auto_ack=True,
                 exclusive=False, prefetch_count=1):
        """Initialize the topic consumer
        
        Args:
            exchange_name: Name of the topic exchange to consume from
            binding_pattern: The topic pattern to subscribe to (e.g., "system.*.error")
            queue_name: Optional queue name (generated if not provided)
            connection_manager: Optional connection manager to use
            config_file: Path to config file with broker information
            auto_ack: Whether to automatically acknowledge messages
            exclusive: Whether the queue should be exclusive to this connection
            prefetch_count: Number of messages to prefetch
        """
        # Setup connection manager
        if connection_manager:
            self.connection_manager = connection_manager
            self._owns_connection = False
        else:
            self.connection_manager = BrokerConnectionManager()
            if config_file:
                # TODO: Implement config file loading
                pass
            self._owns_connection = True
        
        self.exchange_name = exchange_name
        self.binding_pattern = binding_pattern
        # Create a queue name based on the exchange and binding pattern if not provided
        self.queue_name = queue_name or f"topic-{exchange_name}-{binding_pattern.replace('.', '-').replace('*', 'star').replace('#', 'hash')}-{uuid.uuid4().hex[:8]}"
        self.auto_ack = auto_ack
        self.exclusive = exclusive
        self.prefetch_count = prefetch_count
        self._running = False
        self._channel = None
        self._consumer_tag = None
    
    def setup(self, durable=True, broker_id=None) -> bool:
        """Set up the exchange, queue, and binding with the topic pattern
        
        Args:
            durable: Whether the exchange and queue should survive broker restarts
            broker_id: Optional specific broker to use
            
        Returns:
            True if successful, False otherwise
        """
        # Get channel using connection manager
        channel = self.connection_manager.acquire_channel(broker_id)
        if not channel:
            logger.error("Failed to acquire channel for setup")
            return False
        
        try:
            # Declare the exchange as topic type
            channel.exchange_declare(
                exchange=self.exchange_name,
                exchange_type='topic',
                durable=durable
            )
            
            # Declare the queue
            result = channel.queue_declare(
                queue=self.queue_name,
                durable=durable,
                exclusive=self.exclusive
            )
            # Store the generated queue name if one wasn't specified
            self.queue_name = result.method.queue
            
            # Bind the queue to the exchange with the binding pattern
            channel.queue_bind(
                queue=self.queue_name,
                exchange=self.exchange_name,
                routing_key=self.binding_pattern
            )
            
            logger.info(f"Set up topic consumer with exchange: {self.exchange_name}, queue: {self.queue_name}, binding pattern: {self.binding_pattern}")
            return True
        except Exception as e:
            logger.error(f"Failed to set up topic consumer: {e}")
            return False
    
    def start_consuming(self, callback: Callable, broker_id=None):
        """Start consuming messages from the topic exchange that match the binding pattern
        
        Args:
            callback: Function to call when a message is received
            broker_id: Optional specific broker to use
        """
        if self._running:
            logger.warning("Consumer is already running")
            return
        
        # Get thread ID to determine if we're on the main thread or a new thread
        current_thread_id = threading.get_ident()
        
        # Get channel using connection manager
        self._channel = self.connection_manager.acquire_channel(broker_id)
        if not self._channel:
            logger.error("Failed to acquire channel for consuming")
            return
        
        try:
            # Set prefetch
            self._channel.basic_qos(prefetch_count=self.prefetch_count)
            
            # Define the callback wrapper
            def callback_wrapper(ch, method, properties, body):
                try:
                    # Get routing key from the method
                    routing_key = method.routing_key
                    
                    # Convert JSON body if applicable
                    if properties.content_type == 'application/json':
                        try:
                            decoded_body = json.loads(body)
                        except json.JSONDecodeError:
                            decoded_body = body.decode('utf-8')
                    else:
                        decoded_body = body.decode('utf-8')
                    
                    # Call the user callback with the message body, headers, and routing key
                    result = callback(decoded_body, properties.headers, routing_key)
                    
                    # Handle acknowledgment if not auto_ack
                    if not self.auto_ack:
                        if result:
                            ch.basic_ack(delivery_tag=method.delivery_tag)
                        else:
                            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                            
                except Exception as e:
                    logger.error(f"Error in message handler: {e}")
                    # Negative acknowledge on error
                    if not self.auto_ack:
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            
            # Start consuming
            self._consumer_tag = self._channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=callback_wrapper,
                auto_ack=self.auto_ack
            )
            
            self._running = True
            thread_info = f"thread {current_thread_id}"
            logger.info(f"Started consuming from queue: {self.queue_name} bound to topic exchange: {self.exchange_name} with pattern: {self.binding_pattern} on {thread_info}")
            
            # Start IO loop
            self._channel.start_consuming()
            
        except Exception as e:
            logger.error(f"Error starting consumer: {e}")
            self._running = False
    
    def start_consuming_thread(self, callback: Callable, thread_name=None, broker_id=None) -> threading.Thread:
        """Start consuming in a background thread with its own connection
        
        Args:
            callback: Function to call when a message is received
            thread_name: Optional name for the thread
            broker_id: Optional specific broker to use
            
        Returns:
            The thread object
        """
        # Create a new consumer instance for the thread to ensure thread safety
        # This is critical because Pika connections are not thread-safe
        thread_consumer = TopicConsumer(
            exchange_name=self.exchange_name,
            binding_pattern=self.binding_pattern,
            queue_name=self.queue_name,
            connection_manager=self.connection_manager,
            auto_ack=self.auto_ack,
            exclusive=self.exclusive,
            prefetch_count=self.prefetch_count
        )
        
        # Make sure the thread consumer is set up
        thread_consumer.setup(durable=True, broker_id=broker_id)
        
        # Create a wrapper function that will be run in the thread
        def thread_consume():
            try:
                # Each thread gets its own channel and connection
                thread_consumer.start_consuming(callback, broker_id)
            except Exception as e:
                logger.error(f"Error in consumer thread: {e}")
            finally:
                # Make sure to properly close the thread's consumer
                thread_consumer.stop_consuming()
        
        # Create and start the thread
        thread = threading.Thread(
            target=thread_consume,
            name=thread_name or f"topic-consumer-{self.binding_pattern}",
            daemon=True
        )
        thread.start()
        
        # Store the thread consumer for later cleanup
        self._thread_consumers = getattr(self, '_thread_consumers', [])
        self._thread_consumers.append(thread_consumer)
        
        return thread
    
    def stop_consuming(self):
        """Stop consuming messages"""
        if not self._running:
            return
            
        try:
            if self._channel and self._channel.is_open and self._consumer_tag:
                self._channel.basic_cancel(self._consumer_tag)
                self._channel.stop_consuming()
            
            self._running = False
            logger.info(f"Stopped consuming messages from topic {self.binding_pattern}")
        except Exception as e:
            logger.error(f"Error stopping consumer: {e}")
    
    def close(self):
        """Release resources"""
        self.stop_consuming()
        
        # Clean up any thread consumers
        thread_consumers = getattr(self, '_thread_consumers', [])
        for consumer in thread_consumers:
            try:
                consumer.stop_consuming()
            except:
                pass
        
        if self._owns_connection:
            self.connection_manager.release_all_connections()