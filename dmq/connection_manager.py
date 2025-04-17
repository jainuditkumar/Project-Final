#!/usr/bin/env python3
import pika
import random
import time
import logging
import threading
from typing import Dict, List, Optional, Tuple, Any, Callable

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BrokerConnection:
    """Represents a single connection to a RabbitMQ broker"""
    
    def __init__(self, broker_id: int, host: str, port: int, 
                 virtual_host: str = "/", 
                 username: str = "guest", 
                 password: str = "guest"):
        """Initialize a connection to a RabbitMQ broker
        
        Args:
            broker_id: Unique identifier for the broker
            host: Hostname or IP address of the RabbitMQ broker
            port: AMQP port of the RabbitMQ broker
            virtual_host: Virtual host to connect to
            username: Username for authentication
            password: Password for authentication
        """
        self.broker_id = broker_id
        self.host = host
        self.port = port
        self.virtual_host = virtual_host
        self.username = username
        self.password = password
        
        self.connection = None
        self.channel = None
        self.is_connected = False
        self.last_activity_timestamp = 0
        self.failure_count = 0
        self.max_failures = 3  # After this many errors, mark connection as unhealthy
        
    def connect(self) -> bool:
        """Establish a connection to the RabbitMQ broker"""
        logger.debug(f"Attempting to connect to broker {self.broker_id} at {self.host}:{self.port}")
        try:
            credentials = pika.PlainCredentials(self.username, self.password)
            parameters = pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                virtual_host=self.virtual_host,
                credentials=credentials,
                connection_attempts=3,
                retry_delay=2,
                socket_timeout=5,
                heartbeat=60  # Heartbeat to detect dead connections
            )
            
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            self.is_connected = True
            self.last_activity_timestamp = time.time()
            self.failure_count = 0
            logger.info(f"Successfully connected to RabbitMQ broker {self.broker_id} at {self.host}:{self.port}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to broker {self.broker_id} at {self.host}:{self.port}: {e}")
            self.is_connected = False
            self.failure_count += 1
            return False
    
    def disconnect(self) -> bool:
        """Disconnect from the RabbitMQ broker"""
        logger.debug(f"Attempting to disconnect from broker {self.broker_id}")
        try:
            if self.connection and self.connection.is_open:
                self.connection.close()
                logger.info(f"Disconnected from RabbitMQ broker {self.broker_id}")
            self.connection = None
            self.channel = None
            self.is_connected = False
            return True
        except Exception as e:
            logger.error(f"Error disconnecting from broker {self.broker_id}: {e}")
            self.is_connected = False
            return False
    
    def record_activity(self):
        """Record recent activity on this connection"""
        self.last_activity_timestamp = time.time()
        logger.debug(f"Recorded activity for broker {self.broker_id} at {self.last_activity_timestamp}")
    
    def is_healthy(self) -> bool:
        """Check if the broker connection is healthy"""
        logger.debug(f"Checking health of broker {self.broker_id}")
        if not self.is_connected:
            logger.warning(f"Broker {self.broker_id} is not connected")
            return False
        
        if self.failure_count >= self.max_failures:
            logger.warning(f"Broker {self.broker_id} has exceeded max failure count")
            return False
            
        try:
            # Check connection status
            if not self.connection or not self.connection.is_open:
                logger.warning(f"Broker {self.broker_id} connection is not open")
                return False
                
            return True
        except Exception as e:
            logger.error(f"Error checking health of broker {self.broker_id}: {e}")
            return False
    
    def get_channel(self):
        """Get the channel for this broker connection"""
        logger.debug(f"Getting channel for broker {self.broker_id}")
        if not self.is_connected or not self.channel:
            logger.info(f"Broker {self.broker_id} is not connected, attempting to reconnect")
            if not self.connect():
                logger.error(f"Failed to reconnect to broker {self.broker_id}")
                return None
        
        self.record_activity()
        return self.channel


class ConnectionLoadBalancer:
    """Load balancing manager for RabbitMQ broker connections"""
    
    def __init__(self):
        """Initialize the load balancer"""
        self.connections: List[BrokerConnection] = []
        self.next_index = 0
        self.lock = threading.Lock()
    
    def register_connection(self, connection: BrokerConnection):
        """Register a connection with the load balancer"""
        with self.lock:
            self.connections.append(connection)
            logger.info(f"Registered connection to broker {connection.broker_id} with load balancer")
    
    def unregister_connection(self, broker_id: int) -> bool:
        """Unregister a connection from the load balancer"""
        with self.lock:
            for i, conn in enumerate(self.connections):
                if conn.broker_id == broker_id:
                    conn.disconnect()
                    self.connections.pop(i)
                    logger.info(f"Unregistered connection to broker {broker_id} from load balancer")
                    return True
            return False
    
    def get_next_connection(self) -> Optional[BrokerConnection]:
        """Get the next connection using round robin selection"""
        with self.lock:
            if not self.connections:
                logger.warning("No connections available in load balancer")
                return None
            
            # Start from the next index and find a healthy connection
            start_index = self.next_index
            while True:
                conn = self.connections[self.next_index]
                
                # Move to the next connection for the next call
                self.next_index = (self.next_index + 1) % len(self.connections)
                
                # If the connection is healthy, return it
                if conn.is_healthy():
                    conn.record_activity()
                    return conn
                
                # If we've tried all connections and none are healthy, try to reconnect the next one
                if self.next_index == start_index:
                    logger.warning("No healthy connections found, attempting to reconnect")
                    if conn.connect():
                        conn.record_activity()
                        return conn
                    else:
                        return None
    
    def get_connection_by_broker_id(self, broker_id: int) -> Optional[BrokerConnection]:
        """Get a specific connection by broker ID"""
        with self.lock:
            for conn in self.connections:
                if conn.broker_id == broker_id:
                    if not conn.is_healthy():
                        conn.connect()
                    conn.record_activity()
                    return conn
            return None
    
    def get_least_active_connection(self) -> Optional[BrokerConnection]:
        """Get the least recently active connection"""
        with self.lock:
            if not self.connections:
                return None
                
            # Sort connections by last activity time
            healthy_connections = [c for c in self.connections if c.is_healthy()]
            if not healthy_connections:
                # Try to reconnect a random connection
                if self.connections:
                    conn = random.choice(self.connections)
                    if conn.connect():
                        conn.record_activity()
                        return conn
                return None
                
            # Get the least recently active connection
            conn = min(healthy_connections, key=lambda c: c.last_activity_timestamp)
            conn.record_activity()
            return conn
    
    def perform_health_check(self):
        """Check the health of all connections and try to reconnect unhealthy ones"""
        with self.lock:
            for conn in self.connections:
                if not conn.is_healthy():
                    logger.info(f"Attempting to reconnect to broker {conn.broker_id}")
                    conn.connect()


class BrokerConnectionManager:
    """Manages connections to a RabbitMQ broker cluster with load balancing"""
    
    def __init__(self):
        """Initialize the broker connection manager"""
        self.load_balancer = ConnectionLoadBalancer()
        self.health_check_interval = 60  # seconds
        self.last_health_check = 0
        self.primary_broker_id = None  # Primary broker to use if specified
    
    def register_broker(self, broker_id: int, host: str, port: int, 
                 virtual_host: str = "/", 
                 username: str = "guest", 
                 password: str = "guest") -> bool:
        """Register a RabbitMQ broker with the connection manager
        
        Args:
            broker_id: Unique identifier for the broker
            host: Hostname or IP address of the RabbitMQ broker
            port: AMQP port of the RabbitMQ broker
            virtual_host: Virtual host to connect to
            username: Username for authentication
            password: Password for authentication
        """
        conn = BrokerConnection(
            broker_id=broker_id,
            host=host,
            port=port,
            virtual_host=virtual_host,
            username=username,
            password=password
        )
        
        if conn.connect():
            self.load_balancer.register_connection(conn)
            
            # If this is the first broker, set it as the primary
            if self.primary_broker_id is None:
                self.primary_broker_id = broker_id
                
            return True
        else:
            logger.error(f"Failed to register broker {broker_id} with connection manager")
            return False
    
    def unregister_broker(self, broker_id: int) -> bool:
        """Unregister a RabbitMQ broker from the connection manager"""
        result = self.load_balancer.unregister_connection(broker_id)
        
        # If we removed the primary broker, set a new primary if possible
        if result and broker_id == self.primary_broker_id:
            if self.load_balancer.connections:
                self.primary_broker_id = self.load_balancer.connections[0].broker_id
            else:
                self.primary_broker_id = None
                
        return result
    
    def acquire_connection(self, broker_id: Optional[int] = None, strategy: str = "round_robin") -> Optional[BrokerConnection]:
        """Acquire a connection to a RabbitMQ broker
        
        Args:
            broker_id: Optional specific broker ID to connect to
            strategy: Load balancing strategy to use if no broker_id is specified:
                      "round_robin" - Get next connection in round robin order
                      "least_active" - Get least recently active connection
        """
        # Check if we need to run a health check
        current_time = time.time()
        if current_time - self.last_health_check > self.health_check_interval:
            self.load_balancer.perform_health_check()
            self.last_health_check = current_time
        
        # If a specific broker is requested, try to get that one
        if broker_id is not None:
            return self.load_balancer.get_connection_by_broker_id(broker_id)
        
        # Otherwise use the specified load balancing strategy
        if strategy == "least_active":
            return self.load_balancer.get_least_active_connection()
        else:  # Default to round robin
            return self.load_balancer.get_next_connection()
    
    def acquire_channel(self, broker_id: Optional[int] = None, strategy: str = "round_robin"):
        """Acquire a channel to a RabbitMQ broker
        
        This is a convenience method that gets a connection and returns its channel.
        
        Args:
            broker_id: Optional specific broker ID to connect to
            strategy: Load balancing strategy to use if no broker_id is specified
        """
        conn = self.acquire_connection(broker_id, strategy)
        if conn:
            return conn.get_channel()
        return None
    
    def release_all_connections(self):
        """Release all connections managed by this connection manager"""
        for conn in self.load_balancer.connections:
            conn.disconnect()
        logger.info("Released all broker connections")
    
    def close(self):
        """Close all connections managed by this connection manager"""
        self.release_all_connections()
        logger.info("Closed all connections in BrokerConnectionManager")
