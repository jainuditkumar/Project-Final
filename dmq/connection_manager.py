#!/usr/bin/env python3
import pika
import random
import time
import logging
import threading
import requests
import docker
from typing import Dict, List, Optional, Tuple, Any, Callable

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from .docker_utils import client

docker_client = client if client else None

if docker_client:
    logger.info("Docker client initialized successfully.")
else:
    logger.error("Failed to initialize Docker client. Docker features will be disabled.")

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
    
    def __init__(self, auto_discover_cluster=False, api_port=15672, auto_discover_docker=True):
        """Initialize the broker connection manager
        
        Args:
            auto_discover_cluster: If True, automatically discover and register all nodes in a cluster
            api_port: Port for the RabbitMQ management API (typically 15672)
            auto_discover_docker: If True, automatically discover and register RabbitMQ nodes from Docker
        """
        self.load_balancer = ConnectionLoadBalancer()
        self.health_check_interval = 60  # seconds
        self.last_health_check = 0
        self.primary_broker_id = None  # Primary broker to use if specified
        self.auto_discover_cluster = auto_discover_cluster
        self.auto_discover_docker = auto_discover_docker
        self.api_port = api_port
        self.cluster_check_interval = 300  # Check for new nodes every 5 minutes
        self.docker_check_interval = 60  # Check for new Docker containers every minute
        self.last_cluster_check = 0
        self.last_docker_check = 0
        self.cluster_nodes = set()  # Track known cluster nodes
        self.docker_containers = set()  # Track known Docker container IDs
        
        # If auto-discovery of Docker containers is enabled, do an initial discovery
        if self.auto_discover_docker and docker_client:
            self.discover_docker_rabbitmq_nodes()
    
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
                
                # If auto-discovery is enabled, try to discover other nodes in the cluster
                if self.auto_discover_cluster:
                    self.discover_cluster_nodes(host, username, password)
                
            return True
        else:
            logger.error(f"Failed to register broker {broker_id} with connection manager")
            return False
    
    def discover_cluster_nodes(self, seed_host: str, username: str = "guest", password: str = "guest") -> bool:
        """Discover and connect to all nodes in a RabbitMQ cluster using the management API
        
        Args:
            seed_host: Hostname or IP of a known cluster member
            username: Username for authentication to the management API
            password: Password for authentication to the management API
        
        Returns:
            bool: True if discovery was successful
        """
        try:
            logger.info(f"Attempting to discover RabbitMQ cluster nodes from seed host {seed_host}")
            url = f"http://{seed_host}:{self.api_port}/api/nodes"
            response = requests.get(url, auth=(username, password))
            
            logger.debug(f"API response: recieved {response.status_code} from {url}")
            if response.status_code != 200:
                logger.error(f"Failed to discover cluster nodes: HTTP {response.status_code}")
                return False
                
            nodes_data = response.json()
            discovered = False
            
            for i, node in enumerate(nodes_data):
                node_name = node.get('name')
                if not node_name:
                    continue
                    
                # Extract hostname from node name (node names are like 'rabbit@hostname')
                if '@' in node_name:
                    hostname = node_name.split('@')[1]
                else:
                    hostname = node_name
                    
                # Skip already registered nodes
                if hostname in self.cluster_nodes:
                    continue
                    
                # Register this node with a new broker_id
                broker_id = 1000 + len(self.cluster_nodes)  # Start from 1000 for auto-discovered nodes
                
                logger.info(f"Discovered cluster node {hostname}, registering as broker {broker_id}")
                
                # Try standard AMQP port, user can override this if needed
                amqp_port = 5672
                
                if self.register_broker(
                    broker_id=broker_id,
                    host=hostname,
                    port=amqp_port,
                    username=username,
                    password=password
                ):
                    self.cluster_nodes.add(hostname)
                    discovered = True
            
            self.last_cluster_check = time.time()
            return discovered
            
        except Exception as e:
            logger.error(f"Error discovering cluster nodes: {e}")
            return False
            
    def check_for_new_cluster_nodes(self):
        """Periodically check for new nodes in the cluster"""
        current_time = time.time()
        if not self.auto_discover_cluster or current_time - self.last_cluster_check < self.cluster_check_interval:
            return
            
        # Find a working connection to use for discovery
        for conn in self.load_balancer.connections:
            if conn.is_healthy():
                self.discover_cluster_nodes(conn.host, conn.username, conn.password)
                break
    
    def register_cluster(self, seed_host: str, username: str = "guest", password: str = "guest") -> bool:
        """Register an entire RabbitMQ cluster with a single seed node
        
        Args:
            seed_host: Hostname or IP of any cluster member
            username: Username for authentication
            password: Password for authentication
        
        Returns:
            bool: True if at least one node was successfully registered
        """
        # First register the seed host
        initial_broker_id = 1
        success = self.register_broker(
            broker_id=initial_broker_id,
            host=seed_host,
            port=5672,  # Default AMQP port
            username=username,
            password=password
        )
        
        if not success:
            return False
            
        # If automatic discovery is enabled, it will have already run
        # Otherwise, explicitly discover the rest of the cluster
        if not self.auto_discover_cluster:
            return self.discover_cluster_nodes(seed_host, username, password)
        
        return True

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
        
        # Check for new cluster nodes if auto-discovery is enabled
        if self.auto_discover_cluster:
            self.check_for_new_cluster_nodes()
            
        # Check for Docker containers if auto-discovery is enabled
        if self.auto_discover_docker:
            self.check_for_docker_containers()
        
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
    
    def discover_docker_rabbitmq_nodes(self) -> bool:
        """Discover and register RabbitMQ containers running in Docker
        
        Returns:
            bool: True if any new RabbitMQ containers were discovered and registered
        """
        if not docker_client:
            logger.warning("Docker client is not available. Cannot discover Docker containers.")
            return False
            
        try:
            logger.info("Discovering RabbitMQ containers in Docker...")
            # Look for RabbitMQ containers
            containers = docker_client.containers.list(
                filters={"status": "running", "ancestor": "rabbitmq"}
            )
            
            # Also look for our DMQ-specific RabbitMQ containers
            dmq_containers = docker_client.containers.list(
                filters={"status": "running", "name": "dmq-rabbitmq"}
            )
            
            # Combine the results, removing duplicates
            all_containers = list(set(containers + dmq_containers))
            
            if not all_containers:
                logger.info("No running RabbitMQ containers found in Docker")
                return False
                
            logger.info(f"Found {len(all_containers)} potential RabbitMQ containers")
            
            discovered = False
            for container in all_containers:
                # Skip if we've already processed this container
                if container.id in self.docker_containers:
                    continue
                    
                # Get container details
                container.reload()  # Ensure we have the latest info
                
                # Extract the container name without leading slash
                container_name = container.name
                if container_name.startswith('/'):
                    container_name = container_name[1:]
                
                # Try to extract node ID from name for DMQ containers (format: dmq-rabbitmq-X)
                node_id = None
                if container_name.startswith('dmq-rabbitmq-'):
                    try:
                        node_id = int(container_name.split('-')[-1])
                    except (ValueError, IndexError):
                        # If we can't parse the ID, generate one
                        node_id = 2000 + len(self.docker_containers)
                else:
                    # For non-DMQ containers, generate an ID
                    node_id = 2000 + len(self.docker_containers)
                
                # Find the host and port mapping for AMQP
                host = 'localhost'  # Default to localhost for Docker port mappings
                port = None
                
                # Look for port mappings
                port_mappings = container.ports.get('5672/tcp')
                if port_mappings and len(port_mappings) > 0:
                    # Use the first host port mapping
                    port = int(port_mappings[0]['HostPort'])
                else:
                    # If no port mapping is found, check if it's in our network
                    if 'dmq-rabbitmq-network' in container.attrs.get('NetworkSettings', {}).get('Networks', {}):
                        # If in our network, use its IP directly with standard port
                        host = container.attrs['NetworkSettings']['Networks']['dmq-rabbitmq-network'].get('IPAddress')
                        port = 5672  # Standard AMQP port
                    else:
                        logger.warning(f"Container {container_name} has no accessible AMQP port mapping")
                        continue
                
                if not port:
                    logger.warning(f"Could not determine AMQP port for container {container_name}")
                    continue
                
                # Register this broker
                logger.info(f"Registering Docker container {container_name} as broker {node_id} at {host}:{port}")
                
                if self.register_broker(
                    broker_id=node_id,
                    host=host,
                    port=port,
                    username="guest",  # Default credentials, can be customized
                    password="guest"
                ):
                    self.docker_containers.add(container.id)
                    discovered = True
                    logger.info(f"Successfully registered Docker container {container_name}")
                else:
                    logger.warning(f"Failed to register Docker container {container_name}")
            
            self.last_docker_check = time.time()
            return discovered
            
        except Exception as e:
            logger.error(f"Error discovering Docker RabbitMQ containers: {e}")
            return False
    
    def check_for_docker_containers(self):
        """Periodically check for new or removed Docker containers"""
        current_time = time.time()
        if not self.auto_discover_docker or not docker_client or current_time - self.last_docker_check < self.docker_check_interval:
            return
        
        try:
            # Check for new containers
            self.discover_docker_rabbitmq_nodes()
            
            # Check if any previously discovered containers are no longer running
            if not self.docker_containers:
                return
                
            containers_to_remove = set()
            for container_id in self.docker_containers:
                try:
                    container = docker_client.containers.get(container_id)
                    if container.status != "running":
                        containers_to_remove.add(container_id)
                except docker.errors.NotFound:
                    # Container no longer exists
                    containers_to_remove.add(container_id)
            
            # Remove connections for containers that are no longer running
            for container_id in containers_to_remove:
                # Find the broker ID for this container
                for conn in self.load_balancer.connections:
                    # Note: This is a simplistic approach - in a full implementation, 
                    # you would want to store a mapping of container IDs to broker IDs
                    if 2000 <= conn.broker_id < 3000:  # ID range for Docker containers
                        self.unregister_broker(conn.broker_id)
                        logger.info(f"Unregistered broker {conn.broker_id} for stopped/removed container")
                
                self.docker_containers.remove(container_id)
                logger.info(f"Removed tracking for Docker container {container_id}")
                
        except Exception as e:
            logger.error(f"Error checking Docker containers: {e}")
