#!/usr/bin/env python3
"""
RabbitMQ node management for the Distributed Messaging Queue (DMQ) system.
"""
import time
import socket
import logging
import pika
import requests
from requests.exceptions import RequestException
from typing import List, Dict, Optional
import docker

from .docker_utils import client, get_ip_for_node, find_available_port
from .exceptions import RabbitMQNodeError

# Configure logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s]  %(message)s')
logger = logging.getLogger(__name__)

def is_port_available(port):
    """Check if a given port is available"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('', port))
            return True
    except:
        return False

class RabbitMQNode:
    """Represents a single RabbitMQ node in the Docker cluster"""
    
    def __init__(
        self,
        node_id: int,
        simulation_mode: bool = False
    ):
        self.node_id = node_id
        self.node_name = f"rabbitmq-node-{node_id}"  # Descriptive name for the node
        self.container_name = f"dmq-rabbitmq-{node_id}"
        self.ip_address = get_ip_for_node(node_id)
        # Use IP address as the hostname for proper RabbitMQ node identification
        self.hostname = self.ip_address
        
        # Find available ports dynamically
        self.amqp_port = find_available_port(5672 + (node_id - 1) * 10)
        self.management_port = find_available_port(15672 + (node_id - 1) * 10)
        
        self.docker_image = "rabbitmq:3.12-management"
        self.is_active = False
        self.is_primary = False
        self.simulation_mode = simulation_mode
        self.container = None
        self.connection = None
        self.channel = None
        self.erlang_cookie = "DMQRABBITMQCLUSTERCOOKIE"  # Common Erlang cookie for clustering
        logger.info(f"Initialized node {node_id} with AMQP port {self.amqp_port}, Management port {self.management_port}, hostname {self.hostname}")

    def start(self) -> bool:
        """Start the RabbitMQ node container"""
        if self.simulation_mode:
            self.is_active = True
            logger.info(f"[SIMULATION] Started RabbitMQ node {self.node_id}")
            return True
        
        try:
            # Check if container with this name exists
            try:
                existing = client.containers.get(self.container_name)
                # Container exists - remove it if not running
                if existing.status != "running":
                    logger.info(f"Removing existing stopped container: {self.container_name}")
                    existing.remove(force=True)
                else:
                    # Container is already running
                    self.container = existing
                    self.is_active = True
                    
                    # Update IP address from container's network settings
                    if "dmq-rabbitmq-network" in self.container.attrs['NetworkSettings']['Networks']:
                        self.ip_address = self.container.attrs['NetworkSettings']['Networks']["dmq-rabbitmq-network"]['IPAddress']
                        # Update hostname to match IP address
                        self.hostname = self.ip_address
                        logger.info(f"Container {self.container_name} is already running at {self.ip_address}")
                    
                    return True
            except docker.errors.NotFound:
                # Container doesn't exist, will create a new one
                pass
            
            # Basic RabbitMQ configuration without Consul
            environment = {
                "RABBITMQ_ERLANG_COOKIE": self.erlang_cookie,
                "RABBITMQ_DEFAULT_USER": "guest",
                "RABBITMQ_DEFAULT_PASS": "guest",
                "RABBITMQ_NODENAME": f"rabbit{self.node_id}@{self.hostname}",
                "RABBITMQ_USE_LONGNAME": "true",
                "RABBITMQ_CLUSTER_PARTITION_HANDLING": "pause_minority",
            }
            
            ports = {
                '5672/tcp': self.amqp_port,
                '15672/tcp': self.management_port
            }
            
            # Get the network object
            networks = client.networks.list(names=["dmq-rabbitmq-network"])
            if networks:
                network = networks[0]
            else:
                logger.error(f"Network dmq-rabbitmq-network not found!")
                return False
            
            # Create host config with port bindings
            host_config = client.api.create_host_config(
                port_bindings=ports,
                restart_policy={"Name": "unless-stopped"}
            )
            
            # Create networking configuration with fixed IP
            networking_config = docker.types.NetworkingConfig({
                "dmq-rabbitmq-network": docker.types.EndpointConfig(
                    ipv4_address=self.ip_address, version="1.41"
                )
            })
            
            # Create the container with fixed IP using networking_config
            container = client.api.create_container(
                image=self.docker_image,
                name=self.container_name,
                hostname=self.hostname,
                environment=environment,
                host_config=host_config,
                networking_config=networking_config
            )
            
            # Start the container
            client.api.start(container.get('Id'))
            
            # Get the container object
            self.container = client.containers.get(self.container_name)
            
            logger.info(f"Started RabbitMQ node {self.node_id} on ports: AMQP={self.amqp_port}, Management={self.management_port}")
            logger.info(f"Container IP fixed to: {self.ip_address}")
            
            # Wait for a moment to let RabbitMQ start
            time.sleep(5)
            
            # Update container info
            self.container.reload()
            
            # Verify the IP address from container's network settings
            if "dmq-rabbitmq-network" in self.container.attrs['NetworkSettings']['Networks']:
                actual_ip = self.container.attrs['NetworkSettings']['Networks']["dmq-rabbitmq-network"]['IPAddress']
                if actual_ip != self.ip_address:
                    logger.warning(f"Container IP ({actual_ip}) doesn't match expected IP ({self.ip_address})")
                    # Update the IP and hostname if they don't match what we expected
                    self.ip_address = actual_ip
                    self.hostname = actual_ip
                
                logger.info(f"Container running at IP: {self.ip_address}")
            
            self.is_active = True
            return True
            
        except Exception as e:
            logger.error(f"Failed to start RabbitMQ node {self.node_id}: {e}")
            return False

    def stop(self) -> bool:
        """Stop the RabbitMQ node container"""
        if self.simulation_mode:
            self.is_active = False
            logger.info(f"[SIMULATION] Stopped RabbitMQ node {self.node_id}")
            return True
            
        try:
            if self.container:
                self.container.stop(timeout=10)
                logger.info(f"Stopped RabbitMQ node {self.node_id}")
                self.is_active = False
                return True
            else:
                logger.warning(f"No container found for node {self.node_id}")
                self.is_active = False
                return False
        except Exception as e:
            logger.error(f"Failed to stop RabbitMQ node {self.node_id}: {e}")
            return False
    
    def remove(self) -> bool:
        """Remove the RabbitMQ node container"""
        if self.simulation_mode:
            self.is_active = False
            logger.info(f"[SIMULATION] Removed RabbitMQ node {self.node_id}")
            return True
            
        try:
            # First stop if running
            if self.is_active:
                self.stop()
            
            # Then remove
            if self.container:
                self.container.remove(force=True)
                logger.info(f"Removed RabbitMQ node {self.node_id}")
                self.container = None
                return True
            else:
                logger.warning(f"No container found for node {self.node_id}")
                return False
        except Exception as e:
            logger.error(f"Failed to remove RabbitMQ node {self.node_id}: {e}")
            return False
    
    def check_status(self) -> bool:
        """Check if the RabbitMQ node is running"""
        if self.simulation_mode:
            return self.is_active
            
        try:
            if self.container:
                self.container.reload()
                self.is_active = self.container.status == "running"
                return self.is_active
            else:
                try:
                    # Try to get the container
                    self.container = client.containers.get(self.container_name)
                    self.container.reload()
                    self.is_active = self.container.status == "running"
                    return self.is_active
                except docker.errors.NotFound:
                    self.is_active = False
                    return False
        except Exception as e:
            logger.error(f"Error checking status for node {self.node_id}: {e}")
            self.is_active = False
            return False
    
    def restart(self) -> bool:
        """Restart the RabbitMQ node container with improved error handling"""
        if self.simulation_mode:
            logger.info(f"[SIMULATION] Restarted RabbitMQ node {self.node_id}")
            return True
            
        try:
            if not self.container:
                logger.warning(f"No container found for node {self.node_id}")
                return False
            
            # Perform restart with timeout
            logger.info(f"Restarting RabbitMQ container for node {self.node_id}...")
            restart_result = self.container.restart(timeout=20)
            
            # Wait for container to fully restart
            for i in range(10):  # Poll status for up to 10 seconds
                self.container.reload()
                if self.container.status == "running":
                    break
                time.sleep(1)
            
            if self.container.status != "running":
                logger.error(f"Container failed to reach running state after restart: {self.container.status}")
                return False
            
            logger.info(f"Container for node {self.node_id} restarted, waiting for RabbitMQ to initialize...")
            self.is_active = True
            
            # Give RabbitMQ time to initialize
            time.sleep(5)
            
            return True
            
        except docker.errors.APIError as api_err:
            logger.error(f"Docker API error during restart of node {self.node_id}: {api_err}")
            return False
        except Exception as e:
            logger.error(f"Failed to restart RabbitMQ node {self.node_id}: {e}")
            return False
    
    def join_cluster(self, master_node) -> bool:
        """Join this node to the RabbitMQ cluster with the master node"""
        if self.simulation_mode:
            logger.info(f"[SIMULATION] Node {self.node_id} joined cluster with master {master_node.node_id}")
            return True
            
        try:
            # Make sure both nodes are running with timeout
            max_retries = 3
            for attempt in range(max_retries):
                self_status = self.check_status()
                master_status = master_node.check_status()
                
                if self_status and master_status:
                    break
                    
                if attempt == max_retries - 1:
                    logger.error(f"Both nodes need to be running to join a cluster. Self: {self_status}, Master: {master_status}")
                    return False
                    
                logger.info(f"Waiting for nodes to be ready (attempt {attempt+1}/{max_retries})...")
                time.sleep(3)
                
            # Execute join cluster command inside the container
            cmd_stop_app = f"rabbitmqctl stop_app"
            cmd_reset = f"rabbitmqctl reset"
            cmd_join = f"rabbitmqctl join_cluster rabbit1@{master_node.hostname}"
            cmd_start_app = f"rabbitmqctl start_app"
            
            # Stop app
            logger.info(f"Stopping RabbitMQ application on node {self.node_id}")
            exec_result = self.container.exec_run(cmd_stop_app)
            if exec_result.exit_code != 0:
                logger.error(f"Failed to stop RabbitMQ app: {exec_result.output.decode()}")
                return False
            logger.info(f"Stop app result: {exec_result.output.decode()}")
            
            # Reset
            logger.info(f"Resetting node {self.node_id}")
            exec_result = self.container.exec_run(cmd_reset)
            if exec_result.exit_code != 0:
                logger.error(f"Failed to reset node: {exec_result.output.decode()}")
                # Try to start the app again before returning
                self.container.exec_run(cmd_start_app)
                return False
            logger.info(f"Reset result: {exec_result.output.decode()}")
            
            # Join cluster
            logger.info(f"Joining node {self.node_id} to cluster with master {master_node.node_id} (IP: {master_node.hostname})")
            exec_result = self.container.exec_run(cmd_join)
            join_output = exec_result.output.decode()
            logger.info(f"Join result: {join_output}")
            
            # Check join result for specific error patterns
            if exec_result.exit_code != 0:
                if "nodedown" in join_output.lower():
                    logger.error(f"Master node appears to be down: {join_output}")
                elif "already_member" in join_output.lower():
                    logger.warning(f"Node is already a member of the cluster, proceeding with start_app")
                    # This is actually OK, proceed with starting the app
                else:
                    logger.error(f"Failed to join cluster: {join_output}")
                    # Try to start the app anyway before returning
                    self.container.exec_run(cmd_start_app)
                    return False
            
            # Start app
            logger.info(f"Starting RabbitMQ application on node {self.node_id}")
            exec_result = self.container.exec_run(cmd_start_app)
            if exec_result.exit_code != 0:
                logger.error(f"Failed to start RabbitMQ app: {exec_result.output.decode()}")
                return False
            logger.info(f"Start app result: {exec_result.output.decode()}")
            
            # Check if join was successful
            success = False
            for attempt in range(3):
                try:
                    cmd_cluster_status = f"rabbitmqctl cluster_status"
                    status_result = self.container.exec_run(cmd_cluster_status)
                    status_output = status_result.output.decode()
                    
                    # Check if master node's hostname (IP) appears in the cluster status
                    if master_node.hostname in status_output:
                        logger.info(f"Node {self.node_id} successfully joined the cluster")
                        success = True
                        break
                    
                    if attempt < 2:  # Only log warning for non-final attempts
                        logger.warning(f"Cluster status check {attempt+1}/3 failed. Retrying...")
                        time.sleep(5)
                except Exception as check_err:
                    logger.error(f"Error checking cluster status (attempt {attempt+1}/3): {check_err}")
                    if attempt < 2:
                        time.sleep(5)
            
            if not success:
                logger.error("Failed to verify successful cluster join after multiple attempts")
                return False
                
            return True
                
        except docker.errors.APIError as api_err:
            logger.error(f"Docker API error during cluster join: {api_err}")
            return False
        except Exception as e:
            logger.error(f"Error joining cluster: {e}")
            return False
    
    def connect_pika(self) -> bool:
        """Connect to the RabbitMQ node using Pika"""
        if self.simulation_mode or not self.is_active:
            return False
            
        try:
            # Get the host to connect to (use localhost for port mappings)
            host = 'localhost'
            
            credentials = pika.PlainCredentials("guest", "guest")
            parameters = pika.ConnectionParameters(
                host=host,
                port=self.amqp_port,  # Use mapped port
                virtual_host="/",
                credentials=credentials,
                connection_attempts=3,
                retry_delay=2
            )
            
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            logger.info(f"Connected to RabbitMQ node {self.node_id} with Pika")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to node {self.node_id} with Pika: {e}")
            self.connection = None
            self.channel = None
            return False
    
    def disconnect_pika(self) -> bool:
        """Disconnect the Pika connection"""
        try:
            if self.connection and self.connection.is_open:
                self.connection.close()
                self.connection = None
                self.channel = None
                logger.info(f"Disconnected Pika from node {self.node_id}")
            return True
        except Exception as e:
            logger.error(f"Error disconnecting Pika from node {self.node_id}: {e}")
            return False
    
    def create_queue(self, queue_name: str, durable: bool = True, quorum: bool = False) -> bool:
        """Create a queue on the RabbitMQ node
        
        Args:
            queue_name: Name of the queue to create
            durable: Whether the queue should survive broker restarts
            quorum: If True, creates a quorum queue instead of a classic queue
        """
        if self.simulation_mode:
            queue_type = "quorum" if quorum else "classic"
            logger.info(f"[SIMULATION] Created {queue_type} queue '{queue_name}' on node {self.node_id}")
            return True
            
        try:
            # Connect with Pika if not already connected
            if not self.connection or not self.channel:
                if not self.connect_pika():
                    return False
            
            # Create the queue
            arguments = {}
            if quorum:
                arguments['x-queue-type'] = 'quorum'
                
            self.channel.queue_declare(
                queue=queue_name, 
                durable=durable,
                arguments=arguments
            )
            
            queue_type = "quorum" if quorum else "classic"
            logger.info(f"Created {queue_type} queue '{queue_name}' on node {self.node_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to create queue on node {self.node_id}: {e}")
            return False
    
    def delete_queue(self, queue_name: str) -> bool:
        """Delete a queue from the RabbitMQ node"""
        if self.simulation_mode:
            logger.info(f"[SIMULATION] Deleted queue '{queue_name}' from node {self.node_id}")
            return True
            
        try:
            # Connect with Pika if not already connected
            if not self.connection or not self.channel:
                if not self.connect_pika():
                    return False
            
            # Delete the queue
            self.channel.queue_delete(queue=queue_name)
            logger.info(f"Deleted queue '{queue_name}' from node {self.node_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete queue on node {self.node_id}: {e}")
            return False
    
    def list_queues(self) -> List[Dict]:
        """List all queues on the RabbitMQ node"""
        if self.simulation_mode:
            # Return simulated queues in simulation mode
            return [
                {"name": f"queue-{i}", "messages": i*10, "consumers": i}
                for i in range(1, 3)
            ]
            
        try:
            # For real queues, use the HTTP Management API
            url = f"http://localhost:{self.management_port}/api/queues"
            response = requests.get(url, auth=('guest', 'guest'), timeout=5)
            
            if response.status_code == 200:
                queues = response.json()
                formatted_queues = []
                
                for q in queues:
                    formatted_queues.append({
                        "name": q.get("name", "N/A"),
                        "messages": q.get("messages", 0),
                        "consumers": q.get("consumers", 0),
                        "vhost": q.get("vhost", "/")
                    })
                
                logger.info(f"Retrieved {len(formatted_queues)} queues from node {self.node_id}")
                return formatted_queues
            else:
                logger.error(f"Failed to list queues, status code: {response.status_code}")
                return []
                
        except RequestException as e:
            logger.error(f"HTTP request error listing queues on node {self.node_id}: {e}")
            return []
        except Exception as e:
            logger.error(f"Error listing queues on node {self.node_id}: {e}")
            return []
    
    def __str__(self) -> str:
        sim_tag = "[SIM] " if self.simulation_mode else ""
        status = "ACTIVE" if self.is_active else "INACTIVE"
        primary = " (PRIMARY)" if self.is_primary else ""
        return f"{sim_tag}Node {self.node_id}: {status}{primary} - {self.node_name} ({self.hostname})"