#!/usr/bin/env python3
import time
import json
import pika
import docker
from typing import Dict, Optional, List, Tuple
import socket
import uuid
import logging
import subprocess
import sys
import os
import ipaddress
import requests
import argparse
from requests.exceptions import RequestException
import asyncio

"""
Distributed Messaging Queue (DMQ) Manager for RabbitMQ
This script manages a cluster of RabbitMQ containers with operations like 
adding/removing nodes, turning nodes off/on, and monitoring the cluster.
"""

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Docker network settings
DOCKER_NETWORK_NAME = "dmq-rabbitmq-network"
DOCKER_SUBNET = "172.25.0.0/16"
BASE_IP = "172.25.0.10"  # Starting IP for RabbitMQ nodes

def get_current_context_socket():
    """Get the Docker socket for the current context."""
    try:
        # Get the current Docker context
        context = subprocess.check_output(['docker', 'context', 'show']).decode().strip()
        logger.info(f"Using Docker context: {context}")
    
        # Get the Docker socket for the current context
        if context == 'default':
            return 'unix:///var/run/docker.sock'  # Default Docker socket
        else:
            # Get the context-specific Docker socket
            context_info = subprocess.check_output(['docker', 'context', 'inspect', context]).decode()
            context_data = json.loads(context_info)
            return context_data[0]['Endpoints']['docker']['Host']
    except Exception as e:
        logger.error(f"Failed to get Docker context socket: {e}")
        raise

# Update Docker client initialization to use the current context socket
client = docker.DockerClient(base_url=get_current_context_socket())

def check_docker_installed():
    """Check if Docker is installed and running"""
    try:
        client.ping()
        return True
    except PermissionError:
        logger.error("Docker permission denied. You need to be in the docker group or run as sudo.")
        return False
    except Exception as e:
        logger.error(f"Docker check failed: {e}")
        return False

def check_docker_permission():
    """Check if user has permissions to use Docker"""
    # Check if the user has access to the Docker socket
    if os.path.exists('/var/run/docker.sock'):
        if os.access('/var/run/docker.sock', os.R_OK | os.W_OK):
            return True
        else:
            # Check if the user is in the docker group
            try:
                # Using subprocess to check group membership
                result = subprocess.run(
                    ["groups"], 
                    capture_output=True,
                    text=True,
                    check=True
                )
                if 'docker' in result.stdout:
                    return True
                else:
                    return False
            except Exception:
                return False
    return False

def get_permission_instructions():
    """Return instructions for fixing Docker permissions"""
    instructions = [
        "\n⚠️ Permission denied when accessing Docker!",
        "To fix this issue, you can either:",
        "1. Run this script with sudo: sudo python dmq.py",
        "   - OR -",
        "2. Add your user to the docker group and restart your session:",
        "   sudo usermod -aG docker $USER",
        "   newgrp docker  # Apply group changes to current session",
        "\nAfter that, you should be able to run the script without sudo."
    ]
    return "\n".join(instructions)

def setup_docker_network():
    """Create a Docker network for RabbitMQ nodes if it doesn't exist"""
    try:
        # Check if network already exists
        networks = client.networks.list(names=[DOCKER_NETWORK_NAME])
        if networks:
            logger.info(f"Docker network '{DOCKER_NETWORK_NAME}' already exists")
            return networks[0]
        
        # Create network with specific subnet
        network = client.networks.create(
            name=DOCKER_NETWORK_NAME,
            driver="bridge",
            ipam=docker.types.IPAMConfig(
                pool_configs=[docker.types.IPAMPool(subnet=DOCKER_SUBNET)]
            )
        )
        logger.info(f"Created Docker network: {DOCKER_NETWORK_NAME}")
        return network
    except PermissionError:
        logger.error("Permission denied when creating Docker network.")
        raise PermissionError(get_permission_instructions())
    except Exception as e:
        logger.error(f"Failed to setup Docker network: {e}")
        raise

def get_ip_for_node(node_id):
    """Generate an IP address for the RabbitMQ node based on its ID"""
    ip = ipaddress.IPv4Address(BASE_IP)
    return str(ip + node_id - 1)

def is_port_available(port):
    """Check if a given port is available"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('', port))
            return True
    except:
        return False

def find_available_port(start_port):
    """Find the next available port starting from given port"""
    port = start_port
    while port < 65535:
        if is_port_available(port):
            return port
        port += 1
    raise RuntimeError("No available ports found")

class RabbitMQNode:
    """Represents a single RabbitMQ node in the Docker cluster"""
    
    def __init__(self, node_id: int, simulation_mode: bool = False):
        self.node_id = node_id
        self.hostname = f"rabbitmq-node-{node_id}"
        self.container_name = f"dmq-rabbitmq-{node_id}"
        self.ip_address = get_ip_for_node(node_id)
        
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
        logger.info(f"Initialized node {node_id} with AMQP port {self.amqp_port}, Management port {self.management_port}")

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
                    logger.info(f"Container {self.container_name} is already running")
                    return True
            except docker.errors.NotFound:
                # Container doesn't exist, will create a new one
                pass
            
            # Environment variables for the container
            environment = {
                "RABBITMQ_ERLANG_COOKIE": self.erlang_cookie,
                "RABBITMQ_DEFAULT_USER": "guest",
                "RABBITMQ_DEFAULT_PASS": "guest"
            }
            
            # Port mappings - note we need to map both the main port and management port
            ports = {
                '5672/tcp': self.amqp_port,  # Main port stays 5672 inside the container
                '15672/tcp': self.management_port  # Management port
            }
            
            # Create and start the container
            self.container = client.containers.run(
                self.docker_image,
                name=self.container_name,
                hostname=self.hostname,
                detach=True,
                environment=environment,
                ports=ports,
                network=DOCKER_NETWORK_NAME,
                auto_remove=False,
                restart_policy={"Name": "unless-stopped"}
            )
            
            logger.info(f"Started RabbitMQ node {self.node_id} on ports: AMQP={self.amqp_port}, Management={self.management_port}")
            
            # Wait for a moment to let RabbitMQ start
            time.sleep(10)
            
            # Update container info
            self.container.reload()
            
            # Set the IP address from container's network settings
            if DOCKER_NETWORK_NAME in self.container.attrs['NetworkSettings']['Networks']:
                self.ip_address = self.container.attrs['NetworkSettings']['Networks'][DOCKER_NETWORK_NAME]['IPAddress']
                logger.info(f"Container IP: {self.ip_address}")
            
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
        """Restart the RabbitMQ node container"""
        if self.simulation_mode:
            logger.info(f"[SIMULATION] Restarted RabbitMQ node {self.node_id}")
            return True
            
        try:
            if self.container:
                self.container.restart(timeout=10)
                logger.info(f"Restarted RabbitMQ node {self.node_id}")
                self.is_active = True
                return True
            else:
                logger.warning(f"No container found for node {self.node_id}")
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
            # Make sure both nodes are running
            if not self.check_status() or not master_node.check_status():
                logger.error("Both nodes need to be running to join a cluster")
                return False
                
            # Execute join cluster command inside the container
            cmd_stop_app = f"rabbitmqctl stop_app"
            cmd_reset = f"rabbitmqctl reset"
            cmd_join = f"rabbitmqctl join_cluster rabbit@{master_node.hostname}"
            cmd_start_app = f"rabbitmqctl start_app"
            
            logger.info(f"Stopping RabbitMQ application on node {self.node_id}")
            exec_result = self.container.exec_run(cmd_stop_app)
            logger.info(f"Stop app result: {exec_result.output.decode()}")
            
            logger.info(f"Resetting node {self.node_id}")
            exec_result = self.container.exec_run(cmd_reset)
            logger.info(f"Reset result: {exec_result.output.decode()}")
            
            logger.info(f"Joining node {self.node_id} to cluster with master {master_node.node_id}")
            exec_result = self.container.exec_run(cmd_join)
            logger.info(f"Join result: {exec_result.output.decode()}")
            
            logger.info(f"Starting RabbitMQ application on node {self.node_id}")
            exec_result = self.container.exec_run(cmd_start_app)
            logger.info(f"Start app result: {exec_result.output.decode()}")
            
            # Check if join was successful
            cmd_cluster_status = f"rabbitmqctl cluster_status"
            status_result = self.container.exec_run(cmd_cluster_status)
            status_output = status_result.output.decode()
            
            # Check if master node's hostname appears in the cluster status
            if master_node.hostname in status_output:
                logger.info(f"Node {self.node_id} successfully joined the cluster")
                return True
            else:
                logger.error(f"Failed to join cluster. Cluster status: {status_output}")
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
        return f"{sim_tag}Node {self.node_id}: {status}{primary} - {self.hostname} ({self.ip_address})"


class DMQCluster:
    """Manages a cluster of RabbitMQ nodes"""
    
    def __init__(self, simulation_mode: bool = False):
        self.nodes: Dict[int, RabbitMQNode] = {}
        self.primary_node_id: Optional[int] = None
        self.simulation_mode = simulation_mode
        
        if not simulation_mode:
            # Setup Docker network for the cluster
            try:
                self.network = setup_docker_network()
            except Exception as e:
                logger.error(f"Failed to setup Docker network: {e}")
                raise
    
    def initialize_cluster(self, num_nodes: int) -> bool:
        """Initialize the RabbitMQ cluster with the given number of nodes"""
        logger.info(f"Initializing cluster with {num_nodes} nodes...")
        
        # Start the first node (primary)
        primary_node = RabbitMQNode(1, simulation_mode=self.simulation_mode)
        if primary_node.start():
            self.nodes[1] = primary_node
            self.primary_node_id = 1
            self.nodes[1].is_primary = True
            logger.info(f"Started primary node (ID: 1)")
        else:
            logger.error("Failed to start primary node")
            return False
        
        # Wait for the primary node to fully initialize
        logger.info("Waiting for primary node to initialize...")
        time.sleep(7)  # RabbitMQ needs time to start
        
        async def start_and_join(i):
            node = RabbitMQNode(i, simulation_mode=self.simulation_mode)
            start_success = await asyncio.get_event_loop().run_in_executor(None, node.start)
            if start_success:
                self.nodes[i] = node
            logger.info(f"Started node {i}")
            # Wait for the node to fully initialize before joining
            await asyncio.sleep(5)
            join_success = await asyncio.get_event_loop().run_in_executor(
                None, node.join_cluster, self.nodes[self.primary_node_id]
            )
            if not join_success:
                logger.error(f"Failed to join node {i} to the cluster")
            else:
                logger.error(f"Failed to start node {i}")

        async def add_nodes_async():
            tasks = [asyncio.create_task(start_and_join(i)) for i in range(2, num_nodes + 1)]
            await asyncio.gather(*tasks)

        asyncio.run(add_nodes_async())
        
        # Connect to all nodes for management operations
        for node_id, node in self.nodes.items():
            if not self.simulation_mode:
                node.connect_pika()
        
        return True
    
    def add_node(self) -> bool:
        """Add a new node to the cluster"""
        if not self.primary_node_id:
            logger.error("Cannot add node: No primary node exists")
            return False
            
        new_id = max(self.nodes.keys()) + 1 if self.nodes else 1
        
        node = RabbitMQNode(new_id, simulation_mode=self.simulation_mode)
        
        if node.start():
            self.nodes[new_id] = node
            logger.info(f"Started node {new_id}")
            
            # Wait for node to initialize
            time.sleep(10)
            
            # If there's a primary node, join the new node to the cluster
            if self.primary_node_id and new_id != self.primary_node_id:
                if not node.join_cluster(self.nodes[self.primary_node_id]):
                    logger.error(f"Failed to join node {new_id} to the cluster")
                    # Don't fail, we still have the node running
            
            # Connect for management operations
            if not self.simulation_mode:
                node.connect_pika()
                
            return True
        else:
            logger.error(f"Failed to start node {new_id}")
            return False
    
    def remove_node(self, node_id: int) -> bool:
        """Remove a node from the cluster"""
        if node_id not in self.nodes:
            logger.warning(f"Node {node_id} does not exist")
            return False
        
        is_primary = self.nodes[node_id].is_primary
        
        # Disconnect Pika if connected
        self.nodes[node_id].disconnect_pika()
        
        # Remove the container
        if self.nodes[node_id].remove():
            logger.info(f"Removed node {node_id} from the cluster")
            
            # If we removed the primary node, elect a new one
            if is_primary and len(self.nodes) > 1:
                del self.nodes[node_id]
                self._elect_new_primary()
            else:
                del self.nodes[node_id]
                if is_primary:
                    self.primary_node_id = None
            
            return True
        else:
            logger.error(f"Failed to remove node {node_id}")
            return False
    
    def turn_off_node(self, node_id: int) -> bool:
        """Temporarily turn off a node"""
        if node_id not in self.nodes:
            logger.warning(f"Node {node_id} does not exist")
            return False
        
        is_primary = self.nodes[node_id].is_primary
        
        # Disconnect Pika if connected
        self.nodes[node_id].disconnect_pika()
        
        if self.nodes[node_id].stop():
            logger.info(f"Turned off node {node_id}")
            
            # If the primary node was turned off, elect a new one
            if is_primary and len(self.nodes) > 1:
                self._elect_new_primary()
            elif is_primary:
                self.primary_node_id = None
            
            return True
        else:
            logger.error(f"Failed to turn off node {node_id}")
            return False
    
    def revive_node(self, node_id: int) -> bool:
        """Revive a temporarily turned off node"""
        if node_id not in self.nodes:
            logger.warning(f"Node {node_id} does not exist")
            return False
        
        # First check if the node is actually stopped
        node = self.nodes[node_id]
        node.check_status()
        
        if node.is_active:
            logger.info(f"Node {node_id} is already active, no need to revive")
            return True
        
        logger.info(f"Attempting to revive node {node_id}...")
        
        if self.simulation_mode:
            # In simulation mode, just mark as active
            node.is_active = True
            logger.info(f"[SIMULATION] Revived node {node_id}")
            return True
        
        # For real nodes, attempt to restart the container
        if node.container:
            try:
                # Use restart instead of start to handle cases where the container exists but is stopped
                node.container.restart(timeout=10)
                logger.info(f"Restarted container for node {node_id}")
                
                # Wait for RabbitMQ to initialize
                logger.info(f"Waiting for RabbitMQ to initialize on node {node_id}...")
                time.sleep(5)  # Reduced wait time
                
                # Update status
                node.check_status()
                
                # If there's a primary node and this isn't it, rejoin the cluster
                if self.primary_node_id and node_id != self.primary_node_id:
                    logger.info(f"Rejoining node {node_id} to the cluster...")
                    if not node.join_cluster(self.nodes[self.primary_node_id]):
                        logger.warning(f"Node {node_id} could not rejoin the cluster, but is running")
                
                # Reconnect for management operations
                node.connect_pika()
                
                logger.info(f"Node {node_id} successfully revived")
                return True
                
            except Exception as e:
                logger.error(f"Error restarting node {node_id}: {e}")
                
                # Fallback to regular start if restart failed
                logger.info(f"Attempting fallback to regular start...")
                return node.start()
        else:
            # If container doesn't exist, use the regular start method
            logger.info(f"No container found for node {node_id}, creating a new one")
            return node.start()
    
    def _elect_new_primary(self) -> bool:
        """Elect a new primary node"""
        active_nodes = [node_id for node_id, node in self.nodes.items() if node.check_status()]
        if not active_nodes:
            logger.warning("No active nodes available to be primary")
            self.primary_node_id = None
            return False
        
        # Choose the node with the lowest ID as the new primary
        new_primary_id = min(active_nodes)
        self.primary_node_id = new_primary_id
        
        # Update is_primary flags
        for node_id, node in self.nodes.items():
            node.is_primary = (node_id == new_primary_id)
        
        logger.info(f"Node {new_primary_id} is now the primary node")
        return True
    
    def display_status(self) -> None:
        """Display the status of all nodes in the cluster"""
        print("\n=== DMQ Cluster Status ===")
        sim_tag = "[SIMULATION MODE]" if self.simulation_mode else ""
        print(f"Total Nodes: {len(self.nodes)} {sim_tag}")
        print(f"Primary Node: {self.primary_node_id if self.primary_node_id else 'None'}")
        
        # Update status of all nodes
        for node_id, node in self.nodes.items():
            node.check_status()
            
        print("\nNode Details:")
        for node_id, node in sorted(self.nodes.items()):
            print(f"  {node}")
            
        # If we have a real cluster, try to display the cluster status from RabbitMQ's perspective
        if not self.simulation_mode and self.primary_node_id and self.nodes[self.primary_node_id].is_active:
            try:
                # Get cluster status using rabbitmqctl
                cmd = f"rabbitmqctl cluster_status"
                result = self.nodes[self.primary_node_id].container.exec_run(cmd)
                status_output = result.output.decode()
                
                print("\nRabbitMQ Cluster Status:")
                # Display a simplified version of the output
                print("  " + status_output.replace("\n", "\n  "))
            except Exception as e:
                print(f"\nError getting cluster status: {e}")
                
        print("========================\n")
    
    def create_queue(self, queue_name: str, quorum: bool = False) -> bool:
        """Create a queue on the primary node
        
        Args:
            queue_name: Name of the queue to create
            quorum: If True, creates a quorum queue instead of a classic queue
        """
        if not self.primary_node_id:
            logger.warning("No primary node available to create queue")
            return False
        
        # In a clustered RabbitMQ, queues are available across all nodes
        # So we only need to create it on one node
        return self.nodes[self.primary_node_id].create_queue(queue_name, durable=True, quorum=quorum)
    
    def delete_queue(self, queue_name: str) -> bool:
        """Delete a queue from the cluster"""
        if not self.primary_node_id:
            logger.warning("No primary node available to delete queue")
            return False
        
        # Delete from the primary node - in a clustered setup, this removes it from all nodes
        return self.nodes[self.primary_node_id].delete_queue(queue_name)
    
    def list_queues(self) -> None:
        """List all queues in the cluster"""
        if not self.primary_node_id:
            print("No primary node available to list queues")
            return
        
        # Get queues from the primary node - in a clustered setup, this shows all queues
        queues = self.nodes[self.primary_node_id].list_queues()
        
        print("\n=== Queues in the Cluster ===")
        sim_tag = "[SIMULATION MODE]" if self.simulation_mode else ""
        print(f"Cluster status: {sim_tag}")
        
        if not queues:
            print("No queues found")
        else:
            for queue in queues:
                print(f"Queue: {queue.get('name', 'N/A')}")
                print(f"  Virtual Host: {queue.get('vhost', '/')}")
                print(f"  Messages: {queue.get('messages', 'N/A')}")
                print(f"  Consumers: {queue.get('consumers', 'N/A')}")
                print("---")

def parse_arguments():
    """Parse command line arguments for DMQ."""
    parser = argparse.ArgumentParser(description='Distributed Messaging Queue (DMQ) Manager for RabbitMQ')
    
    # Global options
    parser.add_argument('--simulation', '-s', action='store_true', 
                        help='Run in simulation mode without Docker')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Enable verbose logging')
    
    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest='command', help='DMQ Commands')
    
    # Start cluster command
    start_parser = subparsers.add_parser('start', help='Start a new DMQ cluster')
    start_parser.add_argument('nodes', type=int, help='Number of nodes to initialize')
    
    # Status command
    subparsers.add_parser('status', help='Display cluster status')
    
    # Add node command
    subparsers.add_parser('add-node', help='Add a new node to the cluster')
    
    # Remove node command
    remove_parser = subparsers.add_parser('remove-node', help='Remove a node from the cluster')
    remove_parser.add_argument('node_id', type=int, help='ID of the node to remove')
    
    # Stop node command
    stop_parser = subparsers.add_parser('stop-node', help='Temporarily stop a node')
    stop_parser.add_argument('node_id', type=int, help='ID of the node to stop')
    
    # Revive node command
    revive_parser = subparsers.add_parser('revive-node', help='Revive a stopped node')
    revive_parser.add_argument('node_id', type=int, help='ID of the node to revive')
    
    # Create queue command
    create_parser = subparsers.add_parser('create-queue', help='Create a new queue')
    create_parser.add_argument('queue_name', help='Name for the new queue')
    create_parser.add_argument('--quorum', '-q', action='store_true', 
                              help='Create a quorum queue instead of classic queue')
    
    # Delete queue command
    delete_parser = subparsers.add_parser('delete-queue', help='Delete a queue')
    delete_parser.add_argument('queue_name', help='Name of the queue to delete')
    
    # List queues command
    subparsers.add_parser('list-queues', help='List all queues in the cluster')
    
    # Interactive mode
    subparsers.add_parser('interactive', help='Start DMQ in interactive mode')
    
    return parser.parse_args()

def main():
    """Main function to run the DMQ manager"""
    # Parse arguments
    args = parse_arguments()
    
    # Set verbose logging if requested
    if hasattr(args, 'verbose') and args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Determine if we should use simulation mode
    simulation_mode = False
    if hasattr(args, 'simulation') and args.simulation:
        simulation_mode = True
        logger.info("Running in simulation mode")
    elif not args.command or args.command == 'interactive':
        # Check Docker in interactive mode or when no command is given
        print("=== Distributed Messaging Queue Manager ===")
        
        # Check if Docker is installed and running
        if not check_docker_installed():
            print("\n⚠️ Error: Docker is not installed or not running!")
            
            # Check if it's a permissions issue
            if os.path.exists('/var/run/docker.sock') and not check_docker_permission():
                print(get_permission_instructions())
                print("\nOr you can run in simulation mode for testing purposes.")
            else:
                print("Please install Docker and start the Docker service:")
                print("  Ubuntu/Debian: sudo apt install docker.io && sudo systemctl start docker")
                print("  MacOS: brew install --cask docker")
                print("  Windows: Download and install from https://www.docker.com/products/docker-desktop/")
                print("\nAlternatively, you can run in simulation mode for testing purposes.")
            
            while True:
                choice = input("\nDo you want to run in simulation mode instead? (y/n): ").lower()
                if choice in ('y', 'yes'):
                    simulation_mode = True
                    break
                elif choice in ('n', 'no'):
                    print("Please resolve Docker access issues and try again. Exiting...")
                    return
                else:
                    print("Please enter 'y' or 'n'.")
        else:
            print("Docker is running and accessible.")
            simulation_mode = False
    else:
        # For command-line operations, check Docker directly
        if not simulation_mode and not check_docker_installed():
            print("\n⚠️ Error: Docker is not installed or not running!")
            print("Use --simulation flag to run in simulation mode.")
            return
    
    # Initialize DMQ cluster
    try:
        dmq = DMQCluster(simulation_mode=simulation_mode)
    except PermissionError:
        print(get_permission_instructions())
        print("Exiting...")
        return
    
    try:
        # Handle commands
        if args.command == 'start':
            print(f"Initializing cluster with {args.nodes} nodes...")
            if not dmq.initialize_cluster(args.nodes):
                print("Failed to initialize cluster. Exiting.")
                return
            print(f"\nSuccessfully initialized cluster with {len(dmq.nodes)} nodes!")
            dmq.display_status()
            
        elif args.command == 'status':
            dmq.display_status()
            
        elif args.command == 'add-node':
            if dmq.add_node():
                print("Node added successfully.")
                dmq.display_status()
            else:
                print("Failed to add node.")
                
        elif args.command == 'remove-node':
            if dmq.remove_node(args.node_id):
                print(f"Node {args.node_id} removed successfully.")
                dmq.display_status()
            else:
                print(f"Failed to remove node {args.node_id}.")
                
        elif args.command == 'stop-node':
            if dmq.turn_off_node(args.node_id):
                print(f"Node {args.node_id} stopped successfully.")
                dmq.display_status()
            else:
                print(f"Failed to stop node {args.node_id}.")
                
        elif args.command == 'revive-node':
            if dmq.revive_node(args.node_id):
                print(f"Node {args.node_id} revived successfully.")
                dmq.display_status()
            else:
                print(f"Failed to revive node {args.node_id}.")
                
        elif args.command == 'create-queue':
            quorum = args.quorum if hasattr(args, 'quorum') else False
            if dmq.create_queue(args.queue_name, quorum=quorum):
                print(f"Queue '{args.queue_name}' created successfully.")
            else:
                print(f"Failed to create queue '{args.queue_name}'.")
                
        elif args.command == 'delete-queue':
            if dmq.delete_queue(args.queue_name):
                print(f"Queue '{args.queue_name}' deleted successfully.")
            else:
                print(f"Failed to delete queue '{args.queue_name}'.")
                
        elif args.command == 'list-queues':
            dmq.list_queues()
                
        elif args.command == 'interactive' or not args.command:
            # Interactive mode
            # If no cluster is initialized, ask the user
            if len(dmq.nodes) == 0:
                # Get number of nodes from user
                while True:
                    try:
                        num_nodes = int(input("Enter the number of RabbitMQ nodes to initialize: "))
                        if num_nodes <= 0:
                            print("Please enter a positive number")
                            continue
                        break
                    except ValueError:
                        print("Please enter a valid number")
                
                # Initialize the cluster
                if not dmq.initialize_cluster(num_nodes):
                    print("Failed to initialize cluster. Exiting.")
                    return
                
                print(f"\nSuccessfully initialized cluster with {len(dmq.nodes)} nodes!")
                dmq.display_status()
            
            # Main command loop
            while True:
                print("\n=== DMQ Management Console ===")
                print("1. Display cluster status")
                print("2. Add a node")
                print("3. Remove a node")
                print("4. Turn off a node temporarily")
                print("5. Revive a node")
                print("6. Create a queue")
                print("7. Delete a queue")
                print("8. List all queues")
                print("0. Exit")
                
                choice = input("\nEnter your choice (0-8): ")
                
                if choice == '0':
                    print("Shutting down DMQ manager...")
                    break
                elif choice == '1':
                    dmq.display_status()
                elif choice == '2':
                    dmq.add_node()
                    dmq.display_status()
                elif choice == '3':
                    dmq.display_status()
                    node_id = int(input("Enter the ID of the node to remove: "))
                    dmq.remove_node(node_id)
                    dmq.display_status()
                elif choice == '4':
                    dmq.display_status()
                    node_id = int(input("Enter the ID of the node to turn off: "))
                    dmq.turn_off_node(node_id)
                    dmq.display_status()
                elif choice == '5':
                    dmq.display_status()
                    node_id = int(input("Enter the ID of the node to revive: "))
                    dmq.revive_node(node_id)
                    dmq.display_status()
                elif choice == '6':
                    queue_name = input("Enter the name for the new queue: ")
                    quorum = input("Is this a quorum queue? (y/n): ").lower() in ('y', 'yes')
                    dmq.create_queue(queue_name, quorum=quorum)
                elif choice == '7':
                    queue_name = input("Enter the name of the queue to delete: ")
                    dmq.delete_queue(queue_name)
                elif choice == '8':
                    dmq.list_queues()
                else:
                    print("Invalid choice. Please try again.")
    
    except KeyboardInterrupt:
        print("\nInterrupted. Shutting down DMQ manager...")
    except PermissionError:
        print(get_permission_instructions())
    except Exception as e:
        print(f"\nError occurred: {e}")
    finally:
        # Cleanup - only if not simulation mode and if we had permission
        # Don't clean up if this is just a status check or other non-destructive operation
        cleanup_operations = ['start', 'add-node', 'remove-node', 'stop-node', 'revive-node', 'interactive', None]
        if not simulation_mode and dmq.nodes and (not args.command or args.command in cleanup_operations):
            try:
                cleanup = True
                
                # If in interactive mode, ask for confirmation
                if args.command == 'interactive' or not args.command:
                    choice = input("\nDo you want to remove all nodes and cleanup? (y/n): ").lower()
                    cleanup = choice in ('y', 'yes')
                
                if cleanup:
                    # Cleanup - remove all nodes before exiting
                    print("Removing all nodes...")
                    for node_id in list(dmq.nodes.keys()):
                        dmq.remove_node(node_id)
                        
                    networks = client.networks.list(names=[DOCKER_NETWORK_NAME])
                    if networks:
                        networks[0].remove()
                        print(f"Removed Docker network: {DOCKER_NETWORK_NAME}")
            except PermissionError:
                print("Permission denied when cleaning up Docker resources.")
            except Exception as e:
                print(f"Error during cleanup: {e}")
                
        print("DMQ manager shutdown complete")

if __name__ == "__main__":
    main()