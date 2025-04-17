#!/usr/bin/env python3
"""
Cluster management for the Distributed Messaging Queue (DMQ) system.
"""
import time
import logging
import asyncio
from typing import Dict, Optional, List

from .docker_utils import client, setup_docker_network, DOCKER_NETWORK_NAME
from .node import RabbitMQNode
from .exceptions import RabbitMQNodeError, ClusterOperationError

# Configure logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s]  %(message)s')
logger = logging.getLogger(__name__)

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
        try:
            primary_node = RabbitMQNode(
                1,
                simulation_mode=self.simulation_mode
            )
            if primary_node.start():
                self.nodes[1] = primary_node
                self.primary_node_id = 1
                self.nodes[1].is_primary = True
                logger.info(f"Started primary node (ID: 1)")
            else:
                logger.error("Failed to start primary node")
                return False
        except Exception as e:
            logger.error(f"Error creating primary node: {e}")
            return False
        
        # Wait for the primary node to fully initialize
        logger.info("Waiting for primary node to initialize...")
        time.sleep(7)  # RabbitMQ needs time to start
        
        # If only 1 node requested, we're done
        if num_nodes == 1:
            if not self.simulation_mode:
                primary_node.connect_pika()
            return True
        
        # Track successes and failures for additional nodes
        node_results = {"success": [], "failed": []}
        
        async def start_and_join(i):
            try:
                node = RabbitMQNode(
                    i,
                    simulation_mode=self.simulation_mode
                )
                start_success = await asyncio.get_event_loop().run_in_executor(None, node.start)
                if not start_success:
                    logger.error(f"Failed to start node {i}")
                    node_results["failed"].append(i)
                    return
                    
                self.nodes[i] = node
                logger.info(f"Started node {i}")
                
                # Wait for the node to fully initialize before joining
                await asyncio.sleep(5)
                
                # Join the cluster
                join_success = await asyncio.get_event_loop().run_in_executor(
                    None, node.join_cluster, self.nodes[self.primary_node_id]
                )
                
                if not join_success:
                    logger.error(f"Failed to join node {i} to the cluster")
                    node_results["failed"].append(i)
                else:
                    node_results["success"].append(i)
                    
            except Exception as e:
                logger.error(f"Error in start_and_join for node {i}: {e}")
                node_results["failed"].append(i)

        async def add_nodes_async():
            try:
                tasks = [asyncio.create_task(start_and_join(i)) for i in range(2, num_nodes + 1)]
                await asyncio.gather(*tasks)
            except Exception as e:
                logger.error(f"Error in add_nodes_async: {e}")

        try:
            asyncio.run(add_nodes_async())
        except Exception as e:
            logger.error(f"Failed to run async node creation: {e}")
        
        # Summary of results
        if node_results["failed"]:
            logger.warning(f"Failed to initialize nodes: {node_results['failed']}")
        
        if node_results["success"]:
            logger.info(f"Successfully initialized nodes: {node_results['success']}")
        
        # Connect to all nodes for management operations
        for node_id, node in self.nodes.items():
            if not self.simulation_mode and node.is_active:
                try:
                    node.connect_pika()
                except Exception as e:
                    logger.error(f"Failed to connect to node {node_id} with Pika: {e}")
        
        # Return True if primary and at least some other nodes succeeded
        return self.primary_node_id is not None
    
    def add_node(self) -> bool:
        """Add a new node to the cluster"""
        if not self.primary_node_id:
            logger.error("Cannot add node: No primary node exists")
            return False
            
        new_id = max(self.nodes.keys()) + 1 if self.nodes else 1
        
        node = RabbitMQNode(
            new_id,
            simulation_mode=self.simulation_mode
        )
        
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
                
    def cleanup(self, ask_confirmation=True) -> bool:
        """Clean up all resources created by this cluster"""
        if not self.nodes:
            logger.info("No nodes to clean up")
            return True
            
        if ask_confirmation and not self.simulation_mode:
            choice = input("\nDo you want to remove all nodes and cleanup? (Y/n): ").lower()
            if choice not in ('y', 'yes', ''):
                logger.info("Cleanup cancelled")
                return False
                
        # Remove all nodes
        logger.info("Removing all nodes...")
        for node_id in list(self.nodes.keys()):
            self.remove_node(node_id)
            
        # Remove Docker network
        if not self.simulation_mode:
            try:
                networks = client.networks.list(names=[DOCKER_NETWORK_NAME])
                if networks:
                    networks[0].remove()
                    logger.info(f"Removed Docker network: {DOCKER_NETWORK_NAME}")
            except Exception as e:
                logger.error(f"Error removing network: {e}")
                return False
                
        return True