#!/usr/bin/env python3
"""
Docker utilities for the Distributed Messaging Queue (DMQ) system.
"""
import os
import json
import subprocess
import logging
import docker
from typing import Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s]  %(message)s')
logger = logging.getLogger(__name__)

# Docker network settings
DOCKER_NETWORK_NAME = "dmq-rabbitmq-network"
DOCKER_SUBNET = "172.25.0.0/16"
BASE_IP = "172.25.0.10"  # Starting IP for RabbitMQ nodes

# Initialize Docker client
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

# Create Docker client
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
            # Check if existing network is healthy by trying to inspect it
            try:
                network_data = client.api.inspect_network(networks[0].id)
                logger.info(f"Network inspection successful, using existing network")
            except Exception as e:
                logger.warning(f"Existing network may be problematic: {e}")
                logger.info(f"Removing and recreating Docker network '{DOCKER_NETWORK_NAME}'")
                # Try to remove any containers on this network first
                try:
                    for container in client.containers.list(all=True):
                        if DOCKER_NETWORK_NAME in container.attrs.get('NetworkSettings', {}).get('Networks', {}):
                            logger.info(f"Disconnecting container {container.name} from network")
                            client.api.disconnect_container_from_network(container.id, networks[0].id)
                except Exception as container_err:
                    logger.warning(f"Error disconnecting containers: {container_err}")
                    
                # Try to remove the network
                try:
                    networks[0].remove()
                except Exception as remove_err:
                    logger.warning(f"Could not remove network: {remove_err}")
                    # If we can't remove, try to continue with the existing network
                    return networks[0]
            else:
                return networks[0]
        
        # Create network with specific subnet and enable DNS
        # Use options dict instead of driver_opts which is causing the error
        network = client.networks.create(
            name=DOCKER_NETWORK_NAME,
            driver="bridge",
            ipam=docker.types.IPAMConfig(
                pool_configs=[docker.types.IPAMPool(subnet=DOCKER_SUBNET)]
            ),
            options={
                "com.docker.network.bridge.enable_icc": "true",
                "com.docker.network.bridge.enable_ip_masquerade": "true",
                "com.docker.network.bridge.host_binding_ipv4": "0.0.0.0",
                "com.docker.network.driver.mtu": "1500",
                # Move DNS configuration to options
                "com.docker.network.bridge.enable_dns": "true"
            }
        )
        logger.info(f"Created Docker network: {DOCKER_NETWORK_NAME}")
        
        # Verify the network was created with the expected subnet
        try:
            network_data = client.api.inspect_network(network.id)
            subnet = network_data.get('IPAM', {}).get('Config', [{}])[0].get('Subnet')
            logger.info(f"Network created with subnet: {subnet}")
        except Exception as test_err:
            logger.warning(f"Error during network verification: {test_err}")
        
        return network
    except PermissionError:
        logger.error("Permission denied when creating Docker network.")
        raise PermissionError(get_permission_instructions())
    except Exception as e:
        logger.error(f"Failed to setup Docker network: {e}")
        raise

def get_ip_for_node(node_id):
    """Generate an IP address for the RabbitMQ node based on its ID"""
    import ipaddress
    ip = ipaddress.IPv4Address(BASE_IP)
    return str(ip + node_id - 1)

def find_available_port(start_port):
    """Find the next available port starting from given port"""
    import socket
    port = start_port
    while port < 65535:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(('', port))
                return port
        except OSError:
            port += 1
    raise RuntimeError("No available ports found")