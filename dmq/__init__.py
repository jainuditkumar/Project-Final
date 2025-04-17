#!/usr/bin/env python3
"""
Distributed Messaging Queue (DMQ) Manager for RabbitMQ.

This package provides tools for managing a cluster of RabbitMQ containers 
with operations like adding/removing nodes, turning nodes off/on, and monitoring the cluster.
"""

from .node import RabbitMQNode
from .cluster import DMQCluster
from .exceptions import DMQError, RabbitMQNodeError, ClusterOperationError
from .docker_utils import DOCKER_NETWORK_NAME, DOCKER_SUBNET, BASE_IP

__version__ = "1.0.0"

# Export public API
__all__ = [
    'RabbitMQNode',
    'DMQCluster',
    'DMQError',
    'RabbitMQNodeError',
    'ClusterOperationError',
    'DOCKER_NETWORK_NAME',
    'DOCKER_SUBNET',
    'BASE_IP',
]