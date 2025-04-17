#!/usr/bin/env python3
"""
Custom exceptions for the Distributed Messaging Queue (DMQ) system.
"""

class DMQError(Exception):
    """Base exception class for DMQ errors"""
    pass

class RabbitMQNodeError(DMQError):
    """Error with RabbitMQ node operations"""
    pass

class ClusterOperationError(DMQError):
    """Error during cluster operations"""
    pass