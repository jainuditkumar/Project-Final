#!/usr/bin/env python3
import os
import secrets

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY') or secrets.token_hex(16)
    
    # RabbitMQ default connection settings
    RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
    RABBITMQ_PORT = int(os.environ.get('RABBITMQ_PORT', 5672))
    RABBITMQ_USER = os.environ.get('RABBITMQ_USER', 'guest')
    RABBITMQ_PASS = os.environ.get('RABBITMQ_PASS', 'guest')
    RABBITMQ_MGMT_PORT = int(os.environ.get('RABBITMQ_MGMT_PORT', 15672))