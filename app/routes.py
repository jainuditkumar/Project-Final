#!/usr/bin/env python3
from flask import Blueprint, render_template, request, jsonify, flash, redirect, url_for
import pika
import json
import sys
import os
from dmq.connection_manager import BrokerConnectionManager
from dmq.dmq import DMQCluster, RabbitMQNode
import examples.direct_exchange_example as direct_exchange
import examples.fanout_exchange_example as fanout_exchange
import examples.topic_exchange_example as topic_exchange
import examples.headers_exchange_example as headers_exchange
import examples.message_persistence_example as persistence
import examples.acknowledge_retry_example as ack_retry
import examples.load_balancing_example as load_balancing
import examples.work_queue_example as work_queue
import threading
import time
import random
import uuid

# Create blueprints
main_bp = Blueprint('main', __name__)
examples_bp = Blueprint('examples', __name__)
cluster_bp = Blueprint('cluster', __name__)

# Store message results for display
message_results = {}
active_consumers = {}
active_producers = {}
exchange_configs = {}

# Helper functions
def get_connection_params(host="localhost", port=5672, username="guest", password="guest"):
    """Get connection parameters for RabbitMQ"""
    credentials = pika.PlainCredentials(username, password)
    return pika.ConnectionParameters(
        host=host,
        port=port,
        virtual_host="/",
        credentials=credentials,
        connection_attempts=3,
        retry_delay=2
    )

def safe_connect(params):
    """Safely connect to RabbitMQ and return connection or None"""
    try:
        connection = pika.BlockingConnection(params)
        return connection
    except Exception as e:
        flash(f"Failed to connect to RabbitMQ: {str(e)}", "danger")
        return None

def get_mime_headers(content_type):
    """Get MIME type headers for messages"""
    if content_type == "application/json":
        return {"content-type": "application/json"}
    elif content_type == "text/plain":
        return {"content-type": "text/plain"}
    elif content_type == "application/xml":
        return {"content-type": "application/xml"}
    elif content_type == "application/octet-stream":
        return {"content-type": "application/octet-stream"}
    else:
        return {}

def serialize_message(message, content_type):
    """Serialize message based on content type"""
    if content_type == "application/json":
        if isinstance(message, dict):
            return json.dumps(message)
        else:
            return json.dumps({"data": message})
    elif content_type == "application/xml":
        if isinstance(message, dict):
            # Simple XML serialization for demo
            xml = '<?xml version="1.0"?><data>'
            for key, value in message.items():
                xml += f'<{key}>{value}</{key}>'
            xml += '</data>'
            return xml
        else:
            return f'<?xml version="1.0"?><data>{message}</data>'
    else:
        # For text/plain and application/octet-stream, just return as string
        return str(message)

def start_consumer(exchange_type, exchange_name, queue_name, routing_key="", headers=None,
                  mime_type="text/plain", host="localhost", port=5672, result_id=None):
    """Start a consumer thread for receiving messages"""
    
    # Generate a result ID if not provided
    if not result_id:
        result_id = str(uuid.uuid4())
        message_results[result_id] = []
    
    def consumer_thread():
        consumer_id = f"consumer-{uuid.uuid4()}"
        active_consumers[consumer_id] = {
            "exchange_type": exchange_type,
            "exchange_name": exchange_name,
            "queue_name": queue_name,
            "routing_key": routing_key,
            "status": "starting"
        }
        
        params = get_connection_params(host, port)
        try:
            # Connect to RabbitMQ
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            
            # Declare exchange
            channel.exchange_declare(
                exchange=exchange_name,
                exchange_type=exchange_type,
                durable=True
            )
            
            # Declare queue
            result = channel.queue_declare(queue=queue_name, durable=True)
            actual_queue_name = result.method.queue
            
            # Bind queue to exchange based on exchange type
            if exchange_type == 'fanout':
                channel.queue_bind(
                    exchange=exchange_name,
                    queue=actual_queue_name
                )
            elif exchange_type == 'direct':
                channel.queue_bind(
                    exchange=exchange_name,
                    queue=actual_queue_name,
                    routing_key=routing_key
                )
            elif exchange_type == 'topic':
                channel.queue_bind(
                    exchange=exchange_name,
                    queue=actual_queue_name,
                    routing_key=routing_key
                )
            elif exchange_type == 'headers':
                bind_arguments = headers if headers else {}
                if 'x-match' not in bind_arguments:
                    bind_arguments['x-match'] = 'all'  # Default to match all headers
                channel.queue_bind(
                    exchange=exchange_name,
                    queue=actual_queue_name,
                    arguments=bind_arguments
                )
            
            # Update status
            active_consumers[consumer_id]["status"] = "running"
            
            def callback(ch, method, properties, body):
                try:
                    # Try to decode based on content type
                    content_type = properties.content_type if properties.content_type else mime_type
                    
                    if content_type == 'application/json':
                        try:
                            decoded = json.loads(body)
                            message = f"JSON: {decoded}"
                        except:
                            message = f"Raw: {body.decode('utf-8', errors='replace')}"
                    elif content_type == 'application/xml':
                        message = f"XML: {body.decode('utf-8', errors='replace')}"
                    else:
                        message = f"Text: {body.decode('utf-8', errors='replace')}"
                    
                    # Include headers in the result
                    headers_info = {}
                    if properties.headers:
                        headers_info = properties.headers
                    
                    # Save message
                    message_results[result_id].append({
                        "content": message,
                        "exchange": exchange_name,
                        "routing_key": method.routing_key,
                        "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
                        "headers": headers_info,
                        "content_type": content_type
                    })
                    
                    # Acknowledge message
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    
                except Exception as e:
                    message_results[result_id].append({
                        "content": f"Error processing message: {str(e)}",
                        "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
                    })
                    # Still acknowledge to prevent queue blocking
                    ch.basic_ack(delivery_tag=method.delivery_tag)
            
            # Set up consumer
            channel.basic_consume(
                queue=actual_queue_name,
                on_message_callback=callback
            )
            
            # Start consuming
            channel.start_consuming()
            
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.AMQPChannelError) as e:
            message_results[result_id].append({
                "content": f"Connection error: {str(e)}",
                "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
            })
            active_consumers[consumer_id]["status"] = "error"
        except Exception as e:
            message_results[result_id].append({
                "content": f"Error in consumer: {str(e)}",
                "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
            })
            active_consumers[consumer_id]["status"] = "error"
        finally:
            try:
                if connection and connection.is_open:
                    connection.close()
            except:
                pass
            active_consumers[consumer_id]["status"] = "stopped"
    
    # Start the consumer in a thread
    thread = threading.Thread(target=consumer_thread, daemon=True)
    thread.start()
    
    return result_id

def publish_message(exchange_type, exchange_name, routing_key, message, 
                   headers=None, mime_type="text/plain", host="localhost", port=5672):
    """Publish a message to an exchange"""
    try:
        params = get_connection_params(host, port)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        
        # Declare exchange
        channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=exchange_type,
            durable=True
        )
        
        # Serialize message based on MIME type
        message_body = serialize_message(message, mime_type)
        
        # Prepare properties
        properties = pika.BasicProperties(
            content_type=mime_type,
            delivery_mode=2,  # Make message persistent
            headers=headers if headers else {}
        )
        
        # Publish message
        channel.basic_publish(
            exchange=exchange_name,
            routing_key=routing_key,
            body=message_body,
            properties=properties
        )
        
        connection.close()
        return True, "Message published successfully"
    except Exception as e:
        return False, f"Error publishing message: {str(e)}"

# Main routes
@main_bp.route('/')
def index():
    return render_template('index.html')

# Exchange examples routes
@examples_bp.route('/direct_exchange')
def direct_exchange_page():
    return render_template('examples/direct_exchange.html')

@examples_bp.route('/fanout_exchange')
def fanout_exchange_page():
    return render_template('examples/fanout_exchange.html')

@examples_bp.route('/topic_exchange')
def topic_exchange_page():
    return render_template('examples/topic_exchange.html')

@examples_bp.route('/headers_exchange')
def headers_exchange_page():
    return render_template('examples/headers_exchange.html')

@examples_bp.route('/message_persistence')
def message_persistence_page():
    return render_template('examples/message_persistence.html')

@examples_bp.route('/acknowledge_retry')
def acknowledge_retry_page():
    return render_template('examples/acknowledge_retry.html')

@examples_bp.route('/load_balancing')
def load_balancing_page():
    return render_template('examples/load_balancing.html')

# API Routes for messaging operations
@examples_bp.route('/api/start_consumer', methods=['POST'])
def api_start_consumer():
    data = request.json
    exchange_type = data.get('exchange_type', 'direct')
    exchange_name = data.get('exchange_name', 'test_exchange')
    queue_name = data.get('queue_name', 'test_queue')
    routing_key = data.get('routing_key', '')
    mime_type = data.get('mime_type', 'text/plain')
    host = data.get('host', 'localhost')
    port = int(data.get('port', 5672))
    
    # Handle headers for header exchange
    headers = {}
    if exchange_type == 'headers':
        headers = data.get('headers', {})
        if 'x-match' not in headers:
            headers['x-match'] = data.get('header_match_type', 'all')
    
    result_id = start_consumer(
        exchange_type=exchange_type,
        exchange_name=exchange_name,
        queue_name=queue_name,
        routing_key=routing_key,
        headers=headers,
        mime_type=mime_type,
        host=host,
        port=port
    )
    
    # Save exchange config for use in the UI
    config_id = str(uuid.uuid4())
    exchange_configs[config_id] = {
        "exchange_type": exchange_type,
        "exchange_name": exchange_name,
        "queue_name": queue_name,
        "routing_key": routing_key,
        "headers": headers,
        "mime_type": mime_type,
        "host": host,
        "port": port,
        "result_id": result_id
    }
    
    return jsonify({
        "success": True, 
        "result_id": result_id,
        "config_id": config_id
    })

@examples_bp.route('/api/publish_message', methods=['POST'])
def api_publish_message():
    data = request.json
    exchange_type = data.get('exchange_type', 'direct')
    exchange_name = data.get('exchange_name', 'test_exchange')
    routing_key = data.get('routing_key', '')
    message = data.get('message', '')
    mime_type = data.get('mime_type', 'text/plain')
    host = data.get('host', 'localhost')
    port = int(data.get('port', 5672))
    
    # Handle headers
    headers = {}
    if 'headers' in data and data['headers']:
        headers = data['headers']
    
    # Try to convert message to JSON if the mime type is application/json
    if mime_type == 'application/json' and isinstance(message, str):
        try:
            message = json.loads(message)
        except json.JSONDecodeError:
            # If not valid JSON, wrap it in a simple object
            message = {"message": message}
    
    success, message_result = publish_message(
        exchange_type=exchange_type,
        exchange_name=exchange_name,
        routing_key=routing_key,
        message=message,
        headers=headers,
        mime_type=mime_type,
        host=host,
        port=port
    )
    
    return jsonify({
        "success": success, 
        "message": message_result
    })

@examples_bp.route('/api/get_messages/<result_id>', methods=['GET'])
def api_get_messages(result_id):
    if result_id in message_results:
        return jsonify({
            "success": True,
            "messages": message_results[result_id]
        })
    else:
        return jsonify({
            "success": False,
            "message": "Result ID not found"
        })

@examples_bp.route('/api/clear_messages/<result_id>', methods=['POST'])
def api_clear_messages(result_id):
    if result_id in message_results:
        message_results[result_id] = []
        return jsonify({
            "success": True,
            "message": "Messages cleared"
        })
    else:
        return jsonify({
            "success": False,
            "message": "Result ID not found"
        })

@examples_bp.route('/api/get_exchange_config/<config_id>', methods=['GET'])
def api_get_exchange_config(config_id):
    if config_id in exchange_configs:
        return jsonify({
            "success": True,
            "config": exchange_configs[config_id]
        })
    else:
        return jsonify({
            "success": False,
            "message": "Configuration not found"
        })

# Cluster management routes
@cluster_bp.route('/status')
def cluster_status():
    return render_template('cluster/status.html')

@cluster_bp.route('/manage')
def cluster_manage():
    return render_template('cluster/manage.html')