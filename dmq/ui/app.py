#!/usr/bin/env python3
import os
import sys
import json
import threading
import time
from datetime import datetime
from flask import Flask, render_template, request, jsonify

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from dmq.connection_manager import BrokerConnectionManager
from dmq.messaging import DirectProducer, DirectConsumer
from dmq.dmq import DMQCluster

app = Flask(__name__)

# Global variables for state management
broker_manager = BrokerConnectionManager()
cluster_status = {"nodes": [], "queues": []}
message_history = []

# Custom filter for timestamps
@app.template_filter('timestamp')
def format_timestamp(value):
    """Format a UNIX timestamp as a human-readable date/time string"""
    if not value:
        return ""
    return datetime.fromtimestamp(value).strftime('%Y-%m-%d %H:%M:%S')

def initialize_connections():
    """Initialize connections to the RabbitMQ cluster"""
    # Register the default RabbitMQ cluster nodes
    broker_manager.register_broker(1, "localhost", 5672)
    broker_manager.register_broker(2, "localhost", 5682)
    broker_manager.register_broker(3, "localhost", 5692)
    
    # Update the cluster status
    update_cluster_status()

def update_cluster_status():
    """Update the cluster status information"""
    global cluster_status
    
    # Get node information
    nodes = []
    for node_id, conn in broker_manager.load_balancer.connections.items():
        try:
            nodes.append({
                "id": node_id,
                "host": conn.params.host,
                "port": conn.params.port,
                "status": "online" if conn.connection and conn.connection.is_open else "offline"
            })
        except Exception:
            nodes.append({
                "id": node_id,
                "host": conn.params.host,
                "port": conn.params.port,
                "status": "error"
            })
    
    # Get queue information (simplistic implementation)
    # In a real application, you would query the RabbitMQ API for this information
    queues = [
        {"name": "work_queue", "messages": len(message_history)}
    ]
    
    cluster_status = {"nodes": nodes, "queues": queues}

@app.route('/')
def index():
    """Render the main dashboard page"""
    update_cluster_status()
    return render_template('index.html', 
                           cluster_status=cluster_status, 
                           message_history=message_history)

@app.route('/api/cluster-status')
def get_cluster_status():
    """API endpoint to get the cluster status"""
    update_cluster_status()
    return jsonify(cluster_status)

@app.route('/api/messages')
def get_messages():
    """API endpoint to get message history"""
    return jsonify(message_history)

@app.route('/api/publish', methods=['POST'])
def publish_message():
    """API endpoint to publish a new message"""
    data = request.json
    queue_name = data.get('queue', 'work_queue')
    message_content = data.get('message', {})
    
    # Create a producer to send the message
    producer = DirectProducer(connection_manager=broker_manager)
    
    # Create a consumer for queue declaration only
    consumer = DirectConsumer(
        queue_name=queue_name,
        connection_manager=broker_manager,
        auto_ack=False
    )
    
    # Declare the queue with durable=True for persistence
    if not consumer.declare_queue(durable=True):
        consumer.close()
        producer.close()
        return jsonify({"status": "error", "message": "Failed to declare queue"})
        
    # Add a timestamp to the message
    message_content['timestamp'] = time.time()
    
    # Publish the message
    if producer.publish(routing_key=queue_name, message=message_content):
        # Add to message history
        message_history.append({
            "id": len(message_history) + 1,
            "queue": queue_name,
            "content": message_content,
            "status": "published",
            "timestamp": message_content['timestamp']
        })
        
        consumer.close()
        producer.close()
        return jsonify({"status": "success", "message": "Message published successfully"})
    else:
        consumer.close()
        producer.close()
        return jsonify({"status": "error", "message": "Failed to publish message"})

def message_consumer(message, headers=None):
    """Consumer callback for processing messages"""
    message_id = message.get('id', len(message_history) + 1)
    
    # Find the message in history or add it
    found = False
    for msg in message_history:
        if msg.get('id') == message_id:
            msg['status'] = 'consumed'
            found = True
            break
            
    if not found:
        message_history.append({
            "id": message_id,
            "queue": "work_queue",
            "content": message,
            "status": "consumed",
            "timestamp": time.time()
        })
    
    # Simulate processing
    time.sleep(1)
    return True

def start_consumer():
    """Start a consumer in the background"""
    consumer_manager = BrokerConnectionManager()
    consumer_manager.register_broker(1, "localhost", 5672)
    consumer_manager.register_broker(2, "localhost", 5682)
    consumer_manager.register_broker(3, "localhost", 5692)
    
    consumer = DirectConsumer(
        queue_name="work_queue",
        connection_manager=consumer_manager,
        auto_ack=False
    )
    
    # Declare the queue
    if not consumer.declare_queue(durable=True):
        print("Failed to declare queue for consumer")
        return
        
    # Start consuming thread
    consumer.start_consuming_thread(message_consumer)

if __name__ == "__main__":
    # Initialize connections
    initialize_connections()
    
    # Start a consumer in the background
    threading.Thread(target=start_consumer, daemon=True).start()
    
    # Run the Flask app
    app.run(debug=True)