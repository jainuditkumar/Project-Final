# Distributed Messaging Queue with RabbitMQ

This project implements a distributed messaging queue system using RabbitMQ, enabling reliable communication between different services in a scalable and fault-tolerant manner.

## Features

- **Message Queuing and Delivery**: Ensures messages are processed in order
- **Multiple Exchange Types**: Implements Direct, Topic, Fanout, and Headers exchanges
- **Message Persistence**: Ensures messages are not lost during failures
- **Acknowledge & Retry Mechanisms**: Handles failures gracefully with manual acknowledgments
- **Load Balancing**: Distributes work across multiple RabbitMQ nodes
- **Clustering & High Availability**: Runs RabbitMQ across multiple nodes for fault tolerance
- **Connection Management**: Manages connections to multiple brokers efficiently
- **Topic-Based Routing**: Routes messages based on routing patterns
- **Web Interface**: Monitor and manage the cluster through a web application

## Project Structure

```
dmq-project/
├── dmq/                   # Core messaging library
│   ├── connection_manager.py  # Manages broker connections
│   ├── messaging.py       # Producer and consumer implementations
│   ├── cluster.py         # RabbitMQ cluster management
│   └── docker_utils.py    # Docker container utilities
├── examples/              # Example implementations
│   ├── direct_exchange_example.py
│   ├── topic_exchange_example.py
│   ├── fanout_exchange_example.py
│   ├── headers_exchange_example.py
│   ├── message_persistence_example.py
│   ├── load_balancing_example.py
│   └── acknowledge_retry_example.py
├── app/                   # Web application for monitoring
│   ├── templates/         # HTML templates
│   ├── static/            # Static assets (CSS, JS)
│   └── routes.py          # Web routes
├── scripts/               # Utility scripts
│   ├── setup_cluster.sh   # Creates RabbitMQ cluster
│   └── teardown_cluster.sh # Removes RabbitMQ cluster
├── dmq-cli.sh             # Command-line interface script
├── requirements.txt       # Python dependencies
└── README.md              # Project documentation
```

## Prerequisites

- Python 3.8+
- Docker and Docker Compose
- RabbitMQ

## Setup

1. Clone the repository:

```bash
git clone https://github.com/yourusername/dmq-project.git
cd dmq-project
```

2. Create and activate a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

4. Make scripts executable:

```bash
chmod +x make-scripts-executable.sh
./make-scripts-executable.sh
```

5. Set up the RabbitMQ cluster:

```bash
./scripts/setup_cluster.sh
```

## Usage

### Command-Line Interface

Use the DMQ CLI to manage the cluster:

```bash
./dmq-cli.sh start  # Start the cluster
./dmq-cli.sh stop   # Stop the cluster
./dmq-cli.sh status # Check cluster status
```

### Run Examples

To run the examples to see different messaging patterns in action:

```bash
python -m examples.direct_exchange_example
python -m examples.topic_exchange_example
python -m examples.fanout_exchange_example
```

### Web Interface

Start the web interface to monitor and manage the cluster:

```bash
python -m app.app
```

Then navigate to `http://localhost:5000` in your web browser.

## Library Usage

### Create a Producer

```python
from dmq.connection_manager import BrokerConnectionManager
from dmq.messaging import DirectProducer

# Create connection manager
connection_manager = BrokerConnectionManager()
connection_manager.register_broker(1, "localhost", 5672)

# Create producer
producer = DirectProducer(connection_manager=connection_manager)

# Publish message
producer.publish(
    routing_key="queue_name",
    message="Hello World!",
    properties=None  # Optional message properties
)
```

### Create a Consumer

```python
from dmq.connection_manager import BrokerConnectionManager
from dmq.messaging import DirectConsumer

# Create connection manager
connection_manager = BrokerConnectionManager()
connection_manager.register_broker(1, "localhost", 5672)

# Create consumer
consumer = DirectConsumer(
    queue_name="queue_name",
    connection_manager=connection_manager,
    auto_ack=False  # Manual acknowledgment
)

# Define message handler
def message_handler(message, headers):
    print(f"Received message: {message}")
    return True  # Acknowledge message

# Start consuming
consumer.declare_queue(durable=True)
consumer.start_consuming(message_handler)
```

## Tear Down Cluster

To stop and remove the RabbitMQ cluster:

```bash
./scripts/teardown_cluster.sh
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.
