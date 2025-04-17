# Distributed Messaging Queue with RabbitMQ

This project implements a distributed messaging queue system using RabbitMQ, enabling reliable communication between different services in a scalable and fault-tolerant manner.

## Features

- **Message Queuing and Delivery**: Ensures messages are processed in order
- **Publisher-Subscriber and Work Queue Models**: Implements different messaging patterns
- **Message Persistence**: Ensures messages are not lost during failures
- **Acknowledge & Retry Mechanisms**: Handles failures gracefully
- **Load Balancing with Multiple Consumers**: Distributes work efficiently
- **Clustering & High Availability**: Runs RabbitMQ across multiple nodes
- **Multiple Exchange Types**: Implements Direct, Topic, and Fanout exchanges

## Project Structure

```
distributed-messaging-queue-rabbitmq/
├── src/
│   ├── producers/          # Message publishers
│   ├── consumers/          # Message consumers
│   ├── utils/              # Utility functions
│   └── config/             # Configuration settings
├── docker/                 # Docker configuration
├── tests/                  # Unit and integration tests
├── scripts/                # Cluster setup/teardown scripts
├── .env                    # Environment variables
├── .gitignore              # Git ignore file
├── README.md               # Project documentation
└── requirements.txt        # Python dependencies
```

## Prerequisites

- Python 3.8+
- Docker and Docker Compose
- RabbitMQ

## Setup

1. Clone the repository:

```bash
git clone https://github.com/yourusername/distributed-messaging-queue-rabbitmq.git
cd distributed-messaging-queue-rabbitmq
```

2. Create and activate a virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

4. Set up the RabbitMQ cluster:

```bash
cd scripts
chmod +x setup_cluster.sh
./setup_cluster.sh
```

## Usage

### Start a Direct Exchange Consumer

```bash
python -m src.consumers.consumer_direct
```

### Start a Topic Exchange Consumer

```bash
python -m src.consumers.consumer_topic
```

### Start a Fanout Exchange Consumer

```bash
python -m src.consumers.consumer_fanout
```

## Testing

Run the test suite:

```bash
pytest
```

## Tear Down Cluster

To stop and remove the RabbitMQ cluster:

```bash
cd scripts
chmod +x teardown_cluster.sh
./teardown_cluster.sh
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.
