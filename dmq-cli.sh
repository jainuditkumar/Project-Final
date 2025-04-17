#!/bin/bash

# DMQ Command Line Interface wrapper
# Makes it easier to access DMQ functionality

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DMQ_PATH="${SCRIPT_DIR}/dmq/dmq.py"

# Check if dmq.py exists and is executable
if [ ! -f "$DMQ_PATH" ]; then
    echo "Error: DMQ script not found at $DMQ_PATH"
    exit 1
fi

# Make sure dmq.py is executable
chmod +x "$DMQ_PATH"

# Show help information
function show_help {
    echo "DMQ Cluster Management Tool"
    echo
    echo "Usage: ./dmq-cli.sh COMMAND [OPTIONS]"
    echo
    echo "Commands:"
    echo "  start <num_nodes>     Start a new DMQ cluster with specified number of nodes"
    echo "  status                Display the current cluster status"
    echo "  add-node              Add a new node to the cluster"
    echo "  remove-node <id>      Remove the node with the specified ID"
    echo "  stop-node <id>        Temporarily stop the node with the specified ID"
    echo "  revive-node <id>      Revive a stopped node with the specified ID"
    echo "  create-queue <name>   Create a new queue with the given name"
    echo "  create-quorum <name>  Create a new quorum queue with the given name"
    echo "  delete-queue <name>   Delete the queue with the given name"
    echo "  list-queues           List all queues in the cluster"
    echo "  interactive           Start DMQ in interactive mode"
    echo
    echo "Options:"
    echo "  --simulation, -s      Run in simulation mode without Docker"
    echo "  --verbose, -v         Enable verbose logging"
    echo "  --help, -h            Show this help message"
    echo
    echo "Examples:"
    echo "  ./dmq-cli.sh start 3             # Start a cluster with 3 nodes"
    echo "  ./dmq-cli.sh add-node            # Add a new node to the cluster"
    echo "  ./dmq-cli.sh remove-node 2       # Remove node with ID 2"
    echo "  ./dmq-cli.sh -s start 3          # Start a simulated cluster with 3 nodes"
}

# Parse command line arguments
if [ $# -eq 0 ] || [ "$1" == "--help" ] || [ "$1" == "-h" ]; then
    show_help
    exit 0
fi

# Check for simulation flag
SIMULATION_FLAG=""
VERBOSE_FLAG=""

while [[ "$1" == -* ]]; do
    case "$1" in
        --simulation|-s)
            SIMULATION_FLAG="--simulation"
            shift
            ;;
        --verbose|-v)
            VERBOSE_FLAG="--verbose"
            shift
            ;;
        *)
            echo "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Process commands
if [ $# -eq 0 ]; then
    # If only flags were given, start in interactive mode
    "$DMQ_PATH" $SIMULATION_FLAG $VERBOSE_FLAG interactive
    exit $?
fi

COMMAND="$1"
shift

case "$COMMAND" in
    start)
        if [ $# -lt 1 ]; then
            echo "Error: Number of nodes required"
            echo "Usage: ./dmq-cli.sh start <num_nodes>"
            exit 1
        fi
        "$DMQ_PATH" $SIMULATION_FLAG $VERBOSE_FLAG start "$1"
        ;;
    status)
        "$DMQ_PATH" $SIMULATION_FLAG $VERBOSE_FLAG status
        ;;
    add-node)
        "$DMQ_PATH" $SIMULATION_FLAG $VERBOSE_FLAG add-node
        ;;
    remove-node)
        if [ $# -lt 1 ]; then
            echo "Error: Node ID required"
            echo "Usage: ./dmq-cli.sh remove-node <node_id>"
            exit 1
        fi
        "$DMQ_PATH" $SIMULATION_FLAG $VERBOSE_FLAG remove-node "$1"
        ;;
    stop-node)
        if [ $# -lt 1 ]; then
            echo "Error: Node ID required"
            echo "Usage: ./dmq-cli.sh stop-node <node_id>"
            exit 1
        fi
        "$DMQ_PATH" $SIMULATION_FLAG $VERBOSE_FLAG stop-node "$1"
        ;;
    revive-node)
        if [ $# -lt 1 ]; then
            echo "Error: Node ID required"
            echo "Usage: ./dmq-cli.sh revive-node <node_id>"
            exit 1
        fi
        "$DMQ_PATH" $SIMULATION_FLAG $VERBOSE_FLAG revive-node "$1"
        ;;
    create-queue)
        if [ $# -lt 1 ]; then
            echo "Error: Queue name required"
            echo "Usage: ./dmq-cli.sh create-queue <queue_name>"
            exit 1
        fi
        "$DMQ_PATH" $SIMULATION_FLAG $VERBOSE_FLAG create-queue "$1"
        ;;
    create-quorum)
        if [ $# -lt 1 ]; then
            echo "Error: Queue name required"
            echo "Usage: ./dmq-cli.sh create-quorum <queue_name>"
            exit 1
        fi
        "$DMQ_PATH" $SIMULATION_FLAG $VERBOSE_FLAG create-queue "$1" --quorum
        ;;
    delete-queue)
        if [ $# -lt 1 ]; then
            echo "Error: Queue name required"
            echo "Usage: ./dmq-cli.sh delete-queue <queue_name>"
            exit 1
        fi
        "$DMQ_PATH" $SIMULATION_FLAG $VERBOSE_FLAG delete-queue "$1"
        ;;
    list-queues)
        "$DMQ_PATH" $SIMULATION_FLAG $VERBOSE_FLAG list-queues
        ;;
    interactive)
        "$DMQ_PATH" $SIMULATION_FLAG $VERBOSE_FLAG interactive
        ;;
    *)
        echo "Error: Unknown command '$COMMAND'"
        show_help
        exit 1
        ;;
esac

exit $?
