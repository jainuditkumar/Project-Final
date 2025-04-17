#!/usr/bin/env python3
"""
Command-line interface for the Distributed Messaging Queue (DMQ) system.
"""
import argparse
import logging
import sys

from .docker_utils import check_docker_installed, check_docker_permission, get_permission_instructions
from .cluster import DMQCluster

# Configure logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s]  %(message)s')
logger = logging.getLogger(__name__)

def parse_arguments():
    """Parse command line arguments for DMQ."""
    parser = argparse.ArgumentParser(description='Distributed Messaging Queue (DMQ) Manager for RabbitMQ')
    
    # Global options
    parser.add_argument('--simulation', '-s', action='store_true', 
                        help='Run in simulation mode without Docker')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Enable verbose logging')
    
    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest='command', help='DMQ Commands')
    
    # Start cluster command
    start_parser = subparsers.add_parser('start', help='Start a new DMQ cluster')
    start_parser.add_argument('nodes', type=int, help='Number of nodes to initialize')
    
    # Status command
    subparsers.add_parser('status', help='Display cluster status')
    
    # Add node command
    subparsers.add_parser('add-node', help='Add a new node to the cluster')
    
    # Remove node command
    remove_parser = subparsers.add_parser('remove-node', help='Remove a node from the cluster')
    remove_parser.add_argument('node_id', type=int, help='ID of the node to remove')
    
    # Stop node command
    stop_parser = subparsers.add_parser('stop-node', help='Temporarily stop a node')
    stop_parser.add_argument('node_id', type=int, help='ID of the node to stop')
    
    # Revive node command
    revive_parser = subparsers.add_parser('revive-node', help='Revive a stopped node')
    revive_parser.add_argument('node_id', type=int, help='ID of the node to revive')
    
    # Create queue command
    create_parser = subparsers.add_parser('create-queue', help='Create a new queue')
    create_parser.add_argument('queue_name', help='Name for the new queue')
    create_parser.add_argument('--quorum', '-q', action='store_true', 
                              help='Create a quorum queue instead of classic queue')
    
    # Delete queue command
    delete_parser = subparsers.add_parser('delete-queue', help='Delete a queue')
    delete_parser.add_argument('queue_name', help='Name of the queue to delete')
    
    # List queues command
    subparsers.add_parser('list-queues', help='List all queues in the cluster')
    
    # Interactive mode
    subparsers.add_parser('interactive', help='Start DMQ in interactive mode')
    
    return parser.parse_args()

def interactive_mode(dmq):
    """Run DMQ in interactive mode"""
    # If no cluster is initialized, ask the user
    if len(dmq.nodes) == 0:
        # Get number of nodes from user
        while True:
            try:
                num_nodes = int(input("Enter the number of RabbitMQ nodes to initialize: "))
                if num_nodes <= 0:
                    print("Please enter a positive number")
                    continue
                break
            except ValueError:
                print("Please enter a valid number")
        
        # Initialize the cluster
        if not dmq.initialize_cluster(num_nodes):
            print("Failed to initialize cluster. Exiting.")
            return
        
        print(f"\nSuccessfully initialized cluster with {len(dmq.nodes)} nodes!")
        dmq.display_status()
    
    # Main command loop
    while True:
        print("\n=== DMQ Management Console ===")
        print("1. Display cluster status")
        print("2. Add a node")
        print("3. Remove a node")
        print("4. Turn off a node temporarily")
        print("5. Revive a node")
        print("6. Create a queue")
        print("7. Delete a queue")
        print("8. List all queues")
        print("0. Exit")
        
        choice = input("\nEnter your choice (0-8): ")
        
        if choice == '0':
            print("Shutting down DMQ manager...")
            break
        elif choice == '1':
            dmq.display_status()
        elif choice == '2':
            dmq.add_node()
            dmq.display_status()
        elif choice == '3':
            dmq.display_status()
            node_id = int(input("Enter the ID of the node to remove: "))
            dmq.remove_node(node_id)
            dmq.display_status()
        elif choice == '4':
            dmq.display_status()
            node_id = int(input("Enter the ID of the node to turn off: "))
            dmq.turn_off_node(node_id)
            dmq.display_status()
        elif choice == '5':
            dmq.display_status()
            node_id = int(input("Enter the ID of the node to revive: "))
            dmq.revive_node(node_id)
            dmq.display_status()
        elif choice == '6':
            queue_name = input("Enter the name for the new queue: ")
            quorum = input("Is this a quorum queue? (y/n): ").lower() in ('y', 'yes')
            dmq.create_queue(queue_name, quorum=quorum)
        elif choice == '7':
            queue_name = input("Enter the name of the queue to delete: ")
            dmq.delete_queue(queue_name)
        elif choice == '8':
            dmq.list_queues()
        else:
            print("Invalid choice. Please try again.")

def main():
    """Main function to run the DMQ manager"""
    # Parse arguments
    args = parse_arguments()
    
    # Set verbose logging if requested
    if hasattr(args, 'verbose') and args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Determine if we should use simulation mode
    simulation_mode = False
    if hasattr(args, 'simulation') and args.simulation:
        simulation_mode = True
        logger.info("Running in simulation mode")
    elif not args.command or args.command == 'interactive':
        # Check Docker in interactive mode or when no command is given
        print("=== Distributed Messaging Queue Manager ===")
        
        # Check if Docker is installed and running
        if not check_docker_installed():
            print("\n⚠️ Error: Docker is not installed or not running!")
            
            # Check if it's a permissions issue
            if check_docker_permission() is False:
                print(get_permission_instructions())
                print("\nOr you can run in simulation mode for testing purposes.")
            else:
                print("Please install Docker and start the Docker service:")
                print("  Ubuntu/Debian: sudo apt install docker.io && sudo systemctl start docker")
                print("  MacOS: brew install --cask docker")
                print("  Windows: Download and install from https://www.docker.com/products/docker-desktop/")
                print("\nAlternatively, you can run in simulation mode for testing purposes.")
            
            while True:
                choice = input("\nDo you want to run in simulation mode instead? (y/n): ").lower()
                if choice in ('y', 'yes'):
                    simulation_mode = True
                    break
                elif choice in ('n', 'no'):
                    print("Please resolve Docker access issues and try again. Exiting...")
                    return
                else:
                    print("Please enter 'y' or 'n'.")
        else:
            print("Docker is running and accessible.")
            simulation_mode = False
    else:
        # For command-line operations, check Docker directly
        if not simulation_mode and not check_docker_installed():
            print("\n⚠️ Error: Docker is not installed or not running!")
            print("Use --simulation flag to run in simulation mode.")
            return
    
    # Initialize DMQ cluster
    try:
        dmq = DMQCluster(simulation_mode=simulation_mode)
    except PermissionError:
        print(get_permission_instructions())
        print("Exiting...")
        return
    
    try:
        # Handle commands
        if args.command == 'start':
            print(f"Initializing cluster with {args.nodes} nodes...")
            if not dmq.initialize_cluster(args.nodes):
                print("Failed to initialize cluster. Exiting.")
                return
            print(f"\nSuccessfully initialized cluster with {len(dmq.nodes)} nodes!")
            dmq.display_status()
            
        elif args.command == 'status':
            dmq.display_status()
            
        elif args.command == 'add-node':
            if dmq.add_node():
                print("Node added successfully.")
                dmq.display_status()
            else:
                print("Failed to add node.")
                
        elif args.command == 'remove-node':
            if dmq.remove_node(args.node_id):
                print(f"Node {args.node_id} removed successfully.")
                dmq.display_status()
            else:
                print(f"Failed to remove node {args.node_id}.")
                
        elif args.command == 'stop-node':
            if dmq.turn_off_node(args.node_id):
                print(f"Node {args.node_id} stopped successfully.")
                dmq.display_status()
            else:
                print(f"Failed to stop node {args.node_id}.")
                
        elif args.command == 'revive-node':
            if dmq.revive_node(args.node_id):
                print(f"Node {args.node_id} revived successfully.")
                dmq.display_status()
            else:
                print(f"Failed to revive node {args.node_id}.")
                
        elif args.command == 'create-queue':
            quorum = args.quorum if hasattr(args, 'quorum') else False
            if dmq.create_queue(args.queue_name, quorum=quorum):
                print(f"Queue '{args.queue_name}' created successfully.")
            else:
                print(f"Failed to create queue '{args.queue_name}'.")
                
        elif args.command == 'delete-queue':
            if dmq.delete_queue(args.queue_name):
                print(f"Queue '{args.queue_name}' deleted successfully.")
            else:
                print(f"Failed to delete queue '{args.queue_name}'.")
                
        elif args.command == 'list-queues':
            dmq.list_queues()
                
        elif args.command == 'interactive' or not args.command:
            # Run interactive mode
            interactive_mode(dmq)
    
    except KeyboardInterrupt:
        print("\nInterrupted. Shutting down DMQ manager...")
    except PermissionError:
        print(get_permission_instructions())
    except Exception as e:
        print(f"\nError occurred: {e}")
    finally:
        # Cleanup - only if not simulation mode and if we had permission
        # Don't clean up if this is just a status check or other non-destructive operation
        cleanup_operations = ['start', 'add-node', 'remove-node', 'stop-node', 'revive-node', 'interactive', None]
        if not simulation_mode and dmq.nodes and (not args.command or args.command in cleanup_operations):
            try:
                dmq.cleanup(ask_confirmation=(args.command == 'interactive' or not args.command))
            except PermissionError:
                print("Permission denied when cleaning up Docker resources.")
            except Exception as e:
                print(f"Error during cleanup: {e}")
                
        print("DMQ manager shutdown complete")

if __name__ == "__main__":
    main()