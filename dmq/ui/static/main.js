// Main JavaScript file for DMQ Dashboard

document.addEventListener('DOMContentLoaded', function() {
    // Set up auto-refresh for cluster status
    setInterval(refreshClusterStatus, 5000);
    
    // Set up event listeners
    document.getElementById('refresh-history').addEventListener('click', refreshMessageHistory);
    document.getElementById('publish-form').addEventListener('submit', publishMessage);
    
    // Format timestamps on page load
    formatTimestamps();
});

// Function to refresh cluster status
function refreshClusterStatus() {
    fetch('/api/cluster-status')
        .then(response => response.json())
        .then(data => {
            updateNodesTable(data.nodes);
            updateQueuesTable(data.queues);
        })
        .catch(error => console.error('Error fetching cluster status:', error));
}

// Function to update the nodes table
function updateNodesTable(nodes) {
    const table = document.getElementById('nodes-table');
    table.innerHTML = '';
    
    nodes.forEach(node => {
        const row = document.createElement('tr');
        
        const idCell = document.createElement('td');
        idCell.textContent = node.id;
        
        const hostCell = document.createElement('td');
        hostCell.textContent = node.host;
        
        const portCell = document.createElement('td');
        portCell.textContent = node.port;
        
        const statusCell = document.createElement('td');
        const statusBadge = document.createElement('span');
        statusBadge.className = `badge ${node.status === 'online' ? 'bg-success' : 'bg-danger'}`;
        statusBadge.textContent = node.status;
        statusCell.appendChild(statusBadge);
        
        row.appendChild(idCell);
        row.appendChild(hostCell);
        row.appendChild(portCell);
        row.appendChild(statusCell);
        
        table.appendChild(row);
    });
}

// Function to update the queues table
function updateQueuesTable(queues) {
    const table = document.getElementById('queues-table');
    table.innerHTML = '';
    
    queues.forEach(queue => {
        const row = document.createElement('tr');
        
        const nameCell = document.createElement('td');
        nameCell.textContent = queue.name;
        
        const messagesCell = document.createElement('td');
        messagesCell.textContent = queue.messages;
        
        const statusCell = document.createElement('td');
        const statusBadge = document.createElement('span');
        statusBadge.className = 'badge bg-success';
        statusBadge.textContent = 'Active';
        statusCell.appendChild(statusBadge);
        
        row.appendChild(nameCell);
        row.appendChild(messagesCell);
        row.appendChild(statusCell);
        
        table.appendChild(row);
    });
}

// Function to refresh message history
function refreshMessageHistory() {
    const button = document.getElementById('refresh-history');
    button.disabled = true;
    button.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Refreshing...';
    
    fetch('/api/messages')
        .then(response => response.json())
        .then(messages => {
            updateMessageHistory(messages);
            button.disabled = false;
            button.innerHTML = 'Refresh';
            formatTimestamps();
        })
        .catch(error => {
            console.error('Error fetching message history:', error);
            button.disabled = false;
            button.innerHTML = 'Refresh';
        });
}

// Function to update the message history table
function updateMessageHistory(messages) {
    const table = document.getElementById('message-history');
    table.innerHTML = '';
    
    messages.forEach(message => {
        const row = document.createElement('tr');
        
        // ID cell
        const idCell = document.createElement('td');
        idCell.textContent = message.id;
        
        // Queue cell
        const queueCell = document.createElement('td');
        queueCell.textContent = message.queue;
        
        // Content cell
        const contentCell = document.createElement('td');
        const pre = document.createElement('pre');
        pre.textContent = JSON.stringify(message.content, null, 2);
        contentCell.appendChild(pre);
        
        // Status cell
        const statusCell = document.createElement('td');
        const statusBadge = document.createElement('span');
        statusBadge.className = `badge ${message.status === 'published' ? 'bg-warning' : 'bg-success'}`;
        statusBadge.textContent = message.status;
        statusCell.appendChild(statusBadge);
        
        // Timestamp cell
        const timestampCell = document.createElement('td');
        timestampCell.textContent = new Date(message.timestamp * 1000).toLocaleString();
        timestampCell.setAttribute('data-timestamp', message.timestamp);
        
        row.appendChild(idCell);
        row.appendChild(queueCell);
        row.appendChild(contentCell);
        row.appendChild(statusCell);
        row.appendChild(timestampCell);
        
        table.appendChild(row);
    });
}

// Function to publish a new message
function publishMessage(event) {
    event.preventDefault();
    
    const queueName = document.getElementById('queue-name').value;
    const taskId = document.getElementById('task-id').value;
    const description = document.getElementById('task-description').value;
    const complexity = document.getElementById('complexity').value;
    
    const resultDiv = document.getElementById('publish-result');
    resultDiv.innerHTML = '<div class="alert alert-info">Publishing message...</div>';
    
    const message = {
        task_id: parseInt(taskId),
        description: description,
        complexity: parseInt(complexity)
    };
    
    fetch('/api/publish', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({
            queue: queueName,
            message: message
        })
    })
    .then(response => response.json())
    .then(data => {
        if (data.status === 'success') {
            resultDiv.innerHTML = '<div class="alert alert-success">Message published successfully!</div>';
            // Increment task ID for convenience
            document.getElementById('task-id').value = parseInt(taskId) + 1;
            // Refresh message history
            refreshMessageHistory();
        } else {
            resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${data.message}</div>`;
        }
    })
    .catch(error => {
        console.error('Error publishing message:', error);
        resultDiv.innerHTML = '<div class="alert alert-danger">Error publishing message. Check console for details.</div>';
    });
}

// Function to format timestamps
function formatTimestamps() {
    document.querySelectorAll('[data-timestamp]').forEach(element => {
        const timestamp = element.getAttribute('data-timestamp');
        if (timestamp) {
            element.textContent = new Date(timestamp * 1000).toLocaleString();
        }
    });
}