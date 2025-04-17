/**
 * Main JavaScript file for DMQ RabbitMQ Testing App
 */

// Common utilities for the DMQ RabbitMQ application
const DMQ = {
    // Format a JSON string for display
    formatJSON: function(json) {
        if (typeof json === 'string') {
            try {
                json = JSON.parse(json);
            } catch (e) {
                return json; // Not valid JSON, return as is
            }
        }
        return JSON.stringify(json, null, 2);
    },

    // Show a temporary notification
    notify: function(message, type = 'success', duration = 3000) {
        const alertClass = `alert-${type}`;
        const alert = $(`<div class="alert ${alertClass} alert-dismissible fade show" role="alert">
            ${message}
            <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
        </div>`);
        
        $('.container').first().prepend(alert);
        
        // Auto-dismiss after duration
        setTimeout(() => {
            alert.alert('close');
        }, duration);
    },

    // Convert form data to an object
    formToObject: function(form) {
        const formData = new FormData(form);
        const obj = {};
        
        for (const [key, value] of formData.entries()) {
            obj[key] = value;
        }
        
        return obj;
    },

    // Generate a unique ID
    generateId: function() {
        return Date.now().toString(36) + Math.random().toString(36).substr(2);
    }
};

// Initialize Bootstrap tooltips and popovers when document is ready
$(document).ready(function() {
    // Initialize Bootstrap tooltips
    const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });

    // Initialize Bootstrap popovers
    const popoverTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="popover"]'));
    popoverTriggerList.map(function (popoverTriggerEl) {
        return new bootstrap.Popover(popoverTriggerEl);
    });
});