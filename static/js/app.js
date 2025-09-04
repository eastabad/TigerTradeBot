// Main application JavaScript

// Global variables
let statusCheckInterval;

// Initialize application when DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    initializeApp();
    startStatusMonitoring();
});

// Initialize the application
function initializeApp() {
    console.log('Trading System initialized');
    
    // Add click handlers for copy buttons
    const copyButtons = document.querySelectorAll('[onclick*="copy"]');
    copyButtons.forEach(button => {
        button.addEventListener('click', function(e) {
            e.stopPropagation();
        });
    });
    
    // Initialize tooltips if Bootstrap is available
    if (typeof bootstrap !== 'undefined') {
        const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
        tooltipTriggerList.map(function (tooltipTriggerEl) {
            return new bootstrap.Tooltip(tooltipTriggerEl);
        });
    }
}

// Start monitoring system status
function startStatusMonitoring() {
    // Check status immediately
    checkSystemStatus();
    
    // Set up periodic status checks
    statusCheckInterval = setInterval(checkSystemStatus, 30000); // Every 30 seconds
}

// Check system status
function checkSystemStatus() {
    const statusIndicator = document.getElementById('status-indicator');
    const statusText = document.getElementById('status-text');
    
    fetch('/api/health')
        .then(response => {
            if (!response.ok) {
                throw new Error('Network response was not ok');
            }
            return response.json();
        })
        .then(data => {
            updateStatusIndicator(true, 'Online');
            updateTradingStatus(data.trading_enabled);
        })
        .catch(error => {
            console.error('Status check failed:', error);
            updateStatusIndicator(false, 'Offline');
        });
}

// Update status indicator
function updateStatusIndicator(online, text) {
    const statusIndicator = document.getElementById('status-indicator');
    const statusText = document.getElementById('status-text');
    
    if (statusIndicator && statusText) {
        statusIndicator.className = `fas fa-circle ${online ? 'text-success' : 'text-danger'} me-1`;
        statusText.textContent = text;
    }
}

// Update trading status
function updateTradingStatus(enabled) {
    const tradingStatusElements = document.querySelectorAll('.trading-status');
    tradingStatusElements.forEach(element => {
        if (enabled) {
            element.classList.remove('text-danger');
            element.classList.add('text-success');
            element.textContent = 'Enabled';
        } else {
            element.classList.remove('text-success');
            element.classList.add('text-danger');
            element.textContent = 'Disabled';
        }
    });
}

// Utility function to copy text to clipboard
function copyToClipboard(text, button = null) {
    navigator.clipboard.writeText(text).then(function() {
        if (button) {
            showCopySuccess(button);
        }
        console.log('Text copied to clipboard');
    }).catch(function(err) {
        console.error('Failed to copy text: ', err);
        
        // Fallback for older browsers
        const textArea = document.createElement('textarea');
        textArea.value = text;
        document.body.appendChild(textArea);
        textArea.focus();
        textArea.select();
        
        try {
            document.execCommand('copy');
            if (button) {
                showCopySuccess(button);
            }
        } catch (fallbackErr) {
            console.error('Fallback copy failed: ', fallbackErr);
        }
        
        document.body.removeChild(textArea);
    });
}

// Show copy success feedback
function showCopySuccess(button) {
    const originalContent = button.innerHTML;
    const originalClasses = button.className;
    
    // Update button to show success
    button.innerHTML = '<i class="fas fa-check"></i>';
    button.className = button.className.replace('btn-outline-secondary', 'btn-success');
    
    // Reset after 2 seconds
    setTimeout(() => {
        button.innerHTML = originalContent;
        button.className = originalClasses;
    }, 2000);
}

// Format currency
function formatCurrency(amount, currency = 'USD') {
    if (amount === null || amount === undefined) {
        return '-';
    }
    
    return new Intl.NumberFormat('en-US', {
        style: 'currency',
        currency: currency,
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
    }).format(amount);
}

// Format date/time
function formatDateTime(dateString, options = {}) {
    if (!dateString) return '-';
    
    const defaultOptions = {
        year: 'numeric',
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit'
    };
    
    const formatOptions = { ...defaultOptions, ...options };
    
    try {
        const date = new Date(dateString);
        return new Intl.DateTimeFormat('en-US', formatOptions).format(date);
    } catch (error) {
        console.error('Date formatting error:', error);
        return dateString;
    }
}

// Show loading state on element
function showLoading(element, text = 'Loading...') {
    if (!element) return;
    
    element.classList.add('loading');
    const originalContent = element.innerHTML;
    element.setAttribute('data-original-content', originalContent);
    element.innerHTML = `<i class="fas fa-spinner fa-spin me-1"></i>${text}`;
}

// Hide loading state
function hideLoading(element) {
    if (!element) return;
    
    element.classList.remove('loading');
    const originalContent = element.getAttribute('data-original-content');
    if (originalContent) {
        element.innerHTML = originalContent;
        element.removeAttribute('data-original-content');
    }
}

// Show notification/toast
function showNotification(message, type = 'info', duration = 5000) {
    // Create notification element
    const notification = document.createElement('div');
    notification.className = `alert alert-${type} alert-dismissible fade show position-fixed`;
    notification.style.cssText = `
        top: 20px;
        right: 20px;
        z-index: 9999;
        min-width: 300px;
        box-shadow: 0 0.5rem 1rem rgba(0, 0, 0, 0.3);
    `;
    
    notification.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
    `;
    
    // Add to page
    document.body.appendChild(notification);
    
    // Auto-remove after duration
    if (duration > 0) {
        setTimeout(() => {
            if (notification.parentNode) {
                notification.remove();
            }
        }, duration);
    }
}

// Debounce function for API calls
function debounce(func, wait, immediate = false) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            timeout = null;
            if (!immediate) func(...args);
        };
        const callNow = immediate && !timeout;
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
        if (callNow) func(...args);
    };
}

// Validate form data
function validateForm(formElement) {
    if (!formElement) return false;
    
    const requiredFields = formElement.querySelectorAll('[required]');
    let isValid = true;
    
    requiredFields.forEach(field => {
        if (!field.value.trim()) {
            field.classList.add('is-invalid');
            isValid = false;
        } else {
            field.classList.remove('is-invalid');
        }
    });
    
    return isValid;
}

// Handle fetch errors
function handleFetchError(error, context = 'API request') {
    console.error(`${context} failed:`, error);
    
    let message = 'An error occurred. Please try again.';
    
    if (error.name === 'NetworkError' || error.message.includes('fetch')) {
        message = 'Network error. Please check your connection.';
    } else if (error.name === 'AbortError') {
        message = 'Request timed out. Please try again.';
    }
    
    showNotification(message, 'danger');
    return message;
}

// Cleanup when page unloads
window.addEventListener('beforeunload', function() {
    if (statusCheckInterval) {
        clearInterval(statusCheckInterval);
    }
});

// Export functions for global use
window.TradingSystem = {
    copyToClipboard,
    formatCurrency,
    formatDateTime,
    showLoading,
    hideLoading,
    showNotification,
    debounce,
    validateForm,
    handleFetchError
};
