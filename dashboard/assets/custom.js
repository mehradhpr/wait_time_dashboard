// Custom Dashboard JavaScript

// Initialize tooltips
document.addEventListener('DOMContentLoaded', function() {
    // Bootstrap tooltip initialization
    var tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    var tooltipList = tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });
    
    // Smooth scrolling for anchor links
    document.querySelectorAll('a[href^="#"]').forEach(anchor => {
        anchor.addEventListener('click', function (e) {
            e.preventDefault();
            const target = document.querySelector(this.getAttribute('href'));
            if (target) {
                target.scrollIntoView({
                    behavior: 'smooth',
                    block: 'start'
                });
            }
        });
    });
});

// Chart resize handler
window.addEventListener('resize', function() {
    // Trigger Plotly relayout for responsive charts
    const charts = document.querySelectorAll('.js-plotly-plot');
    charts.forEach(chart => {
        if (chart && chart.layout) {
            Plotly.Plots.resize(chart);
        }
    });
});

// Loading state management
function showLoading(elementId) {
    const element = document.getElementById(elementId);
    if (element) {
        element.innerHTML = `
            <div class="d-flex justify-content-center align-items-center" style="min-height: 200px;">
                <div class="spinner-border text-primary" role="status">
                    <span class="visually-hidden">Loading...</span>
                </div>
            </div>
        `;
    }
}

function hideLoading(elementId) {
    const element = document.getElementById(elementId);
    if (element) {
        const spinner = element.querySelector('.spinner-border');
        if (spinner) {
            spinner.parentElement.style.display = 'none';
        }
    }
}

// Data export functionality
function exportTableToCSV(tableId, filename) {
    const table = document.getElementById(tableId);
    if (!table) return;
    
    let csv = [];
    const rows = table.querySelectorAll('tr');
    
    for (let i = 0; i < rows.length; i++) {
        const row = [], cols = rows[i].querySelectorAll('td, th');
        
        for (let j = 0; j < cols.length; j++) {
            row.push(cols[j].innerText);
        }
        
        csv.push(row.join(','));
    }
    
    // Download CSV
    const csvFile = new Blob([csv.join('\\n')], {type: 'text/csv'});
    const downloadLink = document.createElement('a');
    downloadLink.download = filename || 'data.csv';
    downloadLink.href = window.URL.createObjectURL(csvFile);
    downloadLink.style.display = 'none';
    document.body.appendChild(downloadLink);
    downloadLink.click();
    document.body.removeChild(downloadLink);
}

// Theme management
function setTheme(theme) {
    document.documentElement.setAttribute('data-theme', theme);
    localStorage.setItem('dashboard-theme', theme);
}

function getTheme() {
    return localStorage.getItem('dashboard-theme') || 'light';
}

// Initialize theme
document.addEventListener('DOMContentLoaded', function() {
    setTheme(getTheme());
});

// Utility functions
const DashboardUtils = {
    formatNumber: function(num, decimals = 1) {
        if (isNaN(num)) return 'N/A';
        return num.toFixed(decimals);
    },
    
    formatPercentage: function(num, decimals = 1) {
        if (isNaN(num)) return 'N/A';
        return num.toFixed(decimals) + '%';
    },
    
    debounce: function(func, wait) {
        let timeout;
        return function(...args) {
            clearTimeout(timeout);
            timeout = setTimeout(() => func.apply(this, args), wait);
        };
    }
};

// Make utils globally available
window.DashboardUtils = DashboardUtils;