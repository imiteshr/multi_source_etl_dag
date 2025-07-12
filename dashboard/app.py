from flask import Flask, render_template, jsonify
import json
import os
from datetime import datetime

app = Flask(__name__)

def load_analytics_data():
    """Load analytics data from JSON files"""
    analytics_path = '/opt/airflow/data/analytics'
    data = {}
    
    # Load all analytics files
    analytics_files = {
        'executive_summary': 'executive_summary.json',
        'airport_traffic': 'airport_traffic.json',
        'airline_performance': 'airline_performance.json',
        'flight_patterns': 'flight_patterns.json'
    }
    
    for key, filename in analytics_files.items():
        file_path = os.path.join(analytics_path, filename)
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r') as f:
                    data[key] = json.load(f)
            except Exception as e:
                print(f"Error loading {filename}: {e}")
                data[key] = {}
        else:
            data[key] = {}
    
    return data

@app.route('/')
def dashboard():
    """Main dashboard page"""
    data = load_analytics_data()
    return render_template('dashboard.html', 
                         executive_summary=data.get('executive_summary', {}),
                         airport_traffic=data.get('airport_traffic', {}),
                         airline_performance=data.get('airline_performance', {}),
                         flight_patterns=data.get('flight_patterns', {}))

@app.route('/api/analytics')
def api_analytics():
    """API endpoint for analytics data"""
    data = load_analytics_data()
    return jsonify(data)

@app.route('/api/summary')
def api_summary():
    """API endpoint for executive summary"""
    data = load_analytics_data()
    return jsonify(data.get('executive_summary', {}))

@app.route('/health')
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'service': 'flight-analytics-dashboard'
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True) 