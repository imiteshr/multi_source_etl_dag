from flask import Flask, render_template, jsonify, request
import json
import os
import pandas as pd
from datetime import datetime, timedelta
import psycopg2
from sqlalchemy import create_engine
import plotly.graph_objs as go
import plotly.express as px
import plotly.utils

app = Flask(__name__)

# Database configuration
DB_CONFIG = {
    'host': 'postgres',
    'port': 5432,
    'database': 'airflow',
    'user': 'airflow',
    'password': 'airflow'
}

def get_db_connection():
    """Get database connection"""
    try:
        engine = create_engine(f'postgresql://{DB_CONFIG["user"]}:{DB_CONFIG["password"]}@{DB_CONFIG["host"]}:{DB_CONFIG["port"]}/{DB_CONFIG["database"]}')
        return engine
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

def load_realtime_analytics():
    """Load real-time analytics data"""
    analytics_path = '/opt/airflow/data/analytics/realtime_flight_analytics.json'
    if os.path.exists(analytics_path):
        try:
            with open(analytics_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading analytics: {e}")
    return {}

def load_flight_summary():
    """Load flight summary data"""
    summary_path = '/opt/airflow/data/analytics/realtime_flight_summary.json'
    if os.path.exists(summary_path):
        try:
            with open(summary_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading summary: {e}")
    return {}

@app.route('/')
def realtime_dashboard():
    """Main real-time dashboard"""
    analytics = load_realtime_analytics()
    summary = load_flight_summary()
    
    return render_template('realtime_dashboard.html', 
                         analytics=analytics, 
                         summary=summary,
                         current_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC'))

@app.route('/api/live_data')
def get_live_data():
    """API endpoint for live flight data"""
    try:
        engine = get_db_connection()
        if not engine:
            return jsonify({'error': 'Database connection failed'})
        
        # Get recent flight data
        query = """
        SELECT 
            icao24, callsign, origin_country, latitude, longitude, 
            altitude, speed, heading, airline, aircraft_type, 
            region, data_source, data_quality_score,
            extraction_time
        FROM realtime_global_flights 
        WHERE extraction_time >= NOW() - INTERVAL '1 hour'
        AND latitude IS NOT NULL 
        AND longitude IS NOT NULL
        ORDER BY extraction_time DESC
        LIMIT 1000
        """
        
        df = pd.read_sql(query, engine)
        
        if df.empty:
            return jsonify({'flights': [], 'count': 0})
        
        # Convert to JSON format
        flights = df.to_dict('records')
        
        # Convert datetime objects to strings
        for flight in flights:
            if 'extraction_time' in flight and flight['extraction_time']:
                flight['extraction_time'] = flight['extraction_time'].isoformat()
        
        return jsonify({
            'flights': flights,
            'count': len(flights),
            'last_updated': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/regional_stats')
def get_regional_stats():
    """API endpoint for regional statistics"""
    try:
        engine = get_db_connection()
        if not engine:
            return jsonify({'error': 'Database connection failed'})
        
        query = """
        SELECT 
            region,
            COUNT(*) as flight_count,
            AVG(altitude) as avg_altitude,
            AVG(speed) as avg_speed,
            AVG(data_quality_score) as avg_quality,
            COUNT(DISTINCT origin_country) as countries,
            COUNT(DISTINCT airline) as airlines
        FROM realtime_global_flights 
        WHERE extraction_time >= NOW() - INTERVAL '1 hour'
        GROUP BY region
        ORDER BY flight_count DESC
        """
        
        df = pd.read_sql(query, engine)
        
        if df.empty:
            return jsonify({'regions': []})
        
        # Convert NaN values to None for JSON serialization
        df = df.where(pd.notnull(df), None)
        
        regions = df.to_dict('records')
        
        return jsonify({
            'regions': regions,
            'total_regions': len(regions),
            'last_updated': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/flight_map')
def get_flight_map():
    """API endpoint for flight map visualization"""
    try:
        engine = get_db_connection()
        if not engine:
            return jsonify({'error': 'Database connection failed'})
        
        query = """
        SELECT 
            icao24, callsign, origin_country, latitude, longitude, 
            altitude, speed, airline, region, data_source
        FROM realtime_global_flights 
        WHERE extraction_time >= NOW() - INTERVAL '30 minutes'
        AND latitude IS NOT NULL 
        AND longitude IS NOT NULL
        AND latitude BETWEEN -90 AND 90
        AND longitude BETWEEN -180 AND 180
        ORDER BY extraction_time DESC
        LIMIT 2000
        """
        
        df = pd.read_sql(query, engine)
        
        if df.empty:
            return jsonify({'map_data': []})
        
        # Create map data
        map_data = []
        for _, row in df.iterrows():
            map_data.append({
                'lat': row['latitude'],
                'lon': row['longitude'],
                'callsign': row['callsign'] or 'Unknown',
                'altitude': row['altitude'] or 0,
                'speed': row['speed'] or 0,
                'airline': row['airline'] or 'Unknown',
                'country': row['origin_country'] or 'Unknown',
                'region': row['region'] or 'Unknown',
                'source': row['data_source'] or 'Unknown'
            })
        
        return jsonify({
            'map_data': map_data,
            'count': len(map_data),
            'last_updated': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/data_quality')
def get_data_quality():
    """API endpoint for data quality metrics"""
    try:
        engine = get_db_connection()
        if not engine:
            return jsonify({'error': 'Database connection failed'})
        
        query = """
        SELECT 
            data_source,
            COUNT(*) as total_flights,
            AVG(data_quality_score) as avg_quality,
            COUNT(CASE WHEN has_position THEN 1 END) as with_position,
            COUNT(CASE WHEN has_speed THEN 1 END) as with_speed,
            COUNT(CASE WHEN has_altitude THEN 1 END) as with_altitude,
            COUNT(CASE WHEN data_quality_score >= 80 THEN 1 END) as high_quality
        FROM realtime_global_flights 
        WHERE extraction_time >= NOW() - INTERVAL '1 hour'
        GROUP BY data_source
        ORDER BY total_flights DESC
        """
        
        df = pd.read_sql(query, engine)
        
        if df.empty:
            return jsonify({'sources': []})
        
        # Convert NaN values to None for JSON serialization
        df = df.where(pd.notnull(df), None)
        
        sources = df.to_dict('records')
        
        return jsonify({
            'sources': sources,
            'last_updated': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/live_metrics')
def get_live_metrics():
    """API endpoint for live metrics"""
    try:
        engine = get_db_connection()
        if not engine:
            return jsonify({'error': 'Database connection failed'})
        
        # Get current metrics
        query = """
        SELECT 
            COUNT(*) as total_flights,
            COUNT(DISTINCT origin_country) as countries,
            COUNT(DISTINCT airline) as airlines,
            COUNT(DISTINCT aircraft_type) as aircraft_types,
            AVG(altitude) as avg_altitude,
            AVG(speed) as avg_speed,
            MAX(speed) as max_speed,
            AVG(data_quality_score) as avg_quality
        FROM realtime_global_flights 
        WHERE extraction_time >= NOW() - INTERVAL '30 minutes'
        """
        
        result = pd.read_sql(query, engine)
        
        if result.empty:
            return jsonify({'metrics': {}})
        
        metrics = result.iloc[0].to_dict()
        
        # Convert NaN values to None
        for key, value in metrics.items():
            if pd.isna(value):
                metrics[key] = None
        
        return jsonify({
            'metrics': metrics,
            'last_updated': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/altitude_distribution')
def get_altitude_distribution():
    """API endpoint for altitude distribution"""
    try:
        engine = get_db_connection()
        if not engine:
            return jsonify({'error': 'Database connection failed'})
        
        query = """
        SELECT 
            CASE 
                WHEN altitude < 10000 THEN 'Low (< 10k ft)'
                WHEN altitude < 20000 THEN 'Medium (10k-20k ft)'
                WHEN altitude < 30000 THEN 'High (20k-30k ft)'
                WHEN altitude < 40000 THEN 'Very High (30k-40k ft)'
                ELSE 'Extreme (> 40k ft)'
            END as altitude_range,
            COUNT(*) as count
        FROM realtime_global_flights 
        WHERE extraction_time >= NOW() - INTERVAL '30 minutes'
        AND altitude IS NOT NULL
        GROUP BY altitude_range
        ORDER BY count DESC
        """
        
        df = pd.read_sql(query, engine)
        
        if df.empty:
            return jsonify({'distribution': []})
        
        distribution = df.to_dict('records')
        
        return jsonify({
            'distribution': distribution,
            'last_updated': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5002, debug=True) 