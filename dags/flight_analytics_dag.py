from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import json
import os
from sqlalchemy import create_engine

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'flight_analytics_dag',
    default_args=default_args,
    description='Flight data analytics and insights generation',
    schedule_interval=timedelta(hours=12),
    catchup=False,
    tags=['analytics', 'flights', 'insights'],
)

def get_sqlalchemy_engine():
    """Get SQLAlchemy engine for pandas operations"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = postgres_hook.get_connection('postgres_default')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    return engine

def analyze_airport_traffic(**context):
    """Analyze airport traffic patterns"""
    try:
        engine = get_sqlalchemy_engine()
        
        # Airport traffic analysis
        traffic_query = """
        SELECT 
            origin_airport,
            destination_airport,
            COUNT(*) as flight_count,
            COUNT(DISTINCT airline) as airline_count,
            AVG(flight_duration_minutes) as avg_duration_minutes
        FROM scheduled_flights 
        GROUP BY origin_airport, destination_airport
        ORDER BY flight_count DESC
        LIMIT 20;
        """
        
        traffic_data = pd.read_sql(traffic_query, engine)
        
        if not traffic_data.empty:
            # Save to JSON for dashboard
            traffic_analysis = {
                'top_routes': traffic_data.to_dict('records'),
                'total_routes': len(traffic_data),
                'analysis_timestamp': datetime.now().isoformat()
            }
            
            # Save analysis results
            os.makedirs('/opt/airflow/data/analytics', exist_ok=True)
            with open('/opt/airflow/data/analytics/airport_traffic.json', 'w') as f:
                json.dump(traffic_analysis, f, indent=2)
            
            print(f"✅ Airport traffic analysis completed - {len(traffic_data)} routes analyzed")
            print(f"Top route: {traffic_data.iloc[0]['origin_airport']} -> {traffic_data.iloc[0]['destination_airport']} ({traffic_data.iloc[0]['flight_count']} flights)")
        else:
            print("⚠️ No flight data available for analysis")
            
    except Exception as e:
        print(f"❌ Error in airport traffic analysis: {str(e)}")
        raise

def analyze_airline_performance(**context):
    """Analyze airline performance metrics"""
    try:
        engine = get_sqlalchemy_engine()
        
        # Airline performance analysis
        performance_query = """
        SELECT 
            airline,
            COUNT(*) as total_flights,
            COUNT(CASE WHEN status = 'On Time' THEN 1 END) as on_time_flights,
            COUNT(CASE WHEN status = 'Delayed' THEN 1 END) as delayed_flights,
            COUNT(CASE WHEN status = 'Cancelled' THEN 1 END) as cancelled_flights,
            ROUND(
                COUNT(CASE WHEN status = 'On Time' THEN 1 END) * 100.0 / COUNT(*), 2
            ) as on_time_percentage,
            AVG(flight_duration_minutes) as avg_flight_duration,
            COUNT(DISTINCT origin_airport) as airports_served
        FROM scheduled_flights 
        GROUP BY airline
        ORDER BY total_flights DESC;
        """
        
        performance_data = pd.read_sql(performance_query, engine)
        
        if not performance_data.empty:
            # Calculate additional metrics
            performance_data['delay_rate'] = performance_data['delayed_flights'] / performance_data['total_flights'] * 100
            performance_data['cancellation_rate'] = performance_data['cancelled_flights'] / performance_data['total_flights'] * 100
            
            # Save analysis results
            airline_analysis = {
                'airline_performance': performance_data.to_dict('records'),
                'best_on_time_airline': performance_data.loc[performance_data['on_time_percentage'].idxmax()]['airline'],
                'busiest_airline': performance_data.loc[performance_data['total_flights'].idxmax()]['airline'],
                'analysis_timestamp': datetime.now().isoformat()
            }
            
            with open('/opt/airflow/data/analytics/airline_performance.json', 'w') as f:
                json.dump(airline_analysis, f, indent=2)
            
            print(f"✅ Airline performance analysis completed - {len(performance_data)} airlines analyzed")
            print(f"Best on-time performance: {airline_analysis['best_on_time_airline']}")
            print(f"Busiest airline: {airline_analysis['busiest_airline']}")
        else:
            print("⚠️ No airline data available for analysis")
            
    except Exception as e:
        print(f"❌ Error in airline performance analysis: {str(e)}")
        raise

def analyze_flight_patterns(**context):
    """Analyze flight patterns and trends"""
    try:
        engine = get_sqlalchemy_engine()
        
        # Flight patterns analysis - cast scheduled_departure to timestamp first
        patterns_query = """
        SELECT 
            EXTRACT(HOUR FROM scheduled_departure::timestamp) as departure_hour,
            COUNT(*) as flight_count,
            AVG(flight_duration_minutes) as avg_duration,
            COUNT(DISTINCT airline) as airlines_operating
        FROM scheduled_flights 
        WHERE scheduled_departure IS NOT NULL 
        GROUP BY EXTRACT(HOUR FROM scheduled_departure::timestamp)
        ORDER BY departure_hour;
        """
        
        patterns_data = pd.read_sql(patterns_query, engine)
        
        if not patterns_data.empty:
            # Aircraft type analysis
            aircraft_query = """
            SELECT 
                aircraft_type,
                COUNT(*) as flight_count,
                AVG(flight_duration_minutes) as avg_duration,
                COUNT(DISTINCT airline) as airlines_using
            FROM scheduled_flights 
            GROUP BY aircraft_type
            ORDER BY flight_count DESC;
            """
            
            aircraft_data = pd.read_sql(aircraft_query, engine)
            
            # Save analysis results
            patterns_analysis = {
                'hourly_patterns': patterns_data.to_dict('records'),
                'aircraft_usage': aircraft_data.to_dict('records'),
                'peak_hour': int(patterns_data.loc[patterns_data['flight_count'].idxmax()]['departure_hour']),
                'most_used_aircraft': aircraft_data.iloc[0]['aircraft_type'] if not aircraft_data.empty else 'N/A',
                'analysis_timestamp': datetime.now().isoformat()
            }
            
            with open('/opt/airflow/data/analytics/flight_patterns.json', 'w') as f:
                json.dump(patterns_analysis, f, indent=2)
            
            print(f"✅ Flight patterns analysis completed")
            print(f"Peak departure hour: {patterns_analysis['peak_hour']}:00")
            print(f"Most used aircraft: {patterns_analysis['most_used_aircraft']}")
        else:
            print("⚠️ No flight pattern data available for analysis")
            
    except Exception as e:
        print(f"❌ Error in flight patterns analysis: {str(e)}")
        raise

def generate_executive_summary(**context):
    """Generate executive summary report"""
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Get overall statistics
        summary_query = """
        SELECT 
            COUNT(*) as total_flights,
            COUNT(DISTINCT airline) as total_airlines,
            COUNT(DISTINCT origin_airport) as origin_airports,
            COUNT(DISTINCT destination_airport) as destination_airports,
            COUNT(DISTINCT aircraft_type) as aircraft_types,
            AVG(flight_duration_minutes) as avg_flight_duration,
            MIN(scheduled_departure::timestamp) as earliest_flight,
            MAX(scheduled_departure::timestamp) as latest_flight
        FROM scheduled_flights
        WHERE scheduled_departure IS NOT NULL;
        """
        
        summary_data = postgres_hook.get_first(summary_query)
        
        # Get real-time flight count
        realtime_query = "SELECT COUNT(*) FROM realtime_flights;"
        realtime_count = postgres_hook.get_first(realtime_query)[0] if postgres_hook.get_first(realtime_query) else 0
        
        # Get airports count
        airports_query = "SELECT COUNT(*) FROM airports;"
        airports_count = postgres_hook.get_first(airports_query)[0] if postgres_hook.get_first(airports_query) else 0
        
        # Create executive summary
        executive_summary = {
            'data_overview': {
                'total_scheduled_flights': summary_data[0],
                'total_realtime_flights': realtime_count,
                'total_airports_in_system': airports_count,
                'unique_airlines': summary_data[1],
                'origin_airports': summary_data[2],
                'destination_airports': summary_data[3],
                'aircraft_types': summary_data[4]
            },
            'operational_metrics': {
                'average_flight_duration_minutes': round(summary_data[5], 2) if summary_data[5] else 0,
                'average_flight_duration_hours': round(summary_data[5] / 60, 2) if summary_data[5] else 0,
                'data_coverage_start': str(summary_data[6]) if summary_data[6] else 'N/A',
                'data_coverage_end': str(summary_data[7]) if summary_data[7] else 'N/A'
            },
            'data_quality': {
                'scheduled_flights_completeness': 'Good' if summary_data[0] > 0 else 'No Data',
                'realtime_data_availability': 'Available' if realtime_count > 0 else 'Limited',
                'airport_data_coverage': 'Complete' if airports_count > 0 else 'Missing'
            },
            'generated_at': datetime.now().isoformat(),
            'pipeline_status': 'Operational'
        }
        
        # Save executive summary
        with open('/opt/airflow/data/analytics/executive_summary.json', 'w') as f:
            json.dump(executive_summary, f, indent=2)
        
        print("✅ Executive Summary Generated")
        print(f"📊 Total Flights: {executive_summary['data_overview']['total_scheduled_flights']}")
        print(f"✈️ Airlines: {executive_summary['data_overview']['unique_airlines']}")
        print(f"🏢 Airports: {executive_summary['data_overview']['total_airports_in_system']}")
        print(f"⏱️ Avg Flight Duration: {executive_summary['operational_metrics']['average_flight_duration_hours']} hours")
        
    except Exception as e:
        print(f"❌ Error generating executive summary: {str(e)}")
        raise

def create_data_export(**context):
    """Export data for external analysis"""
    try:
        engine = get_sqlalchemy_engine()
        
        # Export scheduled flights
        flights_df = pd.read_sql("SELECT * FROM scheduled_flights;", engine)
        if not flights_df.empty:
            flights_df.to_csv('/opt/airflow/data/analytics/scheduled_flights_export.csv', index=False)
            print(f"✅ Exported {len(flights_df)} scheduled flights to CSV")
        
        # Export airports
        airports_df = pd.read_sql("SELECT * FROM airports;", engine)
        if not airports_df.empty:
            airports_df.to_csv('/opt/airflow/data/analytics/airports_export.csv', index=False)
            print(f"✅ Exported {len(airports_df)} airports to CSV")
        
        # Export realtime flights if available
        try:
            realtime_df = pd.read_sql("SELECT * FROM realtime_flights;", engine)
            if not realtime_df.empty:
                realtime_df.to_csv('/opt/airflow/data/analytics/realtime_flights_export.csv', index=False)
                print(f"✅ Exported {len(realtime_df)} real-time flights to CSV")
        except:
            print("ℹ️ No real-time flight data available for export")
        
        print("✅ Data export completed successfully")
        
    except Exception as e:
        print(f"❌ Error in data export: {str(e)}")
        raise

# Define tasks
airport_traffic_task = PythonOperator(
    task_id='analyze_airport_traffic',
    python_callable=analyze_airport_traffic,
    dag=dag,
)

airline_performance_task = PythonOperator(
    task_id='analyze_airline_performance',
    python_callable=analyze_airline_performance,
    dag=dag,
)

flight_patterns_task = PythonOperator(
    task_id='analyze_flight_patterns',
    python_callable=analyze_flight_patterns,
    dag=dag,
)

executive_summary_task = PythonOperator(
    task_id='generate_executive_summary',
    python_callable=generate_executive_summary,
    dag=dag,
)

data_export_task = PythonOperator(
    task_id='create_data_export',
    python_callable=create_data_export,
    dag=dag,
)

# Set up task dependencies
[airport_traffic_task, airline_performance_task, flight_patterns_task] >> executive_summary_task >> data_export_task 