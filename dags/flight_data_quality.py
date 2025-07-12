from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

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
    'flight_data_quality_check',
    default_args=default_args,
    description='Data quality monitoring for flight data',
    schedule_interval=timedelta(hours=12),
    catchup=False,
    tags=['data-quality', 'flights', 'monitoring'],
)

def check_data_freshness(**context):
    """Check if data is fresh (updated within expected timeframe)"""
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Check airports table
        airports_query = """
        SELECT COUNT(*) as count, MAX(extraction_date) as latest_date 
        FROM airports
        """
        airports_result = postgres_hook.get_first(airports_query)
        
        # Check scheduled flights table
        flights_query = """
        SELECT COUNT(*) as count, MAX(extraction_date) as latest_date 
        FROM scheduled_flights
        """
        flights_result = postgres_hook.get_first(flights_query)
        
        print(f"Airports: {airports_result[0]} records, latest: {airports_result[1]}")
        print(f"Scheduled flights: {flights_result[0]} records, latest: {flights_result[1]}")
        
        # Check if data is from today
        today = datetime.now().date()
        
        if airports_result[1] and str(airports_result[1]) != str(today):
            print(f"WARNING: Airport data is not from today. Latest: {airports_result[1]}")
        
        if flights_result[1] and str(flights_result[1]) != str(today):
            print(f"WARNING: Flight data is not from today. Latest: {flights_result[1]}")
            
    except Exception as e:
        print(f"Error checking data freshness: {str(e)}")
        raise

def check_data_completeness(**context):
    """Check for data completeness and missing values"""
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Check airports completeness
        airports_query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(code) as code_count,
            COUNT(name) as name_count,
            COUNT(latitude) as lat_count,
            COUNT(longitude) as lon_count
        FROM airports
        """
        airports_result = postgres_hook.get_first(airports_query)
        
        print(f"Airports completeness check:")
        print(f"Total records: {airports_result[0]}")
        print(f"Code completeness: {airports_result[1]}/{airports_result[0]} ({airports_result[1]/airports_result[0]*100:.1f}%)")
        print(f"Name completeness: {airports_result[2]}/{airports_result[0]} ({airports_result[2]/airports_result[0]*100:.1f}%)")
        print(f"Coordinates completeness: {airports_result[3]}/{airports_result[0]} ({airports_result[3]/airports_result[0]*100:.1f}%)")
        
        # Check scheduled flights completeness
        flights_query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(flight_number) as flight_num_count,
            COUNT(origin_airport) as origin_count,
            COUNT(destination_airport) as dest_count,
            COUNT(scheduled_departure) as dept_count
        FROM scheduled_flights
        """
        flights_result = postgres_hook.get_first(flights_query)
        
        print(f"Scheduled flights completeness check:")
        print(f"Total records: {flights_result[0]}")
        print(f"Flight number completeness: {flights_result[1]}/{flights_result[0]} ({flights_result[1]/flights_result[0]*100:.1f}%)")
        print(f"Origin completeness: {flights_result[2]}/{flights_result[0]} ({flights_result[2]/flights_result[0]*100:.1f}%)")
        print(f"Destination completeness: {flights_result[3]}/{flights_result[0]} ({flights_result[3]/flights_result[0]*100:.1f}%)")
        
    except Exception as e:
        print(f"Error checking data completeness: {str(e)}")
        raise

def check_data_validity(**context):
    """Check for data validity and anomalies"""
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Check for duplicate flight numbers
        duplicate_query = """
        SELECT flight_number, COUNT(*) as count
        FROM scheduled_flights
        GROUP BY flight_number
        HAVING COUNT(*) > 1
        LIMIT 10
        """
        duplicates = postgres_hook.get_records(duplicate_query)
        
        if duplicates:
            print(f"WARNING: Found {len(duplicates)} duplicate flight numbers:")
            for dup in duplicates:
                print(f"  {dup[0]}: {dup[1]} occurrences")
        else:
            print("No duplicate flight numbers found")
        
        # Check for invalid coordinates in airports
        invalid_coords_query = """
        SELECT code, latitude, longitude
        FROM airports
        WHERE latitude NOT BETWEEN -90 AND 90 
           OR longitude NOT BETWEEN -180 AND 180
        """
        invalid_coords = postgres_hook.get_records(invalid_coords_query)
        
        if invalid_coords:
            print(f"WARNING: Found {len(invalid_coords)} airports with invalid coordinates:")
            for coord in invalid_coords:
                print(f"  {coord[0]}: lat={coord[1]}, lon={coord[2]}")
        else:
            print("All airport coordinates are valid")
        
        # Check for flights with same origin and destination
        same_airports_query = """
        SELECT flight_number, origin_airport, destination_airport
        FROM scheduled_flights
        WHERE origin_airport = destination_airport
        LIMIT 5
        """
        same_airports = postgres_hook.get_records(same_airports_query)
        
        if same_airports:
            print(f"WARNING: Found {len(same_airports)} flights with same origin and destination:")
            for flight in same_airports:
                print(f"  {flight[0]}: {flight[1]} -> {flight[2]}")
        else:
            print("No flights with same origin and destination found")
            
    except Exception as e:
        print(f"Error checking data validity: {str(e)}")
        raise

def generate_data_summary(**context):
    """Generate summary statistics for the flight data"""
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Airlines summary
        airlines_query = """
        SELECT airline, COUNT(*) as flight_count
        FROM scheduled_flights
        GROUP BY airline
        ORDER BY flight_count DESC
        """
        airlines = postgres_hook.get_records(airlines_query)
        
        print("Airlines summary:")
        for airline in airlines:
            print(f"  {airline[0]}: {airline[1]} flights")
        
        # Route summary
        routes_query = """
        SELECT origin_airport, destination_airport, COUNT(*) as route_count
        FROM scheduled_flights
        GROUP BY origin_airport, destination_airport
        ORDER BY route_count DESC
        LIMIT 10
        """
        routes = postgres_hook.get_records(routes_query)
        
        print("\nTop 10 routes:")
        for route in routes:
            print(f"  {route[0]} -> {route[1]}: {route[2]} flights")
        
        # Status summary
        status_query = """
        SELECT status, COUNT(*) as status_count
        FROM scheduled_flights
        GROUP BY status
        ORDER BY status_count DESC
        """
        statuses = postgres_hook.get_records(status_query)
        
        print("\nFlight status summary:")
        for status in statuses:
            print(f"  {status[0]}: {status[1]} flights")
            
    except Exception as e:
        print(f"Error generating data summary: {str(e)}")
        raise

# Define tasks
freshness_check = PythonOperator(
    task_id='check_data_freshness',
    python_callable=check_data_freshness,
    dag=dag,
)

completeness_check = PythonOperator(
    task_id='check_data_completeness',
    python_callable=check_data_completeness,
    dag=dag,
)

validity_check = PythonOperator(
    task_id='check_data_validity',
    python_callable=check_data_validity,
    dag=dag,
)

summary_task = PythonOperator(
    task_id='generate_data_summary',
    python_callable=generate_data_summary,
    dag=dag,
)

# Set up task dependencies
[freshness_check, completeness_check, validity_check] >> summary_task 