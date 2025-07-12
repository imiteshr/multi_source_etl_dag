from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import requests
import json
import os
from typing import Dict, List, Optional
import logging
from sqlalchemy import create_engine

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

# Create the DAG
dag = DAG(
    'realtime_global_flight_data',
    default_args=default_args,
    description='Real-time global flight data extraction from multiple sources',
    schedule_interval=timedelta(minutes=15),  # Every 15 minutes for real-time updates
    catchup=False,
    tags=['real-time', 'global', 'flights', 'multi-source'],
)

# API Configuration (In production, use Airflow Variables or Secrets)
API_CONFIGS = {
    'opensky': {
        'base_url': 'https://opensky-network.org/api',
        'rate_limit': 10,  # requests per second
        'free_tier': True
    },
    'aviationstack': {
        'base_url': 'http://api.aviationstack.com/v1',
        'api_key': None,  # Set via Airflow Variable: aviationstack_api_key
        'rate_limit': 1000,  # requests per month for free tier
        'free_tier': True
    },
    'airlabs': {
        'base_url': 'https://airlabs.co/api/v9',
        'api_key': None,  # Set via Airflow Variable: airlabs_api_key
        'rate_limit': 1000,  # requests per month for free tier
        'free_tier': True
    }
}

def get_sqlalchemy_engine():
    """Get SQLAlchemy engine for pandas operations"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = postgres_hook.get_connection('postgres_default')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    return engine

def extract_opensky_data(**context) -> str:
    """Extract real-time flight data from OpenSky Network API (Free)"""
    try:
        print("🌍 Fetching global flight data from OpenSky Network...")
        
        # Get all states (worldwide)
        url = f"{API_CONFIGS['opensky']['base_url']}/states/all"
        
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        if data and 'states' in data and data['states']:
            columns = [
                'icao24', 'callsign', 'origin_country', 'time_position',
                'last_contact', 'longitude', 'latitude', 'baro_altitude',
                'on_ground', 'velocity', 'true_track', 'vertical_rate',
                'sensors', 'geo_altitude', 'squawk', 'spi', 'position_source'
            ]
            
            # Process and clean data
            clean_states = []
            for state in data['states']:
                if state and len(state) >= len(columns):
                    clean_state = state[:len(columns)]
                    clean_states.append(clean_state)
            
            if clean_states:
                df = pd.DataFrame(clean_states, columns=columns)
                
                # Enhanced data processing
                df['data_source'] = 'opensky'
                df['extraction_time'] = datetime.now().isoformat()
                df['data_timestamp'] = data.get('time', int(datetime.now().timestamp()))
                
                # Clean and validate data
                df['callsign'] = df['callsign'].astype(str).str.strip().replace('None', '')
                df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
                df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
                df['altitude'] = pd.to_numeric(df['baro_altitude'], errors='coerce')
                df['speed'] = pd.to_numeric(df['velocity'], errors='coerce')
                df['heading'] = pd.to_numeric(df['true_track'], errors='coerce')
                
                # Remove invalid coordinates
                df = df.dropna(subset=['icao24', 'latitude', 'longitude'])
                df = df[
                    (df['latitude'].between(-90, 90)) & 
                    (df['longitude'].between(-180, 180))
                ]
                
                # Add geographic regions
                df['region'] = df.apply(lambda row: classify_region(row['latitude'], row['longitude']), axis=1)
                
                output_path = '/opt/airflow/data/opensky_realtime.csv'
                df.to_csv(output_path, index=False)
                
                print(f"✅ OpenSky: Extracted {len(df)} flights from {len(df['origin_country'].unique())} countries")
                print(f"📊 Regional distribution: {df['region'].value_counts().to_dict()}")
                
                return output_path
            else:
                print("⚠️ No valid flight data found in OpenSky response")
        else:
            print("⚠️ No flight data available from OpenSky API")
            
    except Exception as e:
        print(f"❌ Error extracting OpenSky data: {str(e)}")
        
    # Create empty file to prevent downstream failures
    output_path = '/opt/airflow/data/opensky_realtime.csv'
    pd.DataFrame().to_csv(output_path, index=False)
    return output_path

def extract_aviationstack_data(**context) -> str:
    """Extract real-time flight data from AviationStack API"""
    try:
        from airflow.models import Variable
        
        # Get API key from Airflow Variables (optional)
        try:
            api_key = Variable.get("aviationstack_api_key", default_var=None)
        except:
            api_key = None
            
        if not api_key:
            print("⚠️ AviationStack API key not configured, skipping...")
            output_path = '/opt/airflow/data/aviationstack_realtime.csv'
            pd.DataFrame().to_csv(output_path, index=False)
            return output_path
            
        print("🌍 Fetching flight data from AviationStack...")
        
        # Get real-time flights
        url = f"{API_CONFIGS['aviationstack']['base_url']}/flights"
        params = {
            'access_key': api_key,
            'limit': 100  # Free tier limitation
        }
        
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        if data and 'data' in data and data['data']:
            flights = []
            
            for flight in data['data']:
                flight_data = {
                    'icao24': flight.get('flight', {}).get('icao', ''),
                    'callsign': flight.get('flight', {}).get('iata', ''),
                    'origin_country': flight.get('departure', {}).get('country', ''),
                    'latitude': flight.get('live', {}).get('latitude'),
                    'longitude': flight.get('live', {}).get('longitude'),
                    'altitude': flight.get('live', {}).get('altitude'),
                    'speed': flight.get('live', {}).get('speed_horizontal'),
                    'heading': flight.get('live', {}).get('direction'),
                    'departure_airport': flight.get('departure', {}).get('iata', ''),
                    'arrival_airport': flight.get('arrival', {}).get('iata', ''),
                    'airline': flight.get('airline', {}).get('iata', ''),
                    'aircraft_type': flight.get('aircraft', {}).get('iata', ''),
                    'flight_status': flight.get('flight_status', ''),
                    'data_source': 'aviationstack',
                    'extraction_time': datetime.now().isoformat()
                }
                flights.append(flight_data)
            
            df = pd.DataFrame(flights)
            
            # Clean and validate data
            df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
            df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
            
            # Remove invalid coordinates
            df = df.dropna(subset=['latitude', 'longitude'])
            df = df[
                (df['latitude'].between(-90, 90)) & 
                (df['longitude'].between(-180, 180))
            ]
            
            # Add geographic regions
            df['region'] = df.apply(lambda row: classify_region(row['latitude'], row['longitude']), axis=1)
            
            output_path = '/opt/airflow/data/aviationstack_realtime.csv'
            df.to_csv(output_path, index=False)
            
            print(f"✅ AviationStack: Extracted {len(df)} flights")
            print(f"📊 Regional distribution: {df['region'].value_counts().to_dict()}")
            
            return output_path
            
    except Exception as e:
        print(f"❌ Error extracting AviationStack data: {str(e)}")
        
    # Create empty file to prevent downstream failures
    output_path = '/opt/airflow/data/aviationstack_realtime.csv'
    pd.DataFrame().to_csv(output_path, index=False)
    return output_path

def extract_airlabs_data(**context) -> str:
    """Extract real-time flight data from AirLabs API"""
    try:
        from airflow.models import Variable
        
        # Get API key from Airflow Variables (optional)
        try:
            api_key = Variable.get("airlabs_api_key", default_var=None)
        except:
            api_key = None
            
        if not api_key:
            print("⚠️ AirLabs API key not configured, skipping...")
            output_path = '/opt/airflow/data/airlabs_realtime.csv'
            pd.DataFrame().to_csv(output_path, index=False)
            return output_path
            
        print("🌍 Fetching flight data from AirLabs...")
        
        # Get real-time flights
        url = f"{API_CONFIGS['airlabs']['base_url']}/flights"
        params = {
            'api_key': api_key
        }
        
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        if data and 'response' in data and data['response']:
            flights = []
            
            for flight in data['response']:
                flight_data = {
                    'icao24': flight.get('hex', ''),
                    'callsign': flight.get('flight_iata', ''),
                    'origin_country': flight.get('flag', ''),
                    'latitude': flight.get('lat'),
                    'longitude': flight.get('lng'),
                    'altitude': flight.get('alt'),
                    'speed': flight.get('speed'),
                    'heading': flight.get('dir'),
                    'vertical_speed': flight.get('v_speed'),
                    'departure_airport': flight.get('dep_iata', ''),
                    'arrival_airport': flight.get('arr_iata', ''),
                    'airline': flight.get('airline_iata', ''),
                    'aircraft_type': flight.get('aircraft_icao', ''),
                    'registration': flight.get('reg_number', ''),
                    'squawk': flight.get('squawk', ''),
                    'data_source': 'airlabs',
                    'extraction_time': datetime.now().isoformat(),
                    'updated_timestamp': flight.get('updated')
                }
                flights.append(flight_data)
            
            df = pd.DataFrame(flights)
            
            # Clean and validate data
            df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
            df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
            
            # Remove invalid coordinates
            df = df.dropna(subset=['latitude', 'longitude'])
            df = df[
                (df['latitude'].between(-90, 90)) & 
                (df['longitude'].between(-180, 180))
            ]
            
            # Add geographic regions
            df['region'] = df.apply(lambda row: classify_region(row['latitude'], row['longitude']), axis=1)
            
            output_path = '/opt/airflow/data/airlabs_realtime.csv'
            df.to_csv(output_path, index=False)
            
            print(f"✅ AirLabs: Extracted {len(df)} flights")
            print(f"📊 Regional distribution: {df['region'].value_counts().to_dict()}")
            
            return output_path
            
    except Exception as e:
        print(f"❌ Error extracting AirLabs data: {str(e)}")
        
    # Create empty file to prevent downstream failures
    output_path = '/opt/airflow/data/airlabs_realtime.csv'
    pd.DataFrame().to_csv(output_path, index=False)
    return output_path

def classify_region(lat: float, lon: float) -> str:
    """Classify geographic region based on coordinates"""
    if lat is None or lon is None:
        return 'Unknown'
    
    # Define regional boundaries
    if lat >= 35 and lon >= -25 and lon <= 60:
        return 'Europe'
    elif lat >= 15 and lat <= 55 and lon >= 60 and lon <= 150:
        return 'Asia'
    elif lat >= -35 and lat <= 37 and lon >= -20 and lon <= 55:
        return 'Africa'
    elif lat >= 15 and lat <= 72 and lon >= -170 and lon <= -50:
        return 'North America'
    elif lat >= -60 and lat <= 15 and lon >= -85 and lon <= -30:
        return 'South America'
    elif lat >= -50 and lat <= -10 and lon >= 110 and lon <= 180:
        return 'Oceania'
    elif lat <= -60:
        return 'Antarctica'
    elif lat >= 66.5:
        return 'Arctic'
    else:
        return 'Other'

def merge_and_transform_data(**context) -> str:
    """Merge and transform data from all sources"""
    try:
        print("🔄 Merging and transforming data from all sources...")
        
        # Read data from all sources
        sources = ['opensky', 'aviationstack', 'airlabs']
        all_data = []
        
        for source in sources:
            file_path = f'/opt/airflow/data/{source}_realtime.csv'
            if os.path.exists(file_path):
                try:
                    df = pd.read_csv(file_path)
                    if not df.empty:
                        all_data.append(df)
                        print(f"✅ Loaded {len(df)} records from {source}")
                    else:
                        print(f"⚠️ No data from {source}")
                except Exception as e:
                    print(f"❌ Error reading {source} data: {e}")
        
        if not all_data:
            print("⚠️ No data available from any source")
            # Create empty merged file
            output_path = '/opt/airflow/data/merged_realtime_flights.csv'
            pd.DataFrame().to_csv(output_path, index=False)
            return output_path
        
        # Merge all dataframes
        merged_df = pd.concat(all_data, ignore_index=True, sort=False)
        
        # Standardize column names and data types
        merged_df = standardize_flight_data(merged_df)
        
        # Remove duplicates based on ICAO24 (aircraft identifier)
        initial_count = len(merged_df)
        merged_df = merged_df.drop_duplicates(subset=['icao24'], keep='first')
        deduped_count = len(merged_df)
        
        print(f"🔄 Removed {initial_count - deduped_count} duplicate flights")
        
        # Add quality metrics
        merged_df['data_quality_score'] = calculate_data_quality(merged_df)
        
        # Sort by data quality and extraction time
        merged_df = merged_df.sort_values(['data_quality_score', 'extraction_time'], ascending=[False, False])
        
        output_path = '/opt/airflow/data/merged_realtime_flights.csv'
        merged_df.to_csv(output_path, index=False)
        
        # Generate summary statistics
        summary = generate_data_summary(merged_df)
        
        print(f"✅ Merged data summary:")
        print(f"   📊 Total flights: {len(merged_df)}")
        print(f"   🌍 Countries: {len(merged_df['origin_country'].unique()) if 'origin_country' in merged_df.columns else 0}")
        print(f"   🏢 Airlines: {len(merged_df['airline'].dropna().unique()) if 'airline' in merged_df.columns else 0}")
        print(f"   📡 Data sources: {merged_df['data_source'].value_counts().to_dict()}")
        print(f"   🌐 Regions: {merged_df['region'].value_counts().to_dict() if 'region' in merged_df.columns else {}}")
        
        return output_path
        
    except Exception as e:
        print(f"❌ Error merging data: {str(e)}")
        raise

def standardize_flight_data(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize flight data columns and formats"""
    
    # Ensure required columns exist
    required_columns = [
        'icao24', 'callsign', 'origin_country', 'latitude', 'longitude',
        'altitude', 'speed', 'heading', 'data_source', 'extraction_time'
    ]
    
    for col in required_columns:
        if col not in df.columns:
            df[col] = None
    
    # Ensure optional columns exist
    optional_columns = ['airline', 'aircraft_type', 'departure_airport', 'arrival_airport', 'registration']
    for col in optional_columns:
        if col not in df.columns:
            df[col] = None
    
    # Standardize data types
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
    df['altitude'] = pd.to_numeric(df['altitude'], errors='coerce')
    df['speed'] = pd.to_numeric(df['speed'], errors='coerce')
    df['heading'] = pd.to_numeric(df['heading'], errors='coerce')
    
    # Clean text fields
    text_fields = ['callsign', 'origin_country', 'airline', 'aircraft_type', 'departure_airport', 'arrival_airport', 'registration']
    for field in text_fields:
        if field in df.columns:
            df[field] = df[field].astype(str).str.strip().replace('None', '').replace('nan', '')
    
    # Add derived fields
    df['is_airborne'] = ~df.get('on_ground', False)
    df['has_position'] = ~(df['latitude'].isna() | df['longitude'].isna())
    df['has_speed'] = ~df['speed'].isna()
    df['has_altitude'] = ~df['altitude'].isna()
    
    return df

def calculate_data_quality(df: pd.DataFrame) -> pd.Series:
    """Calculate data quality score for each flight record"""
    quality_score = 0
    
    # Position data (40% weight)
    quality_score += df['has_position'].astype(int) * 40
    
    # Speed data (20% weight)
    quality_score += df['has_speed'].astype(int) * 20
    
    # Altitude data (20% weight)
    quality_score += df['has_altitude'].astype(int) * 20
    
    # Callsign data (10% weight)
    quality_score += (df['callsign'].str.len() > 0).astype(int) * 10
    
    # Country data (10% weight)
    quality_score += (df['origin_country'].str.len() > 0).astype(int) * 10
    
    return quality_score

def generate_data_summary(df: pd.DataFrame) -> Dict:
    """Generate comprehensive data summary"""
    summary = {
        'total_flights': len(df),
        'data_sources': df['data_source'].value_counts().to_dict(),
        'regions': df['region'].value_counts().to_dict() if 'region' in df.columns else {},
        'countries': len(df['origin_country'].unique()) if 'origin_country' in df.columns else 0,
        'airlines': len(df['airline'].dropna().unique()) if 'airline' in df.columns else 0,
        'aircraft_types': len(df['aircraft_type'].dropna().unique()) if 'aircraft_type' in df.columns else 0,
        'data_quality': {
            'avg_quality_score': df['data_quality_score'].mean() if 'data_quality_score' in df.columns else 0,
            'high_quality_flights': len(df[df['data_quality_score'] >= 80]) if 'data_quality_score' in df.columns else 0,
            'with_position': len(df[df['has_position']]) if 'has_position' in df.columns else 0,
            'with_speed': len(df[df['has_speed']]) if 'has_speed' in df.columns else 0,
            'with_altitude': len(df[df['has_altitude']]) if 'has_altitude' in df.columns else 0
        },
        'extraction_time': datetime.now().isoformat()
    }
    
    # Save summary to JSON
    os.makedirs('/opt/airflow/data/analytics', exist_ok=True)
    with open('/opt/airflow/data/analytics/realtime_flight_summary.json', 'w') as f:
        json.dump(summary, f, indent=2)
    
    return summary

def load_realtime_data(**context) -> None:
    """Load merged real-time flight data to PostgreSQL"""
    try:
        print("📥 Loading real-time flight data to database...")
        
        # Read merged data
        merged_file = '/opt/airflow/data/merged_realtime_flights.csv'
        if not os.path.exists(merged_file):
            print("⚠️ No merged data file found")
            return
        
        df = pd.read_csv(merged_file)
        if df.empty:
            print("⚠️ No data to load")
            return
        
        # Get database connection
        engine = get_sqlalchemy_engine()
        
        # Load to database with conflict resolution
        df.to_sql(
            'realtime_global_flights',
            engine,
            if_exists='replace',  # Replace for real-time updates
            index=False,
            method='multi'
        )
        
        print(f"✅ Loaded {len(df)} real-time flights to database")
        
        # Update metadata table
        metadata = {
            'table_name': 'realtime_global_flights',
            'record_count': len(df),
            'data_sources': df['data_source'].value_counts().to_dict(),
            'last_updated': datetime.now().isoformat(),
            'data_quality_avg': df['data_quality_score'].mean()
        }
        
        metadata_df = pd.DataFrame([metadata])
        metadata_df.to_sql(
            'realtime_flight_metadata',
            engine,
            if_exists='replace',
            index=False
        )
        
        print("✅ Updated metadata table")
        
    except Exception as e:
        print(f"❌ Error loading data to database: {str(e)}")
        raise

def generate_realtime_analytics(**context) -> None:
    """Generate real-time analytics and insights"""
    try:
        print("📊 Generating real-time flight analytics...")
        
        engine = get_sqlalchemy_engine()
        
        # Get current flight data
        df = pd.read_sql("SELECT * FROM realtime_global_flights", engine)
        
        if df.empty:
            print("⚠️ No flight data available for analytics")
            return
        
        # Generate analytics
        analytics = {
            'global_overview': {
                'total_flights': len(df),
                'countries_active': len(df['origin_country'].unique()),
                'airlines_active': len(df['airline'].unique()),
                'data_sources': df['data_source'].value_counts().to_dict()
            },
            'regional_distribution': df['region'].value_counts().to_dict(),
            'altitude_analysis': {
                'avg_altitude': df['altitude'].mean(),
                'max_altitude': df['altitude'].max(),
                'flights_above_30k': len(df[df['altitude'] > 30000])
            },
            'speed_analysis': {
                'avg_speed': df['speed'].mean(),
                'max_speed': df['speed'].max(),
                'supersonic_flights': len(df[df['speed'] > 343])  # Speed of sound
            },
            'data_quality': {
                'avg_quality_score': df['data_quality_score'].mean(),
                'high_quality_percentage': (len(df[df['data_quality_score'] >= 80]) / len(df)) * 100
            },
            'top_airlines': df['airline'].value_counts().head(10).to_dict(),
            'top_countries': df['origin_country'].value_counts().head(10).to_dict(),
            'timestamp': datetime.now().isoformat()
        }
        
        # Save analytics
        os.makedirs('/opt/airflow/data/analytics', exist_ok=True)
        with open('/opt/airflow/data/analytics/realtime_flight_analytics.json', 'w') as f:
            json.dump(analytics, f, indent=2)
        
        print("✅ Real-time analytics generated")
        print(f"   🌍 Global flights: {analytics['global_overview']['total_flights']}")
        print(f"   🏢 Active airlines: {analytics['global_overview']['airlines_active']}")
        print(f"   📊 Data quality: {analytics['data_quality']['avg_quality_score']:.1f}/100")
        
    except Exception as e:
        print(f"❌ Error generating analytics: {str(e)}")
        raise

# Create database tables
create_realtime_tables = PostgresOperator(
    task_id='create_realtime_tables',
    postgres_conn_id='postgres_default',
    sql="""
    -- Real-time global flights table
    CREATE TABLE IF NOT EXISTS realtime_global_flights (
        icao24 VARCHAR(10),
        callsign VARCHAR(20),
        origin_country VARCHAR(100),
        latitude FLOAT,
        longitude FLOAT,
        altitude FLOAT,
        speed FLOAT,
        heading FLOAT,
        vertical_speed FLOAT,
        departure_airport VARCHAR(10),
        arrival_airport VARCHAR(10),
        airline VARCHAR(10),
        aircraft_type VARCHAR(20),
        registration VARCHAR(20),
        squawk VARCHAR(10),
        flight_status VARCHAR(50),
        data_source VARCHAR(20),
        extraction_time TIMESTAMP,
        region VARCHAR(50),
        is_airborne BOOLEAN,
        has_position BOOLEAN,
        has_speed BOOLEAN,
        has_altitude BOOLEAN,
        data_quality_score INTEGER,
        PRIMARY KEY (icao24, extraction_time)
    );
    
    -- Metadata table
    CREATE TABLE IF NOT EXISTS realtime_flight_metadata (
        table_name VARCHAR(50),
        record_count INTEGER,
        data_sources TEXT,
        last_updated TIMESTAMP,
        data_quality_avg FLOAT,
        PRIMARY KEY (table_name, last_updated)
    );
    
    -- Create indexes for better performance
    CREATE INDEX IF NOT EXISTS idx_realtime_flights_region ON realtime_global_flights(region);
    CREATE INDEX IF NOT EXISTS idx_realtime_flights_country ON realtime_global_flights(origin_country);
    CREATE INDEX IF NOT EXISTS idx_realtime_flights_airline ON realtime_global_flights(airline);
    CREATE INDEX IF NOT EXISTS idx_realtime_flights_source ON realtime_global_flights(data_source);
    CREATE INDEX IF NOT EXISTS idx_realtime_flights_position ON realtime_global_flights(latitude, longitude);
    """,
    dag=dag,
)

# Define tasks
extract_opensky_task = PythonOperator(
    task_id='extract_opensky_data',
    python_callable=extract_opensky_data,
    dag=dag,
)

extract_aviationstack_task = PythonOperator(
    task_id='extract_aviationstack_data',
    python_callable=extract_aviationstack_data,
    dag=dag,
)

extract_airlabs_task = PythonOperator(
    task_id='extract_airlabs_data',
    python_callable=extract_airlabs_data,
    dag=dag,
)

merge_transform_task = PythonOperator(
    task_id='merge_and_transform_data',
    python_callable=merge_and_transform_data,
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_realtime_data',
    python_callable=load_realtime_data,
    dag=dag,
)

analytics_task = PythonOperator(
    task_id='generate_realtime_analytics',
    python_callable=generate_realtime_analytics,
    dag=dag,
)

# Set up task dependencies
create_realtime_tables >> [extract_opensky_task, extract_aviationstack_task, extract_airlabs_task]
[extract_opensky_task, extract_aviationstack_task, extract_airlabs_task] >> merge_transform_task
merge_transform_task >> load_data_task >> analytics_task 