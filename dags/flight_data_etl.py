from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import requests
import json
import os

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'flight_data_etl',
    default_args=default_args,
    description='Multi-source ETL pipeline for flight data',
    schedule_interval=timedelta(hours=6),
    catchup=False,
    tags=['etl', 'flights', 'data-pipeline'],
)

def extract_flight_api_data(**context):
    """Extract flight data from OpenSky Network API (free aviation data)"""
    try:
        # OpenSky Network API - free flight tracking data
        url = "https://opensky-network.org/api/states/all"
        
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        if data and 'states' in data and data['states']:
            # Convert to DataFrame
            columns = [
                'icao24', 'callsign', 'origin_country', 'time_position',
                'last_contact', 'longitude', 'latitude', 'baro_altitude',
                'on_ground', 'velocity', 'true_track', 'vertical_rate',
                'sensors', 'geo_altitude', 'squawk', 'spi', 'position_source'
            ]
            
            # Filter and clean the data before creating DataFrame
            clean_states = []
            for state in data['states']:
                if state and len(state) >= len(columns):
                    # Take only the first 17 columns to match our schema
                    clean_state = state[:len(columns)]
                    clean_states.append(clean_state)
            
            if clean_states:
                df = pd.DataFrame(clean_states, columns=columns)
                
                # Clean and transform data
                df['extraction_time'] = datetime.now().isoformat()
                
                # Clean callsign field - handle None values and strip whitespace
                df['callsign'] = df['callsign'].astype(str).str.strip()
                df['callsign'] = df['callsign'].replace('None', '')
                
                # Clean coordinate fields
                df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
                df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
                
                # Remove rows with invalid coordinates
                df = df.dropna(subset=['icao24', 'latitude', 'longitude'])
                
                # Save to CSV with proper quoting to handle special characters
                output_path = '/opt/airflow/data/flight_api_data.csv'
                df.to_csv(output_path, index=False, quoting=1)  # QUOTE_ALL
                
                print(f"Extracted {len(df)} flight records from API")
                return output_path
            else:
                print("No valid flight data found in API response")
        else:
            print("No flight data available from API")
            
    except Exception as e:
        print(f"Error extracting API data: {str(e)}")
        
    # Create empty file to prevent downstream failures
    output_path = '/opt/airflow/data/flight_api_data.csv'
    pd.DataFrame().to_csv(output_path, index=False)
    return output_path

def extract_flight_schedule_data(**context):
    """Extract flight schedule data (simulated CSV data)"""
    try:
        # Simulate flight schedule data
        schedules = []
        # Major global airlines
        airlines = [
            # US Airlines
            'AA', 'UA', 'DL', 'WN', 'B6', 'AS', 'NK', 'F9', 'G4', 'SY',
            # European Airlines
            'LH', 'AF', 'BA', 'KL', 'IB', 'AZ', 'LX', 'OS', 'SN', 'TP',
            'SK', 'AY', 'OK', 'LO', 'RO', 'JU', 'BT', 'YM', 'FR', 'U2',
            # Asian Airlines
            'SQ', 'CX', 'JL', 'NH', 'KE', 'OZ', 'CI', 'BR', 'TG', 'MH',
            'GA', 'SJ', 'AI', '6E', 'UK', 'IX', 'I5', 'PK', 'TK', 'MS',
            # Middle East Airlines
            'EK', 'QR', 'EY', 'SV', 'GF', 'RJ', 'ME', 'IR', 'W5', 'FZ',
            # Australian/Oceania Airlines
            'QF', 'VA', 'JQ', 'TT', 'NZ', 'DJ', 'FJ', 'PX', 'SB', 'IC',
            # African Airlines
            'SA', 'ET', 'MS', 'AT', 'KQ', 'UU', 'HC', 'SW', 'TD', 'DT',
            # Latin American Airlines
            'LA', 'AV', 'AR', 'JJ', 'G3', 'AD', 'CM', 'P5', 'LP', 'VN',
            # Canadian Airlines
            'AC', 'WS', 'PD', 'TS', 'F8', 'WG', 'MP', 'XG', 'WJ', 'ZX',
            # Low-cost carriers
            'WF', 'PC', 'VY', 'W6', 'D8', 'QZ', 'FD', 'VJ', 'BL', 'HV'
        ]
        
        # Major global airports
        airports = [
            # North America
            'JFK', 'LAX', 'ORD', 'DFW', 'DEN', 'ATL', 'SFO', 'LAS', 'PHX', 'SEA',
            'MIA', 'MCO', 'EWR', 'BOS', 'LGA', 'IAD', 'PHL', 'CLT', 'MSP', 'DTW',
            'YYZ', 'YVR', 'YUL', 'YYC', 'YEG', 'YWG', 'YHZ', 'YQB', 'YOW', 'YHM',
            'MEX', 'CUN', 'GDL', 'MTY', 'TIJ', 'PVR', 'SJD', 'BJX', 'HMO', 'CZM',
            
            # Europe
            'LHR', 'CDG', 'FRA', 'AMS', 'MAD', 'FCO', 'MUC', 'ZUR', 'VIE', 'BRU',
            'ARN', 'HEL', 'CPH', 'OSL', 'WAW', 'OTP', 'BUD', 'PRG', 'LIS', 'ATH',
            'IST', 'SAW', 'AYT', 'ESB', 'ADB', 'DLM', 'GZT', 'TZX', 'MLX', 'BJV',
            'SVO', 'DME', 'LED', 'KZN', 'ROV', 'UFA', 'VOG', 'KRR', 'OVB', 'IKT',
            
            # Asia-Pacific
            'NRT', 'HND', 'KIX', 'NGO', 'FUK', 'CTS', 'OKA', 'KMI', 'HIJ', 'TAK',
            'ICN', 'GMP', 'PUS', 'CJU', 'TAE', 'KWJ', 'YNY', 'RSU', 'USN', 'WJU',
            'TPE', 'KHH', 'RMQ', 'TNN', 'MFK', 'HUN', 'TTT', 'MZG', 'CYI', 'GNI',
            'HKG', 'MFM', 'SIN', 'KUL', 'CGK', 'DPS', 'BKK', 'DMK', 'CNX', 'HKT',
            'MNL', 'CEB', 'DVO', 'ILO', 'CRK', 'TAG', 'BCD', 'CGY', 'PPS', 'SFS',
            'BOM', 'DEL', 'BLR', 'MAA', 'HYD', 'CCU', 'AMD', 'COK', 'GOI', 'PNQ',
            'PEK', 'PVG', 'CAN', 'SZX', 'CTU', 'KMG', 'XMN', 'HGH', 'NKG', 'TSN',
            'SYD', 'MEL', 'BNE', 'PER', 'ADL', 'DRW', 'CNS', 'OOL', 'HBA', 'LST',
            'AKL', 'WLG', 'CHC', 'ZQN', 'ROT', 'NPL', 'PMR', 'NPE', 'DUD', 'IVC',
            
            # Middle East
            'DXB', 'DWC', 'DOH', 'AUH', 'RUH', 'JED', 'DMM', 'MED', 'TUU', 'AHB',
            'AMM', 'BGW', 'BSR', 'EBL', 'NJF', 'IFN', 'SLL', 'XNH', 'OSM', 'HTY',
            'CAI', 'HRG', 'SSH', 'LXR', 'ASW', 'RMF', 'MUH', 'ELG', 'TCP', 'EMY',
            'TLV', 'VDA', 'ETM', 'EIL', 'RPN', 'MTZ', 'KSW', 'HFA', 'BEV', 'JRS',
            
            # Africa
            'JNB', 'CPT', 'DUR', 'PLZ', 'BFN', 'ELS', 'GRJ', 'HDS', 'KIM', 'MQP',
            'CAI', 'HRG', 'SSH', 'LXR', 'ASW', 'RMF', 'MUH', 'ELG', 'TCP', 'EMY',
            'ADD', 'BJR', 'DIR', 'JIJ', 'MQX', 'AWA', 'GDQ', 'LLI', 'SHC', 'GMB',
            'LOS', 'ABV', 'KAN', 'PHC', 'IBA', 'CBQ', 'AKR', 'ENU', 'ILR', 'JOS',
            'CMN', 'RAK', 'AGA', 'FEZ', 'NDR', 'OUD', 'TNG', 'TTU', 'ERH', 'EUN',
            'TUN', 'MIR', 'DJE', 'TOE', 'SFA', 'GAE', 'NBE', 'TAB', 'EBM', 'OGL',
            
            # South America
            'GRU', 'GIG', 'BSB', 'CGH', 'SDU', 'CNF', 'REC', 'FOR', 'SSA', 'CWB',
            'EZE', 'AEP', 'COR', 'MDZ', 'BRC', 'NQN', 'FTE', 'IGR', 'SLA', 'JUJ',
            'SCL', 'IPC', 'ANF', 'CJC', 'LSC', 'PMC', 'PUQ', 'ZCO', 'WCA', 'ZAL',
            'BOG', 'MDE', 'CLO', 'CTG', 'BAQ', 'BGA', 'CUC', 'ADZ', 'AXM', 'EJA',
            'LIM', 'CUZ', 'AQP', 'TRU', 'PIU', 'IQT', 'CHM', 'AYP', 'JUL', 'TCQ',
            'UIO', 'GYE', 'CUE', 'LTX', 'XMS', 'PYO', 'GPS', 'ATF', 'OCC', 'SCY'
        ]
        
        import random
        from datetime import datetime, timedelta
        
        for i in range(1000):
            flight_num = f"{random.choice(airlines)}{random.randint(100, 9999)}"
            origin = random.choice(airports)
            destination = random.choice([a for a in airports if a != origin])
            
            base_time = datetime.now().replace(hour=6, minute=0, second=0, microsecond=0)
            departure_time = base_time + timedelta(
                hours=random.randint(0, 18),
                minutes=random.choice([0, 15, 30, 45])
            )
            flight_duration = timedelta(
                hours=random.randint(1, 6),
                minutes=random.choice([0, 15, 30, 45])
            )
            arrival_time = departure_time + flight_duration
            
            schedules.append({
                'flight_number': flight_num,
                'airline': flight_num[:2],
                'origin_airport': origin,
                'destination_airport': destination,
                'scheduled_departure': departure_time.isoformat(),
                'scheduled_arrival': arrival_time.isoformat(),
                'aircraft_type': random.choice(['B737', 'A320', 'B777', 'A350', 'B787']),
                'status': random.choice(['Scheduled', 'Delayed', 'On Time', 'Cancelled']),
                'gate': f"{random.choice(['A', 'B', 'C', 'D'])}{random.randint(1, 50)}",
                'extraction_date': datetime.now().date().isoformat()
            })
        
        df = pd.DataFrame(schedules)
        output_path = '/opt/airflow/data/flight_schedule_data.csv'
        df.to_csv(output_path, index=False)
        
        print(f"Generated {len(df)} flight schedule records")
        return output_path
        
    except Exception as e:
        print(f"Error generating schedule data: {str(e)}")
        output_path = '/opt/airflow/data/flight_schedule_data.csv'
        pd.DataFrame().to_csv(output_path, index=False)
        return output_path

def extract_airport_data(**context):
    """Extract airport information (comprehensive global data)"""
    try:
        airports = [
            # Major US Airports
            {'code': 'JFK', 'name': 'John F. Kennedy International', 'city': 'New York', 'country': 'USA', 'latitude': 40.6413, 'longitude': -73.7781},
            {'code': 'LAX', 'name': 'Los Angeles International', 'city': 'Los Angeles', 'country': 'USA', 'latitude': 33.9425, 'longitude': -118.4081},
            {'code': 'ORD', 'name': "O'Hare International", 'city': 'Chicago', 'country': 'USA', 'latitude': 41.9742, 'longitude': -87.9073},
            {'code': 'DFW', 'name': 'Dallas/Fort Worth International', 'city': 'Dallas', 'country': 'USA', 'latitude': 32.8998, 'longitude': -97.0403},
            {'code': 'DEN', 'name': 'Denver International', 'city': 'Denver', 'country': 'USA', 'latitude': 39.8561, 'longitude': -104.6737},
            {'code': 'ATL', 'name': 'Hartsfield-Jackson Atlanta International', 'city': 'Atlanta', 'country': 'USA', 'latitude': 33.6407, 'longitude': -84.4277},
            {'code': 'SFO', 'name': 'San Francisco International', 'city': 'San Francisco', 'country': 'USA', 'latitude': 37.6213, 'longitude': -122.3790},
            {'code': 'LAS', 'name': 'Harry Reid International', 'city': 'Las Vegas', 'country': 'USA', 'latitude': 36.0840, 'longitude': -115.1537},
            {'code': 'PHX', 'name': 'Phoenix Sky Harbor International', 'city': 'Phoenix', 'country': 'USA', 'latitude': 33.4342, 'longitude': -112.0116},
            {'code': 'SEA', 'name': 'Seattle-Tacoma International', 'city': 'Seattle', 'country': 'USA', 'latitude': 47.4502, 'longitude': -122.3088},
            {'code': 'MIA', 'name': 'Miami International', 'city': 'Miami', 'country': 'USA', 'latitude': 25.7932, 'longitude': -80.2906},
            {'code': 'MCO', 'name': 'Orlando International', 'city': 'Orlando', 'country': 'USA', 'latitude': 28.4294, 'longitude': -81.3089},
            {'code': 'EWR', 'name': 'Newark Liberty International', 'city': 'Newark', 'country': 'USA', 'latitude': 40.6925, 'longitude': -74.1687},
            {'code': 'BOS', 'name': 'Logan International', 'city': 'Boston', 'country': 'USA', 'latitude': 42.3656, 'longitude': -71.0096},
            {'code': 'LGA', 'name': 'LaGuardia Airport', 'city': 'New York', 'country': 'USA', 'latitude': 40.7769, 'longitude': -73.8740},
            
            # Major European Airports
            {'code': 'LHR', 'name': 'Heathrow Airport', 'city': 'London', 'country': 'United Kingdom', 'latitude': 51.4700, 'longitude': -0.4543},
            {'code': 'CDG', 'name': 'Charles de Gaulle Airport', 'city': 'Paris', 'country': 'France', 'latitude': 49.0097, 'longitude': 2.5479},
            {'code': 'FRA', 'name': 'Frankfurt Airport', 'city': 'Frankfurt', 'country': 'Germany', 'latitude': 50.0379, 'longitude': 8.5622},
            {'code': 'AMS', 'name': 'Amsterdam Airport Schiphol', 'city': 'Amsterdam', 'country': 'Netherlands', 'latitude': 52.3105, 'longitude': 4.7683},
            {'code': 'MAD', 'name': 'Madrid-Barajas Airport', 'city': 'Madrid', 'country': 'Spain', 'latitude': 40.4839, 'longitude': -3.5680},
            {'code': 'FCO', 'name': 'Leonardo da Vinci International', 'city': 'Rome', 'country': 'Italy', 'latitude': 41.8003, 'longitude': 12.2389},
            {'code': 'MUC', 'name': 'Munich Airport', 'city': 'Munich', 'country': 'Germany', 'latitude': 48.3537, 'longitude': 11.7750},
            {'code': 'ZUR', 'name': 'Zurich Airport', 'city': 'Zurich', 'country': 'Switzerland', 'latitude': 47.4647, 'longitude': 8.5492},
            {'code': 'VIE', 'name': 'Vienna International Airport', 'city': 'Vienna', 'country': 'Austria', 'latitude': 48.1103, 'longitude': 16.5697},
            {'code': 'BRU', 'name': 'Brussels Airport', 'city': 'Brussels', 'country': 'Belgium', 'latitude': 50.9010, 'longitude': 4.4856},
            {'code': 'ARN', 'name': 'Stockholm Arlanda Airport', 'city': 'Stockholm', 'country': 'Sweden', 'latitude': 59.6519, 'longitude': 17.9186},
            {'code': 'CPH', 'name': 'Copenhagen Airport', 'city': 'Copenhagen', 'country': 'Denmark', 'latitude': 55.6180, 'longitude': 12.6508},
            {'code': 'HEL', 'name': 'Helsinki Airport', 'city': 'Helsinki', 'country': 'Finland', 'latitude': 60.3172, 'longitude': 24.9633},
            {'code': 'OSL', 'name': 'Oslo Airport', 'city': 'Oslo', 'country': 'Norway', 'latitude': 60.1939, 'longitude': 11.1004},
            {'code': 'IST', 'name': 'Istanbul Airport', 'city': 'Istanbul', 'country': 'Turkey', 'latitude': 41.2753, 'longitude': 28.7519},
            
            # Major Asian Airports
            {'code': 'NRT', 'name': 'Narita International Airport', 'city': 'Tokyo', 'country': 'Japan', 'latitude': 35.7720, 'longitude': 140.3929},
            {'code': 'HND', 'name': 'Haneda Airport', 'city': 'Tokyo', 'country': 'Japan', 'latitude': 35.5494, 'longitude': 139.7798},
            {'code': 'ICN', 'name': 'Incheon International Airport', 'city': 'Seoul', 'country': 'South Korea', 'latitude': 37.4602, 'longitude': 126.4407},
            {'code': 'PEK', 'name': 'Beijing Capital International', 'city': 'Beijing', 'country': 'China', 'latitude': 40.0799, 'longitude': 116.6031},
            {'code': 'PVG', 'name': 'Shanghai Pudong International', 'city': 'Shanghai', 'country': 'China', 'latitude': 31.1443, 'longitude': 121.8083},
            {'code': 'HKG', 'name': 'Hong Kong International Airport', 'city': 'Hong Kong', 'country': 'China', 'latitude': 22.3080, 'longitude': 113.9185},
            {'code': 'SIN', 'name': 'Singapore Changi Airport', 'city': 'Singapore', 'country': 'Singapore', 'latitude': 1.3644, 'longitude': 103.9915},
            {'code': 'KUL', 'name': 'Kuala Lumpur International', 'city': 'Kuala Lumpur', 'country': 'Malaysia', 'latitude': 2.7456, 'longitude': 101.7072},
            {'code': 'BKK', 'name': 'Suvarnabhumi Airport', 'city': 'Bangkok', 'country': 'Thailand', 'latitude': 13.6900, 'longitude': 100.7501},
            {'code': 'CGK', 'name': 'Soekarno-Hatta International', 'city': 'Jakarta', 'country': 'Indonesia', 'latitude': -6.1275, 'longitude': 106.6537},
            {'code': 'MNL', 'name': 'Ninoy Aquino International', 'city': 'Manila', 'country': 'Philippines', 'latitude': 14.5086, 'longitude': 121.0194},
            {'code': 'DEL', 'name': 'Indira Gandhi International', 'city': 'New Delhi', 'country': 'India', 'latitude': 28.5665, 'longitude': 77.1031},
            {'code': 'BOM', 'name': 'Chhatrapati Shivaji International', 'city': 'Mumbai', 'country': 'India', 'latitude': 19.0896, 'longitude': 72.8656},
            {'code': 'TPE', 'name': 'Taiwan Taoyuan International', 'city': 'Taipei', 'country': 'Taiwan', 'latitude': 25.0797, 'longitude': 121.2342},
            
            # Middle East Airports
            {'code': 'DXB', 'name': 'Dubai International Airport', 'city': 'Dubai', 'country': 'UAE', 'latitude': 25.2532, 'longitude': 55.3657},
            {'code': 'DOH', 'name': 'Hamad International Airport', 'city': 'Doha', 'country': 'Qatar', 'latitude': 25.2731, 'longitude': 51.6080},
            {'code': 'AUH', 'name': 'Abu Dhabi International', 'city': 'Abu Dhabi', 'country': 'UAE', 'latitude': 24.4330, 'longitude': 54.6511},
            {'code': 'RUH', 'name': 'King Khalid International', 'city': 'Riyadh', 'country': 'Saudi Arabia', 'latitude': 24.9576, 'longitude': 46.6988},
            {'code': 'JED', 'name': 'King Abdulaziz International', 'city': 'Jeddah', 'country': 'Saudi Arabia', 'latitude': 21.6796, 'longitude': 39.1565},
            {'code': 'TLV', 'name': 'Ben Gurion Airport', 'city': 'Tel Aviv', 'country': 'Israel', 'latitude': 32.0114, 'longitude': 34.8867},
            {'code': 'CAI', 'name': 'Cairo International Airport', 'city': 'Cairo', 'country': 'Egypt', 'latitude': 30.1219, 'longitude': 31.4056},
            {'code': 'AMM', 'name': 'Queen Alia International', 'city': 'Amman', 'country': 'Jordan', 'latitude': 31.7226, 'longitude': 35.9933},
            
            # Australian/Oceania Airports
            {'code': 'SYD', 'name': 'Sydney Kingsford Smith Airport', 'city': 'Sydney', 'country': 'Australia', 'latitude': -33.9399, 'longitude': 151.1753},
            {'code': 'MEL', 'name': 'Melbourne Airport', 'city': 'Melbourne', 'country': 'Australia', 'latitude': -37.6690, 'longitude': 144.8410},
            {'code': 'BNE', 'name': 'Brisbane Airport', 'city': 'Brisbane', 'country': 'Australia', 'latitude': -27.3942, 'longitude': 153.1218},
            {'code': 'PER', 'name': 'Perth Airport', 'city': 'Perth', 'country': 'Australia', 'latitude': -31.9403, 'longitude': 115.9669},
            {'code': 'AKL', 'name': 'Auckland Airport', 'city': 'Auckland', 'country': 'New Zealand', 'latitude': -37.0082, 'longitude': 174.7850},
            {'code': 'CHC', 'name': 'Christchurch Airport', 'city': 'Christchurch', 'country': 'New Zealand', 'latitude': -43.4894, 'longitude': 172.5320},
            
            # Canadian Airports
            {'code': 'YYZ', 'name': 'Toronto Pearson International', 'city': 'Toronto', 'country': 'Canada', 'latitude': 43.6777, 'longitude': -79.6248},
            {'code': 'YVR', 'name': 'Vancouver International', 'city': 'Vancouver', 'country': 'Canada', 'latitude': 49.1967, 'longitude': -123.1815},
            {'code': 'YUL', 'name': 'Montreal-Pierre Elliott Trudeau', 'city': 'Montreal', 'country': 'Canada', 'latitude': 45.4577, 'longitude': -73.7497},
            {'code': 'YYC', 'name': 'Calgary International', 'city': 'Calgary', 'country': 'Canada', 'latitude': 51.1315, 'longitude': -114.0106},
            {'code': 'YEG', 'name': 'Edmonton International', 'city': 'Edmonton', 'country': 'Canada', 'latitude': 53.3097, 'longitude': -113.5801},
            
            # South American Airports
            {'code': 'GRU', 'name': 'São Paulo/Guarulhos International', 'city': 'São Paulo', 'country': 'Brazil', 'latitude': -23.4356, 'longitude': -46.4731},
            {'code': 'GIG', 'name': 'Rio de Janeiro/Galeão International', 'city': 'Rio de Janeiro', 'country': 'Brazil', 'latitude': -22.8099, 'longitude': -43.2505},
            {'code': 'EZE', 'name': 'Ezeiza International Airport', 'city': 'Buenos Aires', 'country': 'Argentina', 'latitude': -34.8222, 'longitude': -58.5358},
            {'code': 'SCL', 'name': 'Santiago International Airport', 'city': 'Santiago', 'country': 'Chile', 'latitude': -33.3930, 'longitude': -70.7858},
            {'code': 'BOG', 'name': 'El Dorado International', 'city': 'Bogotá', 'country': 'Colombia', 'latitude': 4.7016, 'longitude': -74.1469},
            {'code': 'LIM', 'name': 'Jorge Chávez International', 'city': 'Lima', 'country': 'Peru', 'latitude': -12.0219, 'longitude': -77.1143},
            {'code': 'UIO', 'name': 'Mariscal Sucre International', 'city': 'Quito', 'country': 'Ecuador', 'latitude': -0.1292, 'longitude': -78.3576},
            
            # African Airports
            {'code': 'JNB', 'name': 'OR Tambo International', 'city': 'Johannesburg', 'country': 'South Africa', 'latitude': -26.1392, 'longitude': 28.2460},
            {'code': 'CPT', 'name': 'Cape Town International', 'city': 'Cape Town', 'country': 'South Africa', 'latitude': -33.9649, 'longitude': 18.6017},
            {'code': 'ADD', 'name': 'Addis Ababa Bole International', 'city': 'Addis Ababa', 'country': 'Ethiopia', 'latitude': 8.9806, 'longitude': 38.7999},
            {'code': 'LOS', 'name': 'Murtala Muhammed International', 'city': 'Lagos', 'country': 'Nigeria', 'latitude': 6.5774, 'longitude': 3.3212},
            {'code': 'CMN', 'name': 'Mohammed V International', 'city': 'Casablanca', 'country': 'Morocco', 'latitude': 33.3675, 'longitude': -7.5898},
            {'code': 'TUN', 'name': 'Tunis-Carthage International', 'city': 'Tunis', 'country': 'Tunisia', 'latitude': 36.8510, 'longitude': 10.2272},
            
            # Mexican Airports
            {'code': 'MEX', 'name': 'Mexico City International', 'city': 'Mexico City', 'country': 'Mexico', 'latitude': 19.4363, 'longitude': -99.0721},
            {'code': 'CUN', 'name': 'Cancún International', 'city': 'Cancún', 'country': 'Mexico', 'latitude': 21.0365, 'longitude': -86.8771},
            {'code': 'GDL', 'name': 'Guadalajara International', 'city': 'Guadalajara', 'country': 'Mexico', 'latitude': 20.5218, 'longitude': -103.3106},
            {'code': 'MTY', 'name': 'Monterrey International', 'city': 'Monterrey', 'country': 'Mexico', 'latitude': 25.7785, 'longitude': -100.1077},
            {'code': 'PVR', 'name': 'Puerto Vallarta International', 'city': 'Puerto Vallarta', 'country': 'Mexico', 'latitude': 20.6801, 'longitude': -105.2544},
        ]
        
        df = pd.DataFrame(airports)
        df['extraction_date'] = datetime.now().date().isoformat()
        
        output_path = '/opt/airflow/data/airport_data.csv'
        df.to_csv(output_path, index=False)
        
        print(f"Extracted {len(df)} airport records")
        return output_path
        
    except Exception as e:
        print(f"Error extracting airport data: {str(e)}")
        output_path = '/opt/airflow/data/airport_data.csv'
        pd.DataFrame().to_csv(output_path, index=False)
        return output_path

def transform_flight_data(**context):
    """Transform and clean all flight data"""
    try:
        # Read all extracted data with improved error handling
        try:
            api_data = pd.read_csv('/opt/airflow/data/flight_api_data.csv', 
                                 on_bad_lines='skip', 
                                 encoding='utf-8',
                                 quoting=1)  # QUOTE_ALL
        except Exception as e:
            print(f"Error reading API data CSV: {e}")
            api_data = pd.DataFrame()
        
        try:
            schedule_data = pd.read_csv('/opt/airflow/data/flight_schedule_data.csv',
                                      on_bad_lines='skip',
                                      encoding='utf-8')
        except Exception as e:
            print(f"Error reading schedule data CSV: {e}")
            schedule_data = pd.DataFrame()
        
        try:
            airport_data = pd.read_csv('/opt/airflow/data/airport_data.csv',
                                     on_bad_lines='skip',
                                     encoding='utf-8')
        except Exception as e:
            print(f"Error reading airport data CSV: {e}")
            airport_data = pd.DataFrame()
        
        # Transform API data (real-time flights)
        if not api_data.empty:
            api_data_clean = api_data.copy()
            api_data_clean['data_source'] = 'api'
            api_data_clean['flight_type'] = 'real_time'
            
            # Convert coordinates to proper format
            api_data_clean['latitude'] = pd.to_numeric(api_data_clean['latitude'], errors='coerce')
            api_data_clean['longitude'] = pd.to_numeric(api_data_clean['longitude'], errors='coerce')
            
            # Remove flights with invalid coordinates
            api_data_clean = api_data_clean.dropna(subset=['latitude', 'longitude'])
            api_data_clean = api_data_clean[
                (api_data_clean['latitude'].between(-90, 90)) & 
                (api_data_clean['longitude'].between(-180, 180))
            ]
        else:
            api_data_clean = pd.DataFrame()
        
        # Transform schedule data
        if not schedule_data.empty:
            schedule_data_clean = schedule_data.copy()
            schedule_data_clean['data_source'] = 'schedule'
            schedule_data_clean['flight_type'] = 'scheduled'
            
            # Parse datetime fields
            schedule_data_clean['scheduled_departure'] = pd.to_datetime(schedule_data_clean['scheduled_departure'])
            schedule_data_clean['scheduled_arrival'] = pd.to_datetime(schedule_data_clean['scheduled_arrival'])
            
            # Calculate flight duration
            schedule_data_clean['flight_duration_minutes'] = (
                schedule_data_clean['scheduled_arrival'] - schedule_data_clean['scheduled_departure']
            ).dt.total_seconds() / 60
        else:
            schedule_data_clean = pd.DataFrame()
        
        # Save transformed data
        if not api_data_clean.empty:
            api_data_clean.to_csv('/opt/airflow/data/transformed_realtime_flights.csv', index=False)
        
        if not schedule_data_clean.empty:
            schedule_data_clean.to_csv('/opt/airflow/data/transformed_scheduled_flights.csv', index=False)
        
        airport_data.to_csv('/opt/airflow/data/transformed_airports.csv', index=False)
        
        print(f"Transformed data: {len(api_data_clean)} real-time flights, {len(schedule_data_clean)} scheduled flights, {len(airport_data)} airports")
        
    except Exception as e:
        print(f"Error transforming data: {str(e)}")
        raise

def load_to_database(**context):
    """Load transformed data to PostgreSQL database"""
    try:
        # Get PostgreSQL connection
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Load airports data
        try:
            if os.path.exists('/opt/airflow/data/transformed_airports.csv'):
                airport_data = pd.read_csv('/opt/airflow/data/transformed_airports.csv')
                if not airport_data.empty:
                    airport_data.to_sql(
                        'airports', 
                        postgres_hook.get_sqlalchemy_engine(), 
                        if_exists='replace', 
                        index=False
                    )
                    print(f"Loaded {len(airport_data)} airport records")
                else:
                    print("Airport data file is empty")
            else:
                print("Airport data file not found")
        except Exception as e:
            print(f"Error loading airport data: {e}")
        
        # Load scheduled flights data
        try:
            if os.path.exists('/opt/airflow/data/transformed_scheduled_flights.csv'):
                schedule_data = pd.read_csv('/opt/airflow/data/transformed_scheduled_flights.csv')
                if not schedule_data.empty:
                    schedule_data.to_sql(
                        'scheduled_flights', 
                        postgres_hook.get_sqlalchemy_engine(), 
                        if_exists='replace', 
                        index=False
                    )
                    print(f"Loaded {len(schedule_data)} scheduled flight records")
                else:
                    print("Scheduled flights data file is empty")
            else:
                print("Scheduled flights data file not found")
        except Exception as e:
            print(f"Error loading schedule data: {e}")
        
        # Load real-time flights data
        try:
            if os.path.exists('/opt/airflow/data/transformed_realtime_flights.csv'):
                realtime_data = pd.read_csv('/opt/airflow/data/transformed_realtime_flights.csv')
                if not realtime_data.empty:
                    realtime_data.to_sql(
                        'realtime_flights', 
                        postgres_hook.get_sqlalchemy_engine(), 
                        if_exists='replace', 
                        index=False
                    )
                    print(f"Loaded {len(realtime_data)} real-time flight records")
                else:
                    print("Real-time flights data file is empty")
            else:
                print("Real-time flights data file not found")
        except Exception as e:
            print(f"Error loading real-time data: {e}")
            
    except Exception as e:
        print(f"Error in load_to_database: {str(e)}")
        raise

# Define tasks
extract_api_task = PythonOperator(
    task_id='extract_flight_api_data',
    python_callable=extract_flight_api_data,
    dag=dag,
)

extract_schedule_task = PythonOperator(
    task_id='extract_flight_schedule_data',
    python_callable=extract_flight_schedule_data,
    dag=dag,
)

extract_airport_task = PythonOperator(
    task_id='extract_airport_data',
    python_callable=extract_airport_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_flight_data',
    python_callable=transform_flight_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_database',
    python_callable=load_to_database,
    dag=dag,
)

# Set up task dependencies
[extract_api_task, extract_schedule_task, extract_airport_task] >> transform_task >> load_task 