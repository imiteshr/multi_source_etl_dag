#!/usr/bin/env python3
"""
Test script to verify the flight data ETL pipeline is working correctly.
"""

import subprocess
import time
import sys

def run_command(command):
    """Run a command and return the result"""
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        return result.returncode == 0, result.stdout, result.stderr
    except Exception as e:
        return False, "", str(e)

def trigger_dag(dag_id):
    """Trigger a DAG run"""
    command = f"docker-compose exec airflow-webserver airflow dags trigger {dag_id}"
    success, stdout, stderr = run_command(command)
    
    if success:
        print(f"✅ Successfully triggered DAG: {dag_id}")
        return True
    else:
        print(f"❌ Failed to trigger DAG: {dag_id}")
        print(f"Error: {stderr}")
        return False

def check_dag_status(dag_id):
    """Check the status of a DAG"""
    command = f"docker-compose exec airflow-webserver airflow dags list | grep {dag_id}"
    success, stdout, stderr = run_command(command)
    
    if success and dag_id in stdout:
        print(f"✅ DAG {dag_id} is available")
        return True
    else:
        print(f"❌ DAG {dag_id} is not available")
        return False

def check_database_tables():
    """Check if tables were created in the database"""
    command = """docker-compose exec postgres psql -U airflow -d airflow -c "\\dt" """
    success, stdout, stderr = run_command(command)
    
    if success:
        print("✅ Database connection successful")
        print("Available tables:")
        print(stdout)
        return True
    else:
        print("❌ Database connection failed")
        print(f"Error: {stderr}")
        return False

def main():
    print("🧪 Testing Flight Data ETL Pipeline")
    print("=" * 40)
    
    # Check if services are running
    print("\n1. Checking if services are running...")
    success, stdout, stderr = run_command("docker-compose ps")
    if success:
        print("✅ Docker Compose services are running")
        print(stdout)
    else:
        print("❌ Docker Compose services are not running")
        return False
    
    # Check DAG availability
    print("\n2. Checking DAG availability...")
    dags = ["flight_data_etl", "flight_data_quality_check"]
    
    for dag_id in dags:
        if not check_dag_status(dag_id):
            print(f"❌ DAG {dag_id} is not available. Please check the DAG files.")
            return False
    
    # Check database connectivity
    print("\n3. Checking database connectivity...")
    if not check_database_tables():
        print("❌ Database connectivity issues")
        return False
    
    # Trigger the main ETL DAG
    print("\n4. Triggering the main ETL DAG...")
    if trigger_dag("flight_data_etl"):
        print("⏳ Waiting for DAG to complete (this may take a few minutes)...")
        time.sleep(60)  # Wait for DAG to process
        
        # Check if data was loaded
        print("\n5. Checking if data was loaded...")
        tables_to_check = ["airports", "scheduled_flights"]
        
        for table in tables_to_check:
            command = f"""docker-compose exec postgres psql -U airflow -d airflow -c "SELECT COUNT(*) FROM {table};" """
            success, stdout, stderr = run_command(command)
            
            if success:
                print(f"✅ Table {table} exists and has data")
                print(f"   {stdout.strip()}")
            else:
                print(f"❌ Table {table} check failed: {stderr}")
    
    print("\n🎉 Pipeline test completed!")
    print("\nNext steps:")
    print("1. Access Airflow UI at: http://localhost:8081")
    print("2. Username: airflow, Password: airflow")
    print("3. Monitor DAG runs in the UI")
    print("4. Check data quality with the quality check DAG")

if __name__ == "__main__":
    main() 