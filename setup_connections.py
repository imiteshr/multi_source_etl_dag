#!/usr/bin/env python3
"""
Script to set up Airflow connections for the flight data ETL pipeline.
Run this script after Airflow is running to create the necessary database connections.
"""

import subprocess
import sys

def run_airflow_command(command):
    """Run an Airflow CLI command in the Docker container"""
    full_command = f"docker-compose exec airflow-webserver airflow {command}"
    print(f"Running: {full_command}")
    
    try:
        result = subprocess.run(full_command, shell=True, capture_output=True, text=True)
        print(f"Return code: {result.returncode}")
        if result.stdout:
            print(f"Output: {result.stdout}")
        if result.stderr:
            print(f"Error: {result.stderr}")
        return result.returncode == 0
    except Exception as e:
        print(f"Exception: {e}")
        return False

def setup_postgres_connection():
    """Set up the PostgreSQL connection"""
    command = """connections add postgres_default \\
        --conn-type postgres \\
        --conn-host postgres \\
        --conn-login airflow \\
        --conn-password airflow \\
        --conn-schema airflow \\
        --conn-port 5433"""
    
    return run_airflow_command(command)

def list_connections():
    """List all connections to verify setup"""
    command = "connections list"
    return run_airflow_command(command)

def main():
    print("Setting up Airflow connections for Flight Data ETL Pipeline")
    print("=" * 60)
    
    # Set up PostgreSQL connection
    print("\n1. Setting up PostgreSQL connection...")
    if setup_postgres_connection():
        print("✅ PostgreSQL connection created successfully")
    else:
        print("❌ Failed to create PostgreSQL connection")
        print("Note: Connection might already exist, which is fine.")
    
    # List all connections
    print("\n2. Listing all connections...")
    list_connections()
    
    print("\n✅ Connection setup complete!")
    print("\nNext steps:")
    print("1. Access Airflow UI at: http://localhost:8081")
    print("2. Username: airflow")
    print("3. Password: airflow")
    print("4. Enable the DAGs: flight_data_etl and flight_data_quality_check")
    print("5. Trigger the flight_data_etl DAG to start processing data")

if __name__ == "__main__":
    main() 