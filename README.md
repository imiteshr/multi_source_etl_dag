# Multi-Source Flight Data ETL Pipeline

A comprehensive Apache Airflow-based ETL pipeline for processing flight data from multiple sources including real-time APIs, scheduled flight data, and airport information.

## Architecture

This pipeline extracts flight data from multiple sources:
- **Real-time Flight Data**: OpenSky Network API (live flight tracking)
- **Flight Schedules**: Simulated airline schedule data
- **Airport Information**: Airport codes, names, and coordinates

The data is then transformed, cleaned, and loaded into a PostgreSQL database for analysis.

## Features

- **Multi-source data extraction** from APIs and simulated datasets
- **Data transformation and cleaning** with pandas
- **PostgreSQL data warehouse** for structured storage
- **Data quality monitoring** with automated checks
- **Dockerized deployment** for easy setup
- **Airflow web interface** for pipeline monitoring

## Quick Start

### Prerequisites

- Docker and Docker Compose
- At least 4GB RAM available for Docker

### Setup

1. **Clone and navigate to the project**:
   ```bash
   cd multi_source_etl_dag
   ```

2. **Set environment variables**:
   ```bash
   export AIRFLOW_UID=50000
   ```

3. **Start the services**:
   ```bash
   docker-compose up -d
   ```

4. **Wait for initialization** (first run takes 2-3 minutes):
   ```bash
   docker-compose logs airflow-init
   ```

5. **Access Airflow UI**:
   - URL: http://localhost:8081
   - Username: `airflow`
   - Password: `airflow`

### Running the Pipeline

1. **Enable the DAGs** in the Airflow UI:
   - `flight_data_etl` - Main ETL pipeline
   - `flight_data_quality_check` - Data quality monitoring

2. **Trigger the main ETL DAG**:
   - Click on `flight_data_etl`
   - Click the play button to trigger manually
   - Or wait for the scheduled run (every 6 hours)

3. **Monitor execution**:
   - View task progress in the Graph or Grid view
   - Check logs for each task
   - Monitor data quality with the quality check DAG

## Pipeline Details

### Main ETL DAG (`flight_data_etl`)

**Schedule**: Every 6 hours  
**Tasks**:

1. **Extract Tasks** (run in parallel):
   - `extract_flight_api_data`: Fetches real-time flight data from OpenSky Network
   - `extract_flight_schedule_data`: Generates simulated airline schedule data
   - `extract_airport_data`: Creates airport reference data

2. **Transform Task**:
   - `transform_flight_data`: Cleans and standardizes all extracted data

3. **Load Task**:
   - `load_to_database`: Loads transformed data into PostgreSQL tables

### Data Quality DAG (`flight_data_quality_check`)

**Schedule**: Every 12 hours  
**Checks**:

- **Data Freshness**: Ensures data is current
- **Data Completeness**: Checks for missing values
- **Data Validity**: Identifies anomalies and duplicates
- **Data Summary**: Generates statistics and insights

### Database Schema

**Tables**:
- `airports`: Airport reference data (code, name, location)
- `scheduled_flights`: Airline schedule information
- `realtime_flights`: Live flight tracking data

## Data Sources

### OpenSky Network API
- **Type**: Real-time flight tracking
- **Coverage**: Global aircraft positions
- **Rate Limit**: Public API (may have limitations)
- **Data**: Aircraft positions, callsigns, velocities

### Simulated Data
- **Flight Schedules**: Generated airline timetables
- **Airport Data**: Major US airports with coordinates
- **Airlines**: Major US carriers (AA, UA, DL, etc.)

## Configuration

### Environment Variables
- `AIRFLOW_UID`: User ID for Airflow (default: 50000)
- `AIRFLOW_PROJ_DIR`: Project directory (default: current directory)

### Airflow Configuration
- **Executor**: LocalExecutor (suitable for single-machine deployment)
- **Database**: PostgreSQL
- **Web UI Port**: 8081 (to avoid conflicts with other services)

## Monitoring and Troubleshooting

### View Logs
```bash
# All services
docker-compose logs

# Specific service
docker-compose logs airflow-webserver
docker-compose logs airflow-scheduler
```

### Database Access
```bash
# Connect to PostgreSQL
docker-compose exec postgres psql -U airflow -d airflow

# View tables
\dt

# Query data
SELECT COUNT(*) FROM airports;
SELECT COUNT(*) FROM scheduled_flights;
```

### Common Issues

1. **Port 8081 already in use**:
   ```bash
   # Change port in docker-compose.yml
   ports:
     - "8082:8080"  # Use 8082 instead
   ```

2. **Insufficient memory**:
   - Ensure Docker has at least 4GB RAM allocated
   - Close other applications to free memory

3. **API rate limits**:
   - OpenSky Network may limit requests
   - Pipeline handles failures gracefully with empty datasets

## Extending the Pipeline

### Adding New Data Sources

1. **Create extraction function**:
   ```python
   def extract_new_source(**context):
       # Your extraction logic
       pass
   ```

2. **Add to DAG**:
   ```python
   new_extract_task = PythonOperator(
       task_id='extract_new_source',
       python_callable=extract_new_source,
       dag=dag,
   )
   ```

3. **Update dependencies**:
   ```python
   [extract_api_task, extract_schedule_task, new_extract_task] >> transform_task
   ```

### Custom Data Quality Checks

Add new quality check functions to `flight_data_quality.py`:

```python
def custom_quality_check(**context):
    # Your quality check logic
    pass

custom_check = PythonOperator(
    task_id='custom_quality_check',
    python_callable=custom_quality_check,
    dag=dag,
)
```

## Deployment

### Production Considerations

1. **Use CeleryExecutor** for distributed processing
2. **Set up proper authentication** (LDAP, OAuth)
3. **Configure email notifications** for failures
4. **Use external PostgreSQL** for better performance
5. **Set up monitoring** with Prometheus/Grafana

### Scaling

- **Horizontal**: Add more worker nodes with CeleryExecutor
- **Vertical**: Increase memory/CPU for Docker containers
- **Database**: Use managed PostgreSQL service (AWS RDS, etc.)

## License

This project is for educational and demonstration purposes.

## Support

For issues and questions:
1. Check the logs for error messages
2. Verify all services are running: `docker-compose ps`
3. Ensure sufficient system resources
4. Review Airflow documentation for advanced configuration 