#!/bin/bash

echo "🚀 Starting Multi-Source Flight Data ETL Pipeline"
echo "=================================================="

# Set environment variables
export AIRFLOW_UID=50000

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

echo "✅ Docker is running"

# Create directories if they don't exist
mkdir -p dags logs plugins data

echo "📁 Created necessary directories"

# Start the services
echo "🐳 Starting Docker Compose services..."
docker-compose up -d

echo ""
echo "⏳ Waiting for services to initialize..."
echo "   This may take 2-3 minutes on first run..."

# Wait for the init container to complete
echo "📋 Waiting for Airflow initialization..."
docker-compose logs -f airflow-init | grep -q "airflow version"

echo ""
echo "🎉 Airflow is ready!"
echo ""
echo "📊 Access the Airflow Web UI:"
echo "   URL: http://localhost:8081"
echo "   Username: airflow"
echo "   Password: airflow"
echo ""
echo "🔍 Monitor services:"
echo "   docker-compose logs -f"
echo ""
echo "🛑 Stop services:"
echo "   docker-compose down"
echo ""
echo "📋 Available DAGs:"
echo "   - flight_data_etl (Main ETL pipeline)"
echo "   - flight_data_quality_check (Data quality monitoring)"
echo ""
echo "Happy data engineering! ✈️📊" 