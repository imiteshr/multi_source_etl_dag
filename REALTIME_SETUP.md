# Real-Time Global Flight Tracking System

A comprehensive real-time flight tracking system that aggregates data from multiple sources worldwide, providing live flight monitoring, analytics, and interactive visualizations.

## 🌍 Features

### Multi-Source Data Integration
- **OpenSky Network**: Free global ADS-B flight tracking (primary source)
- **AviationStack**: Commercial aviation data with comprehensive coverage
- **AirLabs**: High-quality flight data with global reach
- **ADS-B Exchange**: Unfiltered flight data (future integration)

### Real-Time Capabilities
- **Live Flight Tracking**: Updates every 15 minutes
- **Global Coverage**: Flights from all continents
- **Regional Analysis**: Automatic geographic classification
- **Data Quality Scoring**: Intelligent data validation and scoring

### Interactive Dashboards
- **Real-Time Map**: Live flight positions on interactive world map
- **Analytics Dashboard**: Flight statistics and insights
- **Data Quality Monitoring**: Source reliability and coverage metrics
- **Regional Distribution**: Geographic flight patterns

## 🚀 Quick Start

### Prerequisites
- Docker and Docker Compose
- At least 6GB RAM available for Docker
- Internet connection for API access

### 1. Clone and Setup
```bash
cd multi_source_etl_dag
```

### 2. Configure API Keys (Optional but Recommended)
For enhanced coverage, set up API keys for commercial services:

#### AviationStack API
1. Register at [aviationstack.com](https://aviationstack.com)
2. Get your free API key (100 requests/month)
3. Set in Airflow UI: Admin → Variables → Create:
   - Key: `aviationstack_api_key`
   - Value: `your_api_key_here`

#### AirLabs API
1. Register at [airlabs.co](https://airlabs.co)
2. Get your free API key (1000 requests/month)
3. Set in Airflow UI: Admin → Variables → Create:
   - Key: `airlabs_api_key`
   - Value: `your_api_key_here`

### 3. Start Services
```bash
# Start all services
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f
```

### 4. Access Applications
- **Airflow UI**: http://localhost:8081 (admin/airflow)
- **Real-Time Dashboard**: http://localhost:5002
- **Analytics Dashboard**: http://localhost:5001
- **PostgreSQL**: localhost:5432

## 📊 System Architecture

### Data Flow
```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   OpenSky API   │    │ AviationStack API│    │   AirLabs API   │
│   (Free/Global) │    │  (Commercial)    │    │  (Commercial)   │
└─────────┬───────┘    └────────┬─────────┘    └─────────┬───────┘
          │                     │                        │
          └─────────────────────┼────────────────────────┘
                                │
                    ┌───────────▼───────────┐
                    │   Airflow ETL DAG     │
                    │   (realtime_global_   │
                    │    flight_data)       │
                    └───────────┬───────────┘
                                │
                    ┌───────────▼───────────┐
                    │   Data Processing     │
                    │   • Merge Sources     │
                    │   • Quality Scoring   │
                    │   • Geographic Class  │
                    │   • Deduplication     │
                    └───────────┬───────────┘
                                │
                    ┌───────────▼───────────┐
                    │   PostgreSQL DB       │
                    │   • realtime_global_  │
                    │     flights           │
                    │   • metadata tables   │
                    └───────────┬───────────┘
                                │
                    ┌───────────▼───────────┐
                    │   Dashboards          │
                    │   • Real-time Map     │
                    │   • Analytics         │
                    │   • Data Quality      │
                    └───────────────────────┘
```

### DAG Structure
```
realtime_global_flight_data
├── create_realtime_tables
├── extract_opensky_data (parallel)
├── extract_aviationstack_data (parallel)
├── extract_airlabs_data (parallel)
├── merge_and_transform_data
├── load_realtime_data
└── generate_realtime_analytics
```

## 🔧 Configuration

### Airflow DAG Settings
- **Schedule**: Every 15 minutes (`timedelta(minutes=15)`)
- **Retries**: 2 attempts with 3-minute delays
- **Timeout**: 30 seconds per API call
- **Data Retention**: Last 1 hour for real-time views

### Data Quality Scoring
Quality scores are calculated based on:
- **Position Data** (40%): Latitude/longitude availability
- **Speed Data** (20%): Velocity information
- **Altitude Data** (20%): Altitude information
- **Callsign Data** (10%): Flight identifier
- **Country Data** (10%): Origin country

### Geographic Regions
Flights are automatically classified into regions:
- **Europe**: 35°N-72°N, 25°W-60°E
- **Asia**: 15°N-55°N, 60°E-150°E
- **North America**: 15°N-72°N, 170°W-50°W
- **South America**: 60°S-15°N, 85°W-30°W
- **Africa**: 35°S-37°N, 20°W-55°E
- **Oceania**: 50°S-10°S, 110°E-180°E
- **Antarctica**: Below 60°S
- **Arctic**: Above 66.5°N

## 📱 Dashboard Features

### Real-Time Dashboard (Port 5002)
- **Live Flight Map**: Interactive Leaflet map with real-time positions
- **Global Metrics**: Total flights, countries, airlines, average altitude
- **Regional Distribution**: Flight counts by geographic region
- **Data Sources**: Quality metrics for each API source
- **Live Flight Table**: Detailed flight information with quality scores
- **Altitude Distribution**: Flight altitude patterns
- **Auto-refresh**: Updates every 30 seconds

### Analytics Dashboard (Port 5001)
- **Historical Analytics**: Flight patterns and trends
- **Airline Performance**: On-time rates and statistics
- **Airport Traffic**: Busiest routes and airports
- **Executive Summary**: High-level insights and KPIs

## 🔍 Monitoring & Troubleshooting

### Check DAG Status
```bash
# View Airflow logs
docker-compose logs airflow-scheduler

# Check DAG runs
# Go to Airflow UI → DAGs → realtime_global_flight_data
```

### Monitor Data Quality
```bash
# Check database contents
docker-compose exec postgres psql -U airflow -d airflow -c "
SELECT 
    data_source, 
    COUNT(*) as flights, 
    AVG(data_quality_score) as avg_quality,
    MAX(extraction_time) as last_update
FROM realtime_global_flights 
GROUP BY data_source;
"
```

### View Real-Time Data
```bash
# Recent flights
docker-compose exec postgres psql -U airflow -d airflow -c "
SELECT 
    callsign, 
    origin_country, 
    region, 
    data_source, 
    extraction_time 
FROM realtime_global_flights 
ORDER BY extraction_time DESC 
LIMIT 10;
"
```

### Common Issues

#### No Data from Commercial APIs
- **Cause**: API keys not configured
- **Solution**: Set API keys in Airflow Variables (see setup section)

#### Low Flight Count
- **Cause**: API rate limits or network issues
- **Solution**: Check logs and wait for next scheduled run

#### Dashboard Not Loading
- **Cause**: Database connection issues
- **Solution**: Ensure PostgreSQL is running and accessible

## 🌟 Advanced Features

### API Rate Limiting
- **OpenSky**: Built-in rate limiting (10 requests/second)
- **AviationStack**: Free tier limited to 1000 requests/month
- **AirLabs**: Free tier limited to 1000 requests/month

### Data Deduplication
- Automatic removal of duplicate flights based on ICAO24 identifier
- Prioritizes higher quality data sources
- Maintains data freshness

### Error Handling
- Graceful degradation when APIs are unavailable
- Retry logic with exponential backoff
- Comprehensive error logging

### Performance Optimization
- Parallel API calls for faster data collection
- Database indexing for quick queries
- Efficient data structures for large datasets

## 🔄 Maintenance

### Regular Tasks
- Monitor API usage and quotas
- Review data quality metrics
- Update API keys when needed
- Check system resource usage

### Scaling Options
- Add more API sources
- Implement caching for frequently accessed data
- Set up database replicas for read scaling
- Consider premium API tiers for higher limits

## 📈 Metrics & KPIs

### Data Coverage
- **Global Flights**: 5,000-15,000 simultaneous flights
- **Geographic Coverage**: All continents
- **Update Frequency**: Every 15 minutes
- **Data Retention**: Real-time (last hour focus)

### Quality Metrics
- **Average Quality Score**: Target >80%
- **Position Accuracy**: GPS-based coordinates
- **Data Freshness**: <15 minutes old
- **Source Reliability**: Multi-source validation

## 🚨 Alerts & Notifications

### Data Quality Alerts
- Quality score drops below 70%
- No data received for >30 minutes
- API rate limit exceeded
- Database connection issues

### System Health Monitoring
- DAG failure notifications
- Resource usage alerts
- Dashboard availability checks

## 🔐 Security Considerations

### API Key Management
- Store keys in Airflow Variables (encrypted)
- Rotate keys regularly
- Monitor API usage for anomalies

### Data Privacy
- No personally identifiable information stored
- Flight data is publicly available via ADS-B
- Comply with API terms of service

## 📚 Additional Resources

### Documentation
- [OpenSky Network API](https://openskynetwork.github.io/opensky-api/)
- [AviationStack API](https://aviationstack.com/documentation)
- [AirLabs API](https://airlabs.co/docs)
- [Apache Airflow](https://airflow.apache.org/docs/)

### Community
- [OpenSky Network](https://opensky-network.org/)
- [ADS-B Exchange](https://adsbexchange.com/)
- [FlightAware](https://flightaware.com/)

---

## 🎯 Next Steps

1. **Set up API keys** for enhanced coverage
2. **Enable the real-time DAG** in Airflow UI
3. **Monitor the dashboards** for live data
4. **Customize regions** or add new data sources
5. **Scale up** with premium API tiers if needed

Happy flight tracking! ✈️🌍 