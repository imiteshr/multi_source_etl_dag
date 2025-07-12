from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import numpy as np
import json
import os
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score
import joblib
from sqlalchemy import create_engine

default_args = {
    'owner': 'data_scientist',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'flight_ml_dag',
    default_args=default_args,
    description='Machine Learning pipeline for flight analytics',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['ml', 'flights', 'prediction'],
)

def get_sqlalchemy_engine():
    """Get SQLAlchemy engine for pandas operations"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = postgres_hook.get_connection('postgres_default')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    return engine

def prepare_ml_data(**context):
    """Prepare data for machine learning models"""
    try:
        engine = get_sqlalchemy_engine()
        
        # Get flight data for ML
        ml_query = """
        SELECT 
            airline,
            origin_airport,
            destination_airport,
            aircraft_type,
            EXTRACT(HOUR FROM scheduled_departure::timestamp) as departure_hour,
            EXTRACT(DOW FROM scheduled_departure::timestamp) as day_of_week,
            flight_duration_minutes,
            status,
            CASE 
                WHEN status IN ('Delayed', 'Cancelled') THEN 1 
                ELSE 0 
            END as is_delayed
        FROM scheduled_flights
        WHERE scheduled_departure IS NOT NULL
        AND flight_duration_minutes IS NOT NULL;
        """
        
        df = pd.read_sql(ml_query, engine)
        
        if not df.empty:
            # Feature engineering
            df['route'] = df['origin_airport'] + '_' + df['destination_airport']
            df['is_weekend'] = df['day_of_week'].isin([0, 6]).astype(int)  # Sunday=0, Saturday=6
            df['is_peak_hour'] = df['departure_hour'].between(7, 9).astype(int) | df['departure_hour'].between(17, 19).astype(int)
            
            # Create duration categories
            df['duration_category'] = pd.cut(df['flight_duration_minutes'], 
                                           bins=[0, 120, 240, 480, float('inf')], 
                                           labels=['short', 'medium', 'long', 'very_long'])
            
            # Save prepared data
            os.makedirs('/opt/airflow/data/ml', exist_ok=True)
            df.to_csv('/opt/airflow/data/ml/prepared_data.csv', index=False)
            
            # Create feature summary
            feature_summary = {
                'total_records': len(df),
                'delay_rate': df['is_delayed'].mean(),
                'airlines_count': df['airline'].nunique(),
                'routes_count': df['route'].nunique(),
                'features': list(df.columns),
                'preparation_timestamp': datetime.now().isoformat()
            }
            
            with open('/opt/airflow/data/ml/feature_summary.json', 'w') as f:
                json.dump(feature_summary, f, indent=2)
            
            print(f"✅ ML data prepared: {len(df)} records")
            print(f"📊 Delay rate: {feature_summary['delay_rate']:.2%}")
            print(f"🛫 Airlines: {feature_summary['airlines_count']}")
            print(f"🗺️ Routes: {feature_summary['routes_count']}")
            
        else:
            print("⚠️ No data available for ML preparation")
            
    except Exception as e:
        print(f"❌ Error preparing ML data: {str(e)}")
        raise

def train_delay_prediction_model(**context):
    """Train a model to predict flight delays"""
    try:
        # Load prepared data
        df = pd.read_csv('/opt/airflow/data/ml/prepared_data.csv')
        
        if df.empty:
            print("⚠️ No data available for training")
            return
        
        # Prepare features for ML
        features_to_encode = ['airline', 'origin_airport', 'destination_airport', 
                            'aircraft_type', 'route', 'duration_category']
        
        # Create label encoders
        label_encoders = {}
        df_encoded = df.copy()
        
        for feature in features_to_encode:
            if feature in df_encoded.columns:
                le = LabelEncoder()
                df_encoded[feature] = le.fit_transform(df_encoded[feature].astype(str))
                label_encoders[feature] = le
        
        # Select features for training
        feature_columns = ['airline', 'origin_airport', 'destination_airport', 
                         'aircraft_type', 'departure_hour', 'day_of_week',
                         'flight_duration_minutes', 'is_weekend', 'is_peak_hour']
        
        # Filter available features
        available_features = [col for col in feature_columns if col in df_encoded.columns]
        
        if len(available_features) < 3:
            print("⚠️ Not enough features available for training")
            return
        
        X = df_encoded[available_features]
        y = df_encoded['is_delayed']
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        # Train model
        model = RandomForestClassifier(n_estimators=100, random_state=42)
        model.fit(X_train, y_train)
        
        # Make predictions
        y_pred = model.predict(X_test)
        
        # Evaluate model
        accuracy = accuracy_score(y_test, y_pred)
        
        # Save model and encoders
        joblib.dump(model, '/opt/airflow/data/ml/delay_prediction_model.pkl')
        joblib.dump(label_encoders, '/opt/airflow/data/ml/label_encoders.pkl')
        
        # Feature importance
        feature_importance = dict(zip(available_features, model.feature_importances_))
        
        # Model metrics
        model_metrics = {
            'accuracy': accuracy,
            'training_samples': len(X_train),
            'test_samples': len(X_test),
            'feature_importance': feature_importance,
            'features_used': available_features,
            'model_type': 'RandomForestClassifier',
            'training_timestamp': datetime.now().isoformat()
        }
        
        with open('/opt/airflow/data/ml/model_metrics.json', 'w') as f:
            json.dump(model_metrics, f, indent=2)
        
        print(f"✅ Delay prediction model trained")
        print(f"🎯 Accuracy: {accuracy:.2%}")
        print(f"📊 Training samples: {len(X_train)}")
        print(f"🔍 Test samples: {len(X_test)}")
        
        # Top 3 important features
        top_features = sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)[:3]
        print("🏆 Top 3 important features:")
        for feature, importance in top_features:
            print(f"   {feature}: {importance:.3f}")
            
    except Exception as e:
        print(f"❌ Error training delay prediction model: {str(e)}")
        raise

def detect_flight_anomalies(**context):
    """Detect anomalies in flight data"""
    try:
        engine = get_sqlalchemy_engine()
        
        # Get recent flight data for anomaly detection
        anomaly_query = """
        SELECT 
            flight_number,
            airline,
            origin_airport,
            destination_airport,
            flight_duration_minutes,
            status,
            scheduled_departure,
            EXTRACT(HOUR FROM scheduled_departure::timestamp) as departure_hour
        FROM scheduled_flights
        WHERE scheduled_departure::timestamp >= CURRENT_DATE - INTERVAL '7 days'
        ORDER BY scheduled_departure::timestamp DESC;
        """
        
        df = pd.read_sql(anomaly_query, engine)
        
        if df.empty:
            print("⚠️ No recent flight data for anomaly detection")
            return
        
        anomalies = []
        
        # Detect duration anomalies
        duration_stats = df.groupby(['origin_airport', 'destination_airport'])['flight_duration_minutes'].agg(['mean', 'std']).reset_index()
        duration_stats.columns = ['origin_airport', 'destination_airport', 'avg_duration', 'std_duration']
        
        df_with_stats = df.merge(duration_stats, on=['origin_airport', 'destination_airport'], how='left')
        
        # Flag flights with unusual durations (more than 2 standard deviations)
        df_with_stats['duration_zscore'] = abs(
            (df_with_stats['flight_duration_minutes'] - df_with_stats['avg_duration']) / 
            df_with_stats['std_duration'].fillna(1)
        )
        
        duration_anomalies = df_with_stats[df_with_stats['duration_zscore'] > 2]
        
        for _, row in duration_anomalies.iterrows():
            anomalies.append({
                'type': 'duration_anomaly',
                'flight_number': row['flight_number'],
                'airline': row['airline'],
                'route': f"{row['origin_airport']} -> {row['destination_airport']}",
                'actual_duration': row['flight_duration_minutes'],
                'expected_duration': row['avg_duration'],
                'deviation': row['duration_zscore'],
                'severity': 'high' if row['duration_zscore'] > 3 else 'medium'
            })
        
        # Detect scheduling anomalies (flights at unusual hours)
        hourly_counts = df['departure_hour'].value_counts()
        unusual_hours = hourly_counts[hourly_counts < hourly_counts.quantile(0.1)].index
        
        unusual_hour_flights = df[df['departure_hour'].isin(unusual_hours)]
        
        for _, row in unusual_hour_flights.iterrows():
            anomalies.append({
                'type': 'scheduling_anomaly',
                'flight_number': row['flight_number'],
                'airline': row['airline'],
                'route': f"{row['origin_airport']} -> {row['destination_airport']}",
                'departure_hour': row['departure_hour'],
                'severity': 'low'
            })
        
        # Detect airline performance anomalies
        airline_delay_rates = df.groupby('airline').apply(
            lambda x: (x['status'].isin(['Delayed', 'Cancelled'])).mean()
        ).reset_index()
        airline_delay_rates.columns = ['airline', 'delay_rate']
        
        high_delay_airlines = airline_delay_rates[airline_delay_rates['delay_rate'] > 0.3]
        
        for _, row in high_delay_airlines.iterrows():
            anomalies.append({
                'type': 'performance_anomaly',
                'airline': row['airline'],
                'delay_rate': row['delay_rate'],
                'severity': 'high' if row['delay_rate'] > 0.5 else 'medium'
            })
        
        # Save anomalies
        anomaly_report = {
            'total_anomalies': len(anomalies),
            'anomalies_by_type': {
                'duration_anomaly': len([a for a in anomalies if a['type'] == 'duration_anomaly']),
                'scheduling_anomaly': len([a for a in anomalies if a['type'] == 'scheduling_anomaly']),
                'performance_anomaly': len([a for a in anomalies if a['type'] == 'performance_anomaly'])
            },
            'anomalies': anomalies[:20],  # Top 20 anomalies
            'detection_timestamp': datetime.now().isoformat()
        }
        
        with open('/opt/airflow/data/ml/anomaly_report.json', 'w') as f:
            json.dump(anomaly_report, f, indent=2)
        
        print(f"✅ Anomaly detection completed")
        print(f"🚨 Total anomalies found: {len(anomalies)}")
        print(f"⏱️ Duration anomalies: {anomaly_report['anomalies_by_type']['duration_anomaly']}")
        print(f"📅 Scheduling anomalies: {anomaly_report['anomalies_by_type']['scheduling_anomaly']}")
        print(f"📊 Performance anomalies: {anomaly_report['anomalies_by_type']['performance_anomaly']}")
        
    except Exception as e:
        print(f"❌ Error in anomaly detection: {str(e)}")
        raise

def generate_ml_insights(**context):
    """Generate insights from ML models and analysis"""
    try:
        insights = {
            'ml_pipeline_status': 'active',
            'models_trained': [],
            'key_insights': [],
            'recommendations': [],
            'generated_at': datetime.now().isoformat()
        }
        
        # Load model metrics if available
        try:
            with open('/opt/airflow/data/ml/model_metrics.json', 'r') as f:
                model_metrics = json.load(f)
                insights['models_trained'].append({
                    'model_type': 'delay_prediction',
                    'accuracy': model_metrics['accuracy'],
                    'features_count': len(model_metrics['features_used'])
                })
                
                # Generate insights from model
                if model_metrics['accuracy'] > 0.8:
                    insights['key_insights'].append("Flight delay prediction model shows high accuracy (>80%)")
                
                # Top feature insights
                top_feature = max(model_metrics['feature_importance'].items(), key=lambda x: x[1])
                insights['key_insights'].append(f"Most important factor for delays: {top_feature[0]}")
                
        except FileNotFoundError:
            insights['key_insights'].append("Delay prediction model not yet trained")
        
        # Load anomaly report if available
        try:
            with open('/opt/airflow/data/ml/anomaly_report.json', 'r') as f:
                anomaly_report = json.load(f)
                
                if anomaly_report['total_anomalies'] > 0:
                    insights['key_insights'].append(f"Found {anomaly_report['total_anomalies']} anomalies in recent flight data")
                    
                    # Generate recommendations based on anomalies
                    if anomaly_report['anomalies_by_type']['duration_anomaly'] > 5:
                        insights['recommendations'].append("Review flight duration planning - multiple routes showing unusual durations")
                    
                    if anomaly_report['anomalies_by_type']['performance_anomaly'] > 0:
                        insights['recommendations'].append("Investigate airline performance issues - some carriers showing high delay rates")
                
        except FileNotFoundError:
            insights['key_insights'].append("Anomaly detection not yet completed")
        
        # General recommendations
        insights['recommendations'].extend([
            "Continue monitoring flight patterns for seasonal trends",
            "Consider weather data integration for improved delay prediction",
            "Implement real-time alerting for critical anomalies"
        ])
        
        # Save insights
        with open('/opt/airflow/data/ml/ml_insights.json', 'w') as f:
            json.dump(insights, f, indent=2)
        
        print("✅ ML insights generated")
        print(f"🧠 Models trained: {len(insights['models_trained'])}")
        print(f"💡 Key insights: {len(insights['key_insights'])}")
        print(f"📋 Recommendations: {len(insights['recommendations'])}")
        
    except Exception as e:
        print(f"❌ Error generating ML insights: {str(e)}")
        raise

# Define tasks
prepare_data_task = PythonOperator(
    task_id='prepare_ml_data',
    python_callable=prepare_ml_data,
    dag=dag,
)

train_model_task = PythonOperator(
    task_id='train_delay_prediction_model',
    python_callable=train_delay_prediction_model,
    dag=dag,
)

detect_anomalies_task = PythonOperator(
    task_id='detect_flight_anomalies',
    python_callable=detect_flight_anomalies,
    dag=dag,
)

generate_insights_task = PythonOperator(
    task_id='generate_ml_insights',
    python_callable=generate_ml_insights,
    dag=dag,
)

# Set up task dependencies
prepare_data_task >> [train_model_task, detect_anomalies_task] >> generate_insights_task 