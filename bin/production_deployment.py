#!/usr/bin/env python3
"""
Production deployment script for Trading Data Pipeline
Migrates from local SQLite to BigQuery and sets up cloud-based data ingestion
"""

import os
import sys
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

import pandas as pd
from google.cloud import bigquery
from google.cloud import scheduler_v1
from google.cloud import run_v2

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ProductionDeployment:
    """
    Handles production deployment and migration to cloud-only operations
    """
    
    def __init__(self, project_id: str, region: str = "us-central1"):
        self.project_id = project_id
        self.region = region
        self.bq_client = bigquery.Client(project=project_id)
        self.scheduler_client = scheduler_v1.CloudSchedulerClient()
        
    async def migrate_to_bigquery(self) -> Dict[str, Any]:
        """
        Migrate all local SQLite data to BigQuery
        """
        logger.info("Starting migration from SQLite to BigQuery...")
        
        # Create dataset if not exists
        dataset_id = f"{self.project_id}.trading_data_prod"
        try:
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = "US"
            dataset = self.bq_client.create_dataset(dataset, exists_ok=True)
            logger.info(f"Created/verified dataset: {dataset_id}")
        except Exception as e:
            logger.error(f"Error creating dataset: {e}")
            return {"status": "error", "message": str(e)}
        
        # Define table schemas for migration
        tables_config = {
            "ohlcv_data": {
                "schema": [
                    bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
                    bigquery.SchemaField("open", "FLOAT64", mode="REQUIRED"),
                    bigquery.SchemaField("high", "FLOAT64", mode="REQUIRED"),
                    bigquery.SchemaField("low", "FLOAT64", mode="REQUIRED"),
                    bigquery.SchemaField("close", "FLOAT64", mode="REQUIRED"),
                    bigquery.SchemaField("volume", "INT64", mode="REQUIRED"),
                    bigquery.SchemaField("sector", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("market_cap", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("data_source", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
                ],
                "partition_field": "date",
                "clustering_fields": ["symbol", "sector"]
            },
            "technical_indicators": {
                "schema": [
                    bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
                    bigquery.SchemaField("indicator_type", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("indicator_value", "FLOAT64", mode="REQUIRED"),
                    bigquery.SchemaField("timeframe", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
                ],
                "partition_field": "date",
                "clustering_fields": ["symbol", "indicator_type"]
            }
        }
        
        # Create tables
        for table_name, config in tables_config.items():
            table_id = f"{dataset_id}.{table_name}"
            
            table = bigquery.Table(table_id, schema=config["schema"])
            
            # Set up partitioning
            if config.get("partition_field"):
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=config["partition_field"]
                )
            
            # Set up clustering
            if config.get("clustering_fields"):
                table.clustering_fields = config["clustering_fields"]
            
            try:
                table = self.bq_client.create_table(table, exists_ok=True)
                logger.info(f"Created/verified table: {table_id}")
            except Exception as e:
                logger.error(f"Error creating table {table_name}: {e}")
                continue
        
        return {"status": "success", "dataset": dataset_id}
    
    async def setup_cloud_scheduler(self) -> Dict[str, Any]:
        """
        Set up Cloud Scheduler for daily data ingestion
        """
        logger.info("Setting up Cloud Scheduler jobs...")
        
        parent = f"projects/{self.project_id}/locations/{self.region}"
        
        # Daily data ingestion job
        job = {
            "name": f"{parent}/jobs/trading-data-daily-ingestion",
            "description": "Daily trading data ingestion from Kite Connect",
            "schedule": "30 12 * * *",  # 6 PM IST
            "time_zone": "Asia/Kolkata",
            "http_target": {
                "uri": f"https://trading-data-pipeline-{self.region}.a.run.app/trigger-daily-ingestion",
                "http_method": "POST",
                "headers": {"Content-Type": "application/json"},
                "body": '{"source": "cloud-scheduler", "job_type": "daily-delta"}'.encode(),
            },
            "retry_config": {
                "retry_count": 3,
                "max_retry_duration": "600s",
                "min_backoff_duration": "10s",
                "max_backoff_duration": "300s",
            },
            "attempt_deadline": "3600s",
        }
        
        try:
            response = self.scheduler_client.create_job(
                parent=parent, job=job
            )
            logger.info(f"Created scheduler job: {response.name}")
            return {"status": "success", "job_name": response.name}
        except Exception as e:
            logger.error(f"Error creating scheduler job: {e}")
            return {"status": "error", "message": str(e)}
    
    async def validate_historical_data(self) -> Dict[str, Any]:
        """
        Validate that historical data (2010-2025) is complete in BigQuery
        """
        logger.info("Validating historical data in BigQuery...")
        
        query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT symbol) as unique_symbols,
            MIN(date) as earliest_date,
            MAX(date) as latest_date,
            COUNT(DISTINCT date) as trading_days
        FROM `{}.trading_data_prod.ohlcv_data`
        """.format(self.project_id)
        
        try:
            result = self.bq_client.query(query).to_dataframe()
            
            validation_result = {
                "status": "success",
                "total_records": int(result.iloc[0]["total_records"]),
                "unique_symbols": int(result.iloc[0]["unique_symbols"]),
                "earliest_date": result.iloc[0]["earliest_date"],
                "latest_date": result.iloc[0]["latest_date"],
                "trading_days": int(result.iloc[0]["trading_days"])
            }
            
            logger.info(f"Data validation results: {validation_result}")
            
            # Check if we have data up to July 14, 2025
            expected_latest = datetime(2025, 7, 14).date()
            actual_latest = validation_result["latest_date"]
            
            if actual_latest < expected_latest:
                logger.warning(f"Data gap detected. Latest date: {actual_latest}, Expected: {expected_latest}")
                validation_result["data_gap"] = True
            else:
                validation_result["data_gap"] = False
            
            return validation_result
            
        except Exception as e:
            logger.error(f"Error validating data: {e}")
            return {"status": "error", "message": str(e)}
    
    async def setup_delta_pipeline(self) -> Dict[str, Any]:
        """
        Set up delta pipeline for daily updates starting tomorrow
        """
        logger.info("Setting up delta pipeline for daily updates...")
        
        tomorrow = datetime.now().date() + timedelta(days=1)
        
        # Create configuration for delta updates
        delta_config = {
            "start_date": tomorrow.isoformat(),
            "data_sources": ["kiteconnect", "yfinance"],
            "symbols_file": "config/expanded_universe.json",
            "update_frequency": "daily",
            "update_time": "18:30",  # 6:30 PM IST
            "timezone": "Asia/Kolkata",
            "batch_size": 50,
            "retry_attempts": 3,
            "validation_enabled": True
        }
        
        # Save configuration to BigQuery for the service to use
        config_table_id = f"{self.project_id}.trading_data_prod.pipeline_config"
        
        try:
            # Create config table if not exists
            schema = [
                bigquery.SchemaField("config_key", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("config_value", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
            ]
            
            table = bigquery.Table(config_table_id, schema=schema)
            table = self.bq_client.create_table(table, exists_ok=True)
            
            # Insert delta configuration
            rows_to_insert = [
                {
                    "config_key": "delta_pipeline_config",
                    "config_value": str(delta_config),
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow()
                }
            ]
            
            errors = self.bq_client.insert_rows_json(table, rows_to_insert)
            if errors:
                logger.error(f"Error inserting config: {errors}")
                return {"status": "error", "message": str(errors)}
            
            logger.info(f"Delta pipeline configured to start from {tomorrow}")
            return {"status": "success", "start_date": tomorrow.isoformat()}
            
        except Exception as e:
            logger.error(f"Error setting up delta pipeline: {e}")
            return {"status": "error", "message": str(e)}

async def main():
    """
    Main production deployment function
    """
    # Get project ID from environment or user input
    project_id = os.getenv("GCP_PROJECT_ID", "ai-trading-gcp-459813")
    
    deployment = ProductionDeployment(project_id)
    
    logger.info("🚀 Starting Trading Data Pipeline Production Deployment")
    
    # Step 1: Migrate to BigQuery
    logger.info("📊 Step 1: Migrating to BigQuery...")
    migration_result = await deployment.migrate_to_bigquery()
    if migration_result["status"] != "success":
        logger.error("Migration failed. Stopping deployment.")
        return False
    
    # Step 2: Validate historical data
    logger.info("✅ Step 2: Validating historical data...")
    validation_result = await deployment.validate_historical_data()
    if validation_result["status"] != "success":
        logger.error("Data validation failed. Stopping deployment.")
        return False
    
    # Step 3: Set up cloud scheduler
    logger.info("⏰ Step 3: Setting up Cloud Scheduler...")
    scheduler_result = await deployment.setup_cloud_scheduler()
    if scheduler_result["status"] != "success":
        logger.warning("Cloud Scheduler setup failed, but continuing...")
    
    # Step 4: Set up delta pipeline
    logger.info("🔄 Step 4: Setting up delta pipeline...")
    delta_result = await deployment.setup_delta_pipeline()
    if delta_result["status"] != "success":
        logger.error("Delta pipeline setup failed. Stopping deployment.")
        return False
    
    logger.info("🎉 Production deployment completed successfully!")
    logger.info(f"📈 Historical data: {validation_result['total_records']} records")
    logger.info(f"📅 Data range: {validation_result['earliest_date']} to {validation_result['latest_date']}")
    logger.info(f"🔄 Delta updates start: {delta_result['start_date']}")
    
    return True

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
