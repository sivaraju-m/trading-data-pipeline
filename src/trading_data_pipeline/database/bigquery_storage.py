"""
Database utility functions for trading data pipeline.
"""

import pandas as pd
import logging
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

logger = logging.getLogger(__name__)

class BigQueryStorage:
    """
    Class for handling BigQuery storage operations.
    """
    
    def __init__(self, project_id, dataset_id):
        """
        Initialize BigQuery client.
        
        Args:
            project_id (str): GCP project ID
            dataset_id (str): BigQuery dataset ID
        """
        self.client = bigquery.Client(project=project_id)
        self.dataset_id = dataset_id
        self.project_id = project_id
        
        # Ensure dataset exists
        self._ensure_dataset_exists()
        
    def _ensure_dataset_exists(self):
        """
        Create dataset if it doesn't exist.
        """
        dataset_ref = self.client.dataset(self.dataset_id)
        try:
            self.client.get_dataset(dataset_ref)
            logger.info(f"Dataset {self.dataset_id} already exists")
        except NotFound:
            logger.info(f"Creating dataset {self.dataset_id}")
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            self.client.create_dataset(dataset)
            
    def ensure_table_exists(self, table_id, schema):
        """
        Create table if it doesn't exist.
        
        Args:
            table_id (str): Table ID
            schema (list): BigQuery table schema
        """
        table_ref = self.client.dataset(self.dataset_id).table(table_id)
        try:
            self.client.get_table(table_ref)
            logger.info(f"Table {table_id} already exists")
        except NotFound:
            logger.info(f"Creating table {table_id}")
            table = bigquery.Table(table_ref, schema=schema)
            self.client.create_table(table)
            
    def upload_dataframe(self, df, table_id, write_disposition="WRITE_APPEND"):
        """
        Upload DataFrame to BigQuery table.
        
        Args:
            df (pandas.DataFrame): DataFrame to upload
            table_id (str): Table ID
            write_disposition (str): Write disposition (WRITE_APPEND, WRITE_TRUNCATE, WRITE_EMPTY)
            
        Returns:
            bool: Success status
        """
        try:
            table_ref = self.client.dataset(self.dataset_id).table(table_id)
            job_config = bigquery.LoadJobConfig(
                write_disposition=write_disposition,
            )
            
            job = self.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
            job.result()  # Wait for job to complete
            
            logger.info(f"Uploaded {len(df)} rows to {table_id}")
            return True
        
        except Exception as e:
            logger.error(f"Error uploading data to {table_id}: {str(e)}")
            return False
