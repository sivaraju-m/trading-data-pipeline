"""
BigQuery Manager for Trading Data Pipeline
Handles all BigQuery operations for data storage and retrieval
"""

import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import os


class BigQueryManager:
    """Manages BigQuery operations for trading data"""
    
    def __init__(self, project_id: str = None, dataset_id: str = None):
        self.project_id = project_id or os.getenv('GCP_PROJECT_ID', 'ai-trading-gcp-459813')
        self.dataset_id = dataset_id or os.getenv('BQ_DATASET', 'trading_data')
        self.client = bigquery.Client(project=self.project_id)
        self.logger = logging.getLogger(__name__)
        
        # Ensure dataset exists
        self._ensure_dataset_exists()
    
    def _ensure_dataset_exists(self):
        """Ensure the dataset exists, create if not"""
        try:
            dataset_ref = self.client.dataset(self.dataset_id)
            self.client.get_dataset(dataset_ref)
            self.logger.debug(f"✅ Dataset {self.dataset_id} exists")
        except NotFound:
            self.logger.info(f"📊 Creating dataset {self.dataset_id}")
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            self.client.create_dataset(dataset)
            self.logger.info(f"✅ Created dataset {self.dataset_id}")
    
    def insert_daily_data(self, symbol: str, data: pd.DataFrame) -> bool:
        """Insert daily data for a symbol into BigQuery"""
        try:
            if data.empty:
                self.logger.warning(f"⚠️ No data to insert for {symbol}")
                return False
            
            # Prepare data for BigQuery
            df = data.copy()
            df['symbol'] = symbol
            df['updated_at'] = datetime.utcnow()
            
            # Ensure proper column types
            df = self._prepare_dataframe_for_bq(df)
            
            # Define table
            table_id = f"{self.project_id}.{self.dataset_id}.historical_prices_cleaned"
            
            # Configure job
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                create_disposition="CREATE_IF_NEEDED",
                autodetect=True
            )
            
            # Insert data
            job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()  # Wait for job to complete
            
            self.logger.info(f"✅ Inserted {len(df)} records for {symbol}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to insert data for {symbol}: {e}")
            return False
    
    def _prepare_dataframe_for_bq(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare DataFrame for BigQuery insertion"""
        # Ensure date column is properly formatted
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date']).dt.date
        
        # Ensure numeric columns are properly typed
        numeric_columns = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Remove any NaN values in critical columns
        df = df.dropna(subset=['date', 'close'])
        
        return df
    
    def get_data_coverage_stats(self) -> Dict[str, Any]:
        """Get data coverage statistics"""
        try:
            query = f"""
            SELECT 
                COUNT(DISTINCT symbol) as unique_symbols,
                COUNT(*) as total_records,
                MIN(date) as earliest_date,
                MAX(date) as latest_date,
                COUNT(DISTINCT date) as unique_dates
            FROM `{self.project_id}.{self.dataset_id}.historical_prices_cleaned`
            """
            
            result = self.client.query(query).to_dataframe()
            
            if not result.empty:
                stats = result.iloc[0].to_dict()
                # Convert timestamps to strings for JSON serialization
                for key, value in stats.items():
                    if pd.isna(value):
                        stats[key] = None
                    elif hasattr(value, 'strftime'):
                        stats[key] = value.strftime('%Y-%m-%d')
                
                return stats
            else:
                return {}
                
        except Exception as e:
            self.logger.error(f"❌ Failed to get coverage stats: {e}")
            return {}
    
    def get_quality_metrics(self) -> Dict[str, Any]:
        """Get data quality metrics"""
        try:
            query = f"""
            SELECT 
                symbol,
                COUNT(*) as record_count,
                COUNT(CASE WHEN open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL THEN 1 END) as null_ohlc_count,
                COUNT(CASE WHEN volume IS NULL OR volume = 0 THEN 1 END) as zero_volume_count,
                MIN(date) as first_date,
                MAX(date) as last_date
            FROM `{self.project_id}.{self.dataset_id}.historical_prices_cleaned`
            GROUP BY symbol
            ORDER BY symbol
            """
            
            result = self.client.query(query).to_dataframe()
            
            if not result.empty:
                # Calculate quality metrics
                total_symbols = len(result)
                symbols_with_nulls = len(result[result['null_ohlc_count'] > 0])
                symbols_with_zero_volume = len(result[result['zero_volume_count'] > 0])
                
                return {
                    'total_symbols': total_symbols,
                    'symbols_with_null_ohlc': symbols_with_nulls,
                    'symbols_with_zero_volume': symbols_with_zero_volume,
                    'null_ohlc_percentage': (symbols_with_nulls / total_symbols * 100) if total_symbols > 0 else 0,
                    'zero_volume_percentage': (symbols_with_zero_volume / total_symbols * 100) if total_symbols > 0 else 0,
                    'quality_score': max(0, 100 - (symbols_with_nulls / total_symbols * 50) - (symbols_with_zero_volume / total_symbols * 30)) if total_symbols > 0 else 0
                }
            else:
                return {'quality_score': 0}
                
        except Exception as e:
            self.logger.error(f"❌ Failed to get quality metrics: {e}")
            return {'quality_score': 0}
    
    def run_data_quality_checks(self) -> Dict[str, Any]:
        """Run comprehensive data quality checks"""
        try:
            issues = []
            
            # Check for missing recent data
            yesterday = (datetime.now() - timedelta(days=1)).date()
            query = f"""
            SELECT 
                COUNT(DISTINCT symbol) as symbols_updated_yesterday
            FROM `{self.project_id}.{self.dataset_id}.historical_prices_cleaned`
            WHERE date = '{yesterday}'
            """
            
            result = self.client.query(query).to_dataframe()
            symbols_updated = result.iloc[0]['symbols_updated_yesterday'] if not result.empty else 0
            
            if symbols_updated < 50:  # Expecting at least 50 symbols
                issues.append(f"Only {symbols_updated} symbols updated for {yesterday}")
            
            # Check for data gaps
            query = f"""
            WITH date_gaps AS (
                SELECT 
                    symbol,
                    date,
                    LAG(date) OVER (PARTITION BY symbol ORDER BY date) as prev_date,
                    DATE_DIFF(date, LAG(date) OVER (PARTITION BY symbol ORDER BY date), DAY) as gap_days
                FROM `{self.project_id}.{self.dataset_id}.historical_prices_cleaned`
                WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
            )
            SELECT 
                symbol,
                COUNT(*) as gap_count
            FROM date_gaps
            WHERE gap_days > 3  # More than 3 days gap (accounting for weekends)
            GROUP BY symbol
            HAVING COUNT(*) > 0
            """
            
            gap_result = self.client.query(query).to_dataframe()
            if not gap_result.empty:
                symbols_with_gaps = len(gap_result)
                issues.append(f"{symbols_with_gaps} symbols have data gaps > 3 days")
            
            # Check for duplicate records
            query = f"""
            SELECT 
                symbol,
                date,
                COUNT(*) as duplicate_count
            FROM `{self.project_id}.{self.dataset_id}.historical_prices_cleaned`
            WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
            GROUP BY symbol, date
            HAVING COUNT(*) > 1
            """
            
            dup_result = self.client.query(query).to_dataframe()
            if not dup_result.empty:
                duplicate_count = len(dup_result)
                issues.append(f"{duplicate_count} duplicate records found in last 7 days")
            
            return {
                'timestamp': datetime.now().isoformat(),
                'issues': issues,
                'symbols_updated_yesterday': symbols_updated,
                'checks_passed': len(issues) == 0
            }
            
        except Exception as e:
            self.logger.error(f"❌ Data quality checks failed: {e}")
            return {
                'timestamp': datetime.now().isoformat(),
                'issues': [f"Quality check failed: {e}"],
                'checks_passed': False
            }
    
    def get_symbol_data(self, symbol: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """Get historical data for a symbol"""
        try:
            conditions = [f"symbol = '{symbol}'"]
            
            if start_date:
                conditions.append(f"date >= '{start_date}'")
            if end_date:
                conditions.append(f"date <= '{end_date}'")
            
            where_clause = " AND ".join(conditions)
            
            query = f"""
            SELECT *
            FROM `{self.project_id}.{self.dataset_id}.historical_prices_cleaned`
            WHERE {where_clause}
            ORDER BY date
            """
            
            result = self.client.query(query).to_dataframe()
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Failed to get data for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_latest_data_date(self, symbol: str = None) -> Optional[str]:
        """Get the latest date for which data is available"""
        try:
            if symbol:
                query = f"""
                SELECT MAX(date) as latest_date
                FROM `{self.project_id}.{self.dataset_id}.historical_prices_cleaned`
                WHERE symbol = '{symbol}'
                """
            else:
                query = f"""
                SELECT MAX(date) as latest_date
                FROM `{self.project_id}.{self.dataset_id}.historical_prices_cleaned`
                """
            
            result = self.client.query(query).to_dataframe()
            if not result.empty and result.iloc[0]['latest_date'] is not None:
                return result.iloc[0]['latest_date'].strftime('%Y-%m-%d')
            else:
                return None
                
        except Exception as e:
            self.logger.error(f"❌ Failed to get latest date: {e}")
            return None
    
    def delete_symbol_data(self, symbol: str, date: str = None) -> bool:
        """Delete data for a symbol (optionally for a specific date)"""
        try:
            conditions = [f"symbol = '{symbol}'"]
            if date:
                conditions.append(f"date = '{date}'")
            
            where_clause = " AND ".join(conditions)
            
            query = f"""
            DELETE FROM `{self.project_id}.{self.dataset_id}.historical_prices_cleaned`
            WHERE {where_clause}
            """
            
            job = self.client.query(query)
            job.result()
            
            self.logger.info(f"🗑️ Deleted data for {symbol}" + (f" on {date}" if date else ""))
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to delete data for {symbol}: {e}")
            return False
