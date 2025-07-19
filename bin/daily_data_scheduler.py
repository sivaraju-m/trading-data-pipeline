#!/usr/bin/env python3
"""
Daily Data Scheduler for Trading Data Pipeline
Automated daily data pull from Kite Connect API to BigQuery
"""

import schedule
import time
import logging
import yaml
import os
from datetime import datetime, timedelta
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from trading_data_pipeline.ingest.kite_loader import KiteDataLoader
from trading_data_pipeline.database.bigquery_manager import BigQueryManager
from trading_data_pipeline.utils.logger import setup_logger


class DailyDataScheduler:
    """Daily data scheduler for automated ETL operations"""
    
    def __init__(self):
        self.setup_logging()
        self.load_config()
        self.kite_loader = KiteDataLoader()
        self.bq_manager = BigQueryManager()
        self.logger.info("🚀 Daily Data Scheduler initialized")
    
    def setup_logging(self):
        """Setup logging configuration"""
        os.makedirs("logs", exist_ok=True)
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('logs/daily_scheduler.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def load_config(self):
        """Load scheduler configuration"""
        config_path = project_root / "config" / "scheduler" / "daily_config.yaml"
        if config_path.exists():
            with open(config_path, 'r') as f:
                self.config = yaml.safe_load(f)
        else:
            # Default configuration
            self.config = {
                'scheduler': {
                    'daily_pull_time': '18:00',
                    'retry_attempts': 3,
                    'retry_delay': 300
                },
                'data_sources': {
                    'primary': 'kiteconnect',
                    'fallback': 'yfinance'
                },
                'universe': {
                    'nifty50': True,
                    'nifty_next50': True,
                    'custom_symbols': []
                }
            }
    
    def run_daily_data_pull(self):
        """Execute daily data pull for all symbols"""
        try:
            self.logger.info("🌅 Starting daily data pull...")
            start_time = datetime.now()
            
            # Load symbol universe
            symbols = self.load_trading_universe()
            self.logger.info(f"📊 Processing {len(symbols)} symbols")
            
            successful_updates = 0
            failed_updates = 0
            
            # Pull data for each symbol
            for i, symbol in enumerate(symbols, 1):
                try:
                    self.logger.info(f"🔄 Processing {symbol} ({i}/{len(symbols)})")
                    success = self.pull_symbol_data(symbol)
                    
                    if success:
                        successful_updates += 1
                    else:
                        failed_updates += 1
                        
                    # Rate limiting - wait between requests
                    time.sleep(1)
                    
                except Exception as e:
                    self.logger.error(f"❌ Failed to process {symbol}: {e}")
                    failed_updates += 1
            
            duration = datetime.now() - start_time
            self.logger.info(f"✅ Daily data pull completed in {duration}")
            self.logger.info(f"📊 Results: {successful_updates} successful, {failed_updates} failed")
            
            # Send completion notification
            self.send_completion_notification(successful_updates, failed_updates, duration)
            
        except Exception as e:
            self.logger.error(f"❌ Daily data pull failed: {e}")
            self.send_error_notification(str(e))
    
    def pull_symbol_data(self, symbol):
        """Pull data for a single symbol"""
        try:
            # Get yesterday's data (since we run after market close)
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=1)
            
            # Try primary source (Kite Connect)
            data = None
            if self.config['data_sources']['primary'] == 'kiteconnect':
                data = self.kite_loader.fetch_historical_data(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    interval='day'
                )
            
            # Fallback to yfinance if needed
            if data is None or data.empty:
                self.logger.warning(f"⚠️ Primary source failed for {symbol}, trying fallback")
                # Implement yfinance fallback here
                pass
            
            if data is not None and not data.empty:
                # Save to BigQuery
                success = self.bq_manager.insert_daily_data(symbol, data)
                if success:
                    self.logger.info(f"✅ Updated data for {symbol}")
                    return True
                else:
                    self.logger.error(f"❌ Failed to save data for {symbol}")
                    return False
            else:
                self.logger.warning(f"⚠️ No data available for {symbol}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Error processing {symbol}: {e}")
            return False
    
    def load_trading_universe(self):
        """Load trading universe from config"""
        symbols = []
        
        # Load from config
        if self.config['universe']['nifty50']:
            symbols.extend(self.get_nifty50_symbols())
        
        if self.config['universe']['nifty_next50']:
            symbols.extend(self.get_nifty_next50_symbols())
        
        # Add custom symbols
        symbols.extend(self.config['universe']['custom_symbols'])
        
        # Remove duplicates and return
        return list(set(symbols))
    
    def get_nifty50_symbols(self):
        """Get Nifty 50 symbol list"""
        return [
            'RELIANCE', 'TCS', 'HDFCBANK', 'INFY', 'HINDUNILVR',
            'ICICIBANK', 'KOTAKBANK', 'BHARTIARTL', 'ITC', 'SBIN',
            'BAJFINANCE', 'ASIANPAINT', 'MARUTI', 'HCLTECH', 'AXISBANK',
            'LT', 'WIPRO', 'NESTLEIND', 'ULTRACEMCO', 'POWERGRID',
            'TITAN', 'SUNPHARMA', 'NTPC', 'JSWSTEEL', 'TATAMOTORS',
            'COALINDIA', 'TECHM', 'GRASIM', 'INDUSINDBK', 'BAJAJFINSV',
            'EICHERMOT', 'BPCL', 'HEROMOTOCO', 'TATACONSUM', 'ADANIENT',
            'BAJAJ-AUTO', 'TATASTEEL', 'UPL', 'SHRIRAMFIN', 'SBILIFE',
            'APOLLOHOSP', 'HINDALCO', 'DIVISLAB', 'CIPLA', 'BRITANNIA',
            'ONGC', 'DRREDDY', 'TRENT', 'ADANIPORTS', 'HDFCLIFE'
        ]
    
    def get_nifty_next50_symbols(self):
        """Get Nifty Next 50 symbol list"""
        return [
            'ADANIGREEN', 'ADANIPOWER', 'ATGL', 'BOSCHLTD', 'COLPAL',
            'DMART', 'GAIL', 'GODREJCP', 'HAL', 'HAVELLS',
            'HDFCLIFE', 'ICICIPRULI', 'IOC', 'IRCTC', 'JINDALSTEL',
            'LTIM', 'MOTHERSON', 'MPHASIS', 'NMDC', 'PAGEIND',
            'PIDILITIND', 'POLYCAB', 'PVR', 'SAIL', 'SIEMENS',
            'TORNTPHARM', 'VOLTAS', 'ZEEL', 'BANKBARODA', 'BERGEPAINT',
            'CADILAHC', 'CONCOR', 'COROMANDEL', 'CUMMINSIND', 'DABUR',
            'GLENMARK', 'IDFCFIRSTB', 'LUPIN', 'MARICO', 'MCDOWELL-N',
            'MFSL', 'MGL', 'OFSS', 'PETRONET', 'PIIND',
            'PFC', 'RECLTD', 'SRF', 'ZYDUSLIFE', 'ACC'
        ]
    
    def run_weekly_cleanup(self):
        """Weekly data cleanup and maintenance"""
        try:
            self.logger.info("🧹 Running weekly data cleanup...")
            
            # Clean up old log files
            self.cleanup_old_logs()
            
            # Validate data quality
            self.validate_data_quality()
            
            # Generate weekly data quality report
            self.generate_weekly_report()
            
            self.logger.info("✅ Weekly cleanup completed")
            
        except Exception as e:
            self.logger.error(f"❌ Weekly cleanup failed: {e}")
    
    def cleanup_old_logs(self):
        """Clean up log files older than 30 days"""
        log_dir = Path("logs")
        if log_dir.exists():
            cutoff_date = datetime.now() - timedelta(days=30)
            for log_file in log_dir.glob("*.log"):
                if log_file.stat().st_mtime < cutoff_date.timestamp():
                    log_file.unlink()
                    self.logger.info(f"🗑️ Deleted old log file: {log_file}")
    
    def validate_data_quality(self):
        """Validate data quality in BigQuery"""
        try:
            # Run data quality checks
            quality_report = self.bq_manager.run_data_quality_checks()
            
            if quality_report['issues']:
                self.logger.warning(f"⚠️ Data quality issues found: {quality_report['issues']}")
                # Send alert for data quality issues
                self.send_data_quality_alert(quality_report)
            else:
                self.logger.info("✅ Data quality validation passed")
                
        except Exception as e:
            self.logger.error(f"❌ Data quality validation failed: {e}")
    
    def generate_weekly_report(self):
        """Generate weekly data pipeline report"""
        try:
            report_data = {
                'timestamp': datetime.now().isoformat(),
                'data_pipeline_status': 'healthy',
                'total_symbols': len(self.load_trading_universe()),
                'data_coverage': self.bq_manager.get_data_coverage_stats(),
                'quality_metrics': self.bq_manager.get_quality_metrics()
            }
            
            # Save report
            os.makedirs("reports", exist_ok=True)
            report_file = f"reports/weekly_report_{datetime.now().strftime('%Y%m%d')}.json"
            
            import json
            with open(report_file, 'w') as f:
                json.dump(report_data, f, indent=2)
            
            self.logger.info(f"📊 Weekly report generated: {report_file}")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to generate weekly report: {e}")
    
    def send_completion_notification(self, successful, failed, duration):
        """Send completion notification"""
        self.logger.info(f"📧 Daily data pull completed - Success: {successful}, Failed: {failed}, Duration: {duration}")
        # Implement notification logic (email, Slack, etc.)
    
    def send_error_notification(self, error_message):
        """Send error notification"""
        self.logger.error(f"📧 Error notification: {error_message}")
        # Implement error notification logic
    
    def send_data_quality_alert(self, quality_report):
        """Send data quality alert"""
        self.logger.warning(f"📧 Data quality alert: {quality_report}")
        # Implement data quality alert logic
    
    def schedule_jobs(self):
        """Schedule daily data pull jobs"""
        pull_time = self.config['scheduler']['daily_pull_time']
        
        # Schedule daily data pull at configured time (after market close)
        schedule.every().day.at(pull_time).do(self.run_daily_data_pull)
        
        # Schedule weekend data cleanup
        schedule.every().sunday.at("09:00").do(self.run_weekly_cleanup)
        
        self.logger.info(f"📅 Scheduled daily data pull at {pull_time}")
        self.logger.info("📅 Scheduled weekly cleanup on Sundays at 09:00")
    
    def start(self):
        """Start the scheduler"""
        self.logger.info("🚀 Starting daily data scheduler...")
        self.schedule_jobs()
        
        while True:
            try:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
            except KeyboardInterrupt:
                self.logger.info("🛑 Scheduler stopped by user")
                break
            except Exception as e:
                self.logger.error(f"❌ Scheduler error: {e}")
                time.sleep(60)  # Wait before retrying


if __name__ == "__main__":
    scheduler = DailyDataScheduler()
    scheduler.start()
