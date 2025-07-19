#!/usr/bin/env python3
"""
Real-time Data Puller for Trading Data Pipeline
Continuously pulls real-time market data during trading hours
"""

import asyncio
import logging
import json
import os
from datetime import datetime, time
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from trading_data_pipeline.ingest.kite_loader import KiteDataLoader
from trading_data_pipeline.database.bigquery_manager import BigQueryManager
from trading_data_pipeline.utils.logger import setup_logger


class RealtimeDataPuller:
    """Real-time data puller for live market data"""
    
    def __init__(self):
        self.setup_logging()
        self.kite_loader = KiteDataLoader()
        self.bq_manager = BigQueryManager()
        
        # Trading hours (IST)
        self.market_start = time(9, 15)  # 9:15 AM
        self.market_end = time(15, 30)   # 3:30 PM
        
        # Pull interval (seconds)
        self.pull_interval = 60  # 1 minute
        
        # Symbol list for real-time data
        self.symbols = self.load_realtime_symbols()
        
        self.logger.info("🚀 Real-time Data Puller initialized")
    
    def setup_logging(self):
        """Setup logging configuration"""
        os.makedirs("logs", exist_ok=True)
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('logs/realtime_puller.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def load_realtime_symbols(self):
        """Load symbols for real-time data collection"""
        # Focus on most liquid stocks for real-time data
        return [
            'RELIANCE', 'TCS', 'HDFCBANK', 'INFY', 'HINDUNILVR',
            'ICICIBANK', 'KOTAKBANK', 'BHARTIARTL', 'ITC', 'SBIN',
            'BAJFINANCE', 'ASIANPAINT', 'MARUTI', 'HCLTECH', 'AXISBANK',
            'LT', 'WIPRO', 'NESTLEIND', 'ULTRACEMCO', 'POWERGRID'
        ]
    
    def is_market_open(self):
        """Check if market is currently open"""
        now = datetime.now()
        current_time = now.time()
        current_day = now.weekday()
        
        # Check if it's a weekday (0=Monday, 6=Sunday)
        if current_day >= 5:  # Saturday or Sunday
            return False
        
        # Check market hours
        return self.market_start <= current_time <= self.market_end
    
    async def pull_realtime_data(self):
        """Pull real-time data for all symbols"""
        try:
            self.logger.info(f"📊 Pulling real-time data for {len(self.symbols)} symbols")
            
            # Get current quotes for all symbols
            quotes = await self.kite_loader.get_live_quotes(self.symbols)
            
            if quotes:
                # Process and save quotes
                await self.process_and_save_quotes(quotes)
                self.logger.info(f"✅ Processed {len(quotes)} real-time quotes")
            else:
                self.logger.warning("⚠️ No real-time data received")
                
        except Exception as e:
            self.logger.error(f"❌ Failed to pull real-time data: {e}")
    
    async def process_and_save_quotes(self, quotes):
        """Process and save real-time quotes"""
        try:
            timestamp = datetime.now()
            
            for symbol, quote_data in quotes.items():
                # Format data for storage
                processed_data = {
                    'symbol': symbol,
                    'timestamp': timestamp,
                    'last_price': quote_data.get('last_price', 0),
                    'open': quote_data.get('ohlc', {}).get('open', 0),
                    'high': quote_data.get('ohlc', {}).get('high', 0),
                    'low': quote_data.get('ohlc', {}).get('low', 0),
                    'close': quote_data.get('ohlc', {}).get('close', 0),
                    'volume': quote_data.get('volume', 0),
                    'change': quote_data.get('net_change', 0),
                    'change_percent': quote_data.get('change', 0)
                }
                
                # Save to local storage for backup
                await self.save_locally(symbol, processed_data)
                
                # Save to BigQuery (async)
                asyncio.create_task(self.save_to_bigquery(symbol, processed_data))
                
        except Exception as e:
            self.logger.error(f"❌ Failed to process quotes: {e}")
    
    async def save_locally(self, symbol, data):
        """Save data locally as backup"""
        try:
            os.makedirs("data/realtime", exist_ok=True)
            
            # Save to daily file
            date_str = datetime.now().strftime("%Y%m%d")
            filename = f"data/realtime/realtime_data_{date_str}.jsonl"
            
            with open(filename, 'a') as f:
                json.dump(data, f, default=str)
                f.write('\n')
                
        except Exception as e:
            self.logger.error(f"❌ Failed to save locally for {symbol}: {e}")
    
    async def save_to_bigquery(self, symbol, data):
        """Save data to BigQuery"""
        try:
            # Convert to DataFrame
            import pandas as pd
            df = pd.DataFrame([data])
            
            # Save using BigQuery manager
            success = self.bq_manager.insert_realtime_data(symbol, df)
            
            if not success:
                self.logger.warning(f"⚠️ Failed to save {symbol} to BigQuery")
                
        except Exception as e:
            self.logger.error(f"❌ BigQuery save failed for {symbol}: {e}")
    
    async def start_realtime_session(self):
        """Start real-time data collection session"""
        try:
            self.logger.info("🌅 Starting real-time data collection session")
            
            # Initialize Kite connection
            if not await self.kite_loader.initialize():
                self.logger.error("❌ Failed to initialize Kite connection")
                return
            
            # Wait for market open if needed
            await self.wait_for_market_open()
            
            # Start real-time data collection loop
            await self.realtime_loop()
            
            self.logger.info("🌆 Real-time session completed")
            
        except Exception as e:
            self.logger.error(f"❌ Real-time session failed: {e}")
    
    async def wait_for_market_open(self):
        """Wait for market to open"""
        while not self.is_market_open():
            current_time = datetime.now().time()
            
            if current_time < self.market_start:
                wait_minutes = (datetime.combine(datetime.today(), self.market_start) - 
                              datetime.combine(datetime.today(), current_time)).total_seconds() / 60
                self.logger.info(f"⏰ Market opens in {wait_minutes:.0f} minutes. Waiting...")
                await asyncio.sleep(60)  # Check every minute
            else:
                # After market hours
                self.logger.info("🌙 Market closed. Waiting for next trading day...")
                await asyncio.sleep(3600)  # Check every hour
    
    async def realtime_loop(self):
        """Main real-time data collection loop"""
        self.logger.info(f"📈 Starting real-time data loop (interval: {self.pull_interval}s)")
        
        while self.is_market_open():
            try:
                # Pull real-time data
                await self.pull_realtime_data()
                
                # Wait for next interval
                await asyncio.sleep(self.pull_interval)
                
            except Exception as e:
                self.logger.error(f"❌ Real-time loop error: {e}")
                await asyncio.sleep(10)  # Short wait on error
        
        self.logger.info("🌆 Market closed. Stopping real-time data collection.")
    
    async def run_continuous(self):
        """Run continuous real-time data collection"""
        self.logger.info("🔄 Starting continuous real-time data collection")
        
        while True:
            try:
                await self.start_realtime_session()
                
                # Wait before next session (overnight)
                self.logger.info("😴 Waiting for next trading session...")
                await asyncio.sleep(3600)  # Check every hour
                
            except KeyboardInterrupt:
                self.logger.info("🛑 Real-time puller stopped by user")
                break
            except Exception as e:
                self.logger.error(f"❌ Continuous loop error: {e}")
                await asyncio.sleep(300)  # Wait 5 minutes on error


async def main():
    """Main function"""
    puller = RealtimeDataPuller()
    await puller.run_continuous()


if __name__ == "__main__":
    asyncio.run(main())
