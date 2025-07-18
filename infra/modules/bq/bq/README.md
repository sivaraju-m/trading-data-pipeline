# BigQuery Module - AI Trading Machine
# ====================================

## 📋 Table Creation Standards

### Naming Convention
```
Resource Name: google_bigquery_table.<category>_<table_name>
Table ID:      <category>_<table_name>
File Name:     tables_<category>.tf
```

### Categories
- **market**: Market data (OHLCV, indicators)
- **trading**: Trade execution, orders
- **portfolio**: Positions, holdings
- **strategy**: Performance, backtesting
- **signal**: Trading signals, alerts
- **risk**: Risk metrics, compliance

## 🚀 Quick Table Creation

### 1. Market Data Tables
```bash
# CREATE TABLE market_ohlcv_data
terraform apply -target=module.bigquery.google_bigquery_table.market_ohlcv_data

# CREATE TABLE market_technical_indicators
terraform apply -target=module.bigquery.google_bigquery_table.market_technical_indicators
```

### 2. Trading Tables
```bash
# CREATE TABLE trading_executions
terraform apply -target=module.bigquery.google_bigquery_table.trading_executions

# CREATE TABLE trading_orders
terraform apply -target=module.bigquery.google_bigquery_table.trading_orders
```

### 3. Portfolio Tables
```bash
# CREATE TABLE portfolio_positions
terraform apply -target=module.bigquery.google_bigquery_table.portfolio_positions

# CREATE TABLE portfolio_snapshots
terraform apply -target=module.bigquery.google_bigquery_table.portfolio_snapshots
```

### 4. Strategy Tables
```bash
# CREATE TABLE strategy_performance
terraform apply -target=module.bigquery.google_bigquery_table.strategy_performance

# CREATE TABLE strategy_validation_summary
terraform apply -target=module.bigquery.google_bigquery_table.strategy_validation_summary
```

### 5. Enhanced Tables
```bash
# CREATE TABLE historical_prices_cleaned
terraform apply -target=module.bigquery.google_bigquery_table.historical_prices_cleaned

# CREATE TABLE historical_prices_filled_enhanced
terraform apply -target=module.bigquery.google_bigquery_table.historical_prices_filled_enhanced

# CREATE TABLE strategy_results_enhanced
terraform apply -target=module.bigquery.google_bigquery_table.strategy_results_enhanced

# CREATE TABLE trading_signals
terraform apply -target=module.bigquery.google_bigquery_table.trading_signals

# CREATE TABLE strategy_performance_summary
terraform apply -target=module.bigquery.google_bigquery_table.strategy_performance_summary
```

## 📊 Current Table Structure

### File Organization
```
infra/modules/bq/
├── dataset.tf              # BigQuery dataset configuration
├── tables_market_data.tf   # Market data tables
├── tables_trading.tf       # Trading execution tables
├── tables_portfolio.tf     # Portfolio management tables
├── tables_strategy.tf      # Strategy performance tables
├── materialized_views.tf   # Performance-optimized views
├── external_tables.tf      # Cost-optimized external tables
├── outputs.tf              # Module outputs
├── variables.tf            # Module variables
└── table_template.tf       # Standardized template
```

### Available Tables
| Table Name | Category | Partition Field | Clustering Fields |
|------------|----------|----------------|-------------------|
| `ohlcv_data` | market | date | symbol, sector |
| `technical_indicators` | market | date | symbol, indicator_type |
| `trades_execution` | trading | trade_date | strategy_id, symbol |
| `portfolio_positions` | portfolio | position_date | portfolio_id, symbol |
| `strategy_performance` | strategy | backtest_date | strategy_id, symbol |
| `strategy_validation_summary` | strategy | validation_date | strategy, time_period |
| `historical_prices_cleaned` | market | date | symbol |
| `historical_prices_filled_enhanced` | market | date | symbol |
| `strategy_results_enhanced` | strategy | result_date | strategy_id |
| `trading_signals` | signal | signal_date | strategy_id, symbol |
| `strategy_performance_summary` | strategy | summary_date | strategy_id |

## 🔧 Standard Configuration

### All Tables Include:
- ✅ **Daily Partitioning** - Cost optimization
- ✅ **Smart Clustering** - Query performance
- ✅ **7-Year Retention** - SEBI compliance
- ✅ **Audit Fields** - created_at, updated_at
- ✅ **Deletion Protection** - Configurable

### Partition Strategy
```hcl
time_partitioning {
  type  = "DAY"
  field = "date"  # or trade_date, position_date, etc.
  require_partition_filter = true
}
```

### Clustering Strategy
```hcl
clustering = ["high_cardinality_field", "medium_cardinality_field"]
```

## 💰 Cost Optimization

### Query Patterns
```sql
-- ✅ GOOD: Uses partition filter
SELECT * FROM trading_data.ohlcv_data
WHERE date >= '2024-01-01' AND symbol = 'RELIANCE'

-- ❌ BAD: No partition filter
SELECT * FROM trading_data.ohlcv_data
WHERE symbol = 'RELIANCE'
```

### Storage Optimization
- **Partitioning**: Reduces scan costs by 95%+
- **Clustering**: Improves query performance by 80%+
- **Columnar**: Only scans required columns
- **Compression**: Automatic data compression

## 🚀 Deployment Commands

### Deploy All Tables
```bash
cd infra
terraform init
terraform plan -target=module.bigquery
terraform apply -target=module.bigquery
```

### Deploy Specific Category
```bash
# Market data tables only
terraform apply -target=module.bigquery.google_bigquery_table.market_ohlcv_data
terraform apply -target=module.bigquery.google_bigquery_table.market_technical_indicators

# Trading tables only
terraform apply -target=module.bigquery.google_bigquery_table.trading_executions
```

### Verify Deployment
```bash
# Check table creation
terraform state list | grep bigquery_table

# View table details
terraform show module.bigquery.google_bigquery_table.market_ohlcv_data
```

## 📈 Materialized Views

### Available Views
| View Name | Purpose | Refresh | Cost Savings |
|-----------|---------|---------|--------------|
| `daily_returns_mv` | Daily price returns | Auto | 90% |
| `strategy_metrics_mv` | Strategy KPIs | Auto | 85% |
| `portfolio_summary_mv` | Portfolio overview | Auto | 80% |
| `risk_metrics_mv` | Risk calculations | Auto | 75% |

### Query Materialized Views
```sql
-- High-performance pre-computed metrics
SELECT * FROM trading_data.daily_returns_mv
WHERE date >= '2024-01-01'

-- Strategy performance dashboard
SELECT * FROM trading_data.strategy_metrics_mv
WHERE strategy_id = 'momentum_v1'
```

## 🗄️ External Tables (Cost Optimization)

### Cold Storage Tables
- `historical_ohlcv_external` - Archive data (>2 years)
- `archived_trades_external` - Old trade logs
- `backup_data_external` - Disaster recovery

### Unified Views
- `ohlcv_unified_view` - Hot + Cold data seamlessly
- `trades_unified_view` - Current + Archived trades

## 🔗 Quick References

### Add New Table
1. Copy template from `table_template.tf`
2. Update category, name, and fields
3. Add to appropriate `tables_<category>.tf` file
4. Update `outputs.tf` with new table reference
5. Deploy: `terraform apply -target=module.bigquery.google_bigquery_table.<new_table>`

### Common Queries
```sql
-- List all tables
SELECT table_name, table_type, creation_time
FROM trading_data.INFORMATION_SCHEMA.TABLES

-- Check table size and costs
SELECT table_name, size_bytes, num_rows
FROM trading_data.__TABLES__

-- View partition info
SELECT partition_id, creation_time, last_modified_time
FROM trading_data.INFORMATION_SCHEMA.PARTITIONS
WHERE table_name = 'ohlcv_data'
```

## 🚨 Best Practices

### DO ✅
- Always use partition filters in queries
- Follow naming conventions consistently
- Include audit fields in all tables
- Use appropriate data types
- Test with small data first

### DON'T ❌
- Query without partition filters
- Use overly generic field names
- Skip clustering on large tables
- Forget SEBI compliance requirements
- Deploy without testing

---

## 📞 Support

For questions about table design or BigQuery optimization:
- Review `table_template.tf` for standards
- Check existing tables in `tables_*.tf` files
- Follow partitioning and clustering best practices
