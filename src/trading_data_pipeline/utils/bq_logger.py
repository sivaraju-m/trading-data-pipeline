from google.cloud import bigquery


def ensure_bq_dataset_and_table():
    client = bigquery.Client()
    dataset_id = "{client.project}.trading_data"
    table_id = "{dataset_id}.backtest_results"

    try:
        client.get_dataset(dataset_id)
    except Exception:
        client.create_dataset(bigquery.Dataset(dataset_id), timeout=30)

    try:
        client.get_table(table_id)
    except Exception:
        schema = [
            bigquery.SchemaField("ticker", "STRING"),
            bigquery.SchemaField("strategy", "STRING"),
            bigquery.SchemaField("start_date", "DATE"),
            bigquery.SchemaField("end_date", "DATE"),
            bigquery.SchemaField("cagr", "FLOAT"),
            bigquery.SchemaField("sharpe_ratio", "FLOAT"),
            bigquery.SchemaField("drawdown", "FLOAT"),
            bigquery.SchemaField("win_rate", "FLOAT"),
            bigquery.SchemaField("trades", "INTEGER"),
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
        ]
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table, timeout=30)


def log_backtest_result(**kwargs):
    ensure_bq_dataset_and_table()
    client = bigquery.Client()
    table_id = "{client.project}.trading_data.backtest_results"
    errors = client.insert_rows_json(table_id, [kwargs])
