import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
from datetime import datetime
from logger_config import setup_logger
logger = setup_logger("increment_run")

# --- CONFIGURATION ---
PARQUET_FILENAME = 'daily_funding.parquet'

def clean_google_dtypes(df):
    """
    Converts Google BigQuery's custom 'dbdate'/'dbtime' types 
    to standard pandas datetime objects to prevent Parquet errors.
    """
    for col in df.columns:
        # Check if the column type is the specific google dbdate type
        if 'dbdate' in str(df[col].dtype):
            df[col] = pd.to_datetime(df[col])
    return df

def get_watermark(filename):
    """
    Reads only the 'Max_InsertedAt' column from the Parquet file
    to find the latest timestamp for incremental filtering.
    """
    if not os.path.exists(filename):
        print("Warning: Parquet file not found. Defaulting to 1970 (Full Load behavior).")
        return '1970-01-01 00:00:00+00:00'

    try:
        # Optimization: Read ONLY the metadata column needed
        df_meta = pd.read_parquet(filename, columns=['Max_InsertedAt'])
        
        # Ensure it is datetime capable
        if not pd.api.types.is_datetime64_any_dtype(df_meta['Max_InsertedAt']):
             df_meta['Max_InsertedAt'] = pd.to_datetime(df_meta['Max_InsertedAt'], utc=True)
             
        max_ts = df_meta['Max_InsertedAt'].max()
        
        # Format for BigQuery TIMESTAMP comparison
        return max_ts.strftime('%Y-%m-%d %H:%M:%S.%f+00:00')
        
    except Exception as e:
        print(f"Error reading watermark from Parquet: {e}")
        return '1970-01-01 00:00:00+00:00'

def run_incremental_update():
    # 1. Determine the Watermark
    watermark = get_watermark(PARQUET_FILENAME)
    print(f"Checking for data inserted after: {watermark}")

    client = bigquery.Client()

    # 2. Configure Query Parameters (Security Best Practice)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("watermark", "TIMESTAMP", watermark)
        ]
    )

    sql_query = """
    WITH raw_data AS (
        SELECT
            f.type, f.createdAt, f.completedAt, f.providerKey, f.method, f.status, f.reqCurrency, 
            f.accountId, f.netAmount, f.insertedAt, 
            SPLIT(f.method, '/')[SAFE_OFFSET(1)] AS channel_type,
            a.name AS brand,
            DATETIME(f.createdAt, CASE f.reqCurrency
                WHEN 'BDT' THEN '+06:00'
                WHEN 'THB' THEN 'Asia/Bangkok'
                WHEN 'MXN' THEN 'America/Mexico_City'
                WHEN 'IDR' THEN 'Asia/Jakarta'
                WHEN 'BRL' THEN 'America/Sao_Paulo'
                WHEN 'PKR' THEN 'Asia/Karachi'
                WHEN 'INR' THEN '+05:30'
                WHEN 'PHP' THEN 'Asia/Manila'
                ELSE 'UTC'
            END) AS local_ts,
            CASE 
                WHEN f.status = 'completed' AND f.type = 'deposit' THEN
                    LEAST(TIMESTAMP_DIFF(f.completedAt, f.createdAt, SECOND), 900)
                WHEN f.status = 'completed' AND f.type = 'withdraw' THEN
                    TIMESTAMP_DIFF(f.completedAt, f.createdAt, SECOND)
                ELSE NULL
            END AS transaction_time
        FROM `kz-dp-prod.kz_pg_to_bq_realtime.ext_funding_tx` AS f
        LEFT JOIN `kz-dp-prod.kz_pg_to_bq_realtime.account` AS a ON f.accountId = a.id
        WHERE 
            f.insertedAt > @watermark
            AND f.type IN ('deposit', 'withdraw')
            AND f.reqCurrency IN ('BDT', 'THB', 'MXN', 'IDR', 'BRL', 'PKR', 'INR', 'PHP')
            AND f.status IN ('completed', 'errors', 'timeout', 'error')
        QUALIFY ROW_NUMBER() OVER (PARTITION BY DATE(local_ts), f.id ORDER BY f.updatedAt DESC) = 1
    ),
    all_transactions AS (
        SELECT
            r.*,
            CASE WHEN LEFT(a.group,3) = 'kzg' THEN 'KZG' ELSE 'KZP' END AS group_re,
            UPPER(a.group) AS account_group,
            DATE(r.local_ts) AS transaction_date,
            FORMAT_DATETIME('%H:00 - %H:59', r.local_ts) AS Hour,
            UPPER(r.type) AS type_formatted, 
            CASE WHEN r.status = 'errors' THEN 'error' ELSE r.status END AS status_formatted
        FROM raw_data r
        LEFT JOIN `kz-dp-prod.kz_pg_to_bq_realtime.account` a ON r.accountId = a.Id
    ),
    quantile_stats AS (
        SELECT
            transaction_date, providerKey, method, channel_type, type_formatted, reqCurrency, Hour,
            APPROX_QUANTILES(transaction_time, 101)[OFFSET(5)] AS p05,
            APPROX_QUANTILES(transaction_time, 101)[OFFSET(50)] AS p50,
            APPROX_QUANTILES(transaction_time, 101)[OFFSET(95)] AS p95
        FROM all_transactions
        WHERE status_formatted = 'completed' AND transaction_time IS NOT NULL
        GROUP BY 1, 2, 3, 4, 5, 6, 7
    )
    SELECT
        t.transaction_date AS Date,
        t.providerKey, t.method, t.channel_type, t.type_formatted AS type, t.reqCurrency,
        account_group, group_re,
        LEFT(t.reqCurrency, 2) AS Country,
        t.status_formatted AS status,
        t.Hour,
        COUNT(*) AS Count,
        SUM(t.netAmount) AS Total_Net_Amount,
        MAX(t.insertedAt) AS Max_InsertedAt,
        ROUND(SUM(CASE WHEN t.status_formatted = 'completed' AND t.transaction_time IS NOT NULL THEN LEAST(GREATEST(t.transaction_time, s.p05), s.p95) ELSE NULL END), 2) AS winsorized_total_time_seconds,
        DATE_TRUNC(t.transaction_date, MONTH) AS DateMonth,
        COUNTIF(t.status_formatted = 'completed' AND t.transaction_time <= 90) AS Count_01m30s_Below,
        COUNTIF(t.status_formatted = 'completed' AND t.transaction_time > 90 AND t.transaction_time <= 120) AS Count_01m31s_to_02m00s,
        COUNTIF(t.status_formatted = 'completed' AND t.transaction_time > 120 AND t.transaction_time <= 180) AS Count_02m01s_to_03m00s,
        COUNTIF(t.status_formatted = 'completed' AND t.transaction_time > 180) AS Count_03m00s_Above,
        COUNTIF(t.status_formatted = 'completed' AND t.transaction_time <= 180) AS Count_03m00s_Below,
        COUNTIF(t.status_formatted = 'completed' AND t.transaction_time > 180 AND t.transaction_time <= 300) AS Count_03m31s_to_05m00s,
        COUNTIF(t.status_formatted = 'completed' AND t.transaction_time > 300 AND t.transaction_time <= 600) AS Count_05m00s_to_10m00s,
        COUNTIF(t.status_formatted = 'completed' AND t.transaction_time > 600) AS Count_10m00s_Above,
        t.providerKey AS providerName,
        SPLIT(t.channel_type, '-')[SAFE_OFFSET(0)] AS channel_main,
        t.brand
    FROM all_transactions t
    LEFT JOIN quantile_stats s
        ON t.transaction_date = s.transaction_date
        AND t.providerKey = s.providerKey
        AND t.method = s.method
        AND t.channel_type = s.channel_type
        AND t.type_formatted = s.type_formatted
        AND t.reqCurrency = s.reqCurrency
        AND t.Hour = s.Hour
    GROUP BY 
        1,2,3,4,5,6,7,8,9,10,11,16,25,26,27
    """

    df_new = client.query(sql_query, job_config=job_config).to_dataframe()

    if df_new.empty:
        print("No new data found. Parquet file remains unchanged.")
        return

    print(f"Retrieved {len(df_new)} new rows.")
    
    # --- CRITICAL FIX: Clean Google specific dtypes ---
    df_new = clean_google_dtypes(df_new)

    # 3. Append to Parquet
    # Since Parquet is immutable, we read the old file, concat, and rewrite.
    if os.path.exists(PARQUET_FILENAME):
        try:
            df_old = pd.read_parquet(PARQUET_FILENAME)
            
            # Align data types (prevents errors if schema slightly drifts)
            df_new = df_new.astype(df_old.dtypes.to_dict(), errors='ignore')
            
            df_combined = pd.concat([df_old, df_new], ignore_index=True)
            
            # Save combined file
            df_combined.to_parquet(PARQUET_FILENAME, index=False, compression='snappy')
            print(f"Successfully appended {len(df_new)} rows. Total rows: {len(df_combined)}.")
            
        except Exception as e:
            print(f"Error updating Parquet file: {e}")
    else:
        # Fallback: Create new file if it was missing
        df_new.to_parquet(PARQUET_FILENAME, index=False, compression='snappy')
        print(f"Parquet file was missing. Created new file with {len(df_new)} rows.")

if __name__ == "__main__":
    run_incremental_update()