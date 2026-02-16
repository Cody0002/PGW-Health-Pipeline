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

    client = bigquery.Client(project="kz-dp-prod")

    # 2. Configure Query Parameters (Security Best Practice)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("watermark", "TIMESTAMP", watermark)
        ]
    )

    sql_query = """
     -- BigQuery SQL (Fixed Logic)
WITH 
    -- 1. Unified Brand Mapping
    brand_mapping AS (
        SELECT
            brand,
            ANY_VALUE(sub_group) AS account_group,
            CASE 
                WHEN LOWER(ANY_VALUE(sub_group)) LIKE 'kzg%' THEN 'KZG' 
                ELSE 'KZP' 
            END AS group_re
        FROM `kz-dp-prod.MAPPING.brand_whitelabel_country_folderid_mapping_tbl`
        GROUP BY brand
    ),

    -- 2. Realtime Data (Main Source)
    raw_realtime AS (
        SELECT
            f.id,
            f.type, 
            f.createdAt, 
            f.completedAt, 
            f.providerKey, 
            f.method, 
            f.status, 
            f.reqCurrency, 
            f.accountId, 
            f.netAmount, 
            f.insertedAt, 
            SPLIT(f.method, '/')[SAFE_OFFSET(1)] AS channel_type,
            a.name AS brand,
            
            -- Mapped Fields
            m.account_group,
            m.group_re,

            DATETIME(f.createdAt, 
                CASE LEFT(f.reqCurrency,2)
                    WHEN 'BD' THEN '+06:00'
                    WHEN 'TH' THEN 'Asia/Bangkok'
                    WHEN 'MX' THEN 'America/Mexico_City'
                    WHEN 'ID' THEN 'Asia/Jakarta'
                    WHEN 'BR' THEN 'America/Sao_Paulo'
                    WHEN 'PK' THEN 'Asia/Karachi'
                    WHEN 'IN' THEN '+05:30'
                    WHEN 'PH' THEN 'Asia/Manila'
                    WHEN 'CO' THEN 'America/Bogota'
                    WHEN 'EG' THEN 'Africa/Cairo'
                    ELSE 'UTC'
                END
            ) AS local_ts,
            
            CASE 
                WHEN f.status = 'completed' AND f.type = 'deposit' THEN
                    LEAST(TIMESTAMP_DIFF(f.completedAt, f.createdAt, SECOND), 900)
                WHEN f.status = 'completed' AND f.type = 'withdraw' THEN
                    TIMESTAMP_DIFF(f.completedAt, f.createdAt, SECOND)
                ELSE NULL
            END AS transaction_time
        FROM `kz-dp-prod.kz_pg_to_bq_realtime.ext_funding_tx` AS f
        LEFT JOIN `kz-dp-prod.kz_pg_to_bq_realtime.account` AS a ON f.accountId = a.id
        LEFT JOIN brand_mapping m ON UPPER(a.name) = UPPER(m.brand)
        WHERE 
            -- Note: Ensure this date matches your intended full reporting window
            f.insertedAt > @watermark 
            AND f.type IN ('deposit', 'withdraw')
            AND f.status IN ('completed', 'errors', 'timeout', 'error')
        QUALIFY ROW_NUMBER() OVER (PARTITION BY f.id ORDER BY f.insertedAt DESC) = 1
    ),

    -- 3. Silver Data (Gap Fill ONLY)
    raw_silver AS (
        SELECT
            REGEXP_REPLACE(d.order_id, r'[="]', '') AS id,
            'deposit' AS type,
            SAFE_CAST(d.created_time AS TIMESTAMP) AS createdAt,
            SAFE_CAST(d.completed_time AS TIMESTAMP) AS completedAt,

            -- Custom Provider Logic
            CASE
                WHEN LOWER(d.deposit_method) LIKE '%xpay%'      THEN 'xpay-bd'
                WHEN LOWER(d.deposit_method) LIKE '%worldpay%' THEN 'worldpay'
                WHEN LOWER(d.deposit_method) LIKE '%tiger%'      THEN 'tgpay-bd'
                WHEN LOWER(d.deposit_method) LIKE '%zenith%'     THEN 'xqpay-bd'
                WHEN LOWER(d.deposit_method) LIKE '%dapay%'      THEN 'dapay-bd'
                WHEN LOWER(d.deposit_method) LIKE '%gopay%'      THEN 'gopay'
                WHEN LOWER(d.deposit_method) LIKE '%bcat%'       THEN 'bcatpay'
                WHEN LOWER(d.deposit_method) LIKE '%leli%'       THEN 'lelipay'
                WHEN LOWER(d.deposit_method) LIKE '%dumpling%'  THEN 'dpp-bd'
                WHEN LOWER(d.deposit_method) LIKE '%swift%'      THEN 'wingpay-bd'
                ELSE NULL
            END AS providerKey,

            -- Custom Method Logic
            CASE
                WHEN LOWER(d.deposit_method) LIKE '%xpay%'      THEN 'xpay-bd'
                WHEN LOWER(d.deposit_method) LIKE '%worldpay%' THEN 'worldpay'
                WHEN LOWER(d.deposit_method) LIKE '%tiger%'      THEN 'tgpay-bd'
                WHEN LOWER(d.deposit_method) LIKE '%zenith%'     THEN 'xqpay-bd'
                WHEN LOWER(d.deposit_method) LIKE '%dapay%'      THEN 'dapay-bd'
                WHEN LOWER(d.deposit_method) LIKE '%gopay%'      THEN 'gopay'
                WHEN LOWER(d.deposit_method) LIKE '%bcat%'       THEN 'bcatpay'
                WHEN LOWER(d.deposit_method) LIKE '%leli%'       THEN 'lelipay'
                WHEN LOWER(d.deposit_method) LIKE '%dumpling%'  THEN 'dpp-bd'
                WHEN LOWER(d.deposit_method) LIKE '%swift%'      THEN 'wingpay-bd'
                ELSE d.deposit_method 
            END AS method,

            d.status,
            d.currency AS reqCurrency,
            d.username AS accountId,
            SAFE_CAST(d.amount AS FLOAT64) AS netAmount,
            d.pulled_at AS insertedAt,

            -- Custom Channel Logic
            CASE
                WHEN LOWER(d.deposit_method) LIKE '%bkash%'  THEN 'bkash'
                WHEN LOWER(d.deposit_method) LIKE '%nagad%'  THEN 'nagad'
                WHEN LOWER(d.deposit_method) LIKE '%rocket%' THEN 'rocket'
                WHEN LOWER(d.deposit_method) LIKE '%upay%'   THEN 'upay'
                ELSE d.deposit_method
            END AS channel_type,

            d.brand,
            
            -- Mapped Fields
            m.account_group,
            m.group_re,

            DATETIME(SAFE_CAST(d.created_time AS TIMESTAMP), '+06:00') AS local_ts,

            LEAST(TIMESTAMP_DIFF(SAFE_CAST(d.completed_time AS TIMESTAMP), SAFE_CAST(d.created_time AS TIMESTAMP), SECOND), 900) AS transaction_time

        FROM `kz-dp-prod.crm_silver_prod.bd_kzg_kz_deposit_transaction` d
        LEFT JOIN brand_mapping m ON UPPER(d.brand) = UPPER(m.brand)
        
        WHERE 
            -- === CRITICAL FIXES HERE ===
            -- 1. Restrict to ONLY the gap period (Prevents duplicate history)
            DATE(SAFE_CAST(d.created_time AS TIMESTAMP)) BETWEEN '2025-12-28' AND '2026-01-05'
            
            -- 2. Restrict to ONLY BDT
            AND d.currency = 'BDT'

            -- 3. Exclude IDs that were successfully captured by Realtime
            AND REGEXP_REPLACE(d.order_id, r'[="]', '') NOT IN (
                SELECT DISTINCT orderRef
                FROM `kz-dp-prod.kz_pg_to_bq_realtime.ext_funding_tx`
                WHERE reqCurrency = 'BDT'
                AND DATE(insertedAt) BETWEEN '2025-12-28' AND '2026-01-05'
            )
    ),

    -- 4. Combine Datasets
    combined_data AS (
        SELECT * EXCEPT(id) FROM raw_realtime
        UNION ALL
        SELECT * EXCEPT(id) FROM raw_silver
    ),

    -- 5. Formatting
    all_transactions AS (
        SELECT
            r.*,
            DATE(r.local_ts) AS transaction_date,
            FORMAT_DATETIME('%H:00 - %H:59', r.local_ts) AS Hour,
            UPPER(r.type) AS type_formatted, 
            CASE WHEN r.status = 'errors' THEN 'error' ELSE r.status END AS status_formatted
        FROM combined_data r
    ),

    -- 6. Quantile Calculations
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

    -- 7. Final Output
    SELECT
        t.transaction_date AS Date,
        t.providerKey,
        t.method,
        t.channel_type,
        t.type_formatted AS type,
        t.reqCurrency,
        t.account_group,
        t.group_re,
        CASE LEFT(t.reqCurrency,2)
            WHEN 'BD' THEN 'Bangladesh'
            WHEN 'TH' THEN 'Thailand'
            WHEN 'MX' THEN 'Mexico'
            WHEN 'ID' THEN 'Indonesia'
            WHEN 'BR' THEN 'Brazil'
            WHEN 'PK' THEN 'Pakistan'
            WHEN 'IN' THEN 'India'
            WHEN 'PH' THEN 'Philippines'
            WHEN 'CO' THEN 'Colombia'
            WHEN 'EG' THEN 'Egypt'
            ELSE 'Other'
        END AS Country,
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
        AND IFNULL(t.providerKey, '') = IFNULL(s.providerKey, '')
        AND IFNULL(t.method, '') = IFNULL(s.method, '')
        AND IFNULL(t.channel_type, '') = IFNULL(s.channel_type, '')
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