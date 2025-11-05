import os
import pandas as pd
from codaio import Coda, Document
from dotenv import load_dotenv
from typing import Optional
from sqlalchemy import create_engine, text

# --- Configuration for Redshift ---
# Base name for the temporary staging table
STAGING_TABLE_BASE_NAME = "coda_staging"
DOC_ID = "m6G_7OVfdq"

# Table configurations: one entry for each Coda table you want to sync
TABLE_CONFIGS = [
    {
        "table_id": "table-RC54btGLAp",
        "target_table": "sku_privileges",
        "primary_key": "privilege_id" # CRITICAL: Update this to match your actual unique column name
    },
    {
        "table_id": "table-c4x55SFhUm",
        "target_table": "sku_extensions",
        "primary_key": "extension_id" # CRITICAL: Update this to match your actual unique column name
    }
]
# --- End Configuration ---

# Load environment variables
load_dotenv()
coda_api_token = os.getenv("CODA_TOKEN")

# --- Connection & Extraction Functions ---

def get_db_engine():
    """Creates and returns a SQLAlchemy Engine for Redshift."""
    user = os.getenv("REDSHIFT_USER")
    password = os.getenv("REDSHIFT_PASSWORD")
    host = os.getenv("REDSHIFT_HOST")
    port = os.getenv("REDSHIFT_PORT")
    db = os.getenv("REDSHIFT_DB")

    if not all([user, password, host, db]):
        print("ERROR: Missing required Redshift environment variables. Check .env file.")
        return None

    rs_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    print(f"INFO: Attempting to connect to Redshift at {host}...")
    return create_engine(rs_url, connect_args={'options': '-c search_path=public'})

def extract_coda_table(table_id: str) -> Optional[pd.DataFrame]:
    """Extracts data from a Coda table."""
    if not coda_api_token:
        print("Cannot run extract: CODA_TOKEN is missing.")
        return None

    try:
        coda_client = Coda(coda_api_token)
        doc = Document(DOC_ID, coda=coda_client)
        table = doc.get_table(table_id)
        df = pd.DataFrame(table.to_dict())
        
        # Clean up column names (lowercase and snake_case for Redshift)
        df.columns = [col.lower().replace(' ', '_').strip() for col in df.columns]
        
        print(f"SUCCESS: Extracted {len(df)} rows from Coda Table ID: {table_id}")
        return df

    except Exception as e:
        print(f"ERROR during Coda extraction for table {table_id}: {e}")
        return None

# --- UPSERT Function for Redshift ---

def upsert_dataframe_to_redshift(df: pd.DataFrame, target_table: str, primary_key: str):
    """
    Loads DataFrame into a staging table, executes the Redshift DELETE then INSERT
    UPSERT strategy, and cleans up the staging table.
    """
    engine = get_db_engine()
    if engine is None:
        return

    if primary_key not in df.columns:
        print(f"FATAL ERROR: Primary key '{primary_key}' not found in DataFrame for target '{target_table}'. Aborting.")
        return

    # Create a unique staging table name using the target table and PID
    temp_staging_table = f"{STAGING_TABLE_BASE_NAME}_{target_table}_{os.getpid()}" 
    
    try:
        # engine.begin() initiates a transaction, ensuring atomicity (all or nothing)
        with engine.begin() as conn: 
            
            # 1. Stage the data (Load DF into a temporary table)
            df.to_sql(temp_staging_table, conn, if_exists='replace', index=False, chunksize=5000)
            print(f"INFO: Data staged into table: '{temp_staging_table}'")

            # 2. DELETE existing records in the target table that match the primary key in the staging table
            print(f"INFO: Deleting matched rows from target table '{target_table}'...")
            delete_sql = f"""
            DELETE FROM {target_table}
            USING {temp_staging_table}
            WHERE {target_table}.{primary_key} = {temp_staging_table}.{primary_key};
            """
            conn.execute(text(delete_sql))
            
            # 3. INSERT all records from the staging table into the target table
            print(f"INFO: Inserting all rows from staging table...")
            insert_sql = f"""
            INSERT INTO {target_table}
            SELECT * FROM {temp_staging_table};
            """
            conn.execute(text(insert_sql))
            
            print(f"SUCCESS: UPSERT complete for '{target_table}'")

            # 4. Cleanup: Drop the temporary staging table
            conn.execute(text(f"DROP TABLE {temp_staging_table}"))
            print(f"INFO: Staging table '{temp_staging_table}' dropped.")
            # Transaction is committed here
            
    except Exception as e:
        print(f"FATAL ERROR during UPSERT for table {target_table}: {e}")
        # Transaction is rolled back automatically
    finally:
        if engine:
            engine.dispose()
            print("INFO: Database connection closed.")


def main():
    """Main execution function to run the pipeline for all configured tables."""
    print("--- Starting Coda to Redshift Pipeline ---")
    
    if not coda_api_token:
        print("Pipeline aborted: Missing CODA_TOKEN.")
        return

    if not get_db_engine():
        print("Pipeline aborted: Failed to configure Redshift connection.")
        return

    for config in TABLE_CONFIGS:
        print(f"\n--- Processing Table: Target '{config['target_table']}' (PK: {config['primary_key']}) ---")
        
        # 1. Extract
        df_coda = extract_coda_table(config['table_id'])
        
        if df_coda is None or df_coda.empty:
            print("WARNING: Extraction failed or data is empty. Skipping UPSERT.")
            continue
        print(df_coda)
        
        # # 2. UPSERT
        # upsert_dataframe_to_redshift(
        #     df_coda,
        #     target_table=config['target_table'],
        #     primary_key=config['primary_key']
        # )
    
    print("\n--- Pipeline Complete ---")

if __name__ == "__main__":
    main()
