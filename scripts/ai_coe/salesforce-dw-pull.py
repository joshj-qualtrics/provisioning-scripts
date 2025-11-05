from redshift import DataWarehouse # type: ignore
import os
import json
import sys
# --- FIX: Import load_dotenv to read .env file ---
from dotenv import load_dotenv # type: ignore
from pathlib import Path

# --- FIX: Explicitly specify the path to the .env file ---
# Since the script is run from 'scripts/ai_coe/', the .env file is likely one level up (at the project root).
env_path = Path(__file__).parent.parent.parent / '.env'
load_dotenv(dotenv_path=env_path) 

# --- DIAGNOSTIC CHECK ---
if os.getenv('DB_USER') is None:
    print(f"WARNING: DB_USER is still None after load_dotenv().")
    print(f"Check 1: Verify 'python-dotenv' is installed (pip install python-dotenv).")
    print(f"Check 2: Verify a file named .env exists at the determined path: {env_path.resolve()}")
# ------------------------

def fetch_and_process_data(brand_id):
    """
    Connects to the Data Warehouse, fetches currently active Salesforce provisioning lines 
    for the specified brand ID, and prints a summary of the fetched data.
    
    Reads configuration from environment variables.
    """
    
    # --- 1. Load Configuration from Environment Variables ---
    
    # These variables are now populated by load_dotenv() if they exist in the .env file.
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD') # Should be kept secret
    DB_CONNECTION = os.getenv('DB_CONNECTION')
    DB_HOST = os.getenv('DB_HOST')

    if not brand_id:
        print("CRITICAL ERROR: brand_id param not passed. Exiting.")
        sys.exit(1)

    print(f"Attempting to fetch data for Brand ID: {brand_id}")

    try:
        # --- 2. Establish Connection ---
        # The DataWarehouse class handles validation and defaults the connection to 'psycopg2'
        dw = DataWarehouse(
            user=DB_USER,
            password=DB_PASSWORD,
            connection=DB_CONNECTION,
            host=DB_HOST
        )

        # --- 3. SQL Query ---
        # Using an f-string to inject the brand_id into the WHERE clause.
        query = f"""
        SELECT id, license_name__c, product_name__c, quantity_allocated__c, unlimited_quantity__c,
            brand_account__c, active__c, lms_configuration__c, start_date__c,  expiration_date__c,
            istrial__c, migrated_active__c, migrated__c, lastmodifieddate, contract__c, subscription__c,
            quote_line__c
        FROM source_ops_sfdc_production_current.provisioning_line__c pl

        WHERE brand_id__c = '{brand_id}'
        AND start_date__c <= CURRENT_DATE
        AND expiration_date__c >= CURRENT_DATE
        AND active__c = true
        AND lms_config_status__c = 'Activated'
        """

        # Execute query
        results = dw.get_pandas_df(query)
        
        # --- 4. Processing Logic ---
        
        record_count = len(results)
        
        print(f"\n--- Salesforce Provisioning Line Data Processing ---")
        
        if record_count > 0:
            print(f"Successfully fetched {record_count} active Salesforce Provisioning Line records for Brand ID: {brand_id}.")
            print("First 5 records (head):")
            # Display a subset of the new columns to confirm data retrieval
            print(results[['id', 'license_name__c', 'product_name__c']].head().to_string(index=False))
        else:
            print(f"No active Salesforce Provisioning Line records were found for Brand ID: {brand_id}.")

    except Exception as e:
        print(f"\nCRITICAL ERROR in fetch_and_process_data: {str(e)}")
        # Re-raise the exception to stop execution on failure
        raise

if __name__ == "__main__":
    fetch_and_process_data('provisioningtest6')
