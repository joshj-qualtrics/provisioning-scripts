import os
import google.auth
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import datetime
from datatools.data_warehouse import DataWarehouse
import json

import prefect
from datatools.prefect import Deployment
from prefect import flow, task
from prefect.blocks.system import Secret
from datatools.prefect.blocks.custom.secret_json import SecretJSON #had to install this separately. ran `prefect block register --module datatools.prefect.blocks.custom.secret_json` in terminal and then run `prefect server start` prefect and then went to `Check out the dashboard at http://127.0.0.1:4200` in my browser to set up the secrects for the local version
from prefect.logging import get_run_logger
import os
import pandas as pd
from codaio import Coda, Document
from dotenv import load_dotenv

# Load .env vars
load_dotenv()
coda_api_token = os.getenv("CODA_TOKEN")

if not coda_api_token:
    print("Error: CODA_TOKEN is not loaded!")


# Need to test this will format the df returned from the export_coda_table_to_df function
@task
def format_data_for_upload(df: pd.DataFrame):
    
    df.replace('null', None, inplace=True)
    df.replace('', None, inplace=True)

    # df = df.drop(columns=['index'])
    return df

def export_coda_table_to_df(document_id: str, table_id: str):
    """
    Returns a DataFrame for coda table

    Args:
        document_id: The ID of the Coda Document (e.g., 'm6G_7OVfdq').
        table_id: The ID of the table within the document (e.g., 'table-RC54btGLAp').
    """
    if not coda_api_token:
        print("Cannot run export: CODA_TOKEN is missing.")
        return

    try:
        coda_client = Coda(coda_api_token)

        print(f"Loading document ID: {document_id}")
        doc = Document(document_id, coda=coda_client)

        print(f"Fetching table ID: {table_id}")
        table = doc.get_table(table_id)

        df = pd.DataFrame(table.to_dict())
        print(f"\n✅ Successfully returning df of row length: {len(df)} ")
        return df

    except Exception as e:
        print(f"An error occurred during the export process: {e}")


def rs_connection(credentials: dict):
    dw = DataWarehouse(
        user = credentials['username'],
        password = credentials['password'],
        connection='psycopg2')
    return dw



# NEED TO REVIEW
@task
def create_staging_table(warehouse_credentials, staging_schema, prod_schema, prod_table):
    logger=get_run_logger()
    query1 = f"""create table if not exists {staging_schema}.{prod_table}_temp (like {prod_schema}.{prod_table})"""
    query2 = f"""delete from {staging_schema}.{prod_table}_temp where 1=1;""" 
    print()
    logger.info('CREATE/CLEAR STAGING DATA')
    logger.info(query1)
    logger.info(query2)
    warehouse_credentials.run(query1)
    warehouse_credentials.run(query2)

    return 1

# NEED TO REVIEW
@task
def write_data_to_staging_table(warehouse_credentials, staging_schema: str, prod_table: str, df:pd.DataFrame, done):
    warehouse_credentials.dataframe_to_sql(dataframe=df, schema=staging_schema, table=(prod_table + '_temp'), replace=False, batch_size=1000, column_names=list(df.columns), index=False)

    return 1

#NEED TO REVIEW
@task
def promote_to_prod(warehouse_credentials, prod_schema, staging_schema, prod_table, done):
    logger=get_run_logger()
    logger.info(' ')
    logger.info('MERGE STAGING DATA WITH PROD DATA')

    # This query uses a composite key: (SKU, Privilege)
    #
    queries = f"""
        merge into {prod_schema}.{prod_table}
        using {staging_schema}.{prod_table}_temp temp 
           on ({prod_table}.SKU = temp.SKU and {prod_table}.Privilege = temp.Privilege) 
        
        when matched then
            update set
                Tab = temp.Tab,
                Category = temp.Category,
                Privilege = temp.Privilege,
                Notes = temp.Notes,
                SKU = temp.SKU
                Enabled = temp.Enabled

        
        when not matched then
            insert (Tab, Category, Privilege, Notes, SKU, Enabled) 
            VALUES (temp.Tab, temp.Category, temp.Privilege, temp.Notes, temp.SKU, temp.Enabled);
    """
    logger.info(queries)
    warehouse_credentials.run(queries)

    logger.info('STAGING DATA MERGED WITH PROD')
    return 1

@flow(name='redshift-coda-sku-update-run')
def main_flow():
    redshift_creds = SecretJSON.load('resolution-redshift-service-account').get()

    # PRIVILEGE TABLE
    # dw set-up
    schema = 'metrics_ops_resolution'
    table = 'sku_privileges'
    staging_schema = 'sandbox_ops_resolution'

    # coda set-up
    DOC_ID = "m6G_7OVfdq"
    TABLE_ID = "table-RC54btGLAp"

    priv_df = export_coda_table_to_df(DOC_ID, TABLE_ID)
    priv_formatted_df = format_data_for_upload(priv_df)

    warehouse_conn = rs_connection(redshift_creds)
    done1 = create_staging_table(warehouse_conn, staging_schema=staging_schema, prod_schema=schema, prod_table=table)
    done2 = write_data_to_staging_table(warehouse_conn, staging_schema=staging_schema, prod_table=table, df=priv_formatted_df, done=done1)
    done3 = promote_to_prod(warehouse_conn, prod_schema=schema, staging_schema=staging_schema, prod_table=table, done=done2)



    # EXTENSION TABLE FLOW
    # dw set-up
    schema = 'metrics_ops_resolution'
    table = 'sku_extensions'
    staging_schema = 'sandbox_ops_resolution'

    # coda set-up
    DOC_ID = "m6G_7OVfdq"
    TABLE_ID = "table-c4x55SFhUm"

    priv_df = export_coda_table_to_df(DOC_ID, TABLE_ID)
    priv_formatted_df = format_data_for_upload(priv_df)

    warehouse_conn = rs_connection(redshift_creds)
    done1 = create_staging_table(warehouse_conn, staging_schema=staging_schema, prod_schema=schema, prod_table=table)
    done2 = write_data_to_staging_table(warehouse_conn, staging_schema=staging_schema, prod_table=table, df=priv_formatted_df, done=done1)
    done3 = promote_to_prod(warehouse_conn, prod_schema=schema, staging_schema=staging_schema, prod_table=table, done=done2)


deployment = Deployment(prefect_version="2.0", entrypoint="scripts/ai_coe/coda/redshift-pipeline.py:main_flow", work_queue="b1-prv", tags=["provisioning"], cron="0 0 * * 0") # run update at midnight on Sundays? Maybe everyday instead would be better?

if __name__ == "__main__":
    main_flow()

