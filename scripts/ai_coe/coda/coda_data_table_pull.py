import os
import pandas as pd
from codaio import Coda, Document
from dotenv import load_dotenv

load_dotenv()
coda_api_token = os.getenv("CODA_TOKEN")

if not coda_api_token:
    print("Error: CODA_TOKEN is not loaded!")

def export_coda_table_to_csv(document_id: str, table_id: str, output_filename: str):
    """
    Exports a specified Coda table to a local CSV file.

    Args:
        document_id: The ID of the Coda Document (e.g., 'm6G_7OVfdq').
        table_id: The ID of the table within the document (e.g., 'table-RC54btGLAp').
        output_filename: The name for the output CSV file (e.g., 'data.csv'). Will be output in directory where the script is initiated
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
        print(df)
        df.to_csv(output_filename, index=False, header=True)
        print(f"\n✅ Successfully exported {len(df)} rows to {output_filename}")

    except Exception as e:
        print(f"An error occurred during the export process: {e}")



# run function to get privilege table and then extension table from coda
# privilege table
DOC_ID = "m6G_7OVfdq"
TABLE_ID = "table-RC54btGLAp"
OUTPUT_FILE = 'sku-privilege-table-export.csv'

export_coda_table_to_csv(DOC_ID, TABLE_ID, OUTPUT_FILE)


# extension table
DOC_ID = "m6G_7OVfdq"
TABLE_ID = "table-c4x55SFhUm"
OUTPUT_FILE = 'sku-extension-table-export.csv'

export_coda_table_to_csv(DOC_ID, TABLE_ID, OUTPUT_FILE)
