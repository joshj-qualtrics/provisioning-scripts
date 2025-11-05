import os
import pandas as pd
from codaio import Coda, Document
from dotenv import load_dotenv


# script to export tables to CSV
# load with Doc and Table Id
doc_id = "m6G_7OVfdq"
table_id = "table-RC54btGLAp"

load_dotenv()
coda_api_token = os.getenv("CODA_TOKEN")
coda_client = Coda(coda_api_token)

print(f"Token Loaded: {bool(coda_api_token)}")
if not coda_api_token:
    print("Error: CODA_TOKEN is not loaded!")
    
#select the relevent document using ID
doc = Document(doc_id, coda=coda_client)

doc.list_tables()
# print(doc.list_tables())
table = doc.get_table(table_id)
# print(table.to_dict())
df = pd.DataFrame(table.to_dict())
df.to_csv('sku-privilege-table.csv', index = False, header=True)
