from azure.cosmos import CosmosClient
import pandas as pd

# url = ""
# key = ""

url = ""
key = ""

db_name = "ryker"
container_name = "items"

query = "SELECT c.CF3461Block4, c.EntryDate FROM c  order by c.CF3461Block4 desc"

client = CosmosClient(url, credential=key)
db = client.get_database_client(db_name)
container = db.get_container_client(container_name)

items = list(container.query_items(query=query, enable_cross_partition_query=True))

df = pd.DataFrame(items)
df.to_excel("cosmos_export.xlsx", index=False)

print("Export complete → cosmos_export.xlsx")
