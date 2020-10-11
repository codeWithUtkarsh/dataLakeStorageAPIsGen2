from azure.cosmosdb.table.tableservice import TableService
from azure.cosmosdb.table.models import Entity
import configs

class MetaStorageClient:
    """ Client to store metadata to table storage """

    def __init__(self, table_name, account_name, account_key):
        self.table = table_name
        self.client = TableService(account_name=account_name, account_key=account_key)

    def upload(self, path: str, partition_key: str, row_key: str, meta: dict):
        """ Upload metadata for image """
        meta['PartitionKey'] = partition_key
        meta['RowKey'] = row_key
        meta['path'] = path
        self.client.insert_or_replace_entity(self.table, meta)

    def search(self, row_key):
        """ Search TableStorage for metadata """

        query = f"RowKey eq '{row_key}'"
        items = self.client.query_entities(
            self.table,
            filter=query
        )
        result = [item for item in items]
        return result