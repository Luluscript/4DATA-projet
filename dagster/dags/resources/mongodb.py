from dagster import resource, Field, StringSource
from pymongo import MongoClient

class MongoDBResource:
    def __init__(self, connection_string, database_name):
        self.connection_string = connection_string
        self.database_name = database_name
        self._client = None
    
    @property
    def client(self):
        if self._client is None:
            self._client = MongoClient(self.connection_string)
        return self._client
    
    def get_database(self):
        if self._client is None:
            self._client = MongoClient(self.connection_string)
        return self._client[self.database_name]
    
    def close(self):
        if self._client is not None:
            self._client.close()
            self._client = None

@resource(
    config_schema={
        "connection_string": Field(StringSource, 
            description="MongoDB connection string",
            default_value="mongodb://supmap:supmap@mongodb:27017/supmap?authSource=admin"),  # Mise à jour avec authentification
        "database_name": Field(StringSource, 
            description="MongoDB database name",
            default_value="supmap"),
    }
)
def mongodb_resource(context):
    return MongoDBResource(
        connection_string=context.resource_config["connection_string"],
        database_name=context.resource_config["database_name"],
    )
