import yaml
from pydantic import BaseSettings
from pymongo import MongoClient


def get_db_connection():
    with open("config.yaml", "r") as f:
        db_config = yaml.safe_load(f)
    return db_config


class Client(BaseSettings):
    uri: str
    client: MongoClient = None

    def connect(self):
        self.client = MongoClient(self.uri)


DB_CONFIG = get_db_connection()
assert (
    DB_CONFIG["mongodb"]["uri"] is not None
), "MongoDB mongodb.uri is not set, check config.yaml"
BUSSE_PRICING_DATA = DB_CONFIG["mongodb"]["databases"]["busse_pricing"]["key"]
assert (
    BUSSE_PRICING_DATA is not None
), "MongoDB databases.busse_pricing.key is not set, check config.yaml"
CONTRACTS = DB_CONFIG["mongodb"]["databases"]["busse_pricing"]["contracts"]
assert (
    CONTRACTS is not None
), "MongoDB databases.busse_pricing.contracts is not set, check config.yaml"
COSTS = DB_CONFIG["mongodb"]["databases"]["busse_pricing"]["costs"]
assert (
    COSTS is not None
), "MongoDB databases.busse_pricing.costs is not set, check config.yaml"
BUSSEREBATETRACES = DB_CONFIG["mongodb"]["databases"]["busserebatetraces"]["key"]
assert (
    BUSSEREBATETRACES is not None
), "MongoDB databases.busserebatetraces.key is not set, check config.yaml"
SCHED_DATA = DB_CONFIG["mongodb"]["databases"]["busserebatetraces"]["sched_data"]
assert (
    SCHED_DATA is not None
), "MongoDB databases.busserebatetraces.sched_data is not set, check config.yaml"

client = Client(uri=DB_CONFIG["mongodb"]["uri"])
client.connect()

contracts = client.client[BUSSE_PRICING_DATA][CONTRACTS]
costs = client.client[BUSSE_PRICING_DATA][COSTS]
sched_data = client.client[BUSSE_PRICING_DATA][SCHED_DATA]

if __name__ == "__main__":
    print(contracts)
