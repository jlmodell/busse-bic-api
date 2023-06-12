import yaml
from pydantic import BaseSettings
from pymongo import MongoClient
import os

def get_db_connection():
    config_path = "config.yaml"
    if not os.path.exists(config_path):
        config_path = "c:\\temp\\global\\config.yaml"

    with open(config_path, "r") as f:
        db_config = yaml.safe_load(f)
    return db_config


class Client(BaseSettings):
    uri: str
    client: MongoClient = None

    def connect(self):
        self.client = MongoClient(self.uri)


DB_CONFIG = get_db_connection()
assert (
    DB_CONFIG["mongodb"]["atlas"]["uri"] is not None
), "MongoDB mongodb.uri is not set, check config.yaml"
BUSSE_PRICING_DATA = "bussepricing"
assert (
    BUSSE_PRICING_DATA is not None
), "MongoDB databases.busse_pricing.key is not set, check config.yaml"
CONTRACTS = "contract_prices"
assert (
    CONTRACTS is not None
), "MongoDB databases.busse_pricing.contracts is not set, check config.yaml"
COSTS = "costs"
assert (
    COSTS is not None
), "MongoDB databases.busse_pricing.costs is not set, check config.yaml"
BUSSEREBATETRACES = "busserebatetraces"
assert (
    BUSSEREBATETRACES is not None
), "MongoDB databases.busserebatetraces.key is not set, check config.yaml"
SCHED_DATA = "sched_data"
assert (
    SCHED_DATA is not None
), "MongoDB databases.busserebatetraces.sched_data is not set, check config.yaml"

client = Client(uri=DB_CONFIG["mongodb"]["atlas"]["uri"])
client.connect()

contracts = client.client[BUSSE_PRICING_DATA][CONTRACTS]
costs = client.client[BUSSE_PRICING_DATA][COSTS]
sched_data = client.client[BUSSEREBATETRACES][SCHED_DATA]

if __name__ == "__main__":
    print(contracts)
