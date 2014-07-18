from mongodb import MongoDBPersister
from foundation import FoundationPersister
from _redis import RedisPersister

def get_backend(name):
    backends = {
        'mongodb': MongoDBPersister,
        'redis': RedisPersister,
        'fdb': FoundationPersister,
    }
    return backends[name]
