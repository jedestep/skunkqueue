from mongodb import MongoDBPersister
from _redis import RedisPersister

def get_backend(name):
    backends = {
        'mongodb': MongoDBPersister,
        'redis': RedisPersister,
    }
    return backends[name]
