from pymongo import MongoClient

class SkunkQueue(object):
    def __init__(self):
        self.cfg = {}

    def configure(self, conn_url='localhost:27017',
                 dbname='skunkqueue', collname='messages'):
        self.cfg['conn_url'] = conn_url
        self.cfg['dbname'] = dbname
        self.cfg['collname'] = collname

    def __getitem__(self, item):
        return self.cfg[item]
