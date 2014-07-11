from pymongo import MongoClient
from datetime import datetime

class QueueListener(object):
    def __init__(self, routing_key, 
                 conn_url='localhost:27017',
                 dbname='skunkqueue', 
                 collname='messages'):
        # The routing key that we listen for
        self.routing_key = routing_key

        # The collection where messages will appear
        self.queue = MongoClient(conn_url)[dbname][collname]

    def cursor(self):
        ts = datetime.utcnow()
        tailer = self.exchange.find({
                'ts': {'$gt': ts}, 
                'routing_key': routing_key
            },
            tailable=True,
            await_data=True,
            no_cursor_timeout=True)
        return tailer

    def stream(self):
        while True:
            for doc in self.cursor():
                yield doc

    def listen(self):
        for doc in self.stream():
            pass # TODO execute!
