import pickle
import base64
import json
import inspect
from datetime import datetime

class Job(object):
    def __init__(self, queue, fn, routes=None):
        routes = routes or []

        self.routes = routes
        self.fn = fn
        self.created = datetime.utcnow()
        self.queue = queue
        self.args = None
        self.varargs = None
        self.kwargs = None

        self.state = 'PENDING'
        self.result = None

    def __call__(self, *args, **kwargs):
        self.fn(*args, **kwargs)

    def fire(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        for route in self.routes:
            self.queue.handlers[route].add_to_queue(self)

    def json(self):
        return {
            'q': self.queue.name,
            'routes': self.routes,
            'body': base64.b64encode(json.dumps({
                'fn': pickle.dumps(self.fn),
                'ts': self.created,
                'args': self.args,
                'kwargs': self.kwargs,
            }))
        }
