import dill
import base64
import json
import inspect
import time

class Job(object):
    def __init__(self, queue, fn, routes=None):
        routes = routes or []

        self.routes = routes
        self.fn = fn
        self.created = time.time()
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
            self.queue.add_to_queue(self, route)

    def json(self):
        return {
            'q': self.queue.name,
            'body': base64.b64encode(json.dumps({
                'fn': dill.dumps(self.fn),
                'ts': self.created,
                'args': self.args,
                'kwargs': self.kwargs,
            }))
        }
