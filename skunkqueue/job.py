# -*- coding: utf-8 -*-

import dill
import pickle
import base64
import json
import inspect
import time
from bson.objectid import ObjectId

from persistence import QueuePersister

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

        self.persister = QueuePersister()

    def __call__(self, *args, **kwargs):
        self.fn(*args, **kwargs)

    def fire(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        for route in self.routes:
            self.queue.add_to_queue(self, route)

    @property
    def state(self):
        return self.persister.job_state(job._id)

    @property
    def result(self):
        return self.persister.job_result(job._id)

    def json(self):
        return {
            'job_id': ObjectId(),
            'q': self.queue.name,
            'body': pickle.dumps({
                'fn': dill.dumps(self.fn),
                'ts': self.created,
                'args': self.args,
                'kwargs': self.kwargs,
            })
        }
