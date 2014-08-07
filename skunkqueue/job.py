# -*- coding: utf-8 -*-

import dill
import pickle
import base64
import json
import inspect
import time
import os

from persistence import get_backend

class Job(object):
    def __init__(self, queue, fn, routes=None):
        routes = routes or []

        self.routes = routes
        self.fn = fn
        self.mod = inspect.getmodulename(inspect.getmodule(fn).__file__)
        self.directory = os.getcwd() + '/'
        self.created = time.time()
        self.queue = queue
        self.args = None
        self.varargs = None
        self.kwargs = None

        self.persister = get_backend(queue.backend)(
                conn_url=queue.conn_url,
                dbname=queue.dbname)

    def __call__(self, *args, **kwargs):
        self.fn(*args, **kwargs)

    def fire(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        for route in self.routes:
            self.queue.add_to_queue(self, route)

    def fire_at(self, ts, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        for route in self.routes:
            self.queue.add_to_queue(self, route, ts)

    @property
    def state(self):
        return self.persister.job_state(self.job_id)

    @property
    def result(self):
        return self.persister.job_result(self.job_id)

    def json(self):
        return {
            'job_id': self.job_id,
            'q': self.queue.name,
            'body': pickle.dumps({
                'dir': self.directory,
                'mod': self.mod,
                'fn': self.fn.__name__,
                'ts': self.created,
                'args': self.args,
                'kwargs': self.kwargs,
            })
        }
