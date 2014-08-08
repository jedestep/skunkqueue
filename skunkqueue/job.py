# -*- coding: utf-8 -*-

import dill
import pickle
import base64
import json
import inspect
import time
import os

from persistence import get_backend

"""Allows users to define Jobs with subclassing"""
class JobMixin(object):
    def __init__(self):
        self.routes = []

    def work(self, *args, **kwargs):
        pass

    def job(self, queue, routes=[]):
        routes = routes or self.routes
        if hasattr(self, '_job'):
            return self._job
        self._job = Job(queue, self.work, routes)
        self._job.args = self.args
        self._job.kwargs = self.kwargs
        return self._job

    def __call__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        return self

    @property
    def state(self):
        if not hasattr(self, '_job'):
            return # TODO raise an exception
        return self._job.state

    @property
    def result(self):
        if not hasattr(self, '_job'):
            return # TODO raise an exception
        return self._job.result

class Job(object):
    def __init__(self, queue, fn, routes=None):
        routes = routes or []

        self.routes = routes
        self.fn = fn
        # we need either a reference to the containing module
        # or to the containing object (instance or class)
        self.parent = None
        self.fn_type = None
        if inspect.ismethod(fn):
            self.fn_type = 'method'
            try:
                self.parent = dill.dumps(fn.__self__)
            except: # TODO what kind of exception is it
                raise ValueError("Instance containing method couldn't be marshalled")
        elif inspect.isfunction(fn):
            self.fn_type = 'function'
        else:
            raise TypeError("Job must be constructed with a function or method")
        self.mod = inspect.getmodulename(
                inspect.getmodule(fn).__file__)
        self.directory = os.getcwd()
        self.created = time.time()
        self.queue = queue
        self.args = None
        self.varargs = None
        self.kwargs = None

        self.persister = get_backend(queue.backend)(
                conn_url=queue.conn_url,
                dbname=queue.dbname)

    def __call__(self, *args, **kwargs):
        return self.fn(*args, **kwargs)

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
            'fn_type': self.fn_type,
            'body': pickle.dumps({
                'dir': self.directory,
                'parent': self.parent,
                'mod': self.mod,
                'fn': self.fn.__name__,
                'ts': self.created,
                'args': self.args,
                'kwargs': self.kwargs,
            })
        }
