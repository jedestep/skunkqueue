# -*- coding: utf-8 -*-

from persistence import get_backend
from job import Job

class SkunkQueue(object):
    def __init__(self, name,
            conn_url='localhost:27017', backend='mongodb',
            dbname='skunkqueue', queue_type='direct'):
        self.name = name
        self.queue_type = queue_type
        self.backend = backend
        self.conn_url = conn_url
        self.dbname = dbname

        self.persister = get_backend(backend)(conn_url=conn_url,
                dbname=dbname)

    def add_to_queue(self, job, route, ts=None):
        self.persister.add_job_to_queue(job, route, ts)

    def event(self, routes=[]):
        def decorator(fn):
            job = Job(self, fn, routes=routes)
            return job
        return decorator
