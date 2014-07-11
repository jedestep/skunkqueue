# -*- coding: utf-8 -*-

from persistence import QueuePersister
from job import Job

class SkunkQueue(object):
    def __init__(self, name, queue_type='direct'):
        self.name = name
        self.queue_type = queue_type

        self.persister = QueuePersister()

    def add_to_queue(self, job, route):
        self.persister.add_job_to_queue(job, route)

    def event(self, routes=[]):
        def decorator(fn):
            job = Job(self, fn, routes=routes)
            return job
        return decorator
