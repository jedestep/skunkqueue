from persistence import QueuePersister
from job import Job

class SkunkQueue(object):
    def __init__(self, name):
        self.handlers = {}

        self.name = name

        self.persister = QueuePersister()

    def add_handler(self, route, handler):
        self.handlers.update({route:handler})

    def add_to_queue(self, job, route):
        self.persister.add_job_to_queue(job, route)

    def event(self, routes=[]):
        def decorator(fn):
            job = Job(self, fn, routes=routes)
            return job
        return decorator
