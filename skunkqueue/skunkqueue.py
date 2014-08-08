# -*- coding: utf-8 -*-

from persistence import get_backend
from job import Job, JobMixin

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

    def __eq__(self, other):
        return isinstance(other, SkunkQueue) and self.name == other.name

    ### Life ###

    """Enqueue a job on this queue"""
    def add_to_queue(self, job, route, ts=None):
        self.persister.add_job_to_queue(job, route, ts)

    def __iadd__(self, other):
        if isinstance(other, JobMixin):
            other = other.job(self)
        for route in other.routes:
            self.add_to_queue(other, route)

    ### Death ###

    """Delete all jobs in this queue by route
       TODO this is not atomic and it should be"""
    def kill_all_by_route(self, route):
        while not self.persister.route_is_empty(self.name, route):
            self.persister.get_job_from_queue(self.name, 0, route)

    """Delete one job in this queue"""
    def kill(self, job_or_jid):
        jid = None
        if isinstance(job_or_jid, Job):
            jid = job_or_jid.job_id
        else:
            jid = job_or_jid
        self.persister.dequeue_job(self.name, jid)

    def job(self, routes=[]):
        def decorator(fn):
            j = Job(self, fn, routes=routes)
            return j
        return decorator
