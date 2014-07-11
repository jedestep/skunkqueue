# -*- coding: utf-8 -*-

from pymongo import MongoClient
from datetime import datetime

class QueuePersister(object):

    def __init__(self,
                 conn_url='localhost:27017',
                 dbname='skunkqueue'):

        self.skunkdb = MongoClient(conn_url)[dbname]
        self.access_collection = self.skunkdb['access']
        self.jobs_collection = self.skunkdb['jobs']

    def add_job_to_queue(self, job, route):
        job_flat = job.json()
        job_flat['now'] = datetime.utcnow()
        job_flat['route'] = route
        self.jobs_collection.insert(job_flat)
