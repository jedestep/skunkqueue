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
        queue_name = job.queue.name
        self.access_collection.find_and_modify(
            {'q': queue_name}, {'q': queue_name, 'locked': False}, upsert=True)
        job_flat = job.json()
        job_flat['now'] = datetime.utcnow()
        job_flat['route'] = route
        self.jobs_collection.insert(job_flat)

    def get_job_from_queue(self, queue_name, route):
        try:
            res = self.access_collection.find_and_modify(
                {'q': queue_name, 'locked': False},
                update={'$set': {'locked': True}})
            if res:
                job = self.jobs_collection.find_and_modify(
                    {'q': queue_name, 'route': route},
                    remove=True, sort=[('now', -1)])

                return job
        finally:
            self.access_collection.update({'q': queue_name},
                {'$set': {'locked': False}})
