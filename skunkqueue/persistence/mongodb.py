# -*- coding: utf-8 -*-

from pymongo import MongoClient
from datetime import datetime
from bson.objectid import ObjectId

default_cfg = {
    'backend': 'mongodb',
    'conn_url': 'localhost:27017',
    'dbname': 'skunkqueue'
}

class MongoDBPersister(object):
    def __init__(self,
                 conn_url='localhost:27017',
                 dbname='skunkqueue'):

        self.skunkdb = MongoClient(conn_url)[dbname]
        self.access_collection = self.skunkdb['access']
        self.jobs_collection = self.skunkdb['jobs']
        self.worker_collection = self.skunkdb['workers']
        self.result_collection = self.skunkdb['result']


    def job_state(self, job_id):
        ret = self.result_collection.find_one({'job_id': job_id})
        if ret:
            return ret['state']
        else:
            return 'pending'

    def job_result(self, job_id):
        ret = self.result_collection.find_one({'job_id': job_id})
        if ret:
            return ret['value']

    def save_result(self, job_id, value, state):
        self.result_collection.insert(
            {'job_id': job_id, 'value': value, 'state': state})

    def add_job_to_queue(self, job, route, ts=None):
        queue_name = job.queue.name
        job.job_id = ObjectId()
        self.access_collection.find_and_modify(
            {'q': queue_name}, {'q': queue_name, 'locked': False}, upsert=True)
        job_flat = job.json()
        # ts should be a datetime.timedelta object
        if ts:
            job_flat['now'] = datetime.utcnow() + ts
        else:
            job_flat['now'] = datetime.utcnow()
        job_flat['route'] = route
        if job.queue.queue_type == 'broadcast':
            for worker in self.worker_collection.find():
                job_flat['q'] = worker['worker_id']
                job_flat['_id'] = ObjectId()
                self.jobs_collection.insert(job_flat)
        else:
            self.jobs_collection.insert(job_flat)

    def add_worker(self, worker_id):
        self.worker_collection.insert(dict(worker_id=worker_id))

    def delete_worker(self, worker_id):
        self.worker_collection.remove(dict(worker_id=worker_id))

    def route_is_empty(self, queue_name, route):
        ret = False
        try:
            res = self.access_collection.find_and_modify(
                    {'q': queue_name, 'locked': False},
                    update={'$set': {'locked': True}})
            if res:
                ret = self.jobs_collection.find({
                    'q': queue_name,
                    'route': route,
                    'now': {'$lte': datetime.utcnow()}
                    }).count() == 0
        finally:
            self.access_collection.update({'q': queue_name},
                {'$set': {'locked': False}})
        return ret

    def get_job_from_queue(self, queue_name, worker_id, route):
        try:
            res = self.access_collection.find_and_modify(
                    {'q': queue_name, 'locked': False},
                    update={'$set': {'locked': True}})
            if res:
                job = self.jobs_collection.find_and_modify(
                        {'$or': [{'q': queue_name},{'q': worker_id}],
                            'now': {'$lte': datetime.utcnow()},
                            'route': route},
                        remove=True, sort=[('now', -1)])

                return job
        finally:
            self.access_collection.update({'q': queue_name},
                {'$set': {'locked': False}})
