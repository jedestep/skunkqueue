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

        self.conn_url=conn_url
        self.dbname=dbname

        self.skunkdb = MongoClient(conn_url)[dbname]
        self.access_collection = self.skunkdb['access']
        self.jobs_collection = self.skunkdb['jobs']
        self.worker_collection = self.skunkdb['workers']
        self.result_collection = self.skunkdb['result']
        
    ### Basic information ###

    def get_location(self):
        return self.conn_url + '/' + self.dbname

    def get_backend_type(self):
        return 'mongodb'

    def get_version(self):
        return self.skunkdb.command({'buildInfo': 1})['version']

    ### Queue manipulation ###

    def get_all_queues(self):
        return self.access_collection.distinct('q')

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

    ### Result access ###

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

    ### Job manipulation ###

    def add_job_to_queue(self, job, route, ts=None):
        queue_name = job.queue.name
        job.job_id = str(ObjectId())
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

    def get_jobs_by_queue(self, queue):
        return [c for c in self.jobs_collection.find({'q': queue})]

    ### Worker manipulation ###

    def add_worker(self, worker_id, host, port):
        self.worker_collection.insert({
            'worker_id': worker_id,
            'host': host,
            'port': port,
            'state': 'waiting'
            })

    def delete_worker(self, worker_id):
        self.worker_collection.remove(dict(worker_id=worker_id))

    def add_monitor(self, host):
        if not self.worker_collection.find_one({'monitor': 1, 'host': host}):
            self.worker_collection.insert({
                'host': host,
                'monitor': 1
            })
            return True
        return False

    def delete_monitor(self, host):
        self.worker_collection.remove({'monitor': 1, 'host': host})

    def set_working(self, worker_id):
        self.worker_collection.update(
                dict(worker_id=worker_id),
                {'$set': {
                    'state': 'working',
                    'start': datetime.utcnow()
                    }}, True)

    def unset_working(self, worker_id):
        self.worker_collection.update(
                dict(worker_id=worker_id),
                {'$set': {
                    'state': 'waiting',
                    }}, True)

    def get_all_workers(self):
        return [w for w in self.worker_collection.find()]
