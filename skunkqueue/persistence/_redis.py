# -*- coding: utf-8 -*-

from redis import Redis
from datetime import datetime

import pickle
import dill

class RedisPersister(object):
    def __init__(self,
            conn_url='localhost:6379',
            dbname=0):
        host,port = conn_url.split(':')
        self.skunkdb = Redis(host=host, port=int(port), db=dbname)

    def add_job_to_queue(self, job, route):
        queue_name = job.queue.name
        job_flat = job.json()

    def add_worker(self, worker_id):
        self.skunkdb.hset('workers', worker_id, 1)

    def delete_worker(self, worker_id):
        self.skunkdb.hdel('workers', worker_id)

    def job_state(self, job_id):
        return self.skunkdb.get('state-'+job_id)

    def job_result(self, job_id):
        return self.skunkdb.get('result-'+job_id)

    def save_result(self, job_id, value, state):
        self.skunkdb.set('result-'+job_id, value)
        self.skunkdb.set('state-'+job_id, state)

    def add_job_to_queue(self, job, route):
        queue_name = job.queue.name
        job.job_id = 'asdf' # TODO get a job id

        job_flat = job.json()
        job_flat['now'] = datetime.utcnow()
        if job.queue.queue_type == 'broadcast':
            for worker in self.skunkdb.hkeys('workers'):
                queue = worker['worker_id']
                self.skunkdb.rpush('__WORKERQUEUE-'+queue, pickle.dumps(job_flat))
        else:
            queue = '-'.join([queue_name, route])
            self.skunkdb.rpush(queue, dill.dumps(job_flat))

    def get_job_from_queue(self, queue_name, worker_id, route):
        # TODO for now only listen for direct queues
        queue = '-'.join([queue_name, route])
        val = self.skunkdb.blpop('-'.join([queue_name, route]))
        return dill.loads(val[1])
