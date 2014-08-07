# -*- coding: utf-8 -*-
from datetime import datetime

import pickle
import json
_redis = __import__('redis')

default_cfg = {
    'backend': 'redis',
    'conn_url': 'localhost:6379',
    'dbname': 0
}

class RedisPersister(object):
    def __init__(self,
            conn_url='localhost:6379',
            dbname=0):
        self.conn_url = conn_url
        self.dbname = str(dbname)
        host,port = conn_url.split(':')
        self.skunkdb = _redis.Redis(host=host, port=int(port), db=dbname)

    ### Basic information ###

    def get_location(self):
        return self.conn_url + '/' + self.dbname

    def get_backend_type(self):
        return 'redis'

    def get_version(self):
        return self.skunkdb.info()['redis_version']

    ### Queue manipulation ###
    def get_all_queues(self):
        return [] # TODO

    def route_is_empty(self, queue_name, route):
        queue = '-'.join([queue_name, route])
        return self.skunkdb.llen(queue) == 0

    ### Result access ###

    def job_state(self, job_id):
        return self.skunkdb.get('state-'+job_id)

    def job_result(self, job_id):
        return self.skunkdb.get('result-'+job_id)

    def save_result(self, job_id, value, state):
        self.skunkdb.set('result-'+job_id, value)
        self.skunkdb.set('state-'+job_id, state)

    ### Job manipulation ###

    def add_job_to_queue(self, job, route, ts=None):
        if ts:
            raise NotImplementedError("fire_at is currently unsupported by redis")
        queue_name = job.queue.name
        job.job_id = str(id(job)) # TODO add more randomness to this

        job_flat = job.json()
        job_flat['now'] = datetime.utcnow()
        if job.queue.queue_type == 'broadcast':
            for worker in self.skunkdb.hkeys('workers'):
                queue = worker['worker_id']
                self.skunkdb.rpush('__WORKERQUEUE-'+queue, job.job_id)
                self.skunkdb.set('job-'+job.job_id, pickle.dumps(job_flat))
        else:
            queue = '-'.join([queue_name, route])
            self.skunkdb.rpush(queue, job.job_id)
            print 'putting job at id location', job.job_id
            self.skunkdb.set('job-'+job.job_id, pickle.dumps(job_flat))

    def get_job_from_queue(self, queue_name, worker_id, route):
        # TODO for now only listen for direct queues
        queue = '-'.join([queue_name, route])
        jid = self.skunkdb.lpop(queue)
        if jid:
            print 'got jid', jid
            jid = jid.strip()
            v = self.skunkdb.get(unicode(jid))
            print 'v was', v
            if v:
                return pickle.loads(v)

    def get_jobs_by_queue(self, queue):
        return [self.skunkdb.get(k) for k in self.skunkdb.keys(queue+'-*')]

    def dequeue_job(self, job_id):
        pass

    # Worker manipulation

    def add_worker(self, worker_id, host, port):
        self.skunkdb.hset('workers', worker_id, json.dumps({
            'host': host,
            'port': port,
            'state': 'waiting'
            }))

    def delete_worker(self, worker_id):
        self.skunkdb.hdel('workers', worker_id)

    def add_monitor(self, host):
        pass # TODO

    def delete_monitor(self, host):
        pass # TODO

    def set_working(self, worker_id):
        v = json.loads(self.skunkdb.hget('workers', worker_id))
        v['state'] = 'working'
        v['start'] = str(datetime.utcnow())
        self.skunkdb.hset('workers', worker_id, json.dumps(v))

    def unset_working(self, worker_id):
        v = json.loads(self.skunkdb.hget('workers', worker_id))
        v['state'] = 'waiting'
        self.skunkdb.hset('workers', worker_id, json.dumps(v))

    def get_all_workers(self):
        return map(json.loads, self.skunkdb.hvals('workers'))
