# -*- coding: utf-8 -*-
from datetime import datetime

import pickle
import json
import random
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
        self.skunkdb = _redis.Redis(host=host, port=int(port), db=int(dbname))
        self.blocking = False # TODO change it

    ### Basic information ###

    def get_location(self):
        return self.conn_url + '/' + self.dbname

    def get_backend_type(self):
        return 'redis'

    def get_version(self):
        return self.skunkdb.info()['redis_version']

    ### Queue manipulation ###
    def get_all_queues(self):
        return [x.split('-')[1] for x in self.skunkdb.scan_iter("queue-*")]

    def route_is_empty(self, queue_name, route):
        queue = '-'.join(['queue', queue_name, route])
        return self.skunkdb.llen(queue) == 0

    ### Result access ###

    def job_state(self, job_id):
        return self.skunkdb.hget('state',job_id)

    def job_result(self, job_id):
        return self.skunkdb.hget('result',job_id)

    def save_result(self, job_id, value, state):
        self.skunkdb.hset('result',job_id, value)
        self.skunkdb.hset('state',job_id, state)

    ### Job manipulation ###

    def add_job_to_queue(self, job, route, ts=None):
        if ts:
            raise NotImplementedError("fire_at is currently unsupported by redis")
        queue_name = job.queue.name
        job.job_id = str(id(job)) + str(random.randint(0,65536)) # TODO add more randomness to this

        job_flat = job.json()
        job_flat['now'] = pickle.dumps(datetime.utcnow())
        if job.queue.queue_type == 'broadcast':
            for worker in self.skunkdb.hkeys('workers'):
                queue = worker['worker_id']
                self.skunkdb.rpush('__WORKERQUEUE-'+queue, job_flat)
        else:
            queue = '-'.join(['queue', queue_name, route])
            self.skunkdb.rpush(queue, json.dumps(job_flat))
        self.skunkdb.set('jobqueue-'+job.job_id, queue)

    def get_job_from_queue(self, queue_name, worker_id, route):
        # TODO for now only listen for direct queues
        queue = '-'.join(['queue',queue_name, route])
        if self.blocking:
            return self.skunkdb.blpop(queue)[1]
        else:
            job = self.skunkdb.lpop(queue)
            if job:
                return json.loads(job)

    def get_jobs_by_queue(self, queue):
        # this returns a list of lists
        jobs = [map(json.loads, self.skunkdb.lrange(k,0,-1))
                for k in self.skunkdb.scan_iter('queue-'+queue+'-*')]
        # concatenate
        return [i for s in jobs for i in s]

    def dequeue_job(self, queue_name, job_id):
        routekey = self.skunkdb.get('jobqueue-'+job_id)
        # copy queue into a temp, removing the matching job
        for _i in xrange(0,self.skunkdb.llen(routekey)):
            val = json.loads(self.skunkdb.rpoplpush(routekey, routekey))
            if 'job_id' in val and val['job_id'] == job_id:
                # we found the job to kill but it was already pushed onto the left side
                # pop from the left side to toss it
                self.skunkdb.lpop(routekey)

    # Worker manipulation

    def add_worker(self, worker_id, host, port):
        # TODO may actually be beneficial to use a set here
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
        # TODO package worker ids
        pairs = self.skunkdb.hscan_iter('workers')
        ret = []
        for k, w in pairs:
            w = json.loads(w)
            w['worker_id'] = k
            ret.append(w)
        return ret
