# -*- coding: utf-8 -*-
from persistence import QueuePersister
from threading import Thread, current_thread
from time import sleep

import pickle
import dill

class Worker(object):

    def __init__(self, queue_name, route, persister):
        self.queue_name = queue_name
        self.route = route
        self.persister = persister
        self.stop = False

    def begin_execution(self, *args):
        self.thread = current_thread()
        self.register()
        while(not self.stop):
            job = self.persister.get_job_from_queue(self.queue_name, self.worker_id, self.route)
            if job:
                self.do_job(job)
            sleep(0.1)

    def register(self):
        self.worker_id = id(self)
        self.persister.worker_collection.insert({'worker_id': self.worker_id})

    def unregister(self):
        self.persister.worker_collection.remove({'worker_id': self.worker_id})

    def do_job(self, job):
        #depickle.
        body = pickle.loads(job['body'])
        fn = dill.loads(body['fn'])
        args = body['args']
        kwargs = body['kwargs']

        #call it
        ret = fn(*args, **kwargs)
        self.persister.save_result(job['job_id'], ret)
        print ret

    def stop_worker(self):
        self.unregister()
        self.stop = True


class WorkerPool(object):

    def __init__(self, queue_name, routing_keys=None):
        """
            routing_keys are a required parameter to specify an n-length list
            of routing keys, which will each be assigned to one worker
        """
        self.queue_name = queue_name
        self.persister = QueuePersister()
        self.workers = []

        for key in routing_keys:
            worker = Worker(queue_name, key, self.persister)
            thread = Thread(target=worker.begin_execution)
            thread.start()
            self.workers.append(worker)

    def __enter__(self, *args, **kwargs):
        pass

    def __exit__(self, *args, **kwargs):
        for worker in self.workers:
            worker.stop_worker()
