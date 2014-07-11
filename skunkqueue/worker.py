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
        while(not self.stop):
            job = self.persister.get_job_from_queue(self.queue_name, self.route)
            if job:
                self.do_job(job)
            sleep(0.1)

    def do_job(self, job):
        #depickle.
        body = pickle.loads(job['body'])
        fn = dill.loads(body['fn'])
        args = body['args']
        kwargs = body['kwargs']

        #call it
        print 'about to call a function'
        fn(*args, **kwargs)

    def stop_worker(self):
        self.stop = True


class WorkerPool(object):

    def __init__(self, queue_name, routing_keys=None):
        """
            routing_keys are a required parameter to specify an n-length list
            of routing keys, which will each be assigned to one worker
        """
        self.persister = QueuePersister()
        self.workers = []

        for key in routing_keys:
            worker = Worker(queue_name, key, self.persister)
            thread = Thread(target=worker.begin_execution)
            thread.start()
            self.workers.append(worker)

    def shutdown(self, *args, **kwargs):
        for worker in self.workers:
            worker.stop_worker()
