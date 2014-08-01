# -*- coding: utf-8 -*-
from persistence import get_backend
from threading import Thread, current_thread
from time import sleep
from util.log import Logger

import signal
import sys
import os

import pickle
import dill

class Worker(object):
    def __init__(self, queue_name, route, persister, wnum, logfile=sys.stdout):
        self.queue_name = queue_name
        self.route = route
        self.persister = persister
        self.stop = False
        self.pid = os.getpid()
        self.worker_id = '-'.join(['worker', str(wnum), queue_name, route, str(self.pid)])
        self.log = Logger(self.worker_id,logfile=logfile)
        self.log.info("starting")

    def begin_execution(self, *args):
        self.thread = current_thread()
        self.register()
        while(not self.stop):
            job = self.persister.get_job_from_queue(self.queue_name, self.worker_id, self.route)
            if job:
                self.do_job(job)
            sleep(0.1)

    def register(self):
        self.persister.add_worker(self.worker_id)

    def unregister(self):
        self.persister.delete_worker(self.worker_id)

    def do_job(self, job):
        #depickle.
        body = pickle.loads(job['body'])
        fn = dill.loads(body['fn'])
        args = body['args']
        kwargs = body['kwargs']

        #call it
        try:
            ret = fn(*args, **kwargs)
            self.persister.save_result(job['job_id'], ret, 'complete')
            self.log.info(ret)
        except Exception as e:
            self.persister.save_result(job['job_id'], None, 'error')
            self.log.error(str(e))

    def stop_worker(self):
        self.unregister()
        self.stop = True


class WorkerPool(object):
    def __init__(self, queue_name, routing_keys=None,
            backend='mongodb', conn_url='localhost:27017',
            dbname='skunkqueue', logfile=sys.stdout):
        """
        routing_keys are a required parameter to specify an n-length list
        of routing keys, which will each be assigned to one worker
        """
        self.queue_name = queue_name
        self.persister = get_backend(backend)(conn_url=conn_url, dbname=dbname)
        self.workers = []
        self.log = Logger('pool-'+queue_name, logfile=logfile)

        wnums = {}

        for key in routing_keys:
            if key not in wnums:
                wnums[key] = 0
            wnums[key] += 1
            worker = Worker(queue_name, key, self.persister, wnums[key], logfile)
            thread = Thread(target=worker.begin_execution)
            thread.start()
            self.workers.append(worker)

    def __enter__(self, *args, **kwargs):
        def gentle(signum, frame):
            self.log.info("Received gentle shutdown signal %d" % signum)
            self.shutdown()
            sys.exit(0)
        def rough(signum, frame):
            self.log.warn("Received non-gentle kill signal %d" % signum)
            self.die()
            sys.exit(0)

        signal.signal(signal.SIGINT,  rough )
        signal.signal(signal.SIGHUP,  gentle)
        signal.signal(signal.SIGTERM, gentle)
        signal.signal(signal.SIGALRM, gentle)
        signal.signal(signal.SIGQUIT, gentle)
        return self

    def __exit__(self, *args, **kwargs):
        self.shutdown()

    def shutdown(self):
        for worker in self.workers:
            worker.stop_worker()

    def die(self):
        for worker in self.workers:
            os.kill(worker.pid, signal.SIGKILL)

    def listen(self):
        while True:
            sleep(0.5)
