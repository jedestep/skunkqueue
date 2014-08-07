# -*- coding: utf-8 -*-
from persistence import get_backend
from util.log import Logger
from util.thread import KillableThread, TriggeredInterrupt

from time import sleep

import signal
import sys
import os
import socket
import traceback

import pickle
import dill
import json

class Worker(object):
    def __init__(self, queue_name, route, persister, wnum, port,
            logfile=sys.stdout):
        self.queue_name = queue_name
        self.route = route
        self.persister = persister
        self.stop = False
        self.pid = os.getpid()
        self.worker_id = '-'.join(['worker', str(wnum), queue_name, route, str(self.pid)])
        self.log = Logger(self.worker_id,logfile=logfile)
        self.log.info("starting")

        self.host = socket.gethostbyname(socket.gethostname())
        self.port = port
        self.register()

    def begin_execution(self, *args):
        while not self.stop:
            try:
                job = self.persister.get_job_from_queue(self.queue_name, self.worker_id, self.route)
                if job:
                    self.do_job(job)
                sleep(0.1)
            except TriggeredInterrupt:
                # we were cut off by an interrupt trigger
                self.log.warn("received interrupt request; stopping current job")
                self.log.warn("no result will be committed and this job will not be restarted")
                self.stop_worker()

    def register(self):
        self.persister.add_worker(self.worker_id, self.host, self.port)

    def unregister(self):
        self.persister.delete_worker(self.worker_id)

    def do_job(self, job):
        # depickle.
        body = pickle.loads(job['body'])
        directory = body['dir']
        self.log.debug("just appended "+str(directory))
        # FIXME a horrible hack where we add ourselves to the pythonpath
        sys.path.append(directory)
        self.log.debug("sys path is "+str(sys.path))
        mod = __import__(body['mod'])
        self.log.debug("just imported "+str(mod))

        #fn = dill.loads(body['fn'])
        fn = getattr(mod, body['fn'])
        args = body['args']
        kwargs = body['kwargs']

        # call it
        self.persister.set_working(self.worker_id)
        try:
            ret = fn(*args, **kwargs)
            self.persister.save_result(job['job_id'], ret, 'complete')
            self.log.info(ret)
        except Exception as e:
            self.persister.save_result(job['job_id'], None, 'error')
            self.log.error(str(e))
            exc_t, exc_v, exc_tr = sys.exc_info()
            self.log.error(str(
                '\n'.join(traceback.format_exception(exc_t, exc_v, exc_tr))))
            self.log.debug("python path is %s" % str(sys.path))
        finally:
            a = sys.path.pop()
        self.persister.unset_working(self.worker_id)

    def stop_worker(self):
        self.log.info("shutting down")
        self.unregister()
        self.stop = True

class WorkerPool(object):
    def __init__(self, queue_name, routing_keys=None,
            backend='mongodb', conn_url='localhost:27017',
            dbname='skunkqueue', logfile=sys.stdout,
            pidfile=None):
        """
        routing_keys are a required parameter to specify an n-length list
        of routing keys, which will each be assigned to one worker
        """
        self.stop = False
        self.queue_name = queue_name
        self.persister = get_backend(backend)(conn_url=conn_url, dbname=dbname)
        self.log = Logger('pool-'+queue_name, logfile=logfile)

        self.host = socket.gethostbyname(socket.gethostname())

        self.workers = {}

        if pidfile:
            self.log.info("writing to pidfile %s" % pidfile)
            with open(pidfile) as f:
                f.write(str(self.pid))
                f.close()

        # TODO this needs to be in shared memory
        wnums = {}

        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.bind((self.host, 0))
        self.s.listen(5)

        self.port = self.s.getsockname()[1]

        for key in routing_keys:
            if key not in wnums:
                wnums[key] = 0
            wnums[key] += 1
            worker = Worker(queue_name, key,
                    self.persister, wnums[key], self.port, logfile)

            thread = KillableThread(target=worker.begin_execution)
            thread.start()
            self.workers[worker.worker_id] = (worker, thread)

    def __enter__(self, *args, **kwargs):
        self.log.info("starting")
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
        for _id in self.workers.keys():
            worker = self.workers[_id]
            worker[0].stop_worker()
            del self.workers[_id]
        self.s.close()
        self.stop = True

    def die(self):
        for _id in self.workers.keys():
            worker = self.workers[_id]
            try:
                worker[1].raise_exc(TriggeredInterrupt)
                self.log.warn("raised an exception in %s" % str(_id))
                del self.workers[_id]
            except ValueError: # it was dead already
                self.log.debug("ignored killing thread %s" % str(_id))
                continue
        self.s.close()
        self.stop = True

    def run_cmd(self, terminate=None, kill=None, suicide=None):
        # requests a termination
        # terminate looks like: <worker_id>
        if terminate:
            self.workers[terminate][0].stop_worker()
            del self.workers[terminate]

        # performs a hard kill
        # kill looks like: <worker_id> 
        if kill:
            try:
                self.workers[kill][1].raise_exc(TriggeredInterrupt)
                del self.workers[kill]
            except ValueError: # it was already dead
                self.log.warn("tried to kill a thread when it was dead already")

        # shuts down the entire pool
        # suicide looks like: 1 
        if suicide == 1:
            self.stop = True

    def listen(self):
        while not self.stop:
            conn, addr = self.s.accept()
            datalen = conn.recv(2)
            data = ""
            while len(data) < int(datalen):
                data += conn.recv(1024)
            data = json.loads(data)
            self.run_cmd(**data)

            # no need to run the pool if it has no workers
            if len(self.workers) == 0:
                self.shutdown()
