# -*- coding: utf-8 -*-
from persistence import QueuePersister
import thread

class Worker(object):

    def __init__(self, queue_name, routing_key, persister):
        self.queue_name = queue_name
        self.routing_key = routing_key
        self.persister = persister

    def begin_execution(self, *args):
        self.thread_id = thread.get_ident()
        while(True):
            job = persister.get_job_from_queue(self.queue_name, self.route)
            if job:
                self.do_job(job)
            sleep(0.1)

    def do_job(self, job):
        #depickle.
        print "de pickle: {0}".format(job)


class WorkerPool(object):

    def __init__(self, queue_name, routing_keys=None):
        """
            routing_keys are a required parameter to specify an n-length list
            of routing keys, which will each be assigned to one worker
        """
        self.persister = QueuePersister()
        workers = []

        for key in routing_keys:
            worker = Worker(queue_name, key, self.persister)
            thread.start_new_thread(worker.begin_execution, (None, ))
