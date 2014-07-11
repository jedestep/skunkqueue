import thread

class Worker(object):

    def __init__(self, queue_name, routing_key):
        self.queue_name = queue_name
        self.routing_key = routing_key

    def begin_execution(self, *args):
        self.thread_id = thread.get_ident()
        while(True):
            self.check_for_work()

    def check_for_work(self):
        # Check for queues
        pass

class WorkerPool(object):

    def __init__(self, queue_name, routing_keys=None):
        """
            routing_keys are a required parameter to specify an n-length list
            of routing keys, which will each be assigned to one worker
        """
        workers = []

        for key in routing_keys:
            worker = Worker(queue_name, key)
            thread.start_new_thread(worker.begin_execution, (None, ))