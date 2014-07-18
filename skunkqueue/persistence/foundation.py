# -*- coding: utf-8 -*-

from datetime import datetime
from fdb_layers.queue import Queue # TODO may need to bundle this

import fdb
import dill
import json

class FoundationPersister(object):
    def __init__(self,
                 conn_url='/usr/local/etc/foundationdb/fdb.cluster',
                 dbname='skunkqueue'):

        fdb.api_version(200)
        self.conn = fdb.open(conn_url)
        self.skunkdb = fdb.directory.create_or_open(self.conn, (dbname,))
        self.worker_space = self.skunkdb['worker']
        self.result_space = self.skunkdb['result']

        self.job_queues = {}

    def add_worker(self, worker_id):
        self.conn[self.worker_space.pack((worker_id,))] = '1'

    def delete_worker(self, worker_id):
        del self.conn[self.worker_space.pack((worker_id,))]

    def job_result(self, job_id):
        try:
            return json.loads(self.conn[self.result_space.pack((job_id,))].split('_:_')[0])
        except TypeError:
            return None

    def job_state(self, job_id):
        try:
            return self.conn[self.result_space.pack((job_id,))].split('_:_')[1]
        except TypeError:
            return 'pending'

    def save_result(self, job_id, value, state):
        self._save_result(job_id, value, state, self.conn)

    @fdb.transactional
    def _save_result(self, job_id, value, state, tr):
        tr[self.result_space.pack((job_id,))] = '_:_'.join([json.dumps(value), state])

    # This is not transactional because it uses the
    # underlying queue layer which is actually transactional
    def add_job_to_queue(self, job, route):
        queue_name = job.queue.name
        job.job_id = str(id(job))

        job_flat = job.json()
        job_flat['now'] = datetime.utcnow()
        if job.queue.queue_type == 'broadcast':
            for worker in self._get_workers(self.skunkdb):
                queue = '__WORKERQUEUE-'+worker['worker_id']
                self._get_queue(queue).push(self.conn, dill.dumps(job_flat))
        else:
            queue = queue_name + '-' + route
            self._get_queue(queue).push(self.conn, dill.dumps(job_flat))

    def get_job_from_queue(self, queue_name, worker_id, route):
        return self._get_job_from_queue(queue_name, worker_id, route, self.conn)

    @fdb.transactional
    def _get_job_from_queue(self, queue_name, worker_id, route, tr):
        rqueue_name = queue_name + '-' + route
        route_queue = self._get_queue(queue_name + '-' + route)
        worker_queue = self._get_queue('__WORKERQUEUE-'+worker_id)
        rret = route_queue.pop(self.conn)
        if rret:
            return dill.loads(rret)
        wret = worker_queue.pop(self.conn)
        if wret:
            return dill.loads(wret)

    # TODO could be a generator
    @fdb.transactional
    def _get_workers(self, tr):
        return [w for w in tr[self.worker_space.range(())]]

    def _get_queue(self, name):
        if name in self.job_queues:
            return self.job_queues[name]
        ret = self.job_queues[name] = Queue(self.skunkdb[name])
        return ret
