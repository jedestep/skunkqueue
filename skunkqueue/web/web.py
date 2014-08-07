from skunkqueue.persistence import get_backend
from skunkqueue import SkunkQueue
from flask import Flask, request, url_for, render_template, redirect
from datetime import datetime, timedelta

import sys
import socket
import json

backend = None

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        host = request.form['host']
        port = request.form['port']
        wid = request.form['wid']
        severity = request.form['severity']

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((host, int(port)))

        msg = json.dumps({severity: wid})
        s.sendall(str(len(msg)))
        s.sendall(msg)
        return redirect('/')

    return render_template('index.html',
            workers=backend.persister.get_all_workers(),
            queues=backend.persister.get_all_queues())

@app.route('/queue/<queue_name>', methods=['GET', 'POST'])
def show_queue(queue_name):
    if request.method == 'POST':
        jid = request.form['jid']
        bname = backend.name
        backend.name = queue_name
        backend.kill(jid)
        backend.name = bname
        return redirect(url_for('show_queue', queue_name=queue_name))
    return render_template('queue.html',
            queue_name=queue_name,
            jobs=backend.persister.get_jobs_by_queue(queue_name))

def run(backend_name, conn_url, dbname):
    app.debug = True
    global backend
    backend = SkunkQueue('__web__',
            backend=backend_name,
            conn_url=conn_url,
            dbname=dbname)
    app.jinja_env.globals['btype'] = backend.persister.get_backend_type()
    app.jinja_env.globals['bloc'] = backend.persister.get_location()
    app.jinja_env.globals['bvers'] = backend.persister.get_version()
    app.jinja_env.globals['str'] = str
    app.run()

if __name__ == '__main__':
    app.debug = True
    #run('mongodb', 'localhost:27017', 'skunkqueue')
    run('redis', 'localhost:6379', 0)
