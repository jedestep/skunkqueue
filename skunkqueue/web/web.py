from skunkqueue.persistence import get_backend
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
            workers=backend.get_all_workers(),
            queues=backend.get_all_queues())

@app.route('/queue/<queue_name>')
def show_queue(queue_name):
    return render_template('queue.html',
            queue_name=queue_name,
            jobs=backend.get_jobs_by_queue(queue_name))

def run(backend_name, conn_url, dbname):
    app.debug = True
    global backend
    backend = get_backend(backend_name)()
    app.jinja_env.globals['btype'] = backend.get_backend_type()
    app.jinja_env.globals['bloc'] = backend.get_location()
    app.jinja_env.globals['bvers'] = backend.get_version()
    app.jinja_env.globals['str'] = str
    app.run()

if __name__ == '__main__':
    app.debug = True
    run(sys.argv[1], 'localhost:27017', 'skunkqueue')
