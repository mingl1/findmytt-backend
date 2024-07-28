#!/bin/bash
# cd ./flask-app
# Start the first process
source ./venv/venv/bin/activate
celery -A tasks worker -l info --concurrency 2 --detach

# Start the second process
# python -m flask --app app.py run -h 0.0.0.0 -p 5000 
#
gunicorn -b 0.0.0.0:5000 app:flask_app --daemon

sudo systemctl start nginx
