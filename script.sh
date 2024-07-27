#!/bin/bash
# cd ./flask-app
# Start the first process
celery -A tasks worker --pool=solo -l info --without-heartbeat --concurrency 1 &

# Start the second process
python -m flask --app app.py run -h 0.0.0.0 -p 5000 
