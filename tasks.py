# from time import sleep
from save import save_videos, query_helper
from celery import Celery
import os
from dotenv import load_dotenv
import asyncio

load_dotenv()
# connection_link = os.environ["BROKER_URL"]
host = os.environ["UPSTASH_REDIS_HOST"]
password = os.environ["UPSTASH_REDIS_PASSWORD"]
port = os.environ["UPSTASH_REDIS_PORT"]
backend = "rediss://:{}@{}:{}/0?ssl_cert_reqs=required".format(password, host, port)
broker = "rediss://:{}@{}:{}?ssl_cert_reqs=required".format(password, host, port)


# Results will expire from database after 16 hours.
# You can remove `result_expires` option so that your results will be persisted and can be fetched whenever.
celery_app = Celery(
    "tasks",
    broker_url=broker,
    result_backend=backend,
    # 30minutes max
    visibility_timeout=60 * 30,
    polling_interval=30,
)
# broker_transport_options = {"global_keyprefix": "{queue}:"}
# celery_app.conf.update(broker_transport_options=broker_transport_options)


@celery_app.task
def process_videos(user_videos, user_id):
    return asyncio.run(save_videos(user_videos, user_id))


@celery_app.task
def query(query_text, user_id, k):
    return query_helper(query_text, user_id, k)
