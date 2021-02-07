import lzma
from celery import Celery

app = Celery('tasks')
app.conf.task_serializer = 'msgpack'
app.conf.result_serializer = 'msgpack'
app.conf.accept_content = ['json', 'msgpack']
app.conf.broker_url = 'amqp://'
app.conf.result_backend = 'rpc://'
# app.conf.broker_url = 'redis://localhost:6379/0'
# app.conf.result_backend = 'redis://localhost:6379/0'


@app.task
def compress(data):
    return lzma.compress(data, preset=lzma.PRESET_EXTREME)

