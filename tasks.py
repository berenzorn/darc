import lzma
from celery import Celery

app = Celery('tasks')
app.conf.task_serializer = 'msgpack'
app.conf.result_serializer = 'msgpack'
app.conf.accept_content = ['json', 'msgpack']
app.conf.broker_url = 'amqp://'
app.conf.result_backend = 'rpc://'

# Remote redis config
# app.conf.broker_url = 'redis://192.168.3.3:6379/0'
# app.conf.result_backend = 'redis://192.168.3.3:6379/0'

# Remote config
# app.conf.broker_url = 'amqp://admin:*@192.168.3.3:5672//'
# app.conf.result_backend = 'rpc://192.168.3.3//'


@app.task
def compress(data):
    return lzma.compress(data, preset=lzma.PRESET_EXTREME)

# How to run on windows
# celery -A tasks worker -P threads
