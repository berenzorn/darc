import sys
import threading
from math import ceil
from pathlib import Path
from sys import argv
from tasks import compress
from queue import Queue

try:
    with open(f'{argv[1]}.lzma', 'w') as f:
        f.truncate(0)
except FileNotFoundError:
    pass

# with open('test2.lzma', 'ab') as file2:
#     with open('test', 'rb') as file:
#         while True:
#             chunk = file.read(25600000)
#             if not chunk:
#                 break
#             res = compress.delay(chunk)
#             file2.write(res.get(timeout=30))
#             print('.', end='')


class Block:

    def __init__(self, index, data):
        self.id = index
        self.data = data


base = {}
CHUNK_SIZE = 2560000
q = Queue(maxsize=128)
event = threading.Event()
try:
    file_size = Path(argv[1]).stat().st_size
except FileNotFoundError:
    print('Wrong source file')
    sys.exit(0)
file2 = open(f'{argv[1]}.lzma', 'ab')
file3 = open(f'{argv[1]}.index', 'w')


def worker():
    while True:
        chunk = q.get()
        data = compress.delay(chunk.data)
        base[chunk.id] = data.get()
        # file2.write(res.get())
        # file3.write(str(chunk.id) + " ")
        q.task_done()


def filer():
    chunks = ceil(file_size / CHUNK_SIZE)
    index = 0
    while index < chunks:
        if index in base.keys():
            file2.write(base.pop(index))
            file3.write(str(index) + ' ')
            base[index] = None
            index += 1
        else:
            event.wait(0.5)


threads = 8
for _ in range(threads):
    threading.Thread(target=worker, daemon=True).start()
filer_thread = threading.Thread(target=filer, daemon=True)
filer_thread.start()

index = 0
with open(argv[1], 'rb') as file:
    while True:
        chunk = Block(index, file.read(CHUNK_SIZE))
        if not chunk.data:
            break
        q.put(chunk)
        index += 1
q.join()
filer_thread.join()
file2.close()
file3.close()

# with open('test2.lzma', 'ab') as file2:
#         res = compress.delay(chunk)
#         file2.write(res.get(timeout=30))
#         print('.', end='')

