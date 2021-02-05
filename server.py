import threading
from sys import argv
from tasks import compress
from queue import Queue

# with open('test.lzma', 'w') as f:
#     f.truncate(0)

# with open('test2.lzma', 'ab') as file2:
#     with open('test', 'rb') as file:
#         while True:
#             chunk = file.read(25600000)
#             if not chunk:
#                 break
#             res = compress.delay(chunk)
#             file2.write(res.get(timeout=30))
#             print('.', end='')

q = Queue(maxsize=128)
file2 = open(f'{argv[1]}.lzma', 'ab')


def worker():
    while True:
        chunk = q.get()
        res = compress.delay(chunk)
        file2.write(res.get())
        q.task_done()


threads = 8
for _ in range(threads):
    threading.Thread(target=worker, daemon=True).start()

with open(argv[1], 'rb') as file:
    while True:
        chunk = file.read(2560000)
        if not chunk:
            break
        q.put(chunk)
q.join()
file2.close()

# with open('test2.lzma', 'ab') as file2:
#         res = compress.delay(chunk)
#         file2.write(res.get(timeout=30))
#         print('.', end='')

