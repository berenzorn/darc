import lzma
import os
import shutil
import sys
import tarfile
import threading
from math import ceil
from pathlib import Path
from sys import argv
from tasks import compress
from queue import Queue


# with open('test2.lzma', 'ab') as file2:
#     with open('test', 'rb') as file:
#         while True:
#             chunk = file.read(25600000)
#             if not chunk:
#                 break
#             res = compress.delay(chunk)
#             file2.write(res.get(timeout=30))
#             print('.', end='')


def list_dir(dir, paths=[]):
    for item in os.listdir(dir):
        full_path = os.path.join(dir, item)
        if os.path.isfile(full_path):
            print('- file ' + full_path)
            paths.append(full_path)
            # os.remove(full_path)
        elif os.path.isdir(full_path):
            # print('- folder ' + full_path)
            list_dir(full_path)
            # shutil.rmtree(full_path)
    return paths


class Block:

    def __init__(self, index, data):
        self.id = index
        self.data = data


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
            # base[index] = None
            index += 1
        else:
            event.wait(0.5)

# with open('test2.lzma', 'ab') as file2:
#         res = compress.delay(chunk)
#         file2.write(res.get(timeout=30))
#         print('.', end='')


if __name__ == '__main__':

    if os.path.isdir(argv[1]):
        paths = list_dir(argv[1])
        print(paths)
        with tarfile.open(argv[1] + '.tar', "w") as tar:
            for name in paths:
                tar.add(name)
        with open(argv[1] + '.tar.xz', 'wb') as xz:
            with open(argv[1] + '.tar', 'rb') as tar:
                xz.write(lzma.compress(tar.read()))
    else:
        with open(argv[1] + '.xz', 'wb') as xz:
            with open(argv[1], 'rb') as tar:
                xz.write(lzma.compress(tar.read()))

    # try:
    #     with open(f'{argv[1]}.lzma', 'w') as f:
    #         f.truncate(0)
    # except FileNotFoundError:
    #     pass
    #
    # base = {}
    # CHUNK_SIZE = 2560000
    # q = Queue(maxsize=128)
    # event = threading.Event()
    # try:
    #     file_size = Path(argv[1]).stat().st_size
    # except FileNotFoundError:
    #     print('Wrong source file')
    #     sys.exit(0)
    # file2 = open(f'{argv[1]}.lzma', 'ab')
    # file3 = open(f'{argv[1]}.index', 'w')
    #
    # threads = 8
    # for _ in range(threads):
    #     threading.Thread(target=worker, daemon=True).start()
    # filer_thread = threading.Thread(target=filer, daemon=True)
    # filer_thread.start()
    #
    # index = 0
    # with open(argv[1], 'rb') as file:
    #     while True:
    #         chunk = Block(index, file.read(CHUNK_SIZE))
    #         if not chunk.data:
    #             break
    #         q.put(chunk)
    #         index += 1
    # q.join()
    # filer_thread.join()
    # file2.close()
    # file3.close()


