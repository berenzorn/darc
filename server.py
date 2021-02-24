import lzma
import os
import shutil
import sys
import tarfile
import threading
from collections import namedtuple
from math import ceil
from pathlib import Path
from sys import argv
from tasks import compress
from queue import Queue

Block = namedtuple('Block', ['index', 'data'])


class Darc:

    base = {}
    CHUNK_SIZE = 2560000
    q = Queue(maxsize=128)
    event = threading.Event()

    def __init__(self, source):
        self.source = source.split('.')[0]
        self.clean()
        self.is_src_dir, self.src_size = self.is_it_dir(source)
        self.dst_name = (f'{self.source}.tar.xz' if self.is_src_dir
                         else f'{self.source}.xz')
        self.dst_file = self.dst_file_open()

    def clean(self):
        try:
            for ext in ['tar', 'xz', 'tar.xz']:
                os.remove(f'{self.source}.{ext}')
        except FileNotFoundError:
            pass

    def is_it_dir(self, source):
        is_dir = False
        if os.path.isdir(source):
            paths = self.list_dir(source)
            with tarfile.open(f'{self.source}.tar', "w") as tar:
                for name in paths:
                    tar.add(name)
            is_dir = True
            src_size = Path(f'{self.source}.tar').stat().st_size
        else:
            src_size = Path(source).stat().st_size
        return is_dir, src_size

    def list_dir(self, folder, paths=[]):
        for item in os.listdir(folder):
            full_path = os.path.join(folder, item)
            if os.path.isfile(full_path):
                paths.append(full_path)
            elif os.path.isdir(full_path):
                self.list_dir(full_path)
        return paths

    def dst_file_open(self):
        file = open(self.dst_name, 'wb')
        return file

    def dst_file_close(self):
        self.dst_file.close()

    def worker(self):
        while True:
            chunk = self.q.get()
            data = lzma.compress(chunk.data)
            # data = compress.delay(chunk.data)
            self.base[chunk.id] = data
            # self.base[chunk.id] = data.get()
            self.q.task_done()

    def filer(self):
        chunks = ceil(self.src_size / self.CHUNK_SIZE)
        index = 0
        while index < chunks:
            if index in self.base.keys():
                self.dst_file.write(self.base.pop(index))
            index += 1
        else:
            self.event.wait(0.5)


if __name__ == '__main__':
    darc = Darc(argv[1])

    threads = 8
    for _ in range(threads):
        threading.Thread(target=darc.worker, daemon=True).start()
    filer_thread = threading.Thread(target=darc.filer, daemon=True)
    filer_thread.start()

    index = 0
    with open(argv[1], 'rb') as file:
        while True:
            chunk = Block(index, file.read(darc.CHUNK_SIZE))
            if not chunk.data:
                break
            darc.q.put(chunk)
            index += 1
    darc.q.join()
    filer_thread.join()
    darc.dst_file_close()

