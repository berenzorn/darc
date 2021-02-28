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

Block = namedtuple('Block', ['id', 'data'])


base = {}


class Darc:

    def __init__(self, source):
        self.CHUNK_SIZE = 2560000
        self.q = Queue(maxsize=128)
        self.event = threading.Event()
        self.source_body = source.split('.')[0]
        self.source_full = source
        self.clean()
        self.is_src_dir = self.src_type()
        self.src_size = self.create_tar()
        self.src_name = (f'{self.source_body}.tar' if self.is_src_dir
                         else f'{self.source_body}')
        self.dst_name = (f'{self.source_body}.tar.xz' if self.is_src_dir
                         else f'{self.source_body}.xz')
        self.dst_file = self.dst_file_open(self.dst_name)

    def clean(self):
        try:
            for ext in ['tar', 'xz', 'tar.xz']:
                os.remove(f'{self.source_body}.{ext}')
        except FileNotFoundError:
            pass

    def src_type(self):
        return os.path.isdir(self.source_full)

    def create_tar(self):
        if os.path.isdir(self.source_full):
            paths = self.list_dir(self.source_full)
            with tarfile.open(f'{self.source_body}.tar', "w") as tar:
                for name in paths:
                    tar.add(name)
            tar_size = Path(f'{self.source_body}.tar').stat().st_size
        else:
            tar_size = Path(self.source_full).stat().st_size
        return tar_size

    def list_dir(self, folder, paths=[]):
        for item in os.listdir(folder):
            full_path = os.path.join(folder, item)
            if os.path.isfile(full_path):
                paths.append(full_path)
            elif os.path.isdir(full_path):
                self.list_dir(full_path)
        return paths

    @staticmethod
    def dst_file_open(destination):
        file = open(destination, 'wb')
        return file

    def dst_file_close(self):
        self.dst_file.close()

    def worker(self):
        while True:
            chunk = self.q.get()
            data = lzma.compress(chunk.data)
            # data = compress.delay(chunk.data)
            base[chunk.id] = data
            print(chunk.id, base[chunk.id][:20])
            # self.base[chunk.id] = data.get()
            self.q.task_done()

    def filer(self):
        chunks = ceil(self.src_size / self.CHUNK_SIZE)
        index = 0
        while index < chunks:
            print(base.__len__())
            if index in base.keys():
                print('w' + str(index))
                self.dst_file.write(base.pop(index))
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
    with open(darc.src_name, 'rb') as file:
        while True:
            chunk = Block(index, file.read(darc.CHUNK_SIZE))
            if not chunk.data:
                break
            darc.q.put(chunk)
            index += 1
    darc.q.join()
    filer_thread.join()

    # print(darc.base)
    # print(darc.source_body)
    # print(darc.source_full)
    # print(darc.src_name)
    # print(darc.dst_name)
    # print(darc.dst_file)
    # print(darc.src_size)

    darc.dst_file_close()

