import os
import lzma
import tarfile
import argparse
import threading
import time
from sys import argv
from math import ceil
from queue import Queue
from pathlib import Path
from tasks import compress


class Block:

    def __init__(self, index, data):
        self.id = index
        self.data = data


class Darc:

    def __init__(self, source, qsize):
        self.base = {}
        self.CHUNK_SIZE = 1280000
        # self.CHUNK_SIZE = 10240000
        self.q = Queue(maxsize=qsize)
        self.event = threading.Event()
        self.source_body = source.split('.')[0]
        self.source_full = source
        self.clean()
        self.is_src_dir = self.src_type()
        self.src_size = self.create_tar()
        self.src_name = (f'{self.source_full}.tar' if self.is_src_dir else f'{self.source_full}')
        self.dst_name = (f'{self.source_full}.tar.xz' if self.is_src_dir else f'{self.source_full}.xz')
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

    def list_dir(self, folder, paths=None):
        if paths is None:
            paths = []
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
            # data = lzma.compress(chunk.data)
            # self.base[chunk.id] = data
            data = compress.delay(chunk.data)
            self.base[chunk.id] = data.get()
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


def get_q_size(threads):
    return {
        0 <= threads <= 2: 32,
        3 <= threads <= 6: 16,
        7 <= threads <= 8: 8,
        9 <= threads <= 10: 4
    }[True]


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("source")
    # group = parser.add_mutually_exclusive_group()
    # group.add_argument("-a", "--arch", action="store_true", help="Add source to archive")
    # group.add_argument("-u", "--unarch", action="store_true", help="Unpack archive")
    parser.add_argument("-t", type=int, metavar=" 0-10", help="Cores in cluster (2^n). Default 2^4", default=4)
    # parser.add_argument("-q", "--quiet", action="store_true", help="Quiet mode, no output")
    # parser.add_argument("-l", type=str, dest="log", metavar=" LOG", help="Write output to log file")
    args = parser.parse_args()

    # threads = 1, 2, 4... 1024
    threads = 2 ** args.t if 0 <= args.t <= 10 else 2 ** 4

    # queue size is q_size times longer
    try:
        darc = Darc(args.source, (get_q_size(args.t) * threads))
    except FileNotFoundError:
        print("Wrong source")
        raise SystemExit(0)

    tm1 = time.time()

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

    darc.dst_file_close()

    print("Time:", time.time() - tm1)
