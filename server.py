from tasks import compress

with open('test2.lzma', 'w') as f:
    f.truncate(0)

with open('test2.lzma', 'ab') as file2:
    with open('test', 'rb') as file:
        while True:
            chunk = file.read(2560000)
            if not chunk:
                break
            res = compress.delay(chunk)
            file2.write(res.get()[1])
            print('.', end='')
