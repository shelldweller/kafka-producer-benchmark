from time import time

def timeit(func):
    def _inner(*args, **kwargs):
        start_time = time()
        func(*args, **kwargs)
        return int( (time() - start_time) * 1000 )
    return _inner
