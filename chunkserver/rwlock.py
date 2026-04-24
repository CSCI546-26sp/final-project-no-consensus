import threading
from contextlib import contextmanager

class RWLock:
    """
    Writer-preferring reader-writer lock. 
    A waiting writer blocks new readers from acquiring; active readers finish
    before the writer runs. Prevents indexer/delete starvation under steady search load.
    """
    def __init__(self):
        self._cond = threading.Condition(threading.Lock())
        self._readers = 0
        self._writerActive = False
        self._writersWaiting = 0

    @contextmanager
    def readLock(self):
        with self._cond:
            while self._writerActive or self._writersWaiting > 0:
                self._cond.wait()
            self._readers += 1
        try:
            yield
        finally:
            with self._cond:
                self._readers -= 1
                if self._readers == 0:
                    self._cond.notify_all()
    
    @contextmanager
    def writeLock(self):
        with self._cond:
            self._writersWaiting += 1
            try:
                while self._writerActive or self._readers > 0:
                    self._cond.wait()
                self._writerActive = True
            finally:
                self._writersWaiting -= 1
        try:
            yield
        finally:
            with self._cond:
                self._writerActive = False
                self._cond.notify_all()
