"""Simple Pythonic(?) actor interface.

An instance of an actor-handling class runs in one thread only, and
each calling thread has a mailbox from which it can retrieve messages
sent back, by type. Types are function names.

class MyActor(object):
    def expensive_computation(self, a0, b0=1):
        ...

    def check_status(self, which):
        return self.status[which]

actor = Actor(MyActor)
actor.spawn(1)
actor['expensive_computation'].send(982, b0='hello')
actor['check_status'].send('hello')
print actor['check_status'].recv()
"""

import sys

from functools import partial
from threading import Thread, local
from datetime import datetime, timedelta
from itertools import takewhile
import inspect
import Queue

class MailBoxLocal(local):
    mailbox = {}

    def mailbox_for(self, meth):
        return self.mailbox.setdefault(meth, Queue.Queue())


class Actor(object):
    actors = []
    # TODO: Remove self from the actors list on __del__ if there
    # are no threads running.  Maybe stop the threads?  Make the list a dict?
    class Periodic(object):
        def __init__(self, meth, timeout):
            self.meth = meth
            self.timeout = timeout

    def __init__(self, cls):
        self._local = MailBoxLocal()
        self._queue = Queue.Queue()
        self._cls = cls
        self.active_threads = []
        Actor.actors.append(self)

        meths = [m[1] for m in inspect.getmembers(cls, predicate=inspect.ismethod)]
        periodic = filter(lambda meth: hasattr(meth, '_actor_call_every'), meths)
        now = datetime.now()
        self.periodic = [Actor.Periodic(m, now) for m in periodic]

    def __getitem__(self, key):
        meth = getattr(self._cls, key)
        class Bound(object):
            @staticmethod
            def send(*args, **kwargs):
                self.send(meth, *args, **kwargs)
                return Bound

            @staticmethod
            def recv(**kwargs):
                return self.recv(meth, **kwargs)

        return Bound

    def _loop(self, *args, **kwargs):
        # A stupid Python half-trick to pring Queue.Empty into our own
        # namespace so it doesn't dissapear on interpreter shutdown?
        # Maybe?
        _queue_empty = Queue.Empty
        instance = self._cls(*args, **kwargs)

        while True:
            if self.periodic:
                now = datetime.now() + timedelta(seconds=1)  # granularity

                for p in takewhile(lambda p: p.timeout < now, self.periodic):
                    p.meth(instance)
                    p.timeout = now + p.meth._actor_call_every

                # re-sort and compute timeout.
                self.periodic.sort(key=lambda p: p.timeout)

                timeout = (self.periodic[0].timeout - now).seconds
            else:
                timeout = None

            before = datetime.now()
            try:
                item = self._queue.get(timeout=timeout)
                if item is None:
                    self._queue.task_done()
                    return  # Exit thread when None added to queue
                mailbox, meth, args, kwargs = item
                ret = meth(instance, *args, **kwargs)
                if ret is not None:
                    mailbox.put(ret)
                self._queue.task_done()
            except SystemExit:
                return
            except _queue_empty:
                pass                    # timeout for periodic tasks.

    def spawn(self, num_threads, *args, **kwargs):
        for i in xrange(num_threads):
            t = Thread(target=self._loop, args=args, kwargs=kwargs)
            t.setDaemon(True)
            t.start()
            self.active_threads.append(t)

    def join(self):
        self._queue.join()

    def send(self, meth, *args, **kwargs):
        mailbox = self._local.mailbox_for(meth)
        self._queue.put((mailbox, meth, args, kwargs))

    def recv(self, meth, block=True, timeout=None):
        return self._local.mailbox_for(meth).get(block=block, timeout=timeout)
        
    def kill(self):
        # queue 1 kill message for each thread, then wait for each to die.
        for _ in self.active_threads:
            self._queue.put(None)
        while self.active_threads:
            self.active_threads.pop().join()
            
    @staticmethod
    def kill_em_all():
        while Actor.actors:
            Actor.actors.pop().kill()
            
            


def call_every(interval):
    def wrapper(fun):
        fun._actor_call_every = interval
        return fun

    return wrapper
