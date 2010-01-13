"""Super simple hooks."""

class Hook(object):
    def __init__(self):
        self._hooks = set()

    def add(self, handler):
        self._hooks.add(handler)

    def remove(self, handler):
        self._hooks.remove(handler)

    def __call__(self, *args, **kwargs):
        for h in self._hooks:
            h(*args, **kwargs)
