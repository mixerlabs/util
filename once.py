"""run things only once."""

import functools

# TODO: make it threadsafe; memoization based on arguments?

def fun(wrapped):
    @functools.wraps(wrapped)
    def wrapper(*args, **kwargs):
        if not hasattr(wrapper, '_once_res'):
            wrapper._once_res = wrapped(*args, **kwargs)            

        return wrapper._once_res

    return wrapper
