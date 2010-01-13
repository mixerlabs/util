from django.db import models

def get_or_none(objects, *args, **kwargs):
    """ Return the specified django model instance from a queryset,
    or None if it does not exist. """
    try:
        return objects.get(*args, **kwargs)
    except objects.model.DoesNotExist:
        return None


def get_or_default(objects, *args, **kwargs):
    """ Return the specified django model instance from a queryset,
    or a new unsaved instance if it does not exist. """
    try:
        return objects.get(*args, **kwargs)
    except objects.model.DoesNotExist:
        return objects.model(*args, **kwargs)
