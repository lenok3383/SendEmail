"""Utilities to use when creating decorators.

:Authors: duncan, ereuveni
:Status: $Id: //prod/main/_is/shared/python/util/decoratorutils.py#5 $
"""

import functools

def allow_no_args_form(decorator_factory):
    """Makes a decorator factory called without arguments act as a decorator.

    Applied to a decorator factory (having no required parameters) to allow the
    decorator factory to be called without arguments and act as a decorator.

    Example:
    @allow_no_args_form
    def my_decorator_factory(foo=1, bar=2):
        # etc.

    # These are equivalent:
    @my_decorator_factory
    def func(self, foo):
        pass

    @my_decorator_factory()
    def func(self, foo):
        pass
    """

    @functools.wraps(decorator_factory)
    def decorator_or_decorator_factory(*args, **kwargs):
        if not kwargs and len(args) == 1 and callable(args[0]):
            # Decorator!
            return decorator_factory()(args[0])
        else:
            # Decorator factory!
            return decorator_factory(*args, **kwargs)

    return decorator_or_decorator_factory

