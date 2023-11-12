# gradient descent optimization with nadam for a two-dimensional test function
import logging
from math import sqrt
from numpy import asarray
from numpy.random import rand
from numpy.random import seed
from typing import Callable, Any
import functools
import colorama
from colorama import Style, Back, Fore

# python decorator to log the function call and return value
parentlogger = logging.getLogger("parent")

# Set parent's level to INFO and assign a new handler
handler = logging.StreamHandler()
handler.setFormatter(
    logging.Formatter("%(asctime)s:%(name)s:%(levelname)s:%(message)s")
)
parentlogger.setLevel(logging.INFO)
parentlogger.addHandler(handler)


def loggingdecorator(name):
    logger = logging.getLogger(name)

    def _deco(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        def _warraper(*args: Any, **kwargs: Any) -> Any:
            ret = func(*args, **kwargs)
            argstr = [str(x) for x in args]
            argstr += [key + "=" + str(val) for key, val in kwargs.items()]
            logger.debug("%s(%s) -> %s", func.__name__, ",".join(argstr), ret)
            return ret

        return _warraper

    return _deco
