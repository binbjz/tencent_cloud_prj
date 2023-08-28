import time
from queue import Queue
from typing import Optional, Callable, ParamSpec, TypeVar

P = ParamSpec("P")
T = TypeVar("T")


class PollingException(Exception):
    """Base exception that stores all return values of attempted polls"""

    def __init__(self, values, last=None):
        self.values = values
        self.last = last


class TimeoutException(PollingException):
    """Exception raised if polling function times out"""


class MaxCallException(PollingException):
    """Exception raised if maximum number of iterations is exceeded"""


def step_constant(step: int) -> int:
    """
    Use this function when you want the step to remain fixed in every iteration (typically
    good for instances when you know approximately how long the function should poll for)
    """
    return step


def step_linear_double(step: int) -> int:
    """
    Use this function when you want the step to double each iteration (e.g. like the way ArrayList
    works in Java). Note that this can result in very long poll times after a few iterations
    """
    return step * 2


def is_truthy(val: T) -> bool:
    """Use this function to test if a return value is truthy"""
    return bool(val)


def poll(target: Callable[P, T], step: int, args: tuple = (), kwargs: Optional[dict] = None,
         timeout: Optional[int] = None, max_tries: Optional[int] = None,
         check_success: Callable[[T], bool] = is_truthy,
         step_function: Callable[[int], int] = step_constant,
         ignore_exceptions: tuple = (), poll_forever: bool = False,
         collect_values: Optional[Queue] = None) -> T:
    """
    Poll by calling a target function until a certain condition is met.
    You must specify at least a target function to be called and the step.
    -- base wait time between each function call.
    :param target: target function
    :param step: Step defines the amount of time to wait (in seconds)
    :param args: Arguments to be passed to the target function
    :type kwargs: dict
    :param kwargs: Keyword arguments to be passed to the target function
    :param timeout: The target function will be called until the time elapsed is greater than the
    maximum timeout (in seconds). NOTE that the actual execution time of the function *can* exceed
    the time specified in the timeout. For instance, if the target function takes 10 seconds to execute
     and the timeout is 21 seconds, the polling function will take a total of 30 seconds (two iterations
     of the target --20s which is less than the timeout--21s, and a final iteration)
    :param max_tries: Maximum number of times the target function will be called before failing
    :param check_success: A callback function that accepts the return value of the target function. It
    should return true if you want the polling function to stop and return this value. It should return
    false if you want it to continue executing. The default is a callback that tests for truthiness
    (anything not False, 0, or empty collection).
    :param step_function: A callback function that accepts each iteration's "step." By default, this is
    constant, but you can also pass a function that will increase or decrease the step. As an example,
    you can increase the wait time between calling the target function by 10 seconds every iteration
    until the step is 120 seconds--at which point it should remain constant at 120 seconds
    >>> def custom_step_function(_step: int) -> int:
    >>>     _step += 10
    >>>     return max(_step, 120)
    :type ignore_exceptions: tuple
    :param ignore_exceptions: You can specify a tuple of exceptions that should be caught and ignored on
    every iteration. If the target function raises one of these exceptions, it will be caught and the
    exception instance will be pushed to the queue of values collected during polling. Any other exceptions
     raised will be raised as normal.
    :param poll_forever: If set to true, this function will retry until an exception is raised or the
    target's return value satisfies the check_success function. If this is not set, then a timeout or
    a max_tries must be set.
    :type collect_values: Queue
    :param collect_values: By default, polling will create a new Queue to store all of the target's
    return values. Optionally, you can specify your own queue to collect these values for access to
    it outside of function scope.
    :return: Polling will return first value from the target function that meets the condions of the
     check_success callback. By default, this will be the first value that is not None, 0, False, '',
     or an empty collection.
    """

    assert (timeout is not None or max_tries is not None) or poll_forever, \
        ("You did not specify a maximum number of tries or a timeout. Without either of "
         "these set, the polling function will poll forever. If this is the behavior you want, "
         "pass 'poll_forever=True'")

    assert not ((timeout is not None or max_tries is not None) and poll_forever), \
        "You cannot specify both the option to poll_forever and max_tries/timeout."

    kwargs = kwargs or dict()
    values = collect_values or Queue()

    max_time = time.time() + timeout if timeout else None
    tries = 0

    last_item = None
    while True:
        if max_tries is not None and tries >= max_tries:
            raise MaxCallException(values, last_item)

        try:
            val = target(*args, **kwargs)
            last_item = val
        except ignore_exceptions as e:
            last_item = e
        else:
            # Condition passes, this is the only "successful" exit from the polling function
            if check_success(val):
                return val
        values.put(last_item)
        tries += 1

        # Check the time after to make sure the poll function is called at least once
        if max_time is not None and time.time() >= max_time:
            raise TimeoutException(values, last_item)

        time.sleep(step)
        step = step_function(step)
