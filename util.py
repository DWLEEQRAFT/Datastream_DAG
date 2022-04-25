import warnings
import functools
import threading
import inspect
import time
import base64
import pandas as pd
from pytz import timezone


def get_function_kwargs():
    frame = inspect.currentframe()
    frame = inspect.getouterframes(frame)[1].frame
    kwargs = inspect.getargvalues(frame).locals
    kwargs.pop('self')
    return kwargs


def pass_args(target):
    @functools.wraps(target)
    def wrapper(*args, **kwargs):
        return target(*args, **kwargs)
    return wrapper


class Spinner:

    busy = False

    @staticmethod
    def spinning_cursor(spinner):
        if spinner == 'slash':
            strings = '|/-\\'
        elif spinner == 'dot':
            strings = "".join(["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"])
        while 1:
            for cursor in strings: yield cursor

    def __init__(self, text, delay=0.3, spinner='dot', enable_spinner=True):
        self.spinner_generator = self.spinning_cursor(spinner)
        self.text = text
        self.init_time = time.time()
        self.enable = enable_spinner
        if delay and float(delay): self.delay = delay

    def spinner_task(self):
        while self.busy:
            print('\r' + next(self.spinner_generator) + ' ' + self.text, end='')
            time.sleep(self.delay)

    def __enter__(self):
        self.busy = self.enable
        if self.busy:
            threading.Thread(target=self.spinner_task).start()

    def __exit__(self, exception, value, tb):
        if self.busy:
            self.busy = False
            print('\r' + '✓ {} done in {} secs\n'.format(self.text, round(time.time() - self.init_time, 3)), end='')
            time.sleep(self.delay)
            if exception is not None:
                return False


def import_formatted_warnings():
    def _format(message, category, filename, lineno, line=None):
        return f'{message}\n'

    warnings.formatwarning = _format
    return warnings


def get_current_newyork_time_without_tz_attribute(strtime=False):
    """
    tz attribute가 붙어 있으면 계산 이 복잡해 지므로 떼버림
    :return:
    """
    newyork_time = pd.Timestamp.now(NYTZ).strftime("%Y-%m-%d %H:%M:%S")
    if not strtime:
        newyork_time = pd.Timestamp(newyork_time)
    return newyork_time


def change_values(df, change_to, shifting=1):
    assert change_to in ('none', 'delta', 'rate')

    if change_to == 'delta':
        return df - df.shift(shifting)
    elif change_to == 'rate':
        return df / df.shift(shifting) - 1.
    return df



def daily_to_monthly(df):
    return df.resample('1M').last()


NYTZ = timezone('US/Eastern')
