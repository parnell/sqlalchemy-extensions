import atexit
import logging
import time
from collections import defaultdict
from dataclasses import dataclass


@dataclass
class Stat:
    num: int = 0
    total: float = 0

    def __str__(self):
        return f"{self.num}, {self.total:.2f}"


class Timer:
    """A class to time and log a metric.
    These metrics can be retrieved later as a Pandas Dataframe"""

    _all = defaultdict(Stat)
    _out_metrics = None
    _prefix = ""

    def __init__(self, output_str, out_metrics=None, count=None, verbose=True):
        self._output_str = output_str
        self._start = time.perf_counter()
        self._out_metrics = out_metrics
        self.count = count
        self.verbose = verbose

        logging.debug(f"Started: {Timer._prefix}{output_str}")

    def __enter__(self):
        return self

    def end(self) -> float:
        """
        Print out the time between this function and the start time
        Logs to metric if metrics exist
        """
        et = time.perf_counter() - self._start
        Timer._all[self._output_str].num += 1
        Timer._all[self._output_str].total += et
        output = [f"{Timer._prefix}{self._output_str}", et]
        if self._out_metrics:
            self._out_metrics.log_metric(*output)
        elif Timer._out_metrics:
            Timer._out_metrics.log_metric(*output)
        if self.count:
            npersec = 1 / (et / self.count)
            if self.verbose:
                logging.info(
                    f"Ended: {Timer._prefix}{self._output_str}, {et:.2f} (s), {npersec:.2f} (n/s)"
                )
        elif self.verbose:
            logging.info(f"Ended: {Timer._prefix}{self._output_str}, {et:.2f} (s)")
        return et

    def __exit__(self, type, value, traceback):
        self.end()


@atexit.register
def print_stats():
    if Timer._all:
        print("----- Printing stats ------")
        for k, v in Timer._all.items():
            print(k, v)
