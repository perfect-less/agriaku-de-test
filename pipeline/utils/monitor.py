from typing import Callable

import logging
import datetime
import time

logger = logging.getLogger(__name__)


def log_job_call(name: str):
    def log_job_call_decorator(func: Callable):
        """Decorator to log when the function being called, and when it finished
        running."""

        def wrapped_job_call(*args, **kwargs):
            start_time = time.time()
            logger.info(f"Begin running '{name}' job at {datetime.datetime.now()}")
            func(*args, **kwargs)
            run_time = time.time() - start_time
            logger.info(f"Finished running '{name}' job at {datetime.datetime.now()}")
            logger.info(f"'{name}' job took {run_time:.2f} s to finish.")

        return wrapped_job_call
    return log_job_call_decorator
