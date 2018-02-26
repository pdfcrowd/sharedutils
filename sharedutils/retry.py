import datetime
import django_rq
from functools import wraps
from rq import get_current_job
import config

RQ_RETRY_DEFAULT_BACKOFF = 60*60

# raise this exception from a failed job to retry it later
class JobFailedTryAgainError(Exception):
    pass

# job decorator that retries an async job if it fails
def rq_retry(max_backoff=RQ_RETRY_DEFAULT_BACKOFF):  # 1 hour default backoff
    def wrapper(fn):
        @wraps(fn)
        def wrapper_fn(*args, **kwargs):
            job = get_current_job()
            if job: # async path
                job.meta.setdefault('retries', 0)
                job.meta.setdefault('max_backoff', max_backoff)
                job.save_meta()
            return fn(*args, **kwargs)
        return wrapper_fn
    return wrapper

# runs job_fn synchronously first; if it fails, the job is then run
# asynchronously; if job_fn is decorated with rq_retry, it is retried multiple
# time with the specified exponencial back-off limit
def retry_job_if_fails(job_fn, job_kwds):
    try:
        job_fn(**job_kwds)
        return True
    except JobFailedTryAgainError:
        job_fn.delay(**job_kwds)
    except Exception:
        config.logger.error('Job failed: %s', str(job_fn), exc_info=True)
        job_fn.delay(**job_kwds)

# rq exception handler that retries a failed job with exponential backoff
#
# total time in seconds is 2**(retries+1)
# #retries   time before give up
#    5      1   minute
#    6      2.1 minutes
#    7      4.3 minutes
#    8      8.5 minutes
#    9     17   minutes
#   10     34   minutes
#   11     68   minutes
#   12      2.2 hours (136 minutes)
#   13      4.5 hours
#   14      9   hours
#   15     18   hours
#   16     36   hours
#   17     72   hours (3days)
def rq_retry_handler(job, *exc_info):
    retries = job.meta.get('retries')
    if retries is None:
        return True
    delay = 1 + (2**retries)
    if delay > job.meta.get('max_backoff', RQ_RETRY_DEFAULT_BACKOFF):
        config.logger.error('Retrying %s failed', job.id, exc_info=exc_info)
        return True
    kwargs = dict(job.kwargs)
    kwargs['timeout'] = job.timeout
    kwargs['job_id'] = job.id
    kwargs['job_ttl'] = job.ttl
    kwargs['job_result_ttl'] = job.result_ttl
    config.logger.debug('meta %s', job.meta)
    config.logger.debug('job.kwargs %s', job.kwargs)
    config.logger.debug('scheduler.kwargs %s', kwargs)
    config.logger.warning("Scheduling %s in %d seconds, this is retry nr %d",
                          job.id, delay, retries+1)
    scheduler = django_rq.get_scheduler(job.origin)
    scheduler.enqueue_in(datetime.timedelta(seconds=delay),
                         job.func, *job.args, **kwargs)
    job.meta['retries'] = retries + 1
    job.meta['delay'] = delay
    job.save_meta()
    return False

def put_to_failed_queue(job, *exc_info):
    import traceback
    l = traceback.format_exception(*exc_info)
    fq = django_rq.queues.get_failed_queue()
    fq.quarantine(job, "".join(l))
    return False
