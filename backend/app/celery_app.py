"""Real Redis transport; tasks contain only JSON command IDs, never credentials."""
import os
from celery import Celery
from celery.signals import worker_process_init
from sqlalchemy.exc import DBAPIError, OperationalError
from .db import engine

if os.environ.get('EXECUTION_ADAPTER', 'disabled') != 'l1':
    raise RuntimeError('异步执行服务仅允许隔离 L1 显式启用；普通部署保留 pending Outbox')

visibility = int(os.environ.get('CELERY_VISIBILITY_TIMEOUT', '3600'))
celery = Celery('etl_commands', broker=os.environ['REDIS_URL'])
celery.conf.update(task_serializer='json', accept_content=['json'], result_serializer='json',
    task_ignore_result=True, task_acks_late=True, task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1, broker_transport_options={'visibility_timeout': visibility},
    visibility_timeout=visibility, broker_connection_retry_on_startup=True,
    task_default_queue='managed_commands')


@worker_process_init.connect
def dispose_inherited_pool(**kwargs):
    engine.dispose(close=False)


@celery.task(name='etl.drive', bind=True, max_retries=None)
def execute(self, command_id):
    from .execution_runtime import drive
    try:
        state = drive(command_id)
    except DBAPIError as error:
        code = error.orig.sqlstate
        # psycopg transport/connect failures have no server SQLSTATE. Keep
        # constraint, data and programming errors out of the retry path.
        if (error.connection_invalidated
                or (isinstance(error, OperationalError) and code is None)
                or (code and code.startswith('08'))
                or code in ('40001', '40P01', '55P03', '57P01', '57P02', '57P03')):
            raise self.retry(exc=error, countdown=10)
        raise
    if state in ('submitted', 'stop_requested', 'stop_delivery_unknown', 'termination_pending', 'final_metrics_pending', 'cleanup_unknown'):
        raise self.retry(countdown=2)
    if state in ('submission_unknown', 'busy'):
        raise self.retry(countdown=10)
    return state
