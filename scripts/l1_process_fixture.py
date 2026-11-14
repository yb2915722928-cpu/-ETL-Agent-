"""Private process-fault wrapper, mounted only by the L1 acceptance harness.

Production Celery/publisher have no fault environment variables or fault API.
"""
import os
from pathlib import Path
import sys
import time

if sys.argv[1] == 'publisher-after-publish':
    from app.outbox_publisher import publish_once

    def crash(command_id):
        if command_id == os.environ['FAULT_COMMAND_ID']:
            os._exit(74)
    publish_once(after_publish=crash)
elif sys.argv[1] == 'worker-before-ack':
    from celery.signals import task_postrun
    from app.celery_app import celery

    @task_postrun.connect
    def barrier(task_id=None, args=None, **kwargs):
        if args and args[0] == os.environ['FAULT_COMMAND_ID']:
            marker = Path('/faults/before-ack')
            marker.write_text(task_id, encoding='utf-8')
            while not Path('/faults/release-ack').exists():
                time.sleep(.1)
    celery.worker_main(['worker', '--loglevel=INFO', '--concurrency=2', '--without-gossip', '--without-mingle'])
else:
    raise SystemExit('unknown private L1 wrapper')
