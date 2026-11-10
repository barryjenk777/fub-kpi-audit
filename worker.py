"""Command Center scheduler worker.

Runs the 77 APScheduler cron jobs in a process of its own so the web app
(gunicorn) only serves pages. Deploy as a second Railway service from this
same repo with start command `python worker.py`, and set RUN_SCHEDULER=0
on the web service so the jobs are not run twice. The scheduler_locks table
already guards against double-firing while both are up during a cutover.

Give each process its own slice of the Follow Up Boss budget (200/min per
key): e.g. FUB_RPM=120 here and FUB_RPM=60 on the web service.
"""
import os
import signal
import sys
import time

os.environ["RUN_SCHEDULER"] = "0"   # app.py must not start its own on import
import app as _app  # noqa: E402  (import wires routes, DB schema, config)


def main():
    _app.warmup_cache()
    _app.start_scheduler()
    print("[WORKER] scheduler running with %d jobs"
          % len(_app._scheduler.get_jobs() if _app._scheduler else []))

    def _stop(signum, frame):
        print("[WORKER] shutting down (signal %s)" % signum)
        try:
            _app._scheduler.shutdown(wait=False)
        except Exception:
            pass
        sys.exit(0)

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
