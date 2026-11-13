"""Command-line entry point."""

import argparse
import json
import sys

from .backpressure import STRATEGIES
from .executor import DRAIN, DROP, Scheduler
from .task import Task

SUMMARY_LINE = (
    "accepted={accepted} executed={executed} merged={merged} "
    "rejected={rejected} blocked_requests={blocked}"
)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog="pqueue",
        description="Priority-aware, deduplicating, scope-enforcing scheduler.",
    )
    parser.add_argument("--tasks", required=True, help="path to task list JSON")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--queue-size", type=int, default=100)
    parser.add_argument("--out", help="path to write the JSON report")
    parser.add_argument("--backpressure", choices=STRATEGIES, default="block")
    parser.add_argument("--shutdown", choices=(DRAIN, DROP), default=DRAIN)
    args = parser.parse_args(argv)

    with open(args.tasks, encoding="utf-8") as fh:
        raw = json.load(fh)
    tasks = [Task.from_dict(item) for item in raw]

    scheduler = Scheduler(
        workers=args.workers,
        queue_size=args.queue_size,
        backpressure=args.backpressure,
        shutdown_mode=args.shutdown,
    )
    report = scheduler.run(tasks)

    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2, ensure_ascii=False)

    print(SUMMARY_LINE.format(
        accepted=report["accepted_count"],
        executed=len(report["executed"]),
        merged=report["merged_count"],
        rejected=report["rejected_count"],
        blocked=len(report["blocked_requests"]),
    ))
    return 0


if __name__ == "__main__":
    sys.exit(main())
