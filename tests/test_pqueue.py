"""Acceptance tests for the pqueue scheduler.

The task fixtures are regenerated inside these tests following the same
rules as demo/tasks.json, so the conclusions do not depend on any one
specific task list.
"""

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from pqueue.cli import main as cli_main
from pqueue.executor import Scheduler
from pqueue.task import Task

REPO_ROOT = Path(__file__).resolve().parent.parent
ALLOWED_DOMAINS = ["api.example.com"]
ALLOWED_PREFIXES = ["/api/", "/public/"]


def make_task_dicts(count=200, dup_count=20, sleep_ms=1):
    """Build a task list per the acceptance rules: count tasks across 4
    priority levels, dup_count duplicate keys, 2 out-of-scope tasks."""
    unique = count - dup_count
    normal = unique - 2
    tasks = []
    for i in range(normal):
        tasks.append({
            "id": f"task-{i:03d}",
            "key": f"key-{i:03d}",
            "priority": i % 4,
            "url": f"https://api.example.com/api/items/{i}",
            "allowed_domains": list(ALLOWED_DOMAINS),
            "allowed_path_prefixes": list(ALLOWED_PREFIXES),
            "payload": {"sleep_ms": sleep_ms},
        })
    tasks.append({
        "id": "task-evil-domain",
        "key": "key-evil-domain",
        "priority": 1,
        "url": "https://evil.example.com/api/items/1",
        "allowed_domains": list(ALLOWED_DOMAINS),
        "allowed_path_prefixes": list(ALLOWED_PREFIXES),
        "payload": {"sleep_ms": sleep_ms},
    })
    tasks.append({
        "id": "task-evil-path",
        "key": "key-evil-path",
        "priority": 2,
        "url": "https://api.example.com/admin/secret",
        "allowed_domains": list(ALLOWED_DOMAINS),
        "allowed_path_prefixes": list(ALLOWED_PREFIXES),
        "payload": {"sleep_ms": sleep_ms},
    })
    for j in range(dup_count):
        dup = dict(tasks[j])
        dup["id"] = f"task-dup-{j:02d}"
        tasks.append(dup)
    return tasks


def make_tasks(**kwargs):
    return [Task.from_dict(d) for d in make_task_dicts(**kwargs)]


def plain_tasks(count, sleep_ms=1):
    """Unique-key, in-scope tasks only."""
    return [
        Task(
            id=f"task-{i:03d}",
            key=f"key-{i:03d}",
            priority=i % 4,
            url=f"https://api.example.com/api/items/{i}",
            allowed_domains=list(ALLOWED_DOMAINS),
            allowed_path_prefixes=list(ALLOWED_PREFIXES),
            payload={"sleep_ms": sleep_ms},
        )
        for i in range(count)
    ]


class TestDedupAndExecution(unittest.TestCase):
    def test_unique_execution_and_merge_count(self):
        report = Scheduler(workers=4, queue_size=300).run(make_tasks())
        self.assertEqual(len(report["executed"]), 180)
        self.assertEqual(report["merged_count"], 20)
        keys = [entry["key"] for entry in report["executed"]]
        self.assertEqual(len(set(keys)), 180)
        self.assertEqual(len(keys), 180)

    def test_no_duplicate_key_runs_even_if_resubmitted_late(self):
        scheduler = Scheduler(workers=2, queue_size=300)
        scheduler.start()
        first = plain_tasks(1, sleep_ms=5)[0]
        self.assertEqual(scheduler.submit(first), "accepted")
        dup = Task.from_dict({
            "id": "task-dup",
            "key": first.key,
            "priority": 3,
            "url": first.url,
            "allowed_domains": list(ALLOWED_DOMAINS),
            "allowed_path_prefixes": list(ALLOWED_PREFIXES),
        })
        self.assertEqual(scheduler.submit(dup), "merged")
        scheduler.close()
        scheduler.join()
        report = scheduler.report()
        self.assertEqual(report["merged_count"], 1)
        self.assertEqual(len(report["executed"]), 1)


class TestScopeEnforcement(unittest.TestCase):
    def test_out_of_scope_requests_are_blocked(self):
        report = Scheduler(workers=4, queue_size=300).run(make_tasks())
        blocked = report["blocked_requests"]
        self.assertEqual(len(blocked), 2)
        for event in blocked:
            self.assertIn("task_id", event)
            self.assertIn("url", event)
            self.assertIn("reason", event)
        reasons = {event["task_id"]: event["reason"] for event in blocked}
        self.assertEqual(reasons["task-evil-domain"], "domain_not_allowed")
        self.assertEqual(reasons["task-evil-path"], "path_prefix_not_allowed")
        status_by_id = {e["id"]: e["status"] for e in report["executed"]}
        self.assertEqual(status_by_id["task-evil-domain"], "blocked")
        self.assertEqual(status_by_id["task-evil-path"], "blocked")


class TestPriority(unittest.TestCase):
    def test_higher_priority_dequeues_first(self):
        tasks = make_tasks()
        priority_by_id = {t.id: t.priority for t in tasks}
        report = Scheduler(workers=1, queue_size=300).run(tasks)
        first50 = report["executed"][:50]
        high = sum(1 for e in first50 if priority_by_id[e["id"]] == 3)
        low = sum(1 for e in first50 if priority_by_id[e["id"]] == 0)
        self.assertGreater(high, low)

    def test_same_priority_is_fifo(self):
        scheduler = Scheduler(workers=1, queue_size=10)
        tasks = [
            Task(id=f"t-{i}", key=f"k-{i}", priority=1,
                 url="https://api.example.com/api/x",
                 allowed_domains=list(ALLOWED_DOMAINS),
                 allowed_path_prefixes=list(ALLOWED_PREFIXES),
                 payload={"sleep_ms": 0})
            for i in range(5)
        ]
        report = scheduler.run(tasks)
        self.assertEqual([e["id"] for e in report["executed"]],
                         [t.id for t in tasks])


class TestConcurrencyLimit(unittest.TestCase):
    def test_peak_concurrency_never_exceeds_workers(self):
        report = Scheduler(workers=4, queue_size=100).run(
            plain_tasks(40, sleep_ms=10))
        self.assertLessEqual(report["peak_concurrency"], 4)
        self.assertGreaterEqual(report["peak_concurrency"], 2)


class TestBackpressure(unittest.TestCase):
    def test_reject_strategy_counts_rejections(self):
        scheduler = Scheduler(workers=1, queue_size=10, backpressure="reject")
        for task in plain_tasks(200, sleep_ms=0):
            scheduler.submit(task)
        scheduler.start()
        scheduler.close()
        scheduler.join()
        report = scheduler.report()
        self.assertGreater(report["rejected_count"], 0)
        self.assertEqual(report["accepted_count"], 10)

    def test_block_strategy_counts_backoff_events(self):
        scheduler = Scheduler(workers=2, queue_size=10, backpressure="block")
        scheduler.start()
        for task in plain_tasks(200, sleep_ms=5):
            self.assertEqual(scheduler.submit(task), "accepted")
        scheduler.close()
        scheduler.join()
        report = scheduler.report()
        self.assertGreater(report["backoff_events"], 0)
        self.assertEqual(len(report["executed"]), 200)


class TestShutdown(unittest.TestCase):
    def test_drain_finishes_queued_tasks(self):
        scheduler = Scheduler(workers=2, queue_size=100, shutdown_mode="drain")
        scheduler.start()
        tasks = plain_tasks(30, sleep_ms=2)
        for task in tasks:
            scheduler.submit(task)
        scheduler.close()
        scheduler.join()
        report = scheduler.report()
        self.assertEqual(len(report["executed"]), 30)
        self.assertEqual(report["dropped_count"], 0)

    def test_drop_discards_and_counts_queued_tasks(self):
        scheduler = Scheduler(workers=1, queue_size=100, shutdown_mode="drop")
        scheduler.start()
        tasks = plain_tasks(50, sleep_ms=20)
        for task in tasks:
            scheduler.submit(task)
        scheduler.close()
        scheduler.join()
        report = scheduler.report()
        executed = len(report["executed"])
        dropped = report["dropped_count"]
        self.assertGreater(dropped, 0)
        self.assertEqual(executed + dropped, 50)

    def test_no_new_tasks_after_close(self):
        scheduler = Scheduler(workers=1, queue_size=10)
        scheduler.start()
        scheduler.close()
        scheduler.join()
        self.assertEqual(scheduler.submit(plain_tasks(1)[0]), "closed")


class TestReportStructure(unittest.TestCase):
    REQUIRED_SECTIONS = (
        "executed", "merged_count", "rejected_count",
        "blocked_requests", "backoff_events", "peak_concurrency",
    )

    def test_report_sections_and_entry_fields(self):
        report = Scheduler(workers=4, queue_size=300).run(make_tasks())
        for section in self.REQUIRED_SECTIONS:
            self.assertIn(section, report)
        self.assertTrue(report["executed"])
        for entry in report["executed"]:
            self.assertIn("id", entry)
            self.assertIn("key", entry)
            self.assertIn("status", entry)
            self.assertIn("duration_ms", entry)
            self.assertIsInstance(entry["duration_ms"], float)
        json.dumps(report)


class TestCli(unittest.TestCase):
    def test_cli_end_to_end(self):
        with tempfile.TemporaryDirectory() as tmp:
            tasks_path = Path(tmp) / "tasks.json"
            out_path = Path(tmp) / "report.json"
            tasks_path.write_text(
                json.dumps(make_task_dicts()), encoding="utf-8")
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                rc = cli_main([
                    "--tasks", str(tasks_path),
                    "--workers", "4",
                    "--queue-size", "100",
                    "--out", str(out_path),
                ])
            self.assertEqual(rc, 0)
            summary = stdout.getvalue().strip()
            self.assertIn("accepted=180", summary)
            self.assertIn("executed=180", summary)
            self.assertIn("merged=20", summary)
            self.assertIn("blocked_requests=2", summary)
            report = json.loads(out_path.read_text(encoding="utf-8"))
            for section in TestReportStructure.REQUIRED_SECTIONS:
                self.assertIn(section, report)

    def test_demo_fixture(self):
        demo = REPO_ROOT / "demo" / "tasks.json"
        self.assertTrue(demo.exists())
        stdout = io.StringIO()
        with tempfile.TemporaryDirectory() as tmp:
            out_path = Path(tmp) / "report.json"
            with redirect_stdout(stdout):
                rc = cli_main([
                    "--tasks", str(demo),
                    "--workers", "4",
                    "--queue-size", "100",
                    "--out", str(out_path),
                ])
            self.assertEqual(rc, 0)
            report = json.loads(out_path.read_text(encoding="utf-8"))
        self.assertEqual(len(report["executed"]), 180)
        self.assertEqual(report["merged_count"], 20)
        self.assertEqual(len(report["blocked_requests"]), 2)
        self.assertLessEqual(report["peak_concurrency"], 4)
        summary = stdout.getvalue().strip()
        self.assertIn("accepted=180", summary)
        self.assertIn("executed=180", summary)
        self.assertIn("merged=20", summary)
        self.assertIn("rejected=0", summary)
        self.assertIn("blocked_requests=2", summary)


if __name__ == "__main__":
    unittest.main()
