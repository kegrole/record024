"""JSON report assembly."""


def build_report(*, executed, merged_count, rejected_count, blocked_requests,
                 backoff_events, peak_concurrency, accepted_count=0,
                 dropped_count=0):
    return {
        "executed": executed,
        "merged_count": merged_count,
        "rejected_count": rejected_count,
        "blocked_requests": blocked_requests,
        "backoff_events": backoff_events,
        "peak_concurrency": peak_concurrency,
        "accepted_count": accepted_count,
        "dropped_count": dropped_count,
    }
