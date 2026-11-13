"""Access-scope policy enforcement."""

from urllib.parse import urlparse

DOMAIN_NOT_ALLOWED = "domain_not_allowed"
PATH_PREFIX_NOT_ALLOWED = "path_prefix_not_allowed"


def check_scope(task) -> str:
    """Return None if the task's URL is inside its authorized scope,
    otherwise a machine-readable reason string."""
    parsed = urlparse(task.url)
    host = parsed.hostname or ""
    if host not in task.allowed_domains:
        return DOMAIN_NOT_ALLOWED
    path = parsed.path or "/"
    if not any(path.startswith(prefix) for prefix in task.allowed_path_prefixes):
        return PATH_PREFIX_NOT_ALLOWED
    return None
