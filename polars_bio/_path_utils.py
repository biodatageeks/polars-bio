from pathlib import Path


def strip_url_parameters(path: str) -> str:
    """Return a path without URL query parameters or a fragment."""
    return str(path).split("#", 1)[0].split("?", 1)[0]


def path_suffixes(path: str) -> tuple[str, ...]:
    """Return lowercase suffixes from the URL path component."""
    return tuple(suffix.lower() for suffix in Path(strip_url_parameters(path)).suffixes)
