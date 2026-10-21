from pathlib import Path


def strip_url_parameters(path: str) -> str:
    """Remove URL parameters while preserving punctuation in local paths."""
    path = str(path)
    if "://" not in path:
        return path
    return path.split("#", 1)[0].split("?", 1)[0]


def path_suffixes(path: str) -> tuple[str, ...]:
    """Return lowercase suffixes from the URL path component."""
    return tuple(suffix.lower() for suffix in Path(strip_url_parameters(path)).suffixes)


def path_format_suffix(path: str) -> str | None:
    """Return the effective format suffix, allowing one compression suffix."""
    suffixes = path_suffixes(path)
    if suffixes and suffixes[-1] in {".gz", ".bgz"} and len(suffixes) > 1:
        return suffixes[-2]
    return suffixes[-1] if suffixes else None
