import pytest


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Mark tests by directory so ``-m "not e2e"`` etc. work."""
    for item in items:
        for marker in ("integration", "e2e"):
            if marker in item.path.parts:
                item.add_marker(getattr(pytest.mark, marker))
