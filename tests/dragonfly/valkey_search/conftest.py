"""
Pytest configuration for valkey-search tests on Dragonfly
"""

from .. import dfly_args


# Apply dfly_args to all test classes in this directory
def pytest_collection_modifyitems(items):
    """Apply dfly_args decorator to all test classes"""
    for item in items:
        if item.cls and not hasattr(item.cls, "_dfly_args_applied"):
            # Apply the decorator to the class
            decorated_class = dfly_args({"proactor_threads": 4})(item.cls)
            item.cls._dfly_args_applied = True
