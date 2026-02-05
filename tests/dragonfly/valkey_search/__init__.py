"""
Valkey-search integration tests for Dragonfly

This module automatically adapts original valkey-search tests to run on Dragonfly
by replacing valkeytestframework imports with Dragonfly equivalents.
"""

import sys
import types
import os
from . import util

# Check if integration directory exists before attempting import
_integration_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "integration")
INTEGRATION_AVAILABLE = os.path.isdir(_integration_dir)

if not INTEGRATION_AVAILABLE:
    pass  # pytest_ignore_collect in conftest.py will skip these tests
else:
    from .integration import compatibility

    # Add current directory to path for imports
    current_dir = os.path.dirname(os.path.abspath(__file__))
    if current_dir not in sys.path:
        sys.path.insert(0, current_dir)

    # Import the Dragonfly-specific test case classes
    with open(os.path.join(current_dir, "valkey_search_test_case_dragonfly.py")) as f:
        exec(f.read())

    # Create a mock module for valkey_search_test_case
    mock_module = types.ModuleType("valkey_search_test_case")
    mock_module.ValkeySearchTestCaseBase = ValkeySearchTestCaseBase
    mock_module.ValkeySearchTestCaseDebugMode = ValkeySearchTestCaseDebugMode
    mock_module.ValkeySearchClusterTestCase = ValkeySearchClusterTestCase
    mock_module.ValkeySearchClusterTestCaseDebugMode = ValkeySearchClusterTestCaseDebugMode
    mock_module.Node = Node
    mock_module.ReplicationGroup = ReplicationGroup

    # Replace the module in sys.modules
    sys.modules["valkey_search_test_case"] = mock_module

    # Also need to provide valkeytestframework modules
    valkey_test_framework = types.ModuleType("valkeytestframework")

    valkey_test_case = types.ModuleType("valkeytestframework.valkey_test_case")
    valkey_test_case.ValkeyTestCase = ValkeyTestCase
    valkey_test_case.ReplicationTestCase = ReplicationTestCase
    valkey_test_case.ValkeyServerHandle = ValkeyServerHandle

    util_module = types.ModuleType("valkeytestframework.util")
    waiters_module = types.ModuleType("valkeytestframework.util.waiters")

    waiters_module.wait_for_true = util.waiters.wait_for_true
    waiters_module.wait_for_equal = util.waiters.wait_for_equal
    waiters_module.wait_for_not_equal = util.waiters.wait_for_not_equal
    waiters_module.wait_for_condition = util.waiters.wait_for_condition
    util_module.waiters = waiters_module

    # Also add direct util module access
    sys.modules["util"] = util_module
    sys.modules["util.waiters"] = waiters_module

    conftest_module = types.ModuleType("valkeytestframework.conftest")
    conftest_module.resource_port_tracker = types.ModuleType("resource_port_tracker")

    # Setup compatibility as a module in sys.modules
    sys.modules["compatibility"] = compatibility

    # Also set up the submodules
    if hasattr(compatibility, "data_sets"):
        sys.modules["compatibility.data_sets"] = compatibility.data_sets

    # Add all modules to sys.modules
    sys.modules["valkeytestframework"] = valkey_test_framework
    sys.modules["valkeytestframework.valkey_test_case"] = valkey_test_case
    sys.modules["valkeytestframework.util"] = util_module
    sys.modules["valkeytestframework.util.waiters"] = waiters_module
    sys.modules["valkeytestframework.conftest"] = conftest_module
