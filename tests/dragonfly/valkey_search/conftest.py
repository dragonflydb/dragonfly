"""
Pytest configuration for valkey-search tests on Dragonfly
"""

import pytest
from .. import dfly_args


# List of tests to skip - add test node IDs here
# Example format: "integration/test_file.py::TestClass::test_method"
SKIP_TESTS = [
    "integration/compatibility_test.py::TestAnswersCMD::test_answers",
    "integration/test_cancel.py::TestCancelCMD::test_timeoutCMD",
    "integration/test_cancel.py::TestCancelCME::test_timeoutCME",
    "integration/test_eviction.py::TestEviction::test_eviction_with_search_index",
    "integration/test_fanout_base.py::TestFanoutBase::test_fanout_retry",
    "integration/test_fanout_base.py::TestFanoutBase::test_fanout_shutdown",
    "integration/test_fanout_base.py::TestFanoutBase::test_fanout_timeout",
    "integration/test_flushall.py::TestFlushAllCME::test_flushallCME",
    "integration/test_ft_create_consistency.py::TestFTCreateConsistency::test_create_force_index_name_error_retry",
    "integration/test_ft_create_consistency.py::TestFTCreateConsistency::test_duplicate_creation",
    "integration/test_ft_create_consistency.py::TestFTCreateConsistency::test_concurrent_creation",
    "integration/test_ft_create_consistency.py::TestFTCreateConsistency::test_create_timeout",
    "integration/test_ft_dropindex_consistency.py::TestFTDropindexConsistency::test_dropindex_synchronize_handle_message_first",
    "integration/test_ft_dropindex_consistency.py::TestFTDropindexConsistency::test_dropindex_synchronize_consistency_check_first",
    "integration/test_info.py::TestVSSBasic::test_info_fields_present",
    "integration/test_info_cluster.py::TestFTInfoCluster::test_ft_info_cluster_success",
    "integration/test_info_cluster.py::TestFTInfoCluster::test_ft_info_cluster_force_index_name_error_retry",
    "integration/test_info_cluster.py::TestFTInfoCluster::test_ft_info_cluster_retry",
    "integration/test_info_primary.py::TestFTInfoPrimary::test_ft_info_primary_success",
    "integration/test_info_primary.py::TestFTInfoPrimary::test_ft_info_primary_force_index_name_error_retry",
    "integration/test_info_primary.py::TestFTInfoPrimary::test_ft_info_primary_retry",
    "integration/test_oom_handling.py::TestSearchOOMHandlingCME::test_search_oom_cme",
    "integration/test_oom_handling.py::TestSearchOOMHandlingCMD::test_search_oom_cmd",
    "integration/test_query_parser.py::TestQueryParser::test_query_string_depth_limit",
    "integration/test_query_parser.py::TestQueryParser::test_query_string_terms_count_limit",
    "integration/test_reclaimable_memory.py::TestReclaimableMemory::test_reclaimable_memory_with_vector_operations",
    "integration/test_reclaimable_memory.py::TestReclaimableMemory::test_reclaimable_memory_multiple_indexes",
    "integration/test_skip_index_load.py::TestRDBCorruptedIndex::test_corrupted_rdb_skip_index_load_succeeds",
    "integration/test_valkey_search_acl.py::TestCommandsACLs::test_acl_specific_search_commands_permissions",
    "integration/test_valkey_search_acl.py::TestCommandsACLs::test_index_with_several_prefixes_permissions",
    "integration/test_valkey_search_acl.py::TestCommandsACLs::test_valkey_search_cmds_categories",
]


# Apply dfly_args to all test classes in this directory
def pytest_collection_modifyitems(items):
    """Apply dfly_args decorator to all test classes and skip marked tests"""
    for item in items:
        if item.cls and not hasattr(item.cls, "_dfly_args_applied"):
            # Apply the decorator to the class
            decorated_class = dfly_args({"proactor_threads": 4})(item.cls)
            item.cls._dfly_args_applied = True

        # Skip tests that are in the skip list
        # Get the relative path from valkey_search directory
        item_path = str(item.nodeid)
        for skip_pattern in SKIP_TESTS:
            if skip_pattern in item_path:
                item.add_marker(pytest.mark.skip(reason=f"Test skipped: {skip_pattern}"))
