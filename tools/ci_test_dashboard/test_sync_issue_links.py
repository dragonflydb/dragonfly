"""Tests that failed dashboard tests link to the correct GitHub issues and pull requests.

The test suite fails if similar but unrelated test names are linked.
"""

import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from tools.ci_test_dashboard import sync_issue_links as links


class IssueLinkTest(unittest.TestCase):
    def setUp(self):
        self.test = {
            "id": "pytest/dragonfly/test_cache.py::test_eviction[param-one]",
            "display_name": "dragonfly/test_cache.py::test_eviction[param-one]",
            "name": "test_eviction[param-one]",
        }
        self.issue = {
            "number": 123,
            "html_url": "https://github.com/dragonflydb/dragonfly/issues/123",
            "title": "test_eviction is flaky",
            "body": "Fails in dragonfly/test_cache.py::test_eviction[param-one].",
            "state": "open",
            "assignees": [{"login": "dragonfly-user"}],
        }

    def test_exact_match_in_issue_body(self):
        """Match a complete parameterized test identifier found in an issue body."""
        self.assertEqual(links.issue_match(self.test, self.issue), "exact")

    def test_parameterized_match_when_issue_omits_parameter(self):
        """Match the base test when an issue omits its parameter value."""
        self.issue["body"] = "Fails in dragonfly/test_cache.py::test_eviction."
        self.assertEqual(links.issue_match(self.test, self.issue), "parameterized")

    def test_parameterized_match_does_not_match_a_different_variant(self):
        """Do not link an issue that names an explicitly different parameter value."""
        self.issue["body"] = "Fails in dragonfly/test_cache.py::test_eviction[param-two]."
        self.assertIsNone(links.issue_match(self.test, self.issue))

    def test_unparameterized_test_name_matches_an_issue(self):
        """Match a short base name when an issue omits both path and parameter value."""
        self.issue["body"] = "test_eviction is flaky."
        self.assertEqual(links.issue_match(self.test, self.issue), "parameterized")

    def test_bare_name_does_not_match_inside_a_qualified_identifier(self):
        """Do not link a short base name to a different test's qualified reference."""
        self.issue["title"] = "Other test is flaky"
        self.issue["body"] = "Fails in other_test.py::test_eviction."
        self.assertIsNone(links.issue_match(self.test, self.issue))

    def test_parameter_with_nested_brackets_matches_an_unparameterized_issue(self):
        """Strip a complete pytest parameter suffix even when it contains brackets."""
        test = {
            "display_name": "dragonfly/json_test.py::test_arrappend[array-[]-array]",
            "name": "test_arrappend[array-[]-array]",
        }
        self.issue["title"] = "JSON test is flaky"
        self.issue["body"] = "Fails in dragonfly/json_test.py::test_arrappend."
        self.assertEqual(links.issue_match(test, self.issue), "parameterized")

    def test_pytest_node_id_matches_an_issue_when_the_name_is_short(self):
        """Match a pytest node ID reconstructed from its JUnit classname."""
        test = {
            "display_name": "dragonfly.connection_test::test_reset",
            "classname": "dragonfly.connection_test",
            "name": "test_reset",
            "level": "pytest",
        }
        self.issue["body"] = "Fails in dragonfly/connection_test.py::test_reset."
        self.assertEqual(links.issue_match(test, self.issue), "exact")

    def test_pytest_node_id_includes_the_test_class(self):
        """Preserve the class component when reconstructing a pytest node ID."""
        test = {
            "display_name": "dragonfly.pymemcached_test.TestMemcached::test_basic",
            "classname": "dragonfly.pymemcached_test.TestMemcached",
            "name": "test_basic",
            "level": "pytest",
        }
        self.issue["body"] = "Fails in dragonfly/pymemcached_test.py::TestMemcached::test_basic."
        self.assertEqual(links.issue_match(test, self.issue), "exact")

    def test_pytest_node_id_is_preferred_over_the_name_alias(self):
        """Keep an explicitly named pytest case eligible for assignee display."""
        test = {
            "display_name": "dragonfly.connection_test::test_lib_name_ver[df_factory0]",
            "classname": "dragonfly.connection_test",
            "name": "test_lib_name_ver[df_factory0]",
            "level": "pytest",
        }
        self.issue["body"] = (
            "Fails in dragonfly/connection_test.py::test_lib_name_ver[df_factory0]."
        )
        self.assertEqual(links.issue_match(test, self.issue), "exact")

    def test_pytest_node_id_matches_an_issue_without_its_parameter(self):
        """Match a qualified pytest node ID that omits its parameter value."""
        test = {
            "display_name": "dragonfly.search_test::test_knn[flat]",
            "classname": "dragonfly.search_test",
            "name": "test_knn[flat]",
            "level": "pytest",
        }
        self.issue["body"] = "Fails in dragonfly/search_test.py::test_knn."
        self.assertEqual(links.issue_match(test, self.issue), "parameterized")

    def test_gtest_dot_name_matches_a_slash_parameterized_test(self):
        """Match dot-separated GTest names without their instance suffix."""
        test = {
            "display_name": "StringFamilyTest::SetGet/custom-instance",
            "name": "SetGet/custom-instance",
            "level": "gtest",
        }
        self.issue["body"] = "StringFamilyTest.SetGet fails intermittently."
        self.assertEqual(links.issue_match(test, self.issue), "parameterized")

    def test_gtest_dot_name_does_not_match_a_different_instance(self):
        """Do not match a dot-separated GTest name with a different instance suffix."""
        test = {
            "display_name": "StringFamilyTest::SetGet/custom-instance",
            "name": "SetGet/custom-instance",
            "level": "gtest",
        }
        self.issue["body"] = "StringFamilyTest.SetGet/other-instance fails intermittently."
        self.assertIsNone(links.issue_match(test, self.issue))

    def test_gtest_dot_name_is_preferred_over_a_shorter_name_alias(self):
        """Preserve assignee eligibility for a fully qualified GTest name."""
        test = {
            "display_name": "StringFamilyTest::LongOperationName/custom-instance",
            "name": "LongOperationName/custom-instance",
            "level": "gtest",
        }
        self.issue["body"] = "StringFamilyTest.LongOperationName/custom-instance fails."
        self.assertEqual(links.issue_match(test, self.issue), "exact")

    def test_gtest_name_matches_an_issue_without_its_instance_suffix(self):
        """Match a long GTest function name when an issue omits its instance suffix."""
        test = {
            "display_name": "Matrix/OptionsTest::DelRangeNodeOverflow/fminus5compr0",
            "name": "DelRangeNodeOverflow/fminus5compr0",
            "level": "gtest",
        }
        self.issue["body"] = "DelRangeNodeOverflow fails intermittently."
        self.assertEqual(links.issue_match(test, self.issue), "parameterized")

    def test_gtest_name_matches_an_issue_without_its_instantiation_prefix(self):
        """Match a qualified GTest name when an issue omits its instantiation prefix."""
        test = {
            "display_name": "Matrix/OptionsTest::DelRangeNodeOverflow/fminus5compr0",
            "name": "DelRangeNodeOverflow/fminus5compr0",
            "level": "gtest",
        }
        self.issue["body"] = "OptionsTest.DelRangeNodeOverflow fails intermittently."
        self.assertEqual(links.issue_match(test, self.issue), "parameterized")

    def test_gtest_name_does_not_match_a_different_instantiation_prefix(self):
        """Do not match a qualified GTest name that explicitly names another instantiation."""
        test = {
            "display_name": "Matrix/OptionsTest::DelRangeNodeOverflow/fminus5compr0",
            "name": "DelRangeNodeOverflow/fminus5compr0",
            "level": "gtest",
        }
        self.issue["body"] = "Other/OptionsTest.DelRangeNodeOverflow fails intermittently."
        self.assertIsNone(links.issue_match(test, self.issue))

    def test_typed_gtest_name_matches_an_issue_without_its_type_index(self):
        """Match a typed GTest name when an issue omits its numeric type index."""
        test = {
            "display_name": "JsonPathTest/0::Parser",
            "name": "Parser",
            "level": "gtest",
        }
        self.issue["body"] = "JsonPathTest.Parser fails intermittently."
        self.assertEqual(links.issue_match(test, self.issue), "parameterized")

    def test_unrelated_issue_does_not_match(self):
        """Do not link an issue that does not mention the dashboard test."""
        self.issue["title"] = "Memory accounting regression"
        self.issue["body"] = "No test failure was recorded."
        self.assertIsNone(links.issue_match(self.test, self.issue))

    def test_test_name_does_not_match_a_longer_identifier(self):
        """Require an identifier boundary so a longer test name is not linked."""
        self.issue["title"] = "Cache test regression"
        self.issue["body"] = "Fails in dragonfly/test_cache.py::test_eviction_policy."
        self.assertIsNone(links.issue_match(self.test, self.issue))

    def test_issue_reference_does_not_match_a_longer_number(self):
        """Require an issue-number boundary so #1234 does not reference #123."""
        pull_request = {"title": "Fixes #1234", "body": ""}
        self.assertFalse(
            links.pull_request_references_issue(pull_request, 123, links.DEFAULT_REPOSITORY)
        )

    def test_issue_reference_matches_local_url_but_not_a_foreign_repository(self):
        """Link local issue URLs while rejecting same-numbered foreign issue references."""
        local = {
            "title": "Fixes https://github.com/dragonflydb/dragonfly/issues/123",
            "body": "",
        }
        foreign = {"title": "Fixes redis/redis#123", "body": ""}
        self.assertTrue(links.pull_request_references_issue(local, 123, links.DEFAULT_REPOSITORY))
        self.assertFalse(
            links.pull_request_references_issue(foreign, 123, links.DEFAULT_REPOSITORY)
        )

    def test_build_links_includes_dedicated_assignee_and_referencing_open_pr(self):
        """Keep assignees for a dedicated open issue and include its referencing PR."""
        pull_request = {
            "number": 456,
            "html_url": "https://github.com/dragonflydb/dragonfly/pull/456",
            "title": "Fixes #123",
            "body": "",
            "state": "open",
        }
        result = links.build_links([self.test], [self.issue], [pull_request])
        match = result[self.test["id"]]
        self.assertEqual(match["issues"][0]["assignees"], ["dragonfly-user"])
        self.assertTrue(match["issues"][0]["eligible_for_assignee"])
        self.assertEqual(match["pull_requests"][0]["number"], 456)

    def test_closed_issue_and_merged_directly_matched_pr_include_states(self):
        """Preserve closed and merged states for historical dashboard links."""
        self.issue["state"] = "closed"
        pull_request = {
            "number": 456,
            "html_url": "https://github.com/dragonflydb/dragonfly/pull/456",
            "title": "Fix test_eviction[param-one]",
            "body": "",
            "state": "closed",
            "merged_at": "2026-10-18T00:00:00Z",
        }
        result = links.build_links([self.test], [self.issue], [pull_request])[self.test["id"]]
        self.assertEqual(result["issues"][0]["state"], "closed")
        self.assertFalse(result["issues"][0]["eligible_for_assignee"])
        self.assertEqual(result["pull_requests"][0]["state"], "merged")

    def test_record_state_prefers_merged(self):
        """Show merged rather than closed when GitHub reports a merge timestamp."""
        self.assertEqual(links.record_state({"state": "open"}), "open")
        self.assertEqual(
            links.record_state({"state": "closed", "merged_at": "2026-10-18T00:00:00Z"}),
            "merged",
        )

    @patch("tools.ci_test_dashboard.sync_issue_links.github_api")
    def test_recent_pages_filters_records_before_applying_limit(self, github_api):
        """Filter pull requests and old records before enforcing the closed-history limit."""
        github_api.return_value = [
            {
                "number": 1,
                "updated_at": "2026-10-18T00:00:00Z",
                "closed_at": "2026-10-18T00:00:00Z",
                "pull_request": {},
            },
            {
                "number": 2,
                "updated_at": "2026-10-17T00:00:00Z",
                "closed_at": "2026-09-01T00:00:00Z",
            },
            {
                "number": 3,
                "updated_at": "2026-10-16T00:00:00Z",
                "closed_at": "2026-10-16T00:00:00Z",
            },
            {
                "number": 4,
                "updated_at": "2026-09-01T00:00:00Z",
                "closed_at": "2026-09-01T00:00:00Z",
            },
        ]
        cutoff = datetime(2026, 10, 1, tzinfo=timezone.utc)
        result = links.github_recent_pages(
            "https://api.github.test/issues?state=closed",
            "token",
            cutoff,
            1,
            lambda item: "pull_request" not in item,
        )
        self.assertEqual([item["number"] for item in result], [3])

    @patch(
        "tools.ci_test_dashboard.sync_issue_links.write_links", side_effect=RuntimeError("offline")
    )
    def test_lookup_failure_fails_without_writing_a_payload(self, write_links):
        """Prevent publishing a dashboard with falsely empty GitHub link data."""
        with tempfile.TemporaryDirectory() as temporary:
            with patch.object(sys, "argv", ["sync_issue_links.py", temporary]):
                self.assertEqual(links.main(), 1)
            self.assertFalse((Path(temporary) / "issue-links.json").exists())
        write_links.assert_called_once()


if __name__ == "__main__":
    unittest.main()
