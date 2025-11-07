# Valkey-Search Integration Tests for Dragonfly

Integration tests from [valkey-search](https://github.com/valkey-io/valkey-search) project, adapted to run on Dragonfly without modifying the original test code.

## Prerequisites

1. Build Dragonfly

2. Install Python dependencies:
   ```bash
   pip install -r tests/dragonfly/requirements.txt
   ```

## Setup

1. Sync tests from valkey-search:
   ```bash
   cd tests/dragonfly/valkey_search
   ./sync-valkey-search-tests.sh
   ```

2. Set environment variables:
   ```bash
   export DRAGONFLY_PATH="/path/to/dragonfly/build-dbg/dragonfly"
   export ROOT_DIR="/path/to/dragonfly/tests/dragonfly/valkey_search"
   ```

## Running Tests

```bash
# All tests
pytest tests/dragonfly/valkey_search/integration/ -v

# Specific test file
pytest tests/dragonfly/valkey_search/integration/test_ft_create.py -v

# Specific test
pytest tests/dragonfly/valkey_search/integration/test_ft_create.py::TestSearchFTCreateCMD::test_ft_create_fails_on_replica_cmd -v
```

## Structure

```
tests/dragonfly/valkey_search/
 __init__.py                          # Mock framework for valkey-search imports
 conftest.py                          # Pytest configuration
 util.py                              # Utility functions (waiters)
 valkey_search_test_case_dragonfly.py # Dragonfly adapter (real replicas, clusters)
 sync-valkey-search-tests.sh          # Script to sync tests
 integration/                         # Synced from valkey-search (not in git)
```

## How It Works

1. **Infrastructure files** (committed to git) provide compatibility layer
2. **Test files** (in `integration/`, not in git) are synced from valkey-search
3. **Mock framework** (`__init__.py`) replaces valkey-search imports with Dragonfly equivalents
4. **Adapter** (`valkey_search_test_case_dragonfly.py`) creates real Dragonfly instances with replicas
5. **Original tests run unchanged** - all adaptation happens in infrastructure layer
6. **Python 3.8 compatibility** - sync script patches all `.py` files to add `from __future__ import annotations`
