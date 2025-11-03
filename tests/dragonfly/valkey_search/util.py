"""
Utility module for valkey-search tests running on Dragonfly
Provides waiters functionality compatible with valkeytestframework.util.waiters
"""

import time


class waiters:
    """Waiters utility class for test synchronization"""

    @staticmethod
    def wait_for_true(func, timeout=30, interval=0.1):
        """
        Wait for a function to return True

        Args:
            func: Function to call repeatedly until it returns True
            timeout: Maximum time to wait in seconds (default: 30)
            interval: Time between checks in seconds (default: 0.1)

        Returns:
            True if function returned True within timeout, False otherwise
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                if func():
                    return True
            except Exception:
                # Ignore exceptions during polling
                pass
            time.sleep(interval)
        return False

    @staticmethod
    def wait_for_equal(func, value, timeout=30, interval=0.1):
        """
        Wait for a function to return a specific value

        Args:
            func: Function to call repeatedly
            value: Expected return value
            timeout: Maximum time to wait in seconds (default: 30)
            interval: Time between checks in seconds (default: 0.1)

        Returns:
            True if function returned expected value within timeout, False otherwise
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                if func() == value:
                    return True
            except Exception:
                # Ignore exceptions during polling
                pass
            time.sleep(interval)
        return False

    @staticmethod
    def wait_for_not_equal(func, value, timeout=30, interval=0.1):
        """
        Wait for a function to return a value different from the specified one

        Args:
            func: Function to call repeatedly
            value: Value that should NOT be returned
            timeout: Maximum time to wait in seconds (default: 30)
            interval: Time between checks in seconds (default: 0.1)

        Returns:
            True if function returned different value within timeout, False otherwise
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                if func() != value:
                    return True
            except Exception:
                # Ignore exceptions during polling
                pass
            time.sleep(interval)
        return False

    @staticmethod
    def wait_for_condition(condition_func, timeout=30, interval=0.1):
        """
        Wait for a condition function to return True
        Alias for wait_for_true for compatibility
        """
        return waiters.wait_for_true(condition_func, timeout, interval)


# For backward compatibility with direct import style
wait_for_true = waiters.wait_for_true
wait_for_equal = waiters.wait_for_equal
wait_for_not_equal = waiters.wait_for_not_equal
wait_for_condition = waiters.wait_for_condition
