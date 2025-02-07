def pytest_ignore_collect(path, config):
    # Ignore test_data directory
    if "test_data" in str(path):
        return True
