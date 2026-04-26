# conftest.py
import pytest
import tempfile
import os

@pytest.fixture
def temp_text_file():
    """
    A fixture that creates a temporary text file for testing.
    It automatically handles teardown by yielding.
    """
    # 1. Setup: Create the temporary file.
    #    The 'delete=False' allows the file to persist long enough for the test.
    temp_file = tempfile.NamedTemporaryFile(mode='w+', delete=False, encoding='utf-8')
    temp_file.write("Hello, world!")
    temp_file.seek(0)  # Rewind to the start of the file
    
    # 2. Yield: Provide the resource to the test function.
    yield temp_file
    
    # 3. Teardown: Clean up the file after the test finishes.
    temp_file.close()
    os.remove(temp_file.name)
