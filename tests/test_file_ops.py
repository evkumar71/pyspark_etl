# test_file_operations.py
import pytest

# Our function to test
def read_and_process(file_path):
    with open(file_path, 'r') as f:
        content = f.read()
    return content.upper()

def test_read_and_process_file(temp_text_file):
    """
    Tests the read_and_process function using the temp_text_file fixture.
    """
    # Act: The fixture has already handled the "Arrange" phase.
    processed_content = read_and_process(temp_text_file.name)
    
    # Assert
    assert processed_content == "HELLO, WORLD!"
