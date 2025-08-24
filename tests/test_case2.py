import unittest

class MyClassLevelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Connect to a test database once for the whole class
        cls.db_connection = "Connected"
        print("Set up database connection.")

    @classmethod
    def tearDownClass(cls):
        # Disconnect from the database once all tests are done
        cls.db_connection = None
        print("Tore down database connection.")

    def test_db_read(self):
        self.assertEqual(self.db_connection, "Connected")
        print("  - Running db_read test")

    def test_db_write(self):
        self.assertEqual(self.db_connection, "Connected")
        print("  - Running db_write test")

if __name__ == '__main__':
    unittest.main()
    
    # To discover tests, run the command:
    # python3 -m unittest discover -s tests -p "test_*.py"
    
    # To run the tests, use the command:
    # python3 -m unittest tests/test_case1.py