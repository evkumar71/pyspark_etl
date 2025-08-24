import unittest

class MyTestCase(unittest.TestCase):
    def setUp(self):
        # This runs before every test method
        self.test_list = [1, 2, 3]

    def tearDown(self):
        # This runs after every test method
        del self.test_list

    def test_add_element(self):
        self.test_list.append(4)
        self.assertEqual(len(self.test_list), 4)

    def test_remove_element(self):
        self.test_list.pop()
        self.assertEqual(self.test_list, [1, 2])

if __name__ == '__main__':
    unittest.main()
    
    # To discover tests, run the command:
    # python3 -m unittest discover -s tests -p "test_*.py"
    
    # To run the tests, use the command:
    # python3 -m unittest tests/test_case1.py