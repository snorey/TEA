import unittest
import idem
import mock


class TestZipCycler(unittest.TestCase, idem.ZipCycler):

    def __init__(self, **arguments):
        super(TestZipCycler, self).__init__(**arguments)

    def test_something(self):
        self.assertEqual(True, False)


if __name__ == '__main__':
    unittest.main()
