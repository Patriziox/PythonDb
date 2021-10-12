import sys

sys.path.append('c:\\Users\\Patrizio\\myProject\\Python\\SqlEngine\\Source')


import unittest
from Source.SqlGeneric import SqlGeneric as Gen

class TestGeneric(unittest.TestCase) :

    def test_Normalize(self) :
        self.assertEqual(Gen.Normalize('(a+b) && "xyz"*123'), '( A + B ) && "xyz" * 123')


if __name__ == '__main__' :
    unittest.main()