import unittest
from semantic_validator import SemanticValidator

class TestSemanticValidator(unittest.TestCase):
    def test_valid_sql(self):
        self.assertTrue(SemanticValidator.validate_sql_expression("age >= 18 AND status = 'ACTIVE'"))

    def test_invalid_sql_with_drop(self):
        with self.assertRaises(ValueError):
            SemanticValidator.validate_sql_expression("DROP TABLE users")

    def test_column_exists(self):
        self.assertTrue(SemanticValidator.validate_column_exists(['id', 'name', 'age'], 'name'))

    def test_column_not_exists(self):
        with self.assertRaises(ValueError):
            SemanticValidator.validate_column_exists(['id', 'name'], 'age')
            
if __name__ == '__main__':
    unittest.main()
