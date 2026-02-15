"""
Unit tests for fill_missing_values() function in Etl_scripts/helper_utils/helper.py
Tests use mocked Spark sessions to avoid Java/Python worker timeout issues.
"""

import unittest
from unittest.mock import MagicMock, patch, call
import sys
from pathlib import Path

# Add the parent directory to the path to import Etl_scripts modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from Etl_scripts.helper_utils.helper import fill_missing_values, anonymize_pii


class TestFillMissingValues(unittest.TestCase):
    """Test cases for the fill_missing_values function using mocked Spark"""

    def setUp(self):
        """Set up test fixtures before each test method"""
        self.mock_df = MagicMock()
        self.mock_logger = MagicMock()

    def test_fill_missing_values_returns_dataframe(self):
        """Test that fill_missing_values returns a DataFrame object"""
        # Setup mock DataFrame behavior
        self.mock_df.dtypes = []
        self.mock_df.withColumn.return_value = self.mock_df
        self.mock_df.fillna.return_value = self.mock_df
        
        # Execute function
        result = fill_missing_values(self.mock_df, self.mock_logger)
        
        # Verify result is the DataFrame
        self.assertIsNotNone(result)
        self.assertEqual(result, self.mock_df)

    def test_fill_missing_values_fills_age_with_negative_one(self):
        """Test that Age column is filled with -1"""
        # Setup mock DataFrame behavior
        self.mock_df.dtypes = []
        self.mock_df.withColumn.return_value = self.mock_df
        self.mock_df.fillna.return_value = self.mock_df
        
        # Execute function
        fill_missing_values(self.mock_df, self.mock_logger)
        
        # Get fillna call arguments
        call_args = self.mock_df.fillna.call_args[0][0]
        
        # Verify Age is filled with -1
        self.assertEqual(call_args['Age'], -1)

    def test_fill_missing_values_fills_numeric_columns_with_zero(self):
        """Test that numeric columns (Tenure, MonthlyCharges, TotalCharges) are filled with 0 or 0.0"""
        # Setup mock DataFrame behavior
        self.mock_df.dtypes = []
        self.mock_df.withColumn.return_value = self.mock_df
        self.mock_df.fillna.return_value = self.mock_df
        
        # Execute function
        fill_missing_values(self.mock_df, self.mock_logger)
        
        # Get fillna call arguments
        call_args = self.mock_df.fillna.call_args[0][0]
        
        # Verify numeric columns are filled with zero
        self.assertEqual(call_args['Tenure'], 0)
        self.assertEqual(call_args['MonthlyCharges'], 0.0)
        self.assertEqual(call_args['TotalCharges'], 0.0)

    def test_fill_missing_values_fills_string_columns_with_unknown(self):
        """Test that string columns are filled with 'Unknown'"""
        # Setup mock DataFrame behavior
        self.mock_df.dtypes = []
        self.mock_df.withColumn.return_value = self.mock_df
        self.mock_df.fillna.return_value = self.mock_df
        
        # Execute function
        fill_missing_values(self.mock_df, self.mock_logger)
        
        # Get fillna call arguments
        call_args = self.mock_df.fillna.call_args[0][0]
        
        # Verify string columns are filled with 'Unknown'
        self.assertEqual(call_args['Gender'], 'Unknown')
        self.assertEqual(call_args['ContractType'], 'Unknown')
        self.assertEqual(call_args['InternetService'], 'Unknown')
        self.assertEqual(call_args['TechSupport'], 'Unknown')
        self.assertEqual(call_args['Churn'], 'Unknown')

    def test_fill_missing_values_logs_completion(self):
        """Test that fill_missing_values logs completion message"""
        # Setup mock DataFrame behavior
        self.mock_df.dtypes = []
        self.mock_df.withColumn.return_value = self.mock_df
        self.mock_df.fillna.return_value = self.mock_df
        
        # Execute function
        fill_missing_values(self.mock_df, self.mock_logger)
        
        # Verify logger.info was called with completion message
        self.mock_logger.info.assert_called_with('filled missing values.')

    def test_fill_missing_values_handles_customer_id_fill(self):
        """Test that CustomerID column is properly handled in fillna"""
        # Setup mock DataFrame behavior
        self.mock_df.dtypes = []
        self.mock_df.withColumn.return_value = self.mock_df
        self.mock_df.fillna.return_value = self.mock_df
        
        # Execute function
        fill_missing_values(self.mock_df, self.mock_logger)
        
        # Get fillna call arguments
        call_args = self.mock_df.fillna.call_args[0][0]
        
        # Verify CustomerID is in the fill dictionary (should have a fill value)
        self.assertIn('CustomerID', call_args)

    def test_fill_missing_values_preserves_dataframe_structure(self):
        """Test that the returned DataFrame is the same as input (method chaining)"""
        # Setup mock DataFrame behavior
        self.mock_df.dtypes = []
        self.mock_df.withColumn.return_value = self.mock_df
        self.mock_df.fillna.return_value = self.mock_df
        
        # Store original mock
        original_df = self.mock_df
        
        # Execute function
        result = fill_missing_values(self.mock_df, self.mock_logger)
        
        # Verify result is the same object (method chaining preserved)
        self.assertIs(result, original_df)


class TestAnonymizePII(unittest.TestCase):
    """Test cases for the anonymize_pii function using mocked Spark"""

    def setUp(self):
        """Set up test fixtures before each test method"""
        self.mock_df = MagicMock()
        self.mock_logger = MagicMock()
        # Patch sha2 and col for all tests
        patcher_sha2 = patch('Etl_scripts.helper_utils.helper.sha2', lambda x, y: 'hashed')
        patcher_col = patch('Etl_scripts.helper_utils.helper.col', lambda x: x)
        self.patcher_sha2 = patcher_sha2
        self.patcher_col = patcher_col
        self.patcher_sha2.start()
        self.patcher_col.start()

    def tearDown(self):
        self.patcher_sha2.stop()
        self.patcher_col.stop()

    def test_anonymize_pii_returns_dataframe(self):
        """Test that anonymize_pii returns a DataFrame object"""
        # Setup mock DataFrame behavior
        self.mock_df.withColumn.return_value = self.mock_df
        
        # Execute function
        result = anonymize_pii(self.mock_df, self.mock_logger)
        
        # Verify result is the DataFrame
        self.assertIsNotNone(result)
        self.assertEqual(result, self.mock_df)

    def test_anonymize_pii_logs_completion(self):
        """Test that anonymize_pii logs completion message"""
        # Setup mock DataFrame behavior
        self.mock_df.withColumn.return_value = self.mock_df
        
        # Execute function
        anonymize_pii(self.mock_df, self.mock_logger)
        
        # Verify logger.info was called with completion message
        self.mock_logger.info.assert_called_with('Anonymized PII values.')

    def test_anonymize_pii_preserves_dataframe_structure(self):
        """Test that the returned DataFrame is the same as input (method chaining)"""
        # Setup mock DataFrame behavior
        self.mock_df.withColumn.return_value = self.mock_df
        
        # Store original mock
        original_df = self.mock_df
        
        # Execute function
        result = anonymize_pii(self.mock_df, self.mock_logger)
        
        # Verify result is the same object (method chaining preserved)
        self.assertIs(result, original_df)

    def test_anonymize_pii_returns_non_none(self):
        """Test that anonymize_pii always returns a non-None result"""
        # Setup mock DataFrame behavior
        self.mock_df.withColumn.return_value = self.mock_df
        
        # Execute function
        result = anonymize_pii(self.mock_df, self.mock_logger)
        
        # Verify result is not None
        self.assertIsNotNone(result)

    def test_anonymize_pii_logger_called_once(self):
        """Test that logger.info is called exactly once"""
        # Setup mock DataFrame behavior
        self.mock_df.withColumn.return_value = self.mock_df
        
        # Execute function
        anonymize_pii(self.mock_df, self.mock_logger)
        
        # Verify logger.info was called exactly once
        self.assertEqual(self.mock_logger.info.call_count, 1)

    def test_anonymize_pii_correct_log_message(self):
        """Test that the correct log message is used"""
        # Setup mock DataFrame behavior
        self.mock_df.withColumn.return_value = self.mock_df
        
        # Execute function
        anonymize_pii(self.mock_df, self.mock_logger)
        
        # Get the actual log message
        logged_message = self.mock_logger.info.call_args[0][0]
        
        # Verify the message matches expected string
        self.assertEqual(logged_message, 'Anonymized PII values.')

    def test_anonymize_pii_accepts_mock_dataframe(self):
        """Test that anonymize_pii accepts a mocked DataFrame"""
        # Setup mock DataFrame behavior
        self.mock_df.withColumn.return_value = self.mock_df
        
        # Execute function without raising exceptions
        try:
            result = anonymize_pii(self.mock_df, self.mock_logger)
            execution_successful = True
        except Exception:
            execution_successful = False
        
        # Verify function executed successfully
        self.assertTrue(execution_successful)

    def test_anonymize_pii_accepts_mock_logger(self):
        """Test that anonymize_pii accepts a mocked logger"""
        # Setup mock DataFrame behavior
        self.mock_df.withColumn.return_value = self.mock_df
        
        # Execute function without raising exceptions
        try:
            result = anonymize_pii(self.mock_df, self.mock_logger)
            execution_successful = True
        except Exception:
            execution_successful = False
        
        # Verify function executed successfully
        self.assertTrue(execution_successful)

    def test_anonymize_pii_multiple_calls(self):
        """Test that anonymize_pii can be called multiple times"""
        # Setup mock DataFrame behavior
        self.mock_df.withColumn.return_value = self.mock_df
        
        # Execute function multiple times
        result1 = anonymize_pii(self.mock_df, self.mock_logger)
        result2 = anonymize_pii(self.mock_df, self.mock_logger)
        
        # Verify both results are DataFrames
        self.assertIsNotNone(result1)
        self.assertIsNotNone(result2)
        
        # Verify logger was called twice
        self.assertEqual(self.mock_logger.info.call_count, 2)

    def test_anonymize_pii_result_is_input_type(self):
        """Test that anonymize_pii returns same type as input"""
        # Setup mock DataFrame behavior
        self.mock_df.withColumn.return_value = self.mock_df
        
        # Execute function
        result = anonymize_pii(self.mock_df, self.mock_logger)
        
        # Verify result type matches input type
        self.assertEqual(type(result), type(self.mock_df))


if __name__ == '__main__':
    unittest.main()
