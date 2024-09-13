import unittest
from unittest.mock import Mock, patch
from nextplace.validator.database.database_manager import DatabaseManager
from nextplace.validator.scoring.scoring import Scorer
from nextplace.validator.api.sold_homes_api import SoldHomesAPI
from nextplace.validator.scoring.scoring_calculator import ScoringCalculator


class TestScorer(unittest.TestCase):

    def setUp(self):
        self.mock_db_manager = Mock(spec=DatabaseManager)
        self.mock_sold_homes_api = Mock(spec=SoldHomesAPI)
        self.mock_scoring_calculator = Mock(spec=ScoringCalculator)
        self.markets = [{"name": "Test Market", "id": "test_id"}]
        
        # Create Scorer with mocked dependencies
        self.scorer = Scorer(self.mock_db_manager, self.markets)
        self.scorer.sold_homes_api = self.mock_sold_homes_api
        self.scorer.scoring_calculator = self.mock_scoring_calculator

    def test_init(self):
        self.assertIsInstance(self.scorer.database_manager, DatabaseManager)
        self.assertIsInstance(self.scorer.sold_homes_api, SoldHomesAPI)
        self.assertIsInstance(self.scorer.scoring_calculator, ScoringCalculator)
        self.assertEqual(self.scorer.markets, self.markets)

    def test_run_score_predictions(self):
        mock_predictions = [
            (1, 'miner1', 100000, '2023-01-15', 105000, '2023-01-20T07:00:00Z'),
            (2, 'miner2', 200000, '2023-02-15', 195000, '2023-02-18T07:00:00Z')
        ]
        self.scorer._get_scorable_predictions = Mock(return_value=mock_predictions)
        
        step = 10
        self.scorer.run_score_predictions()
        
        self.scorer._get_scorable_predictions.assert_called_once()
        self.scorer.scoring_calculator.process_scorable_predictions.assert_called_once_with(step, mock_predictions)

    def test_run_score_predictions_no_predictions(self):
        self.scorer._get_scorable_predictions = Mock(return_value=[])
        
        step = 10
        self.scorer.run_score_predictions()
        
        self.scorer._get_scorable_predictions.assert_called_once()
        self.scorer.scoring_calculator.process_scorable_predictions.assert_called_once_with(step, [])

    @patch('bittensor.logging.trace')
    def test_clear_out_old_scored_predictions(self, mock_logging):
        self.scorer._clear_out_old_scored_predictions()
        
        mock_logging.assert_called_once()
        self.mock_db_manager.query_and_commit.assert_called_once()
        
        # Check if the SQL query is correct
        called_query = self.mock_db_manager.query_and_commit.call_args[0][0]
        self.assertIn("DELETE FROM predictions", called_query)
        self.assertIn("WHERE score_timestamp <", called_query)

    def test_get_scorable_predictions(self):
        mock_query_result = [
            (1, 'miner1', 100000, '2023-01-15', 105000, '2023-01-20T07:00:00Z'),
            (2, 'miner2', 200000, '2023-02-15', 195000, '2023-02-18T07:00:00Z')
        ]
        self.mock_db_manager.query.return_value = mock_query_result
        
        result = self.scorer._get_scorable_predictions()
        
        self.assertEqual(result, mock_query_result)
        self.mock_db_manager.query.assert_called_once()
        
        # Check if the SQL query is correct
        called_query = self.mock_db_manager.query.call_args[0][0]
        self.assertIn("SELECT predictions.property_id", called_query)
        self.assertIn("JOIN sales ON predictions.property_id = sales.property_id", called_query)
        self.assertIn("WHERE predictions.prediction_timestamp < sales.sale_date", called_query)
        self.assertIn("AND predictions.scored = 0", called_query)

    def test_get_scorable_predictions_no_results(self):
        self.mock_db_manager.query.return_value = []
        
        result = self.scorer._get_scorable_predictions()
        
        self.assertEqual(result, [])
        self.mock_db_manager.query.assert_called_once()

if __name__ == '__main__':
    unittest.main()
