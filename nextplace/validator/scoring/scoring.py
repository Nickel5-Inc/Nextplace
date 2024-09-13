from datetime import datetime, timezone, timedelta
import bittensor as bt

from nextplace.validator.scoring.scoring_calculator import ScoringCalculator
from nextplace.validator.api.sold_homes_api import SoldHomesAPI
from nextplace.validator.database.database_manager import DatabaseManager
from nextplace.validator.utils.contants import ISO8601

"""
Helper class manages scoring Miner predictions
"""


class Scorer:

    def __init__(self, database_manager: DatabaseManager, markets: list[dict[str, str]]):
        self.database_manager = database_manager
        self.markets = markets
        self.sold_homes_api = SoldHomesAPI(database_manager, markets)
        self.scoring_calculator = ScoringCalculator(database_manager, self.sold_homes_api)

    def run_score_predictions(self) -> None:
        num_predictions = self.database_manager.get_size_of_table('predictions')
        if num_predictions == 0:
            bt.logging.trace(f"No predictions yet, nothing to score.")
            return
        with self.database_manager.lock:
            self.sold_homes_api.get_sold_properties()
            num_sales = self.database_manager.get_size_of_table('sales')
            bt.logging.info(f"Ingested {num_sales} sold homes since our oldest prediction. Checking for overlapping predictions.")
            scorable_predictions = self._get_scorable_predictions()
            self.scoring_calculator.process_scorable_predictions(scorable_predictions)
            self._cleanup()

    def _cleanup(self) -> None:
        """
        Clean up after scoring. Delete all rows from sales table, clean out old predictions
        Returns: None
        """
        self.database_manager.delete_all_sales()
        self._clear_out_old_scored_predictions()
        bt.logging.info("Finished updating scores")

    def _clear_out_old_scored_predictions(self) -> None:
        """
        Remove predictions that were scored more than 5 days ago
        Returns:
            None
        """
        max_days = 5
        today = datetime.now(timezone.utc)
        min_date = (today - timedelta(days=max_days)).strftime(ISO8601)
        bt.logging.trace(f"Attempting to delete scored predictions older than {min_date}")
        query_str = f"""
                        DELETE FROM predictions
                        WHERE score_timestamp < '{min_date}'
                    """
        self.database_manager.query_and_commit(query_str)

    def _get_scorable_predictions(self) -> list:
        """
        Query to get scorable predictions that haven't been scored yet
        Returns:
            list of query results
        """
        query = """
            SELECT predictions.property_id, predictions.miner_hotkey, predictions.predicted_sale_price, predictions.predicted_sale_date, sales.sale_price, sales.sale_date
            FROM predictions
            JOIN sales ON predictions.nextplace_id = sales.nextplace_id
            WHERE predictions.prediction_timestamp < sales.sale_date
            AND ( predictions.scored = 0 OR predictions.scored = FALSE OR predictions.scored IS NULL )
        """
        return self.database_manager.query(query)  # Get the unscored predictions that we can score
                