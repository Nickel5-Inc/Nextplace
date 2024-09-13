from typing import Dict, List, Tuple
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import bittensor as bt

from nextplace.validator.utils.contants import ISO8601


class ScoringCalculator:

    def __init__(self, database_manager, sold_homes_api):
        self.database_manager = database_manager
        self.sold_homes_api = sold_homes_api
        
    def process_scorable_predictions(self, scorable_predictions) -> None:
        """
        Score miner predictions in bulk
        """
        bt.logging.trace(f"Found {len(scorable_predictions)} scorable predictions")

        cursor, db_connection = self.database_manager.get_cursor()
        try:
            miner_scores = self._fetch_current_miner_scores(cursor)
            new_scores, predictions_to_mark = self._calculate_new_scores(scorable_predictions)
            self._update_miner_scores(cursor, miner_scores, new_scores)
            self._mark_predictions_as_scored(cursor, predictions_to_mark)
            db_connection.commit()
            bt.logging.info(f"Scored {len(scorable_predictions)} predictions")
        finally:
            cursor.close()
            db_connection.close()

    def _fetch_current_miner_scores(self, cursor) -> Dict[str, Dict[str, float]]:
        cursor.execute('SELECT miner_hotkey, lifetime_score, total_predictions FROM miner_scores')
        return {row[0]: {'lifetime_score': row[1], 'total_predictions': row[2]} for row in cursor.fetchall()}

    def _get_num_sold_homes(self) -> int:
        num_sold_homes = self.database_manager.get_size_of_table('sales')
        bt.logging.info(f"Received {num_sold_homes} sold homes")
        return num_sold_homes

    def _calculate_new_scores(self, scorable_predictions: List[Tuple]) -> Tuple[Dict[str, Dict[str, float]], List[Tuple]]:
        new_scores = defaultdict(lambda: {'total_score': 0, 'new_predictions': 0})
        predictions_to_mark = []

        for prediction in scorable_predictions:
            property_id, miner_hotkey, predicted_price, predicted_date, actual_price, actual_date = prediction
            score = self.calculate_score(actual_price, predicted_price, actual_date, predicted_date)

            new_scores[miner_hotkey]['total_score'] += score
            new_scores[miner_hotkey]['new_predictions'] += 1
            predictions_to_mark.append((property_id, miner_hotkey))

        return new_scores, predictions_to_mark

    def _update_miner_scores(self, cursor, miner_scores: Dict[str, Dict[str, float]], new_scores: Dict[str, Dict[str, float]]) -> None:
        updates = []
        inserts = []
        for miner_hotkey, data in new_scores.items():
            if miner_hotkey in miner_scores:
                old_score = miner_scores[miner_hotkey]['lifetime_score']
                old_predictions = miner_scores[miner_hotkey]['total_predictions']
                new_total_score = (old_score * old_predictions) + data['total_score']
                new_total_predictions = old_predictions + data['new_predictions']
                new_lifetime_score = new_total_score / new_total_predictions
                updates.append((new_lifetime_score, new_total_predictions, miner_hotkey))
            else:
                new_lifetime_score = data['total_score'] / data['new_predictions']
                inserts.append((miner_hotkey, new_lifetime_score, data['new_predictions']))

        now = datetime.now(timezone.utc).strftime(ISO8601)
        cursor.executemany(f'''
            UPDATE miner_scores 
            SET lifetime_score = ?, total_predictions = ?, last_update_timestamp = '{now}'
            WHERE miner_hotkey = ?
        ''', updates)

        cursor.executemany(f'''
            INSERT INTO miner_scores (miner_hotkey, lifetime_score, total_predictions, last_update_timestamp)
            VALUES (?, ?, ?, '{now}')
        ''', inserts)

    def _mark_predictions_as_scored(self, cursor, predictions_to_mark: List[Tuple]) -> None:
        now = datetime.now(timezone.utc).strftime(ISO8601)
        cursor.executemany(f'''
            UPDATE predictions
            SET scored = 1, score_timestamp = '{now}'
            WHERE property_id = ? AND miner_hotkey = ?
        ''', predictions_to_mark)

    def calculate_score(self, actual_price: str, predicted_price: str, actual_date: str, predicted_date: str):
        # Convert date strings to datetime objects
        actual_date = datetime.strptime(actual_date, ISO8601).date()
        predicted_date = datetime.strptime(predicted_date, "%Y-%m-%d").date()

        # Calculate the absolute difference in days
        date_difference = abs((actual_date - predicted_date).days)

        # Score based on date accuracy (14 points max, 1 point deducted per day off)
        date_score = (max(0, 14 - date_difference) / 14) * 100

        # Calculate price accuracy
        price_difference = abs(float(actual_price) - float(predicted_price)) / float(actual_price)
        price_score = max(0, 100 - (price_difference * 100))

        # Combine scores (86% weight to price, 14% weight to date)
        final_score = (price_score * 0.86) + (date_score * 0.14)

        return final_score

    def recalculate_all_scores(self):
        """
        Recalculate all miner scores using only the last two weeks of data.
        This method should be called once a day at 0 UTC.
        """
        bt.logging.info("Starting daily recalculation of all miner scores")
        
        two_weeks_ago = datetime.utcnow() - timedelta(days=14)
        
        cursor, db_connection = self.database_manager.get_cursor()
        try:
            # Clear all existing scores
            cursor.execute('DELETE FROM miner_scores') # TODO: move this to be after the loop after confirmation of success in this portion
            
            # Get all scorable predictions from the last two weeks
            cursor.execute('''
                SELECT predictions.miner_hotkey, 
                    predictions.predicted_sale_price, 
                    predictions.predicted_sale_date, 
                    sales.sale_price, 
                    sales.sale_date,
                    predictions.prediction_timestamp
                FROM predictions
                JOIN sales ON predictions.property_id = sales.property_id
                WHERE DATE(predictions.prediction_timestamp) <= DATE(sales.sale_date)
                AND DATE(predictions.prediction_timestamp) >= DATE(?)
            ''', (two_weeks_ago.strftime('%Y-%m-%d'),))
            
            predictions = cursor.fetchall()
            if not predictions:
                bt.logging.info("No predictions to recalculate. If this is NOT a new vali, this is concerning.")

            # Calculate new scores
            new_scores = defaultdict(lambda: {'total_score': 0, 'total_predictions': 0})
            for prediction in predictions:
                miner_hotkey, predicted_price, predicted_date, actual_price, actual_date, prediction_timestamp = prediction
                score = self.calculate_score(actual_price, predicted_price, actual_date, predicted_date)
                new_scores[miner_hotkey]['total_score'] += score
                new_scores[miner_hotkey]['total_predictions'] += 1
            
            # Insert new scores
            for miner_hotkey, data in new_scores.items():
                lifetime_score = data['total_score'] / data['total_predictions']
                cursor.execute('''
                    INSERT INTO miner_scores (miner_hotkey, lifetime_score, total_predictions, last_update_timestamp)
                    VALUES (?, ?, ?, datetime('now'))
                ''', (miner_hotkey, lifetime_score, data['total_predictions']))
            
            db_connection.commit()
            bt.logging.info(f"Recalculated scores for {len(new_scores)} miners")
        
        except Exception as e:
            bt.logging.error(f"Error in recalculating scores: {e}")
            db_connection.rollback()
        finally:
            cursor.close()
            db_connection.close()