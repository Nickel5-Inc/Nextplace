import statistics
from datetime import datetime, timezone
from nextplace.validator.database.database_manager import DatabaseManager
from nextplace.validator.utils.contants import ISO8601


class DailyScoreTableManager:
    def __init__(self, database_manager: DatabaseManager):
        self.database_manager = database_manager

    def populate(self):
        miner_date_map = self.build_miner_date_map()
        miner_data_map = self.build_miner_data_map(miner_date_map)
        self.update_daily_scores(miner_data_map)

    def build_miner_date_map(self) -> dict[str, str]:
        """
        Build a map with miner_hotkey -> earliest_date_from_daily_scores
        Returns:
            Map containing data about miners and their earliest existing daily_score row
        """
        miners = self.database_manager.query("""
            SELECT DISTINCT
                miner_hotkey
            FROM 
                daily_scores
        """)  # Query
        miners = [x[0] for x in miners]  # Format tuples
        miner_date_map = {}
        for miner in miners:  # Iterate hotkeys
            miner_date_map[miner] = self.get_most_recent_daily_score_date(miner)  # Set the date
        return miner_date_map

    def get_most_recent_daily_score_date(self, miner_hotkey: str) -> str:
        """
        Find the most date from the daily_scores table for a given miner
        Args:
            miner_hotkey: Miner's hotkey

        Returns:
            The earliest date
        """
        results = self.database_manager.query_with_values("""
            SELECT
                date
            FROM 
                daily_scores
            WHERE
                miner_hotkey = ?
            ORDER BY
                date
            LIMIT 1
        """, (miner_hotkey,))  # Query

        # Return the date that was found
        if results and len(results) > 0:
            return results[0][0]

        # Return today's date if they have no rows in daily_scores
        return f"{datetime.now(timezone.utc).date()}"

    def build_miner_data_map(self, miner_date_map: dict[str, str]):
        """
        Build a map containing hotkeys, and another map with the date and scores for that date
        Args:
            miner_date_map: Map of hotkey -> earliest date

        Returns:

        """
        miner_data_map = {}
        data = self.database_manager.query("""
                SELECT 
                    miner_hotkey,
                    DATE(score_timestamp),
                    predicted_sale_price,
                    sale_price,
                    predicted_sale_date,
                    sale_date
                FROM 
                    scored_predictions
                WHERE
                    DATE(score_timestamp) >= '2024-10-01'
            """)  # Query for scored predictions

        for row in data:  # Iterate scored predictions

            # Extract members
            miner_hotkey, date, predicted_price, sale_price, predicted_date, sale_date = row

            # Get the miner's most recent date in daily_scores
            try:
                miner_date = miner_date_map[miner_hotkey]
            except KeyError:
                # If they weren't found, continue
                continue

            # Format the relevant dates for comparison
            miner_date = datetime.strptime(miner_date, '%Y-%m-%d').date()
            formatted_score_date = datetime.strptime(date, '%Y-%m-%d').date()

            # Filter out any scored_predictions that are already in daily_scores
            if formatted_score_date >= miner_date:
                continue

            # Build map objects
            if miner_hotkey not in miner_data_map:
                miner_data_map[miner_hotkey] = {}
            if date not in miner_data_map[miner_hotkey]:
                miner_data_map[miner_hotkey][date] = []
            score = self.calculate_score(sale_price, predicted_price, sale_date, predicted_date)
            miner_data_map[miner_hotkey][date].append(score)

        return miner_data_map

    def calculate_score(self, actual_price: str, predicted_price: str, actual_date: str, predicted_date: str):
        """
        Taken from the scoring module
        Args:
            actual_price: actual sale price
            predicted_price: predicted sale price
            actual_date: actual sale date
            predicted_date: predicted sale date

        Returns:
            The score for this prediction
        """
        # Convert date strings to datetime objects
        actual_date = datetime.strptime(actual_date, ISO8601).date()

        try:
            predicted_date = datetime.strptime(predicted_date, "%Y-%m-%d").date()
        except ValueError:
            return None

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

    def update_daily_scores(self, miner_data_map) -> None:
        """
        Update the daily_scores table
        Args:
            miner_data_map: Data structure container miner data

        Returns:
            None
        """
        for hotkey, date_scores_Map in miner_data_map.items():  # Iterate map
            for date, scores in date_scores_Map.items():  # Iterate day's scores
                mean = statistics.mean(scores)  # Calculate mean of scores
                count = len(scores)  # Calculate total_predictions
                self.database_manager.query_and_commit_with_values("""
                    INSERT OR IGNORE INTO 
                        daily_scores (miner_hotkey, date, score, total_predictions)
                    VALUES 
                        (?, ?, ?, ?)
                """, (hotkey, date, mean, count))  # Query & commit
