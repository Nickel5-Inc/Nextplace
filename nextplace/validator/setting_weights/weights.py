import torch
import bittensor as bt
import traceback
import threading
from datetime import datetime, timezone, timedelta
from nextplace.validator.scoring.time_gated_scorer import TimeGatedScorer
from nextplace.validator.utils.contants import build_miner_predictions_table_name
from nextplace import __spec_version__

class WeightSetter:
    def __init__(self, metagraph, wallet, subtensor, config, database_manager):
        self.metagraph = metagraph
        self.wallet = wallet
        self.subtensor = subtensor
        self.config = config
        self.database_manager = database_manager
        self.timer = datetime.now(timezone.utc)

    def is_time_to_set_weights(self) -> bool:
        """
        Check if it has been 1 hour since the timer was last reset
        Returns:
            True if it has been 1 hour, else False
        """
        now = datetime.now(timezone.utc)
        time_diff = now - self.timer
        return time_diff >= timedelta(hours=1)

    def check_timer_set_weights(self) -> None:
        """
        Set weights every 3 hours
        Returns:
            None
        """
        current_thread = threading.current_thread().name
        bt.logging.trace(f"📸 | {current_thread} | Time to set weights, resetting timer and setting weights.")
        self.timer = datetime.now(timezone.utc)  # Reset the timer
        self.set_weights()  # Set weights

    def calculate_miner_scores(self) -> dict[int, float]:
        """
        Calculate scores for miners
        Returns:
            Scores as a dict of UID: Score
        """
        current_thread = threading.current_thread().name
        time_gated_scorer = TimeGatedScorer(self.database_manager)

        miners = {uid: hotkey for uid, hotkey in enumerate(self.metagraph.hotkeys) if self.metagraph.S[uid] < 1000.0}
        bt.logging.debug(f"| {current_thread} | 🔎 Found {len(miners)} miners")
        scores = {uid: 0.0 for uid in miners}

        try:  # database_manager lock is already acquire at this point

            average_markets = self.get_average_markets_in_range()
            bt.logging.trace(f"| {current_thread} | ⏳ Iterating the metagraph and scoring miners...")

            for uid, hotkey in miners.items():
                score = time_gated_scorer.score(hotkey)
                table_name = build_miner_predictions_table_name(hotkey)

                # Handle the case where they're only targeting specific markets
                market_query = f"SELECT COUNT(DISTINCT(market)) FROM {table_name} WHERE prediction_timestamp >= datetime('now', '-5 days')"
                distinct_markets = self.database_manager.query(market_query)

                if len(distinct_markets) > 0:
                    distinct_markets = distinct_markets[0][0]
                    if distinct_markets < int(average_markets * 0.5):
                        score = score * 0.5
                    elif distinct_markets < int(average_markets * 0.75):
                        score = score * 0.6
                    elif distinct_markets < int(average_markets * 0.9):
                        score = score * 0.75

                scores[uid] = score

            bt.logging.trace(f"| {current_thread} | 🧾 Miner scores calculated.")
            return scores

        except Exception as e:
            bt.logging.error(f" | {current_thread} |❗Error fetching miner scores: {str(e)}")
            return {uid: 0.0 for uid in miners}

    def get_average_markets_in_range(self) -> float:
        """
        Calculate the average number of markets across all miners
        Returns:
            The mean number of markets across all miners
        """
        current_thread = threading.current_thread().name
        bt.logging.trace(f"| {current_thread} | 🧮 Calculating market cutoff...")
        all_table_query = "SELECT name FROM sqlite_master WHERE type='table'"
        all_tables = [x[0] for x in self.database_manager.query(all_table_query)]  # Get all tables in database
        predictions_tables = [s for s in all_tables if s.startswith("predictions_")]
        total = 0
        count = 0
        for predictions_table in predictions_tables:
            market_query = f"SELECT COUNT(DISTINCT(market)) FROM {predictions_table} WHERE prediction_timestamp >= datetime('now', '-5 days')"
            with self.database_manager.lock:
                results = self.database_manager.query(market_query)
                if len(results) > 0:
                    value = results[0][0]
                    if value > 0:
                        total += results[0][0]
                        count += 1
        if count == 0:
            bt.logging.debug(f"| {current_thread} | ❗ ERROR Found no predictions tables!")
        average = total / count
        bt.logging.trace(f"| {current_thread} | 🛒 Found {average} as the average number of markets predicted on in the last 5 days")
        return average

    def calculate_weights(self, scores: dict[int, float]) -> list[tuple[int, float]]:
        """
        Calculate weights for all miners
        Args:
            scores: The calculated scores for all miners

        Returns:
            List of tuples (uid, weight) for all miners
        """
        sorted_scores = list(sorted(scores.items(), key=lambda item: item[1], reverse=True))

        top_tier, middle_tier, bottom_tier = self.get_tiers(sorted_scores)
        weights = []

        for tier, weight in [(top_tier, 0.7), (middle_tier, 0.2), (bottom_tier, 0.1)]:
            tier_scores = self.apply_quadratic_scaling(tier)
            calculated_weights = self.calculate_tier_weights(tier_scores, weight)
            weights.extend(calculated_weights)

        return weights

    def get_tiers(self, sorted_indices: list[tuple[int, float]]) -> tuple[list[tuple[int, float]], list[tuple[int, float]], list[tuple[int, float]]]:
        """
        Divide the (uid, score) tuples into 3 tiers
        Args:
            sorted_indices: Sorted list of tuples (sorted by score)

        Returns:
            Sub lists representing the tiers
        """
        n_miners = len(sorted_indices)
        top_10_pct = max(1, int(0.1 * n_miners))
        next_40_pct = max(1, int(0.4 * n_miners))
        top_indices = sorted_indices[:top_10_pct]
        next_indices = sorted_indices[top_10_pct:top_10_pct + next_40_pct]
        bottom_indices = sorted_indices[top_10_pct + next_40_pct:]
        return top_indices, next_indices, bottom_indices

    def apply_quadratic_scaling(self, tier: list[tuple[int, float]]) -> list[tuple[int, float]]:
        """
        Square each score
        Args:
            tier: The current tier

        Returns:
            The update tier list
        """
        return [(miner[0], miner[1] ** 2) for miner in tier]

    def calculate_tier_weights(self, tier: list[tuple[int, float]], total_weight: float) -> list[tuple[int, float]]:
        """
        Calculate weights for all miners
        Args:
            tier: The tier scores for this tier
            total_weight: The weight for this tier

        Returns:
            Tensor of weights
        """
        tier_scores: list[float] = [x[1] for x in tier]
        sum_scores: float = sum(tier_scores)
        if sum_scores > 0:
            return [(miner[0], (miner[1] / sum_scores) * total_weight) for miner in tier]
        else:
            return [(miner[0], (miner[1] / total_weight) * len(tier_scores)) for miner in tier]

    def normalize_tuples(self, data: list[tuple[int, float]]) -> list[tuple[int, float]]:
        """
        Normalize the float values in a list of tuples to the range [0, 1].

        Args:
            data: A list of tuples where each tuple contains an int and a float.

        Returns:
            A new list of tuples with normalized float values.
        """
        # Extract the float values
        floats = [item[1] for item in data]

        # Find the min and max values
        min_val = min(floats)
        max_val = max(floats)

        # Avoid division by zero if all floats are the same
        if min_val == max_val:
            return [(item[0], 0.5) for item in data]  # Default to 0.5 if all values are identical

        # Normalize the floats and construct the new list of tuples
        return [(item[0], (item[1] - min_val) / (max_val - min_val)) for item in data]

    def set_weights(self):
        current_thread = threading.current_thread().name

        scores: dict[int, float] = self.calculate_miner_scores()
        miner_weights: list[tuple[int, float]] = self.calculate_weights(scores)

        # Initialize weights tensor to 0's
        weights: torch.Tensor = torch.tensor([0.0 for _ in range(len(self.metagraph.hotkeys))])

        # Update the UID indices of the tensor with the corresponding scores
        for miner in miner_weights:
            weights[miner[0]] = miner[1]

        list_weights = weights.tolist()
        bt.logging.info(f"| {current_thread} | ⚖️ Calculated weights: {list_weights}")

        try:
            uid = self.metagraph.hotkeys.index(self.wallet.hotkey.ss58_address)
            stake = float(self.metagraph.S[uid])

            if stake < 1000.0:
                bt.logging.trace(f"| {current_thread} | ❗Insufficient stake. Failed in setting weights.")
                return False

            result = self.subtensor.set_weights(
                netuid=self.config.netuid,
                wallet=self.wallet,
                uids=self.metagraph.uids,
                weights=weights,
                wait_for_inclusion=True,
                version_key=__spec_version__,
                wait_for_finalization=False,
            )

            success = result[0] if isinstance(result, tuple) and len(result) >= 1 else False

            if success:
                bt.logging.info(f"| {current_thread} | ✅ Successfully set weights.")
            else:
                bt.logging.trace(f"| {current_thread} | ❗Failed to set weights. Result: {result}")

        except Exception as e:
            bt.logging.error(f"| {current_thread} | ❗Error setting weights: {str(e)}")
            bt.logging.error(traceback.format_exc())
