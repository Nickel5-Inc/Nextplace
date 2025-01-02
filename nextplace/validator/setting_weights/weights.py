import torch
import bittensor as bt
import traceback
import threading
from datetime import datetime, timezone, timedelta
from nextplace.validator.scoring.time_gated_scorer import TimeGatedScorer
from nextplace.validator.utils.contants import build_miner_predictions_table_name


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
        # return time_diff >= timedelta(hours=1)
        return time_diff >= timedelta(minutes = 0)

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

    def calculate_miner_scores(self):
        current_thread = threading.current_thread().name
        time_gated_scorer = TimeGatedScorer(self.database_manager)

        # ToDo Get list of miner-only hotkeys
        stake_uid_0 = self.metagraph.metagraph.S[0]
        stake_uid_1 = self.metagraph.metagraph.S[1]
        bt.logging.debug(f"| {current_thread} | 🪲 First metagraph hotkey: '{self.metagraph.hotkeys[0]}', stake: {stake_uid_0}")
        bt.logging.debug(f"| {current_thread} | 🪲 Second metagraph hotkey: '{self.metagraph.hotkeys[1]}', stake: {stake_uid_1}")
        miner_hotkeys = [hotkey for uid, hotkey in enumerate(self.metagraph.hotkeys) if self.metagraph.S[uid] < 1000.0]

        bt.logging.debug(f"| {current_thread} | 🪲 Found {len(miner_hotkeys)} miner hotkeys, {len(self.metagraph.hotkeys)} miner hotkeys total hotkeys")

        try:  # database_manager lock is already acquire at this point

            # ToDo This is for testing only!
            # average_markets = self.get_average_markets_in_range()
            average_markets = 100

            scores = torch.zeros(len(self.metagraph.hotkeys))
            hotkey_to_uid = {hk: uid for uid, hk in enumerate(self.metagraph.hotkeys)}

            bt.logging.trace(f"| {current_thread} | ⏳ Iterating the metagraph and scoring miners...")

            for miner_hotkey in miner_hotkeys:
                if miner_hotkey in hotkey_to_uid:
                    score = time_gated_scorer.score(miner_hotkey)
                    table_name = build_miner_predictions_table_name(miner_hotkey)
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

                    uid = hotkey_to_uid[miner_hotkey]
                    scores[uid] = score

            bt.logging.trace(f"| {current_thread} | 🧾 Miner scores calculated.")

            return scores

        except Exception as e:
            bt.logging.error(f" | {current_thread} |❗Error fetching miner scores: {str(e)}")
            return torch.zeros(len(self.metagraph.hotkeys))

    def get_average_markets_in_range(self):
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

    def calculate_weights(self, scores):
        n_miners = len(scores)
        sorted_indices = torch.argsort(scores, descending=True)
        weights = torch.zeros(n_miners)

        top_indices, next_indices, bottom_indices = self.get_tier_indices(sorted_indices, n_miners)

        for indices, weight in [(top_indices, 0.7), (next_indices, 0.2), (bottom_indices, 0.1)]:
            tier_scores = self.apply_quadratic_scaling(scores[indices])
            weights[indices] = self.calculate_tier_weights(tier_scores, weight)

        weights /= weights.sum()  # Normalize weights to sum to 1.0
        return weights

    def get_tier_indices(self, sorted_indices, n_miners):
        top_10_pct = max(1, int(0.1 * n_miners))
        next_40_pct = max(1, int(0.4 * n_miners))
        top_indices = sorted_indices[:top_10_pct]
        next_indices = sorted_indices[top_10_pct:top_10_pct + next_40_pct]
        bottom_indices = sorted_indices[top_10_pct + next_40_pct:]
        return top_indices, next_indices, bottom_indices

    def apply_quadratic_scaling(self, scores):
        return scores ** 2

    def calculate_tier_weights(self, tier_scores, total_weight):
        sum_scores = tier_scores.sum()
        if sum_scores > 0:
            return (tier_scores / sum_scores) * total_weight
        else:
            return torch.full_like(tier_scores, total_weight / len(tier_scores))

    def set_weights(self):
        current_thread = threading.current_thread().name

        scores = self.calculate_miner_scores()
        weights = self.calculate_weights(scores)

        bt.logging.info(f"| {current_thread} | ⚖️ Calculated weights: {weights}")

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
