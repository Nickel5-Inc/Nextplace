import torch
import bittensor as bt
import traceback
import threading
from datetime import datetime, timezone, timedelta
from nextplace.validator.utils.system import timeout_with_multiprocess
from nextplace.validator.utils.contants import ISO8601


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
        Check if it has been 3 hours since the timer was last reset
        Returns:
            True if it has been 3 hours, else False
        """
        now = datetime.now(timezone.utc)
        time_diff = now - self.timer
        return time_diff >= timedelta(hours=3)

    def check_timer_set_weights(self) -> None:
        """
        Set weights every 3 hours
        Returns:
            None
        """
        bt.logging.trace("📸 Time to set weights, resetting timer and setting weights.")
        self.timer = datetime.now(timezone.utc)  # Reset the timer
        self.set_weights()  # Set weights
    
    def adjust_scores_based_on_recent_activity(self, scores, hotkey_to_uid):
        """
        Adjust scores to zero for miners with fewer than 10 predictions in the last 5 days.
        """
        # !!! IMPORTANT !!!
        # If we go back to using this idea, need to get data for each miner at a time, from that miner's predictions table

        # # Get the recent date threshold
        # recent_date = (datetime.now(timezone.utc) - timedelta(days=5)).strftime(ISO8601)
        #
        # # Query to get the count of recent predictions for each miner
        # query = f'''
        #     SELECT miner_hotkey, COUNT(*) as recent_count
        #     FROM predictions
        #     WHERE score_timestamp > '{recent_date}'
        #     GROUP BY miner_hotkey
        # '''
        # recent_counts = self.database_manager.query(query)
        #
        # # Build a dictionary of miner_hotkey to recent_count
        # recent_counts_dict = {miner_hotkey: recent_count for miner_hotkey, recent_count in recent_counts}
        #
        # # Set scores to 0 for miners with less than 10 recent predictions
        # for miner_hotkey, uid in hotkey_to_uid.items():
        #     recent_count = recent_counts_dict.get(miner_hotkey, 0)
        #     if recent_count < 8:
        #         scores[uid] = 0

    def calculate_miner_scores(self):
        try:  # database_manager lock is already acquire at this point
            results = self.database_manager.query("SELECT miner_hotkey, lifetime_score FROM miner_scores")

            scores = torch.zeros(len(self.metagraph.hotkeys))
            hotkey_to_uid = {hk: uid for uid, hk in enumerate(self.metagraph.hotkeys)}

            for miner_hotkey, lifetime_score in results:
                if miner_hotkey in hotkey_to_uid:
                    uid = hotkey_to_uid[miner_hotkey]
                    scores[uid] = lifetime_score
                
            # Adjust scores based on recent activity - tune these hyperparameters
            # self.adjust_scores_based_on_recent_activity(scores, hotkey_to_uid)

            return scores

        except Exception as e:
            bt.logging.error(f"❗Error fetching miner scores: {str(e)}")
            return torch.zeros(len(self.metagraph.hotkeys))

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
        next_indices = sorted_indices[top_10_pct:top_10_pct+next_40_pct]
        bottom_indices = sorted_indices[top_10_pct+next_40_pct:]
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
        current_thread = threading.current_thread()
        # Sync the metagraph to get the latest data
        self.metagraph.sync(subtensor=self.subtensor, lite=True)

        scores = self.calculate_miner_scores()
        weights = self.calculate_weights(scores)

        bt.logging.info(f"| {current_thread.name} | ⚖️ Calculated weights: {weights}")

        try:
            uid = self.metagraph.hotkeys.index(self.wallet.hotkey.ss58_address)
            stake = float(self.metagraph.S[uid])

            if stake < 1000.0:
                bt.logging.error(f"| {current_thread.name} | Insufficient stake. Failed in setting weights.")
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
                bt.logging.info(f"| {current_thread.name} | ✅ Successfully set weights.")
            else:
                bt.logging.error(f"| {current_thread.name} | ❗Failed to set weights. Result: {result}")

        except Exception as e:
            bt.logging.error(f"| {current_thread.name} | ❗Error setting weights: {str(e)}")
            bt.logging.error(traceback.format_exc())
