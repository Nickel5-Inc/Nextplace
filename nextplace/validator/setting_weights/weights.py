import torch
import bittensor as bt
import traceback
import threading
from scipy.optimize import minimize_scalar
from datetime import datetime, timezone, timedelta

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
        Check if it has been 2.5 hours since the timer was last reset
        Returns:
            True if it has been 2.5 hours, else False
        """
        now = datetime.now(timezone.utc)
        time_diff = now - self.timer
        return time_diff >= timedelta(hours=3)

    def check_timer_set_weights(self) -> None:
        """
        Set weights every 2.5 hours
        Returns:
            None
        """
        bt.logging.trace("📸 Time to set weights, resetting timer and setting weights.")
        self.timer = datetime.now(timezone.utc)  # Reset the timer
        self.set_weights()  # Set weights

    def calculate_miner_scores(self):
        try:  # database_manager lock is already acquire at this point
            results = self.database_manager.query("SELECT miner_hotkey, lifetime_score FROM miner_scores")

            scores = torch.zeros(len(self.metagraph.hotkeys))
            hotkey_to_uid = {hk: uid for uid, hk in enumerate(self.metagraph.hotkeys)}

            for miner_hotkey, lifetime_score in results:
                if miner_hotkey in hotkey_to_uid:
                    uid = hotkey_to_uid[miner_hotkey]
                    scores[uid] = lifetime_score

            return scores

        except Exception as e:
            bt.logging.error(f"❗Error fetching miner scores: {str(e)}")
            return torch.zeros(len(self.metagraph.hotkeys))

    def calculate_weights(self, scores):
        # Sort miners by score in descending order
        sorted_indices = torch.argsort(scores, descending=True)
        n_miners = len(scores)

        # Computes the optimal constant for the rewards distribution
        def compute_error(lambda_):
            # Compute weights using exponential decay
            ranks = torch.arange(n_miners, dtype=torch.float32)
            weights = torch.exp(-lambda_ * ranks)
            weights = weights / weights.sum()

            # Compute cumulative sums at desired percentiles
            cum_weights = torch.cumsum(weights, dim=0)

            idx_10 = max(1, int(0.1 * n_miners) - 1)
            idx_50 = max(1, int(0.5 * n_miners) - 1)

            C10 = cum_weights[idx_10].item()
            C50 = cum_weights[idx_50].item()

            # Error based on the difference from desired cumulative percentages
            error = (C10 - 0.4)**2 + (C50 - 0.8)**2
            return error

        # Optimize lambda to minimize the error
        try:
            res = minimize_scalar(compute_error, bounds=(0.001, 10), method='bounded')
            lambda_opt = res.x

            # Compute final weights with the optimized lambda
            ranks = torch.arange(n_miners, dtype=torch.float32)
            weights = torch.exp(-lambda_opt * ranks)
            weights = weights / weights.sum()

            # Map weights back to the original miner indices
            final_weights = torch.zeros_like(weights)
            final_weights[sorted_indices] = weights

            return final_weights

        except Exception as e:
            bt.logging.error(f"Error calculating weights: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return torch.zeros(n_miners)

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
                wait_for_finalization=True,
            )

            success = result[0] if isinstance(result, tuple) and len(result) >= 1 else False

            if success:
                bt.logging.info(f"| {current_thread.name} | ✅ Successfully set weights.")
            else:
                bt.logging.error(f"| {current_thread.name} | ❗Failed to set weights. Result: {result}")

        except Exception as e:
            bt.logging.error(f"| {current_thread.name} | ❗Error setting weights: {str(e)}")
            bt.logging.error(traceback.format_exc())
