import torch
import bittensor as bt
import traceback
import threading

class WeightSetter:
    def __init__(self, metagraph, wallet, subtensor, config, database_manager):
        self.metagraph = metagraph
        self.wallet = wallet
        self.subtensor = subtensor
        self.config = config
        self.database_manager = database_manager

    def calculate_miner_scores(self):
        # Use the database_manager to get cursor and connection
        # cursor, conn = self.database_manager.get_cursor()

        try:
            with self.database_manager.lock:
                results = self.database_manager.query("SELECT miner_hotkey, lifetime_score FROM miner_scores")
            # results = cursor.fetchall()

            scores = torch.zeros(len(self.metagraph.hotkeys))
            hotkey_to_uid = {hk: uid for uid, hk in enumerate(self.metagraph.hotkeys)}

            for miner_hotkey, lifetime_score in results:
                if miner_hotkey in hotkey_to_uid:
                    uid = hotkey_to_uid[miner_hotkey]
                    scores[uid] = lifetime_score

            return scores

        except Exception as e:
            bt.logging.error(f"Error fetching miner scores: {str(e)}")
            return torch.zeros(len(self.metagraph.hotkeys))

        # finally:
        #     cursor.close()
        #     conn.close()

    def calculate_weights(self, scores):
        # Sort miners by score in descending order
        sorted_indices = torch.argsort(scores, descending=True)
        n_miners = len(scores)

        # Calculate the number of miners in each tier
        top_25_percent = max(1, n_miners // 4)
        next_25_percent = max(1, n_miners // 4)
        remaining = n_miners - top_25_percent - next_25_percent

        # Create a weights tensor
        weights = torch.zeros_like(scores)

        # Assign weights based on tiers
        weights[sorted_indices[:top_25_percent]] = 0.5 / top_25_percent
        weights[sorted_indices[top_25_percent:top_25_percent+next_25_percent]] = 0.2 / next_25_percent
        if remaining > 0:
            weights[sorted_indices[top_25_percent+next_25_percent:]] = 0.3 / remaining

        # Ensure weights sum to 1
        weights = weights / weights.sum()

        return weights

    def set_weights(self):
        current_thread = threading.current_thread()
        # Sync the metagraph to get the latest data
        self.metagraph.sync(subtensor=self.subtensor, lite=True)

        # with self.database_manager.lock:
        scores = self.calculate_miner_scores()
        weights = self.calculate_weights(scores)

        bt.logging.info(f"| {current_thread.name} | Calculated weights: {weights}")

        try:
            uid = self.metagraph.hotkeys.index(self.wallet.hotkey.ss58_address)
            stake = float(self.metagraph.S[uid])

            if stake < 0.0:
                bt.logging.error(f"| {current_thread.name} | Insufficient stake. Failed in setting weights.")
                return False

            result = self.subtensor.set_weights(
                netuid=self.config.netuid,
                wallet=self.wallet,
                uids=self.metagraph.uids,
                weights=weights,
                #version_key=__spec_version__,
                wait_for_inclusion=True,
                wait_for_finalization=True,
            )

            success = result[0] if isinstance(result, tuple) and len(result) >= 1 else False

            if success:
                bt.logging.info(f"| {current_thread.name} | Successfully set weights.")
            else:
                bt.logging.error(f"| {current_thread.name} | Failed to set weights. Result: {result}")

        except Exception as e:
            bt.logging.error(f"| {current_thread.name} | Error setting weights: {str(e)}")
            bt.logging.error(traceback.format_exc())
