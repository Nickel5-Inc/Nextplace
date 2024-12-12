import bittensor as bt
from template.base.miner import BaseMinerNeuron
from typing import Tuple
from nextplace.protocol import RealEstateSynapse
from nextplace.miner.ml.model import Model
from nextplace.miner.ml.model_loader import ModelArgs
from nextplace.miner.request_logger import RequestLogger


class RealEstateMiner(BaseMinerNeuron):

    def __init__(self, model_args: ModelArgs, force_update_past_predictions: bool, config=None):
        super(RealEstateMiner, self).__init__(config=config)  # call superclass constructor
        self.logger = RequestLogger()  # Initialize the logger
        if force_update_past_predictions:
            bt.logging.trace("🦬 Forcing update of past predictions")
        else:
            bt.logging.trace("🐨 Not forcing update of past predictions")
        self.model = Model(model_args)
        self.force_update_past_predictions = force_update_past_predictions
        
        # 設置詳細的日誌級別
        bt.logging.info("🔄 初始化 RealEstateMiner，設置詳細日誌")
        
    # OVERRIDE | Required
    def forward(self, synapse: RealEstateSynapse) -> RealEstateSynapse:
        # Log the incoming request
        self.logger.log_request(synapse.dendrite.hotkey, str(synapse))
        
        # 添加詳細的 synapse 日誌
        bt.logging.info(f"📥 收到 Synapse 請求：")
        bt.logging.info(f"  - Hotkey: {synapse.dendrite.hotkey}")
        
        # 記錄預測請求的詳細信息
        if hasattr(synapse, 'real_estate_predictions') and hasattr(synapse.real_estate_predictions, 'predictions'):
            bt.logging.info("  - 預測請求詳情：")
            for idx, pred in enumerate(synapse.real_estate_predictions.predictions):
                bt.logging.info(f"    預測 {idx + 1}:")
                bt.logging.info(f"      - 房產ID: {pred.nextplace_id if hasattr(pred, 'nextplace_id') else 'N/A'}")
                bt.logging.info(f"      - 其他屬性: {vars(pred)}")
        
        self.model.run_inference(synapse)
        self._set_force_update_prediction_flag(synapse)
        
        # 記錄處理後的結果
        bt.logging.info("📤 處理完成，準備返回結果")
        
        return synapse

    def _set_force_update_prediction_flag(self, synapse: RealEstateSynapse):
        for prediction in synapse.real_estate_predictions.predictions:
            prediction.force_update_past_predictions = self.force_update_past_predictions

    # OVERRIDE | Required
    def blacklist(self, synapse: RealEstateSynapse) -> Tuple[bool, str]:

        # Check if synapse hotkey is in the metagraph
        if synapse.dendrite.hotkey not in self.metagraph.hotkeys:
            bt.logging.info(f"❗Blacklisted unknown hotkey: {synapse.dendrite.hotkey}")
            return True, f"❗Hotkey {synapse.dendrite.hotkey} was not found from metagraph.hotkeys",

        stake, uid = self.get_validator_stake_and_uid(synapse.dendrite.hotkey)

        # Check if validator has sufficient stake
        validator_min_stake = 0.0
        if stake < validator_min_stake:
            bt.logging.info(f"❗Blacklisted validator {synapse.dendrite.hotkey} with insufficient stake: {stake}")
            return True, f"❗Hotkey {synapse.dendrite.hotkey} has insufficient stake: {stake}",

        # Valid hotkey
        bt.logging.info(f"✅ Accepted hotkey: {synapse.dendrite.hotkey} (UID: {uid} - Stake: {stake})")
        return False, f"✅ Accepted hotkey: {synapse.dendrite.hotkey}"

    # OVERRIDE | Required
    def priority(self, synapse: RealEstateSynapse) -> float:
        bt.logging.debug(f"🧮 Calculating priority for synapse from {synapse.dendrite.hotkey}")
        stake, uid = self.get_validator_stake_and_uid(synapse.dendrite.hotkey)
        bt.logging.debug(f"🏆 Prioritized: {synapse.dendrite.hotkey} (UID: {uid} - Stake: {stake})")
        return stake

    # HELPER
    def get_validator_stake_and_uid(self, hotkey):
        uid = self.metagraph.hotkeys.index(hotkey)  # get uid
        return float(self.metagraph.S[uid]), uid  # return validator stake
