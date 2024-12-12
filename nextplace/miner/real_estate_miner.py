import bittensor as bt
from template.base.miner import BaseMinerNeuron
from typing import Tuple
from nextplace.protocol import RealEstateSynapse
from nextplace.miner.ml.model import Model
from nextplace.miner.ml.model_loader import ModelArgs
from nextplace.miner.request_logger import RequestLogger
from datetime import datetime
import json


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
        start_time = datetime.now()
        
        # 記錄請求
        stake, uid = self.get_validator_stake_and_uid(synapse.dendrite.hotkey)
        
        # 將 synapse 轉換為可序列化的字典
        request_data = {
            'hotkey': synapse.dendrite.hotkey,
            'predictions': []
        }
        
        if hasattr(synapse, 'real_estate_predictions') and hasattr(synapse.real_estate_predictions, 'predictions'):
            for pred in synapse.real_estate_predictions.predictions:
                pred_dict = {
                    'nextplace_id': pred.nextplace_id if hasattr(pred, 'nextplace_id') else None,
                    'property_id': pred.property_id if hasattr(pred, 'property_id') else None,
                    'listing_id': pred.listing_id if hasattr(pred, 'listing_id') else None,
                    'address': pred.address if hasattr(pred, 'address') else None,
                    'price': pred.price if hasattr(pred, 'price') else None,
                    'market': pred.market if hasattr(pred, 'market') else None
                }
                request_data['predictions'].append(pred_dict)
        
        request_id = self.logger.log_request(
            hotkey=synapse.dendrite.hotkey,
            request_data=json.dumps(request_data),
            validator_uid=uid,
            validator_stake=stake
        )
        
        # 處理請求
        self.model.run_inference(synapse)
        self._set_force_update_prediction_flag(synapse)
        
        # 準備響應數據
        response_data = {
            'hotkey': synapse.dendrite.hotkey,
            'predictions': []
        }
        
        if hasattr(synapse, 'real_estate_predictions') and hasattr(synapse.real_estate_predictions, 'predictions'):
            for pred in synapse.real_estate_predictions.predictions:
                pred_dict = {
                    'nextplace_id': pred.nextplace_id if hasattr(pred, 'nextplace_id') else None,
                    'predicted_sale_price': pred.predicted_sale_price if hasattr(pred, 'predicted_sale_price') else None,
                    'predicted_sale_date': pred.predicted_sale_date if hasattr(pred, 'predicted_sale_date') else None
                }
                response_data['predictions'].append(pred_dict)
        
        # 記錄響應
        processing_time = (datetime.now() - start_time).total_seconds()
        self.logger.log_response(request_id, json.dumps(response_data), processing_time)
        
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
