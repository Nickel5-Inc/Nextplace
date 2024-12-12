from nextplace.protocol import RealEstateSynapse
from nextplace.miner.ml.model_loader import ModelArgs
from datetime import datetime, timedelta

'''
This class facilitates running inference on data from a synapse using a model specified by the user
'''


class Model:

    def __init__(self, model_args: ModelArgs):
        self.model_args = model_args

    def run_inference(self, synapse: RealEstateSynapse) -> None:
        """
        使用簡單策略進行預測：
        - predicted_sale_price = 當前掛牌價格
        - predicted_sale_date = 查詢日期 + 30天
        """
        for prediction in synapse.real_estate_predictions.predictions:
            # 設置預測價格為當前掛牌價格
            if hasattr(prediction, 'price'):
                try:
                    prediction.predicted_sale_price = float(prediction.price)
                except (ValueError, TypeError):
                    prediction.predicted_sale_price = 0.0
            else:
                prediction.predicted_sale_price = 0.0
            
            # 設置預測日期為查詢日期 + 30天
            try:
                if hasattr(prediction, 'query_date'):
                    query_date = datetime.strptime(prediction.query_date, "%Y-%m-%d")
                    predicted_date = query_date + timedelta(days=30)
                else:
                    predicted_date = datetime.now() + timedelta(days=30)
                prediction.predicted_sale_date = predicted_date.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                predicted_date = datetime.now() + timedelta(days=30)
                prediction.predicted_sale_date = predicted_date.strftime("%Y-%m-%d")
