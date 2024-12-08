from nextplace.protocol import RealEstateSynapse, RealEstatePrediction
from nextplace.miner.ml.model_loader import ModelArgs
from nextplace.miner.ml.model_loader import ModelLoader
from nextplace.miner.ml.utils import prepare_input
import bittensor as bt

'''
This class facilitates running inference on data from a synapse using a model specified by the user
'''


class Model:

    def __init__(self, model_args: ModelArgs):
        model_loader = ModelLoader(model_args)
        self.model = model_loader.load_model()

    def run_inference(self, predictions: list[RealEstatePrediction]) -> None:
        """
        Run inference on the synapse using the loaded model. Update the synapse.

        Args:
            synapse (RealEstateSynapse): Incoming data from the validator

        Returns:
            None. Synapse is updated by reference.
        """
        for prediction in predictions:
            bt.logging.debug(f"DEBUG Processing prediction {prediction}")
            input_data = prepare_input(prediction)  # transform synapse into dictionary
            price, date = self.model.run_inference(input_data)  # run inference
            prediction.predicted_sale_price = price  # Update price by reference
            prediction.predicted_sale_date = date  # Update price by reference
