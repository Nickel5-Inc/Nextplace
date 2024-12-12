from argparse import ArgumentParser
import sys
import bittensor as bt
from nextplace.miner.ml.model_loader import ModelArgs
from nextplace.miner.real_estate_miner import RealEstateMiner
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Build the ArgumentParser using wallet, subtensor, logging, and validator permit enforcing
def build_argument_parser() -> ArgumentParser:
    parser = ArgumentParser()  # create arg parser
    bt.wallet.add_args(parser)  # add wallet args
    bt.subtensor.add_args(parser)  # add subtensor args
    bt.logging.add_args(parser)  # add logging args
    bt.axon.add_args(parser)  # add axon args
    parser.add_argument("--blacklist.force_validator_permit", action="store_true", default=True)  # enforce validator permit
    parser.add_argument(
        "--model_source",
        default="",
        choices=["hugging_face", "local"],
        help="""
            <hugging_face | local>
            Where the model is located.
        """
    )
    parser.add_argument(
        "--netuid",
        default=1,
        type=int,
        help="""
            <int>
            The net UID you want to use
        """
    )
    parser.add_argument(
        "--force_update_past_predictions",
        default="false",
        choices=["true", "false"],
        help="""
            Add `--force_update_past_predictions true` if you want to use this
            Force validator to update past predictions. DANGER! This will overwrite all of your past predictions,
            and could cause some of your predictions to not be scored if that house sells *before* your prediction
            is updated.
        """
    )
    parser.add_argument(
        "--model_filename",
        default="",
        help="""
            <string>
            The name of the model file. Use this only if using Hugging Face.
        """
    )
    parser.add_argument(
        "--model_path",
        default="",
        help="""
            <DEFAULT | string>
            DEFAULT will run the base model.
            If not DEFAULT, pass either a Hugging Face model (username/model_name) or relative path to a local model.
        """
    )
    parser.add_argument(
        "--hugging_face_api_key",
        default="",
        help="""
            <string>
            Your Hugging Face API key. Use only if you are using a private Hugging Face model.
        """
    )
    return parser


def check_args(args: ModelArgs) -> None:

    # Extract data
    source = args['model_source']
    path = args['model_path']
    filename = args['model_class_filename']
    api_key = args['api_key']

    # If any of these fields are empty, the others need to be as well
    if source == '' or path == '' or filename == '':
        if not (source == '' and path == '' and filename == ''):
            bt.logging.error("If providing one of '--model_source', '--model_path', or '--model_class_filename', you must provide the others.")
            sys.exit(1)

    # Checks if an API key is provided
    if api_key != '':
        if source == 'local':  # '--model_source' must not be 'local'
            bt.logging.error("If providing an api key, you must specify `hugging_face` as your --model_source")
            sys.exit(1)
        if source == '' or path == '' or filename == '':  # All other fields must be specified
            bt.logging.error("If providing an api key, you must provide '--model_source', '--model_path', and '--model_class_filename' as well.")
            sys.exit(1)


# Build RealEstateMiner object, call .run() on it
def main():

    parser = build_argument_parser()  # get arg parser
    config = bt.config(parser)  # build config
    args = parser.parse_args()  # parse args

    # build arguments object for the Model class
    model_args = {
        'model_source': args.model_source,
        'model_path': args.model_path,
        'model_class_filename': args.model_filename,
        'api_key': args.hugging_face_api_key
    }

    force_update_past_predictions = args.force_update_past_predictions
    if force_update_past_predictions == 'true':
        force_update_past_predictions = True
    else:
        force_update_past_predictions = False

    check_args(model_args)

    miner = RealEstateMiner(model_args, force_update_past_predictions, config)  # instantiate Miner object

    bt.logging.info("Miner has been initialized and we are connected to the network. Calling miner.run()")
    miner.run()  # run the miner


# Entrypoint
if __name__ == "__main__":
    main()
