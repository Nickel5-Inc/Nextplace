import time
import argparse
import bittensor as bt
import threading
import traceback
from datetime import datetime
from nextplace.validator.nextplace_validator import RealEstateValidator


def main(validator):
    step = 999  # Initialize step

    # Initialize last_recalculation to the current time
    last_recalculation = datetime.utcnow()

    while True:
        try:
            bt.logging.info(f"Validator step: {step}")

            if step % 5 == 0:  # See if it's time to set weights. If so, set weights.
                validator.check_timer_set_weights()

            validator.sync_metagraph()  # Sync metagraph

            current_time = datetime.utcnow()

            # Check if it's a new day compared to the last recalculation
            if current_time.date() > last_recalculation.date():
                # thread = threading.Thread(target=validator.scorer.scoring_calculator.recalculate_all_scores)
                # thread.start()
                last_recalculation = current_time

            validator.forward(step)  # Get predictions from the Miners

            if step >= 1000:  # Time to update scores and set weights
                thread = threading.Thread(target=validator.scorer.run_score_predictions, name="ScoreThread")  # Create thread
                thread.start()  # Start thread
                step = 0  # Reset the step

            step += 1  # Increment step
            time.sleep(5)  # Sleep for a bit

        except Exception as e:
            bt.logging.error(f"Error in main loop: {str(e)}")
            stack_trace = traceback.format_exc()
            bt.logging.error(f"Stack Trace: {stack_trace}")
            time.sleep(10)


if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser()
    bt.wallet.add_args(parser)
    bt.subtensor.add_args(parser)
    bt.logging.add_args(parser)

    parser.add_argument('--netuid', type=int, default=208, help="The chain subnet uid.")

    config = bt.config(parser)  # Build config object
    validator_instance = RealEstateValidator(config)  # Initialize the validator
    main(validator_instance)  # Run the main loop
