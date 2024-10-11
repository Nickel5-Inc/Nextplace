import time
import argparse
import bittensor as bt
import threading
import traceback
from nextplace.validator.nextplace_validator import RealEstateValidator


def main(validator):
    step = 1  # Initialize step

    while True:
        validator.should_step = True
        try:
            bt.logging.info(f"🦶 Validator step: {step}")

            if step % 5 == 0:  # See if it's time to set weights. If so, set weights.
                validator.check_timer_set_weights()

            validator.sync_metagraph()  # Sync metagraph
            validator.forward(step)  # Get predictions from the Miners

            if step % 100 == 0:  # Check if any registrations/deregistrations have happened, make necessary updates
                thread = threading.Thread(target=validator.manage_miner_data, name="📋 MinerRegistrationThread 📋")
                thread.start()
            
            if step % 200 == 0:  # Send predictions to website
                thread = threading.Thread(target=validator.process_and_send_predictions, name="🛰️ WebsiteConnectionThread 🛰️")
                thread.start()

            if step >= 1000:  # Time to update scores and set weights
                thread = threading.Thread(target=validator.scorer.run_score_predictions, name="🏋🏻 ScoreThread 🏋🏻")  # Create thread
                thread.start()  # Start thread

                # Reset the step
                step = 1
                validator.should_step = False

            if validator.should_step:
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
