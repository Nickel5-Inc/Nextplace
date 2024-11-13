import time
import argparse
import bittensor as bt
import threading
import traceback
from nextplace.validator.nextplace_validator import RealEstateValidator

SCORE_THREAD_NAME = "🏋🏻 ScoreThread 🏋"


def main(validator):
    step = 999  # Initialize step
    current_thread = threading.current_thread().name

    # Start the scoring thread
    scoring_thread = threading.Thread(target=validator.scorer.run_score_thread, name=SCORE_THREAD_NAME)
    scoring_thread.start()

    while True:
        validator.should_step = True
        try:
            bt.logging.info(f"| {current_thread} | 🦶 Validator step: {step}")

            if step % 5 == 0:  # See if it's time to set weights. If so, set weights.
                validator.check_timer_set_weights()

            validator.sync_metagraph()  # Sync metagraph
            validator.forward(step)  # Get predictions from the Miners

            if step % 100 == 0:  # Check if any registrations/deregistrations have happened, make necessary updates
                thread = threading.Thread(target=validator.miner_manager.manage_miner_data, name="📋 MinerManagementThread 📋")
                thread.start()

            if step % 200 == 0:  # Check that the scoring thread is running, if not, start it up
                scoring_thread_is_alive = validator.is_thread_running(SCORE_THREAD_NAME)
                if not scoring_thread_is_alive:
                    bt.logging.info(f"| {current_thread} | ☢️ ScoreThread was found not running, restarting it...")
                    scoring_thread = threading.Thread(target=validator.scorer.run_score_thread, name=SCORE_THREAD_NAME)
                    scoring_thread.start()

            if step % 500 == 0:  # Send miner scores data to website
                thread = threading.Thread(target=validator.miner_score_sender.send_miner_scores_to_website, name="🌊 MinerScoresToWebsiteThread 🌊")
                thread.start()

            if step >= 1000:  # Reset the step
                step = 1
                validator.should_step = False

            if validator.should_step:
                step += 1  # Increment step

            time.sleep(5)  # Sleep for a bit

        except Exception as e:
            bt.logging.error(f"| {current_thread} | Error in main loop: {str(e)}")
            stack_trace = traceback.format_exc()
            bt.logging.error(f"| {current_thread} | Stack Trace: {stack_trace}")
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
