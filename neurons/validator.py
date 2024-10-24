import time
import argparse
import bittensor as bt
import threading
import traceback
from nextplace.validator.nextplace_validator import RealEstateValidator
from nextplace.validator.utils.contants import build_miner_predictions_table_name

SCORE_THREAD_NAME = "🏋🏻 ScoreThread 🏋🏻"

def main(validator):
    step = 1  # Initialize step
    current_thread = threading.current_thread().name

    check_and_migrate_predictions(validator)

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
                thread = threading.Thread(target=validator.manage_miner_data, name="📋 MinerRegistrationThread 📋")
                thread.start()

            if step % 200 == 0:  # Check that the scoring thread is running, if not, start it up
                scoring_thread_is_alive = validator.is_thread_running(SCORE_THREAD_NAME)
                if not scoring_thread_is_alive:
                    scoring_thread = threading.Thread(target=validator.scorer.run_score_thread, name=SCORE_THREAD_NAME)
                    scoring_thread.start()

            if step % 250 == 0:  # Print total predictions across all miners
                thread = threading.Thread(target=validator.print_total_number_of_predictions, name="🔮 PredictionCountingThread 🔮")
                thread.start()

            if step >= 1000:
                # Reset the step
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


def check_and_migrate_predictions(validator) -> None:
    """
    Migrate predictions if we need to

    Returns:
        None
    """
    current_thread = threading.current_thread().name
    bt.logging.trace(f"| {current_thread} | 🔎 Checking if we need to migrate predictions...")

    with validator.database_manager.lock:
        predictions_table_exists = validator.database_manager.table_exists('predictions')

    if not predictions_table_exists:
        return

    # Get all predictions tables
    all_table_query = "SELECT name FROM sqlite_master WHERE type='table'"

    with validator.database_manager.lock:
        all_tables = validator.database_manager.query(all_table_query)

    # Get all predictions tables
    predictions_tables = [table_result[0] for table_result in all_tables if 'predictions_' in table_result[0]]

    if len(predictions_tables) > 235:  # We've already migrated all the miners
        return

    bt.logging.trace(f"| {current_thread} | 💾 Migrating predictions, this may take a while...")

    with validator.database_manager.lock:
        all_hotkeys_in_predictions = [x[0] for x in validator.database_manager.query("SELECT DISTINCT(miner_hotkey) FROM predictions")]

    for idx, miner_hotkey in enumerate(all_hotkeys_in_predictions):

        # Check and set weights during migration
        if idx % 5 == 0:
            validator.check_timer_set_weights()

        # Build table name
        table_name = build_miner_predictions_table_name(miner_hotkey)
        bt.logging.trace(f"| {current_thread} | Migrating predictions to table '{table_name}'")

        # Create and index table
        create_str = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                nextplace_id TEXT,
                miner_hotkey TEXT,
                predicted_sale_price REAL,
                predicted_sale_date TEXT,
                prediction_timestamp TEXT,
                market TEXT,
                PRIMARY KEY (nextplace_id, miner_hotkey)
            )
        """
        idx_str = f"CREATE INDEX IF NOT EXISTS idx_prediction_timestamp ON {table_name}(prediction_timestamp)"

        with validator.database_manager.lock:
            validator.database_manager.query_and_commit(create_str)
            validator.database_manager.query_and_commit(idx_str)

        # Get unscored predictions for migration
        miner_predictions = validator.database_manager.query(f"""
            SELECT nextplace_id, miner_hotkey, predicted_sale_price, predicted_sale_date, prediction_timestamp, market
            FROM predictions
            WHERE miner_hotkey='{miner_hotkey}'
            AND scored IS NOT 1
        """)

        # Migrate predictions
        insert_query = f"""
            INSERT OR IGNORE INTO {table_name}
            (nextplace_id, miner_hotkey, predicted_sale_price, predicted_sale_date, prediction_timestamp, market)
            VALUES(?, ?, ?, ?, ?, ?)
        """

        with validator.database_manager.lock:
            validator.database_manager.query_and_commit_many(insert_query, miner_predictions)

        percent_done = round(((idx + 1) / len(all_hotkeys_in_predictions)) * 100, 2)
        bt.logging.trace(f"| {current_thread} | 📩 {percent_done}% done migrating predictions")


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
