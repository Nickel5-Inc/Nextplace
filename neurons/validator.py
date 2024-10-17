import time
import argparse
import bittensor as bt
import threading
import traceback
from nextplace.validator.nextplace_validator import RealEstateValidator
from nextplace.validator.utils.contants import build_miner_predictions_table_name


def main(validator):
    step = 1  # Initialize step
    current_thread = threading.current_thread().name

    check_and_migrate_predictions(validator)

    # Start the scoring thread
    scoring_thread = threading.Thread(target=validator.scorer.run_score_thread, name="🏋🏻 ScoreThread 🏋🏻")
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

        all_table_query = "SELECT name FROM sqlite_master WHERE type='table'"
        all_tables = [x[0] for x in validator.database_manager.query(all_table_query)]  # Get all tables in database
        miner_predictions_tables_exist = any("predictions_" in s for s in all_tables)  # Check if we have any tables that start with "predictions_"

        if miner_predictions_tables_exist:
            return

        bt.logging.trace(f"| {current_thread} | 💾 Migrating predictions, this may take a while...")

        all_hotkeys_in_predictions = validator.database_manager.query("SELECT DISTINCT(miner_hotkey) FROM predictions")
        for idx, miner_hotkey in enumerate(all_hotkeys_in_predictions):

            # Build table name
            table_name = build_miner_predictions_table_name(miner_hotkey)

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
            validator.database_manager.query_and_commit(create_str)
            validator.database_manager.query_and_commit(idx_str)

            # Get predictions
            miner_predictions = validator.database_manager.query(f"""
                SELECT nextplace_id, miner_hotkey, predicted_sale_price, predicted_sale_date, prediction_timestamp, market
                FROM predictions
                WHERE miner_hotkey='{miner_hotkey}'
            """)

            # Migrate predictions
            insert_query = f"""
                INSERT OR IGNORE INTO {table_name}
                (nextplace_id, miner_hotkey, predicted_sale_price, predicted_sale_date, prediction_timestamp, market),
                VALUES(?, ?, ?, ?, ?, ?)
            """
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
