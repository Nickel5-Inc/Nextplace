from nextplace.validator.database.database_manager import DatabaseManager

ISO8601 = "%Y-%m-%dT%H:%M:%SZ"
NUMBER_OF_PROPERTIES_PER_SYNAPSE = 1200
SYNAPSE_TIMEOUT = 150


def build_miner_predictions_table_name(miner_hotkey):
    return f"predictions_{miner_hotkey}"


def get_miner_hotkeys_from_predictions_tables(database_manager: DatabaseManager) -> list[str]:
    query = """
        SELECT name 
        FROM sqlite_master 
        WHERE type='table' AND name LIKE 'predictions_%';
    """
    with database_manager.lock:
        results = database_manager.query(query)
    return [table[0][len("predictions_"):] for table in results]
