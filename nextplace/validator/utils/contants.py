
ISO8601 = "%Y-%m-%dT%H:%M:%SZ"
NUMBER_OF_PROPERTIES_PER_SYNAPSE = 100

def build_miner_predictions_table_name(miner_hotkey):
    return f"predictions_{miner_hotkey}"
