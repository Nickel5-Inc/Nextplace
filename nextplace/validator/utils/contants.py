import bittensor as bt

ISO8601 = "%Y-%m-%dT%H:%M:%SZ"
NUMBER_OF_PROPERTIES_PER_SYNAPSE = 1200
SYNAPSE_TIMEOUT = 150


def build_miner_predictions_table_name(miner_hotkey):
    return f"predictions_{miner_hotkey}"

def get_miner_uids_from_metagraph(metagraph: bt.metagraph):
    return [uid for uid, dividend in enumerate(metagraph.dividends) if dividend == 0]

def get_miner_hotkeys(metagraph: bt.metagraph):
    miner_uids = set(get_miner_uids_from_metagraph(metagraph))
    return [hotkey for uid, hotkey in enumerate(metagraph.hotkeys) if uid in miner_uids]
