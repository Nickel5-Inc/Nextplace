from nextplace.validator.database.database_manager import DatabaseManager
from nextplace.validator.scoring.time_gated_scorer import TimeGatedScorer
import bittensor as bt


def main():
    database_manager = DatabaseManager()
    hotkeys_to_test = [
        "5F1SGqHYbvaa68EbVEjJ1C8C9gDrMBd8PnkGmR7GzGNnf8A1",
        "5CDUMkVRixHStUXsHFEDGDEuHYnCjbzERCfRRVNLfhuhz1Ae",
        "5DP444BfZekJkpJVfmctr38PaoxtD93okekPoa8bXfsiXYBK",
        "5H1mgA775tz43Lw6je5BjrG41RnHtFeBJfEcgnSSN2mDkGPq",
        "5FWAJmUridD8q4NkQvgoSheATXtVh7qxkT9UU5RXFDeNSLAV",
    ]
    time_gated_scorer = TimeGatedScorer(database_manager)
    for hotkey in hotkeys_to_test:
        score = time_gated_scorer.score(hotkey)
        bt.logging.info(f"| TIME GATED SCORE TESTER | 🧪 '{hotkey}' Score: {score}'")

if __name__ == '__main__':
    main()