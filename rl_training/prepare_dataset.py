"""
Prepare dbt RL prompts for VeRL training.

Converts our prompt dataset into VeRL's expected parquet format with:
- prompt: list of message dicts (system + user)
- data_source: identifier
- reward_model: {"style": "rule", "ground_truth": 0.0}
- extra_info: model_name, expected_columns, expected_row_count, unique_key
"""

import os
import random
import argparse
import datasets
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "rl_sandbox"))
from prompts import RL_PROMPTS

with open(os.path.join(os.path.dirname(__file__), "system_prompt.txt")) as f:
    SYSTEM_PROMPT = f.read()

DATA_SOURCE = "dbt-coder"


def validate_prompts(prompts):
    """Check for duplicate model_names and invalid references."""
    names = [p["model_name"] for p in prompts]
    dupes = [n for n in set(names) if names.count(n) > 1]
    if dupes:
        raise ValueError(f"Duplicate model_names: {dupes}")
    print(f"Validated {len(prompts)} prompts, 0 duplicates")


def make_verl_dataset(prompts, split="train"):
    records = []
    for idx, p in enumerate(prompts):
        record = {
            "data_source": DATA_SOURCE,
            "prompt": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": p["prompt"]},
            ],
            "ability": "dbt",
            "reward_model": {"style": "rule", "ground_truth": 0.0},
            "extra_info": {
                "split": split,
                "index": idx,
                "model_name": p["model_name"],
                "expected_columns": p.get("expected_columns", []),
                "expected_row_count": p.get("expected_row_count"),
                "unique_key": p.get("unique_key"),
                "schema_yml": p.get("schema_yml", ""),
            },
        }
        records.append(record)
    return datasets.Dataset.from_list(records)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--local_dir", default="/workspace/data/dbt/")
    parser.add_argument("--test_ratio", type=float, default=0.15,
                        help="Fraction of prompts for test split (default 0.15)")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    os.makedirs(args.local_dir, exist_ok=True)

    validate_prompts(RL_PROMPTS)

    # Fail loudly if prompt count is suspiciously low (catches stale copies)
    MIN_EXPECTED_PROMPTS = 400
    if len(RL_PROMPTS) < MIN_EXPECTED_PROMPTS:
        raise RuntimeError(
            f"ABORT: Only {len(RL_PROMPTS)} prompts found — expected {MIN_EXPECTED_PROMPTS}+. "
            f"rl_training/prompts.py is likely stale. "
            f"Run: cp ../rl_sandbox/prompts.py ./prompts.py"
        )

    # Shuffle and split
    shuffled = list(RL_PROMPTS)
    random.seed(args.seed)
    random.shuffle(shuffled)

    split_idx = int(len(shuffled) * (1 - args.test_ratio))
    train_prompts = shuffled[:split_idx]
    test_prompts = shuffled[split_idx:]

    train_ds = make_verl_dataset(train_prompts, "train")
    test_ds = make_verl_dataset(test_prompts, "test")

    train_ds.to_parquet(os.path.join(args.local_dir, "train.parquet"))
    test_ds.to_parquet(os.path.join(args.local_dir, "test.parquet"))

    print(f"Saved {len(train_ds)} train, {len(test_ds)} test prompts to {args.local_dir}")
    print(f"Split: {100*(1-args.test_ratio):.0f}/{100*args.test_ratio:.0f} "
          f"(seed={args.seed})")
