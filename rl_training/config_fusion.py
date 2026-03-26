"""
Baseten training config: Model B — dbt-fusion as reward compiler.

Usage:
  truss train push config_fusion.py
"""

from truss_train import definitions
from truss.base import truss_config

BASE_IMAGE = "verlai/verl:app-verl0.5-transformers4.55.4-vllm0.10.0-mcore0.13.0-te2.2"

training_runtime = definitions.Runtime(
    start_commands=[
        "pip install verl==0.6.1 dbt-core>=1.9.0 dbt-duckdb>=1.9.0 duckdb>=1.0.0 requests",
        # Install dbt-fusion binary (fallback if Modal is unavailable)
        "curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh | sh -s -- --update || true",
        "mv /root/.local/bin/dbt /usr/local/bin/dbt-fusion || true",
        "chmod +x ./run.sh",
        "./run.sh",
    ],
    environment_variables={
        "DBT_COMPILER": "dbt-fusion",
        "DBT_CORE_BIN": "dbt",
        "DBT_FUSION_BIN": "/usr/local/bin/dbt-fusion",
        "USE_MODAL": "true",
        "MODAL_REWARD_URL": "https://sai-maddali3--dbt-coder-reward-reward-endpoint.modal.run",
    },
    cache_config=definitions.CacheConfig(enabled=True),
    checkpointing_config=definitions.CheckpointingConfig(
        enabled=True,
        checkpoint_path="/tmp/checkpoints",
    ),
)

training_compute = definitions.Compute(
    accelerator=truss_config.AcceleratorSpec(
        accelerator=truss_config.Accelerator.H100,
        count=4,
    ),
    node_count=1,
)

training_job = definitions.TrainingJob(
    image=definitions.Image(base_image=BASE_IMAGE),
    compute=training_compute,
    runtime=training_runtime,
)

_ = definitions.TrainingProject(
    name="dbt-coder-rl-fusion",
    job=training_job,
)
