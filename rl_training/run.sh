#!/bin/bash
# GRPO training for dbt-coder using VeRL.
#
# Expects train.parquet and test.parquet to already exist in ./data/
# (built locally by train.sh before pushing to Baseten)

set -eux

# Verify data exists (fail fast if train.sh didn't prep correctly)
if [[ ! -f ./data/train.parquet ]] || [[ ! -f ./data/test.parquet ]]; then
    echo "ERROR: Missing parquet files in ./data/. Run train.sh locally first."
    exit 1
fi

TRAIN_COUNT=$(python3 -c "import pandas as pd; print(len(pd.read_parquet('./data/train.parquet')))" 2>/dev/null || echo "0")
echo "Training with $TRAIN_COUNT prompts"
if [[ "$TRAIN_COUNT" -lt 400 ]]; then
    echo "ERROR: Only $TRAIN_COUNT training prompts — expected 400+. Data is stale."
    exit 1
fi

# Download base model
HF_HOME=$BT_RW_CACHE_DIR/huggingface
huggingface-cli download Qwen/Qwen2.5-Coder-7B-Instruct

# Launch VeRL — reward_function.py calls Modal directly for scoring
python3 -m verl.trainer.main_ppo \
    custom_reward_function.path=reward_function.py \
    custom_reward_function.name=compute_score \
    algorithm.adv_estimator=grpo \
    data.train_files=./data/train.parquet \
    data.val_files=./data/test.parquet \
    data.train_batch_size=8 \
    data.max_prompt_length=2048 \
    data.max_response_length=4096 \
    data.filter_overlong_prompts=True \
    data.truncation='error' \
    actor_rollout_ref.model.path=Qwen/Qwen2.5-Coder-7B-Instruct \
    actor_rollout_ref.actor.optim.lr=5e-5 \
    actor_rollout_ref.model.use_remove_padding=True \
    actor_rollout_ref.model.lora_rank=16 \
    actor_rollout_ref.model.lora_alpha=32 \
    actor_rollout_ref.actor.ppo_mini_batch_size=4 \
    actor_rollout_ref.actor.ppo_micro_batch_size_per_gpu=1 \
    actor_rollout_ref.actor.use_kl_loss=True \
    actor_rollout_ref.actor.kl_loss_coef=0.005 \
    actor_rollout_ref.actor.kl_loss_type=low_var_kl \
    actor_rollout_ref.rollout.calculate_log_probs=True \
    actor_rollout_ref.actor.entropy_coeff=0 \
    actor_rollout_ref.model.enable_gradient_checkpointing=True \
    actor_rollout_ref.actor.fsdp_config.param_offload=False \
    actor_rollout_ref.actor.fsdp_config.optimizer_offload=False \
    actor_rollout_ref.actor.checkpoint.save_contents=[model,optimizer,extra,hf_model] \
    actor_rollout_ref.rollout.log_prob_micro_batch_size_per_gpu=1 \
    actor_rollout_ref.rollout.tensor_model_parallel_size=2 \
    actor_rollout_ref.rollout.name=vllm \
    actor_rollout_ref.rollout.gpu_memory_utilization=0.85 \
    actor_rollout_ref.rollout.n=5 \
    actor_rollout_ref.ref.log_prob_micro_batch_size_per_gpu=1 \
    actor_rollout_ref.ref.fsdp_config.param_offload=True \
    actor_rollout_ref.rollout.max_num_batched_tokens=8192 \
    actor_rollout_ref.rollout.max_model_len=8192 \
    algorithm.use_kl_in_reward=False \
    trainer.critic_warmup=0 \
    trainer.logger=['console'] \
    trainer.project_name='dbt-coder-rl' \
    trainer.experiment_name=$BT_TRAINING_JOB_NAME \
    trainer.n_gpus_per_node=$BT_NUM_GPUS \
    trainer.nnodes=$BT_GROUP_SIZE \
    trainer.default_local_dir=$BT_CHECKPOINT_DIR \
    trainer.save_freq=16 \
    trainer.test_freq=8 \
    trainer.total_epochs=3 $@
