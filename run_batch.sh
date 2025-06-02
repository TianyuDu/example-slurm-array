#!/bin/bash
#
#SBATCH --job-name=example-array-job
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=1G
#SBATCH --time=0:30:00
#SBATCH --partition=owners
#SBATCH --mail-type=ALL
#SBATCH --array=0-99
#SBATCH --output=slurm_logs/build-term-centric-networks-%A_%a.out

mkdir -p slurm_logs

ml py-pytorch/2.4.1_py312

python3.12 process.py \
    --input_path="input.csv" \
    --output_path="./output"