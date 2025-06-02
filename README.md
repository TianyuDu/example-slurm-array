# example-slurm-array

1. run
    - input: the input data
    - output: the output data
2. run the script:
    ```bash
    sbatch run_batch.sh
    ```
3. post-process the output:
    ```bash
    python post_analysis.ipynb
    ```