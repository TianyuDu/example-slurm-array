import os
import argparse
import pandas as pd


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract weighted ego networks and compute network statistics")
    parser.add_argument("--input_path", type=str, required=True, help="Path to the input file")
    parser.add_argument("--output_path", type=str, required=True, help="Path to save the network output files")
    args = parser.parse_args()

    # Get SLURM environment variables for chunking
    SLURM_ARRAY_TASK_ID = int(os.environ.get("SLURM_ARRAY_TASK_ID", 0))
    SLURM_ARRAY_TASK_COUNT = int(os.environ.get("SLURM_ARRAY_TASK_COUNT", 1))

    # Check if expected output files already exist
    expected_output_file = os.path.join(args.output_path, f"output_file_{SLURM_ARRAY_TASK_ID}_of_{SLURM_ARRAY_TASK_COUNT}.csv")

    if os.path.exists(expected_output_file):
        print(f"[INFO] All expected output files already exist for chunk {SLURM_ARRAY_TASK_ID}. Skipping processing.")
        exit(0)

    # Create output directory
    os.makedirs(args.output_path, exist_ok=True)

    df_input = pd.read_csv(args.input_path)

    total_num = len(df_input)
    bs = (total_num + SLURM_ARRAY_TASK_COUNT - 1) // SLURM_ARRAY_TASK_COUNT  # Ceiling division
    idx_start = int(SLURM_ARRAY_TASK_ID) * bs
    idx_end = min(total_num, (int(SLURM_ARRAY_TASK_ID) + 1) * bs)

    # Subset input file for this SLURM task
    df_input_subset = df_input.iloc[idx_start:idx_end]
    print(f"[INFO] Processing terms {idx_start} to {idx_end-1} of {total_num} total terms")
    print(f"[INFO] This task will process {len(df_input_subset)} terms")

    df_input_subset["SLURM_ARRAY_TASK_ID"] = SLURM_ARRAY_TASK_ID
    df_input_subset["SLURM_ARRAY_TASK_COUNT"] = SLURM_ARRAY_TASK_COUNT
    # process the input data.
    df_input_subset["output"] = df_input_subset["input"].apply(lambda x: x + 1)

    # save the output data.
    df_input_subset.to_csv(expected_output_file, index=False)
