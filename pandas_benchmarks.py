import os
import time
import pandas as pd

def print_memory_usage(df):
  memory_usage_per_col = df.memory_usage()
  return f"  MEMORY USAGE: {memory_usage_per_col.sum()} B\n"

def benchmark_nyc_pandas(file="results/nyc_pandas.txt"):
  with open(file, "w") as output_file:
    # Initialization. 
    output_file.write("Initialization: _read_csv\n")
    start_time = time.time()
    df = pd.read_csv('datasets/files/central_park_weather.csv')
    end_time = time.time()

    execution_time = (end_time - start_time) * 1000
    output_file.write(f"  EXECUTION TIME: {execution_time} ms\n")
    output_file.write(print_memory_usage(df))


    # Sort by SNWD.
    output_file.write("\nSort by SNWD\n") 
    start_time = time.time()
    df.sort_values("SNWD")
    end_time = time.time()

    execution_time = (end_time - start_time) * 1000
    output_file.write(f"  EXECUTION TIME: {execution_time} ms\n")
    output_file.write(print_memory_usage(df))

    # Filter.
    output_file.write("\nFilter: SNWD != 0\n")
    start_time = time.time()
    df = df[df["SNWD"] != 0]
    end_time = time.time()

    execution_time = (end_time - start_time) * 1000
    output_file.write(f"  EXECUTION TIME: {execution_time} ms\n")
    output_file.write(print_memory_usage(df))


    # Merge.
    output_file.write("\nMerge: self merge on DATE\n")
    start_time = time.time()
    merged_df = pd.merge(df, df, on="DATE", suffixes=('_original', '_merged'))
    end_time = time.time()

    execution_time = (end_time - start_time) * 1000
    output_file.write(f"  EXECUTION TIME: {execution_time} ms\n")
    output_file.write("merged_df:\n")
    output_file.write(print_memory_usage(merged_df))


    # To csv.
    output_file.write("\nto_csv\n")
    to_csv_file = "results/nyc_pandas.csv"
    start_time = time.time()
    df.to_csv(to_csv_file)
    end_time = time.time()
    output_file.write(f"  EXECUTION TIME: {execution_time} ms\n")
    os.remove(to_csv_file) # Remove the saved file.

    # Unsorted filter.
    output_file.write("\nUnsorted filter: SNWD != 0\n")
    df = pd.read_csv('datasets/files/central_park_weather.csv')

    start_time = time.time()
    df = df[df["SNWD"] != 0]
    end_time = time.time()

    execution_time = (end_time - start_time) * 1000
    output_file.write(f"  EXECUTION TIME: {execution_time} ms\n")
    output_file.write(print_memory_usage(df))

def main():
    for i in range(5):
        benchmark_nyc_pandas(f"results/nyc_pandas/nyc_pandas_{i}.txt")

if __name__ == "__main__":
  main()