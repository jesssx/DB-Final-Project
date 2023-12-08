import csv
import io

import os
import pandas as pd
import requests
import sys
from Column import Column

MAX_SIZE = 10**10  # bytes = 100 GB


class ColumnStoreTable:
    def __init__(self, file_path=None, input_columns=None):
        # columns = dictionary of column name to Column
        columns = {}

        if input_columns:
            self.columns = input_columns
            return

        if file_path is None:
            self.columns = columns
            return

        if file_path.startswith("http://") or file_path.startswith("https://"):
            response = requests.get(file_path)
            content = response.text
            num_rows = len(content)
            file_size = len(
                content.encode("utf-8")
            )  # Calculate size from content length
        else:
            with open(file_path, "r") as file:
                content = file.readlines()
                num_rows = len(content)
            file_size = os.path.getsize(file_path)

        # Calculate number of rows per chunk
        chunk_n_rows = None
        if file_size >= MAX_SIZE:
            row_size = file_size / num_rows
            chunk_n_rows = MAX_SIZE // row_size

        # Return Dataframe generator
        df = pd.read_csv(file_path, iterator=True, chunksize=chunk_n_rows)
        for chunk in df:
            for col in chunk.columns:
                if col not in columns:
                    # print(chunk.loc[:, col])
                    columns[col] = Column(col, pd.Series([chunk.loc[:, col]]))
                else:
                    columns[col].add_values(pd.Series([chunk.loc[:, col]]))
        print(columns["NAME"].get_values)
        self.columns = columns

    def get_table_stats(self):
        """
        Returns dictionary of memory benchmarking statistics.
        """
        num_compressed_columns = 0
        memory_usage = sys.getsizeof(self.columns)  # Only the dict.
        for col in self.columns.values():
            if col.get_compression() != Compression.NONE:
                num_compressed_columns += 1
            memory_usage += col.get_memory_usage()
        return {
            "num_columns": len(self.columns),
            "num_compressed_columns": num_compressed_columns,
            "memory_usage": memory_usage,
        }

    def compress(self, desired_compressions):
        """
        Required:
          Columns specified in desired_compressions must already be decompressed.
        Args:
          desired_compressions: dict of col_name to Compression enum, e.g. {"A": Compression.RLE, "B": Compression.DELTA}
        Returns:
          self
        """
        for col_name, compression in desired_compressions:
            col = self.columns[col_name]

            # Check that column starts decompressed.
            assert col.get_compression() == Compression.NONE

            # Mutates col to be compressed.
            col.compress(compression)
        return self

    def decompress(self):
        """
        Decompresses all columns.
        """
        for col_name, col in self.columns:
            col.decompress()
        return self

    def to_row_format(self):
        # transform whole table to row format
        # call self.decompress()
        # returns a pandas dataframe generator
        self.decompress()

    def to_csv(self):
        # save a csv of the current table
        # does not return anything
        print("done")

    def filter(self, column_name, condition):
        # condition is probably a lambda function
        # filter column_name based on condition
        return self

    def merge(self, other, on_column_name):
        # merge self with other based on join predicate (on)
        # error if column_name is not in both tables
        # naive impl: uncompress column before merging
        # currently only support inner join with equality predicate

        # check whether join predicate is valid (column exists in both tables)
        if on_column_name not in self.columns or on_column_name not in other.columns:
            raise ValueError("Join predicate is invalid.")

        # uncompress on_column_name on both tables
        self_col = self.columns[on_column_name]
        self_col.decompress()
        other_col = other.columns[on_column_name]
        other_col.decompress()

        # create dictionary based on on_column_name for both tables
        self_dict = {}
        other_dict = {}
        for i, val in enumerate(self_col.get_values()):
            if val not in self_dict:
                self_dict[val] = []
            self_dict[val].append(i)
        for i, val in enumerate(other_col.get_values()):
            if val not in other_dict:
                other_dict[val] = []
            other_dict[val].append(i)

        print(self_dict)
        print(other_dict)

        # figure out which rows to keep for each table (equalities)
        join_rows = []
        for key in self_dict:
            if key in other_dict:
                for i in self_dict[key]:
                    for j in other_dict[key]:
                        join_rows.append((i, j))

        print(join_rows)

        new_columns = {}

        # for each column, uncompress and keep the specified rows (and in that specific order)
        for col_name, col in self.columns.items():
            col.decompress()
            new_values = []
            for i, j in join_rows:
                new_values.append(col.get_values()[i])
            new_col = Column(col_name, new_values)
            new_columns[col_name] = new_col

        for col_name, col in other.columns.items():
            col.decompress()
            new_values = []
            for i, j in join_rows:
                new_values.append(col.get_values()[j])
            new_col = Column(col_name, new_values)
            new_columns[col_name] = new_col

        print([new_columns[col].values for col in new_columns])

        # create a new ColumnStoreTable with the new columns
        return ColumnStoreTable(None, new_columns)

    def sort(self, column_name, ascending=True):
        # sort column [stretch]
        return self

    def add_column(self, column_name, column):
        # for testing purposes
        self.columns[column_name] = column
        return self
