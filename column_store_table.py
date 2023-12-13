import csv
import io

import os
import pandas as pd
import requests
import sys

from column import Column, Compression

MAX_SIZE = 10**10  # bytes = 100 GB


def _read_csv(file_path):
    return ColumnStoreTable(file_path)


class ColumnStoreTable:
    table_id = 0

    def __init__(self, file_path=None, input_columns=None, name=None):
        # columns = dictionary of column name to Column
        columns = {}
        if name is not None:
            self.name = name
        else:
            self.name = "table" + str(ColumnStoreTable.table_id)
        ColumnStoreTable.table_id += 1

        if input_columns:
            self.columns = input_columns
            return

        if file_path is None:
            self.columns = columns
            return

        print("Before reading file")
        if file_path.startswith("http://") or file_path.startswith("https://"):
            response = requests.get(file_path)
            content = response.text
            num_rows = len(content)
            file_size = len(
                content.encode("utf-8")
            )  # Calculate size from content length
        else:
            # print(f"File Size is {os.stat(file_path).st_size / (1024 * 1024)} MB")
            with open(file_path, "r") as file:
                content = file.readlines()
                num_rows = len(content)
            file_size = os.path.getsize(file_path)

        print("File size: ", str(file_size))
        # Calculate number of rows per chunk
        chunk_n_rows = None
        if file_size >= MAX_SIZE:
            row_size = file_size / num_rows
            chunk_n_rows = MAX_SIZE // row_size

        print("Chunk size: ", str(chunk_n_rows))
        # Return Dataframe generator
        df = pd.read_csv(file_path, iterator=True, chunksize=chunk_n_rows)
        print("Reading CSV...")
        for chunk in df:
            for col in chunk.columns:
                if col not in columns:
                    columns[col] = Column(col, pd.Series(chunk.loc[:, col]))
                else:
                    columns[col].add_values(pd.Series([chunk.loc[:, col]]))
        self.columns = columns

    def get_columns(self):
        return self.columns

    def print_column_stats(self):
        print("++++++++++++++++\n+ Column Stats +\n++++++++++++++++")
        for col in self.columns.values():
            col.print_col_stats()
        print()

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
        for col_name, compression in desired_compressions.items():
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
        for col_name in self.columns:
            self.columns[col_name].decompress()
        return self

    def to_row_format(self):
        # transform whole table to row format
        # call self.decompress()
        # returns a pandas dataframe generator
        self.decompress()
        #   print(self.columns)
        col_data = {}
        for col_name in self.columns:
            col_values = self.columns[col_name].get_values()
            # series_list = self.columns[col_name].get_values()
            # # print(series_list, len(series_list), type(series_list))
            # all_series = None
            # for series in series_list:
            #     #   print(type(series))
            #     if all_series is None:
            #         all_series = series
            #     else:
            #         all_series.append(series, ignore_index=True)
            # # print(type(all_series))
            # col_data[col_name] = all_series
            col_data[col_name] = col_values
        # can return as df.itertuples() or iterrows() or __iter__()
        try:
            return pd.DataFrame(data=col_data)
        except ValueError:
            return pd.DataFrame(data=col_data, index=[0])

    def to_csv(self, name, compression=None):
        # save a csv of the current table
        # does not return anything
        df = self.to_row_format()
        df.to_csv(name, compression=compression)
        print("Saved to csv")

    def filter(self, column_name, condition):
        # condition is probably a lambda function
        # filter column_name based on condition
        # condition is probably a lambda function
        # filter column_name based on condition
        ############# add support for math lambda conditions
        filtered_columns = {}
        col_series = self.columns[column_name].get_values()
        filtered_indices = []
        # loop through all the series for column names
        for i in range(len(col_series)):
            # loop through all the values in each series
            # if the value fulfills condition
            if condition(col_series[i]):
                # print("true")
                # add this value to filtered_columns
                filtered_indices.append(i)

        for col_name in self.columns:
            self.columns[col_name].decompress()
            filtered_values = []
            for i in filtered_indices:
                value = self.columns[col_name].get_values()[i]
                filtered_values.append(value)
            filtered_columns[col_name] = Column(col_name, pd.Series(filtered_values))

        self.columns = filtered_columns
        return self

    def merge(self, other, on=None, self_on=None, other_on=None):
        """
        Merge self with other either on the column on or the two columns self_on and other_on.
        """
        if on is not None and self_on is None and other_on is None:
            return self.merge_same(other, on)
        elif self_on is not None and other_on is not None and on is None:
            return self.merge_different(other, self_on, other_on)
        else:
            raise ValueError(
                "Invalid merge arguments. Must either specify on or both self_on and other_on."
            )

    def merge_same(self, other, on_column_name):
        """
        Merge self with other based on join predicate (on_column_name equality).
        Implements an inner join with an equality predicate.

        Raises ValueError if on_column_name is not in both tables.
        """

        # check whether join predicate is valid (column exists in both tables)
        if on_column_name not in self.columns or on_column_name not in other.columns:
            raise ValueError("Join predicate is invalid.")

        # uncompress "on" column on both tables
        self_col = self.columns[on_column_name]
        self_col.decompress()
        other_col = other.columns[on_column_name]
        other_col.decompress()

        # create dictionary based on "on" columns for both tables
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

        # figure out which rows to keep for each table (equalities)
        join_rows = []
        for key in self_dict:
            if key in other_dict:
                for i in self_dict[key]:
                    for j in other_dict[key]:
                        join_rows.append((i, j))

        new_columns = {}

        # for each column, uncompress and keep the specified rows (and in that specific order)
        for col_name, col in self.columns.items():
            col.decompress()
            new_values = []
            for i, j in join_rows:
                new_values.append(col.get_values()[i])
            new_col = Column(col_name, pd.Series(new_values))
            new_columns[col_name] = new_col

        for col_name, col in other.columns.items():
            col.decompress()
            new_values = []
            for i, j in join_rows:
                new_values.append(col.get_values()[j])
            new_col = Column(col_name, pd.Series(new_values))
            if col_name not in new_columns:
                new_columns[col_name] = new_col
            else:
                if col_name != on_column_name:
                    new_name = col_name + "_" + other.name
                    new_columns[new_name] = new_col

        # print([new_columns[col].values for col in new_columns])

        # create a new ColumnStoreTable with the new columns
        return ColumnStoreTable(None, new_columns)

    def merge_different(self, other, self_on, other_on):
        """
        Merge self with other based on join predicate of their respective arguments self_on and other_on.
        Implements an inner join with an equality predicate.

        Raises ValueError if self_on is not a column in self or other_on is not a column in other.
        """
        # check whether join predicate is valid (column exists in both tables)
        if self_on not in self.columns or other_on not in other.columns:
            raise ValueError("Join predicate is invalid.")

        # uncompress "on" column on both tables
        self_col = self.columns[self_on]
        self_col.decompress()
        other_col = other.columns[other_on]
        other_col.decompress()

        # create dictionary based on "on" columns for both tables
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

        # figure out which rows to keep for each table (equalities)
        join_rows = []
        for key in self_dict:
            if key in other_dict:
                for i in self_dict[key]:
                    for j in other_dict[key]:
                        join_rows.append((i, j))

        # print(join_rows)

        new_columns = {}

        # for each column, uncompress and keep the specified rows (and in that specific order)
        for col_name, col in self.columns.items():
            col.decompress()
            new_values = []
            for i, j in join_rows:
                new_values.append(col.get_values()[i])
            new_col = Column(col_name, pd.Series(new_values))
            new_columns[col_name] = new_col

        for col_name, col in other.columns.items():
            col.decompress()
            new_values = []
            for i, j in join_rows:
                new_values.append(col.get_values()[j])
            new_col = Column(col_name, pd.Series(new_values))
            if col_name not in new_columns:
                new_columns[col_name] = new_col
            else:
                new_name = other.name + "." + col_name
                new_columns[new_name] = new_col

        # print([new_columns[col].values for col in new_columns])

        # create a new ColumnStoreTable with the new columns
        return ColumnStoreTable(None, new_columns)

    def sort(self, column_name, ascending=True):
        # sort column [stretch]
        col = self.columns[column_name]
        compression = col.get_compression()
        col.decompress()
        vals = list(col.get_values())
        col.compress(compression)
        indices = sorted(
            range(len(vals)), key=lambda k: vals[k], reverse=(not ascending)
        )

        for _, col in self.columns.items():
            col.sort_column(indices)
        return self

    def add_column(self, column_name, column):
        # for testing purposes
        self.columns[column_name] = column
        return self
