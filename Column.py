# New column store design.
from enum import Enum
import pandas as pd
import io
import os
import sys


class Compression(Enum):
    NONE = 0
    RLE = 1
    BITMAP = 2


class Column:
    def __init__(self, name, values):
        self.name = name
        if type(values) == pd.Series:
            self.values = values  # pd.Series
        else:
            raise ValueError("Column.values must be pd.Series")

        # Column must be uncompressed when first initialized.
        self.compression = Compression.NONE

    def get_name(self):
        return self.name

    def get_values(self):
        return self.values

    def get_compression(self):
        return self.compression

    def get_memory_usage(self):
        if type(self.values) == pd.Series:
            return self.values.memory_usage()
        elif type(self.values) == list:
            memory_usage = sys.getsizeof(self.values)
            for chunk in self.values:
                if type(chunk) == pd.Series:
                    memory_usage += (
                        chunk.memory_usage()
                    )  # TODO: Should index be T or F?
                else:
                    raise ValueError(
                        "Column.values must be pd.Series or a list of pd.Series."
                    )
            return memory_usage
        else:
            raise ValueError("Column.values must be pd.Series or a list of pd.Series.")

    def print_col_stats(self):
        print(f"{self.name}")
        print(f"  Compressed: {self.compression}")
        print(f"  Data type: {0}")
        print(f"  Memory usage: {self.get_memory_usage()} B")

    def compress(self, compression):
        self.compression = compression
        if compression == Compression.RLE:
            self.compress_RLE()
        elif compression == Compression.BITMAP:
            self.compress_BITMAP()
        # Else, Compression.NONE, do nothing.
        return self

    def compress_RLE(self):
        """
        Run length encodes the column and mutates self.values to hold new pd.Series
        instances.
        Result:
          pd.Series objects in self.values have run_length and value fields.
        """
        self.compression = Compression.RLE

        # Perform RLE encoding
        values = self.values.ne(self.values.shift()).cumsum()
        run_lengths = self.values.groupby(values).size()

        # Mutate self.values and create a new Series with run-length encoded values and run lengths
        self.values = pd.Series(
            {
                "value": self.values.groupby(values).first().values,
                "run_length": run_lengths.values,
            },
        )

        return self

    def compress_BITMAP(self):
        self.compression = Compression.BITMAP

        # Perform bitmap encoding
        bitmap = {}
        self.num_values = len(self.values)

        for j, val in enumerate(self.values):
            if val not in bitmap:
                bitmap[val] = [0] * j
            bitmap[val].append(1)
            for key in bitmap:
                if key != val:
                    bitmap[key].append(0)
        self.values = pd.Series(bitmap)
        return self

    def decompress(self):
        if self.compression == Compression.RLE:
            self.decompress_RLE()
        elif self.compression == Compression.BITMAP:
            self.decompress_BITMAP()
        # Else, Compression.NONE, do nothing.
        return self

    def decompress_RLE(self):
        """
        Decompresses the column from RLE compression back to its original format.
        """
        if self.compression != Compression.RLE:
            raise ValueError("Column is not compressed with RLE.")

        # Perform run-length decoding
        original_values = []
        for value, run_length in zip(self.values["value"], self.values["run_length"]):
            original_values.extend([value] * run_length)

        # # Mutate self.values and create a new Series with the original values
        self.values = pd.Series(original_values)

        # Reset compression type
        self.compression = Compression.NONE

        return self

    def decompress_BITMAP(self):
        """
        Decompresses the column from bitmap compression back to its original format.
        """
        if self.compression != Compression.BITMAP:
            raise ValueError("Column is not compressed with BITMAP.")

        # Perform bitmap decoding
        indices = self.values.index
        values = [0] * self.num_values
        for i in self.values.index:
            positions = self.values[i]
            for j, val in enumerate(positions):
                if val == 1:
                    values[j] = i

        # Create a new Series with the original values
        self.values = pd.Series(values)

        # Reset compression type
        self.compression = Compression.NONE

        return self

    def add_values(self, new_val):
        self.values.extend(new_val)

    def sort_column(self, indices):
        compression = self.compression
        self.decompress()
        new_vals = []
        for i in indices:
            new_vals.append(self.values[i])
        self.values = pd.Series(new_vals)
        self.compress(compression)
