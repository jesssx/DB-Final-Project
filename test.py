import pandas as pd

from column import Column
from column_store_table import ColumnStoreTable


def test_RLE_single_column():
    """
    Tests RLE compression on a Column instance. With memory printouts.
    """
    data = [0, 0, 1, 1, 1, 1, 1, 0]
    col_series = pd.Series(data, name="Column1")
    col = Column("A", [col_series])

    # initialization
    print("INITIALIZATION\n")
    print(col_series.memory_usage())  # 192 B
    print("col get_memory_usage:\n", col.get_memory_usage())  # 256 B
    print(col.get_values())

    # compression
    print("\n\nCOMPRESSION\n")
    col.compress_RLE()
    print(col.get_values()[0].value)
    print(col.get_values()[0].run_length)
    print("col get_memory_usage:\n", col.get_memory_usage())  # 204 B

    # decompression
    print("\n\nDECOMPRESSION\n")
    col.decompress_RLE()
    print(col.get_values())
    print("col get_memory_usage:\n", col.get_memory_usage())


def test_merge():
    ids = [1, 2, 3, 4, 5]
    names = ["A", "B", "C", "D", "E"]
    names2 = ["A", "C", "A", "D", "E"]
    hobbies = ["cooking", "baking", "swimming", "running", "reading"]

    col1 = Column("ids", pd.Series(ids))
    col2 = Column("names", pd.Series(names))
    col3 = Column("names", pd.Series(names2))
    col4 = Column("hobbies", pd.Series(hobbies))

    id_table = ColumnStoreTable(None)
    id_table.add_column(col1.name, col1)
    id_table.add_column(col2.name, col2)
    hobby_table = ColumnStoreTable(None)
    hobby_table.add_column(col3.name, col3)
    hobby_table.add_column(col4.name, col4)

    merged_table = id_table.merge(hobby_table, "names")


def test_sort():
    ids = [3, 2, 1, 4, 5]
    names = ["C", "B", "A", "D", "E"]

    col1 = Column("ids", pd.Series(ids))
    col2 = Column("names", pd.Series(names))

    id_table = ColumnStoreTable(None)
    id_table.add_column(col1.name, col1)
    id_table.add_column(col2.name, col2)

    columns = id_table.get_columns()
    for _, col in columns.items():
        print(col.get_values())

    id_table.sort("ids")
    columns = id_table.get_columns()
    for _, col in columns.items():
        print(col.get_values())


def main():
    test_RLE_single_column()
    test_merge()
    test_sort()


if __name__ == "__main__":
    main()
