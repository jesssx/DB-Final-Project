import pandas as pd
from Column import Column
from ColumnStoreTable import ColumnStoreTable

# RLE testing. Also, preliminary compression testing.
data = [0, 0, 1, 1, 1, 1, 1, 0]
col_series = pd.Series(data, name="Column1")
col = Column("A", [col_series])

print(col_series.memory_usage())  # 192 B
print(col.get_memory_usage())  # 256 B

col.compress_RLE()
print(col.get_values()[0].value)
print(col.get_values().index)
print(col.get_memory_usage())  # 204 B


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


test_merge()
