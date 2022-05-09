import os
import numpy as np
from memory_profiler import profile
import jarrow
import pyarrow as pa
import pyarrow.dataset as ds 


@profile
def a_function():
    a = 1
    bb = 4
    cc = "AAAAAAAAA"
    b = np.arange(1_000_000)
    c = np.random.randint(0,1000, 1_000_000)


@profile
def reading_dataset(dataset_path):
    a = 1
    dataset = ds.dataset(dataset_path, partitioning="hive", format='parquet')
    sc = dataset.scanner(filter = ds.field('c_float') > 0)
    tbl = dataset.to_table()


