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
def process_dataset(input_path, output_path):
    a = 1
    dataset = ds.dataset(input_path, partitioning="hive", format='parquet')
    sc = dataset.scanner(filter=ds.field('c_float') > 0)
    #tbl = dataset.to_table()
    #ds.write_dataset(tbl,os.path.join(output_path, 'tbl'), partitioning_flavor='hive', format='parquet')
    #ds.write_dataset(sc, os.path.join(output_path, 'sc'), partitioning_flavor='hive', format='parquet')
    ds.write_dataset(sc, os.path.join(output_path, 'sc_partitioned'), partitioning=['c_icat', 'c_cat', 'c_cat2'], partitioning_flavor='hive', format='parquet')



