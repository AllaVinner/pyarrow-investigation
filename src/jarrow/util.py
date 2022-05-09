import jarrow
import datetime
import pyarrow
import pyarrow as pa
import pyarrow.dataset as ds
import os
import shutil
import numpy as np
import random
import string


### Table modification methods ###


def sprinkle_null(tbl: pyarrow.Table, column: str, p_null: float = 0.5) -> pyarrow.Table:
    """ Randomly set values in a column to null.

    param tbl: Table to modify. (note that a copy is created)
    param column: Column to insert nulls into.
    param p_null: Probability that a cell is set to null.
    return: Pyarrow table with null values
    """
    col_idx = tbl.schema.get_field_index(column)
    col_type = tbl.schema.types[col_idx]
    col_list = tbl.column(column).to_pylist()
    null_list = np.random.choice([True, False], len(col_list), p=[p_null, 1 - p_null])
    for i, is_null in enumerate(null_list):
        if is_null:
            continue
        col_list[i] = None
    col_arrow = pa.array(col_list, type=col_type)
    tbl = tbl.remove_column(col_idx)
    tbl = tbl.add_column(col_idx, column, col_arrow)
    return tbl


def sprinkle_nan_and_inf(tbl: pyarrow.Table, column: str, p_special: float = 0.5):
    """ Randomly set values in a column to nan, inf, and -inf.

    param tbl: Table to modify. (note that a copy is created)
    param column: Column to insert special values into.
    param p_null: Probability that a cell is set to a special value.
    return: Pyarrow table with null values
    """
    col_idx = tbl.schema.get_field_index(column)
    col_type = tbl.schema.types[col_idx]
    col_list = tbl.column(column).to_pylist()
    null_list = np.random.choice([True, False], len(col_list), p=[p_special, 1 - p_special])
    for i, is_null in enumerate(null_list):
        if is_null:
            continue
        col_list[i] = np.random.choice([float('nan'), float('inf'), float('-inf')])
    col_arrow = pa.array(col_list, type=col_type)
    tbl = tbl.remove_column(col_idx)
    tbl = tbl.add_column(col_idx, column, col_arrow)
    return tbl


### Table creation methods ###


def create_complete_table(n: int = 600) -> pyarrow.Table:
    """ Creates a pyarrow table consisting of the most common datatypes.
    The name of each column starts with 'c_' followed by a type description.

    Columns:
        c_index - Integer from 0 : n.
        c_int - Integer sampled from uniform [0, 999].
        c_float - Float sampled from normal.
        c_str - String sampled. Length from [0, 5]. Only letters.
        c_cat - String sampled from [A, B, C].
        c_cat2 - String sampled from [AA, BB].
        c_icat - Integer sampled from [0,3].
        c_cat - Boolean sampled uniformly.
        c_date - Date sampled uniformly [1950, 2100]. Unit is day.
        c_time - Datetime sampled uniformly [1950, 2100]. Unit is second.

    :param n: Number of rows in the table.
    :return: Pyarrow table.

    """
    c_index = pa.array(np.arange(n), type=pa.int32())
    c_int = pa.array(np.random.randint(0, 1000, n), type=pa.int32())
    c_float = pa.array(np.random.normal(0, 1, n), type=pa.float32())
    c_str = pa.array([random_word(0, 6) for _ in range(n)])
    c_cat = pa.array(np.random.choice(['A', 'B', 'C'], n))
    c_cat2 = pa.array(np.random.choice(['AA', 'BB'], n))
    c_icat = pa.array(np.random.choice(4, n))
    c_bool = pa.array(np.random.choice(2, n), type=pa.bool_())
    c_date = pa.array([random_date(min_year=1950, max_year=2100) for _ in range(n)])
    c_time = pa.array([random_datetime(min_year=1950, max_year=2100) for _ in range(n)], type=pa.timestamp('s'))
    tbl = pa.table([c_index, c_int, c_float, c_str, c_cat, c_cat2, c_icat, c_bool, c_date, c_time],
                   names=['c_index', 'c_int', 'c_float', 'c_str', 'c_cat', 'c_cat2', 'c_icat', 'c_bool', 'c_date',
                          'c_time'])
    return tbl


def create_special_complete_table(n: int = 600) -> pyarrow.Table:
    """Creates a complete table and sprinkle in some null and special values"""
    tbl = create_complete_table(n)
    tbl = sprinkle_null(tbl, 'c_float', p_null=0.3)
    tbl = sprinkle_nan_and_inf(tbl, 'c_float', p_special=0.3)
    tbl = sprinkle_null(tbl, 'c_int', p_null=0.3)
    tbl = sprinkle_null(tbl, 'c_str', p_null=0.3)
    tbl = sprinkle_null(tbl, 'c_date', p_null=0.3)
    tbl = sprinkle_null(tbl, 'c_time', p_null=0.3)
    return tbl


### Folder structure methods ###


def clear_directory(dir_path):
    """Delete all the content of dir_path. Leaves an empty directory.

    If dir_path do not exist, it is created
    :param dir_path: path to directory.
    :return:
    """
    if os.path.isdir(dir_path):
        shutil.rmtree(dir_path)
    os.mkdir(dir_path)


def empty_tree(dir_path):
    """Clears directory and all children directories of files.
    :param dir_path:
    :return:
    """
    if os.path.isfile(dir_path):
        os.remove(dir_path)
    elif os.path.isdir(dir_path):
        [empty_tree(os.path.join(dir_path, d)) for d in os.listdir(dir_path)]
    else:
        print(f"Was neither file or dir {dir_path}")


#### Create Random values ###


def random_word(min_len=0, max_len=6):
    """Generate a random word between the set min and max length"""
    n = random.randint(min_len, max_len)
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=n))


def random_date(min_year=1950, max_year=2100):
    year = np.random.randint(min_year, max_year)
    month = np.random.randint(1, 12 + 1)
    day = np.random.randint(1, 28 + 1)
    return datetime.date(year, month, day)


def random_datetime(min_year=1950, max_year=2100):
    year = np.random.randint(min_year, max_year)
    month = np.random.randint(1, 12 + 1)
    day = np.random.randint(1, 28 + 1)
    hour = np.random.randint(0, 24)
    minute = np.random.randint(0, 60)
    second = np.random.randint(0, 60)
    return datetime.datetime(year, month, day, hour, minute, second)


### Write dataset ###


def write_parquet(dataset_path, num_rows=600, partitions=None, special_values: bool = False):
    if special_values:
        tbl = create_special_complete_table(num_rows)
    else:
        tbl = create_complete_table(num_rows)

    clear_directory(dataset_path)
    if partitions is None:
        ds.write_dataset(tbl, dataset_path, format='parquet')
    else:
        ds.write_dataset(tbl, dataset_path, format='parquet', partitioning=partitions, partitioning_flavor='hive',
                         max_rows_per_file=20, max_rows_per_group=5)


if __name__ == '__main__':
    cwd_dir = os.getcwd()
    data_dir = os.path.join(cwd_dir, 'data')

    # Create single parquet file
    single = os.path.join(data_dir, 'single')
    write_parquet(single, num_rows=600)

    # Create complete table
    complete_table = os.path.join(data_dir, 'complete_table')
    write_parquet(complete_table, partitions=['c_cat', 'c_cat2'], num_rows=600)

    # Create partition on integers
    int_part = os.path.join(data_dir, 'int_partitioned')
    write_parquet(int_part, num_rows=600, partitions=['c_icat', 'c_cat2'])

    # Create special table
    single_null = os.path.join(data_dir, 'special_table')
    write_parquet(single_null, num_rows=50, special_values=True)
