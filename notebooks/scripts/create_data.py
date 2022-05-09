import os
import jarrow
import jarrow.util
"""
Creates datasets in the data folder.
If data aready exist in the datasets, theses are first removed.
"""
if __name__ == '__main__':
    data_path = os.path.join('notebooks', 'data')

    # Create complete table. Partitioned without special values
    complete_path = os.path.join(data_path, 'complete_table')
    jarrow.util.write_parquet(complete_path, num_rows=600, partitions=['c_cat', 'c_cat2'])

    # Create special table without partitions
    special_path = os.path.join(data_path, 'special_table')
    jarrow.util.write_parquet(special_path, num_rows=100, special_values=True)







