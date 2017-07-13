import pandas as pd


class CSVChunkReader(object):
    def __init__(self, chunksize=100000, data_dir='../data'):
        self.chunks_date = pd.read_table(data_dir + '/train_date.csv', sep=',', chunksize=chunksize, index_col='Id')
        self.chunks_numeric = pd.read_table(data_dir + '/train_numeric.csv', sep=',', chunksize=chunksize, index_col='Id')
        self.chunks_categorical = pd.read_table(data_dir + '/train_categorical.csv', dtype=str, sep=',', index_col='Id', chunksize=chunksize)

    def __iter__(self):
        return self

    def __next__(self):
        df_train_date = next(self.chunks_date, None)
        df_train_numeric = next(self.chunks_numeric, None)
        df_train_categorical = next(self.chunks_categorical, None)

        # # As we read df_train_categorical as str, convert the id to int64
        # df_train_categorical['Id'] = df_train_categorical['Id'].astype('int64')

        if df_train_date is not None and \
                df_train_numeric is not None and \
                df_train_categorical is not None:
            df_merged = df_train_date.merge(df_train_numeric, left_index=True, right_index=True)
            df_merged = df_merged.merge(df_train_categorical, left_index=True, right_index=True)
        else:
            raise StopIteration()

        return df_merged

