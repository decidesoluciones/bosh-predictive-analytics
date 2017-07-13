import pandas as pd


class CSVChunkReader(object):
    def __init__(self, chunksize=100000, file_prefix='train', data_dir='../data'):
        self.chunks_date = pd.read_table('{}/{}_date.csv'.format(data_dir, file_prefix), sep=',',
                                         chunksize=chunksize, index_col='Id')
        self.chunks_numeric = pd.read_table('{}/{}_numeric.csv'.format(data_dir, file_prefix), sep=',',
                                            chunksize=chunksize, index_col='Id')
        self.chunks_categorical = pd.read_table('{}/{}_categorical.csv'.format(data_dir, file_prefix),
                                                dtype=str, sep=',', index_col='Id', chunksize=chunksize)

    def __iter__(self):
        return self

    def __next__(self):
        df_train_date = next(self.chunks_date)
        df_train_numeric = next(self.chunks_numeric)
        df_train_categorical = next(self.chunks_categorical)

        df_merged = df_train_date.merge(df_train_numeric, left_index=True, right_index=True)
        df_merged = df_merged.merge(df_train_categorical, left_index=True, right_index=True)

        return df_merged

