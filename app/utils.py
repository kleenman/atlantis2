from datetime import datetime
import pandas as pd
import locale

locale.setlocale(locale.LC_ALL, 'de_DE.UTF8')
pd.options.mode.use_inf_as_na = True


def double_quote(arg):
    encapsulated = [f'"{c}"' for c in arg]
    return encapsulated


def fix_numbers(source):
    for row in source:
        for n in row.values():
            try:
                datetime.strptime(str(n), '%d.%m.%Y').date()
            except ValueError:
                pass
            try:
                locale.atoi(str(n).split()[-1])
            except ValueError:
                pass
            try:
                locale.atof(str(n).split()[-1])
            except ValueError:
                pass


class TableFactory:
    def __init__(self, **kwargs):
        self.data_path = kwargs.pop('data_path', None)
        self.dimension_attributes = kwargs.pop('dimension_attributes', None)
        self.delimiter = kwargs.pop('delimiter', None)
        self.key = kwargs.pop('key', None)
        self.flow = kwargs.pop('flow', None)
        self.conn_wrapper = kwargs.pop('conn_wrapper', None)

    def create_df(self, nrows):
        df = pd.read_csv(
            self.data_path,
            nrows=nrows,
            encoding='utf-8-sig',
            sep=self.delimiter,
            header=0,
            usecols=self.dimension_attributes,
        )
        if self.dimension_attributes is not None:
            df.columns = double_quote(self.dimension_attributes)
        for c in df.columns:
            try:
                df[c] = df[c].apply(lambda n: datetime.strptime(str(n), '%d.%m.%Y'))
                continue
            except ValueError:
                pass
            try:
                df[c] = df[c].apply(lambda n: locale.atoi(str(n).split()[-1]))
                continue
            except ValueError:
                pass
            try:
                df[c] = df[c].apply(lambda n: locale.atof(str(n).split()[-1]))
                continue
            except ValueError:
                pass
        return df

    def create_table(self, df):
        names_dtypes = []
        for name, dtype in df.dtypes.iteritems():
            if str(dtype) == 'int64':
                dtype = 'bigint'
            elif str(dtype) == 'object':
                dtype = 'text'
            elif str(dtype) == 'float64':
                dtype = 'numeric'
            elif str(dtype) == 'datetime64[ns]':
                dtype = 'date'
            else:
                dtype = 'text'
            names_dtypes.append(f'{name} {dtype}')
        str_names_dtypes = ','.join(names_dtypes)
        sql_create_table = 'create table if not exists {dimension_name} ({key} bigint, {str_names_dtypes});' \
            .format(key=self.key, dimension_name=self.flow.dimension_name, str_names_dtypes=str_names_dtypes)
        cursor = self.conn_wrapper.cursor()
        cursor.execute(sql_create_table)
        self.conn_wrapper.commit()


