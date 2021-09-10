import pandas as pd


def double_quote(arg):
    encapsulated = [f'"{c}"' for c in arg]
    return encapsulated


def pd_create_table(data_path, dimension_attributes, delimiter, key, flow, conn_wrapper):
    df = pd.read_csv(
        data_path,
        nrows=100,
        encoding='utf-8-sig',
        sep=delimiter,
        header=0,
        usecols=dimension_attributes)
    df.columns = dimension_attributes
    names_dtypes = []
    for name, dtype in df.dtypes.iteritems():
        if str(dtype) == 'int64':
            dtype = 'bigint'
        elif str(dtype) == 'object':
            dtype = 'text'
        elif str(dtype) == 'float64':
            dtype = 'numeric'
        else:
            dtype = 'text'
        names_dtypes.append(f'{name} {dtype}')
    str_names_dtypes = ','.join(names_dtypes)
    sql_create_table = 'create table if not exists {dimension_name} ({key} bigint, {str_names_dtypes});' \
        .format(key=key, dimension_name=flow.dimension_name, str_names_dtypes=str_names_dtypes)
    cursor = conn_wrapper.cursor()
    cursor.execute(sql_create_table)
    conn_wrapper.commit()
