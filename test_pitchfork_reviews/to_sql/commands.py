import pandas as pd
from pathlib import Path
import sqlalchemy as sqla
import typing
import config


def create_mysql_engine(db_config: dict[str, typing.Any]):
    db_dialect = 'mysql+mysqldb://{db_user}:{db_password}@{db_endpoint}:{db_port}/{db_name}'.format(**db_config)

    return sqla.create_engine(db_dialect)



def drop_tables(mysql_conn: sqla.Connection, *table_names: str):
    if len(table_names) == 0:
        raise ValueError('Should have table name/s provided')
    
    joined_tables = ','.join(table_names)
    
    mysql_conn.execute(sqla.text(f'DROP TABLE IF EXISTS {joined_tables}'))
    print(f'{joined_tables} are dropped')



def create_table(mysql_conn: sqla.Connection, sql_stmt: str, success_msg: str = 'Table successfully created'):
    mysql_conn.execute(sqla.text(sql_stmt))

    mysql_conn.commit()
    print('')
    print(success_msg)
    print('')






def convert_to_python_type(value: typing.Any):
    converter_map = (
        ((pd.StringDtype, ), str),
        ((pd.BooleanDtype, ), bool)
    )

    for d_types, converter in converter_map:
        if isinstance(value, d_types):
            return converter(value)
        
    if pd.isna(value):
        # print(type(value))
        return None
    elif (
        isinstance(value, (int, float, str)) or
        (
            pd.api.types.is_integer(value) or
            pd.api.types.is_float(value)
        )
    ):
        return value
    else:
        raise TypeError(f'This value \'{value}\' has unknown type of \'{type(value)}\'')


def process_values_types(values: dict[str, typing.Any]):
    for value_col in values:
        values[value_col] = convert_to_python_type(values[value_col])
    
    return values


def generate_values(df: pd.DataFrame):
    copied_df = df.copy()
    values_list = []

    for row in copied_df.itertuples(index=False):
        values_list.append(process_values_types(row._asdict())) # type: ignore
        # values_list.append(row._asdict()) # type: ignore
    
    return values_list


def generate_insert_header(table_name: str, columns: list[str]):
    column_separator = ', '

    parametized_columns = list(map(lambda column: ':' + column, columns))

    return f'INSERT INTO {table_name} ({column_separator.join(columns)}) VALUES ({column_separator.join(parametized_columns)})'


def insert_rows(mysql_conn: sqla.Connection, table_name: str):
    db_config = config.get_db_config('db_config.toml')
    
    sqlite_path = str(Path('.', db_config['sqlite_db_relpath']).resolve())

    sqlite_engine = sqla.create_engine(f'sqlite+pysqlite:///{sqlite_path}')

    with sqlite_engine.connect() as sqlite_conn:
        print(f'Querrying from sqlite {table_name} table')

        table_df = pd.read_sql(
            sql=sqla.text(f'SELECT * FROM {table_name}'),
            con=sqlite_conn
        )

        # table_df.info()

        print(f'Generating Values Parameters, number of rows is {len(table_df)}')
        insert_values_parameters = generate_values(table_df)

        print('Inserting to Database')
        mysql_conn.execute(
            sqla.text(generate_insert_header(table_name, table_df.columns.to_list())),
            insert_values_parameters
        )
        print(f'Successfully inserted rows on {table_name} table')

        print('\n==================================================================================\n')

        mysql_conn.commit()