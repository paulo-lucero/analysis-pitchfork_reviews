import sqlite3
from pathlib import Path
import pandas as pd
import traceback
import logging
from to_sql import commands
import typing
from to_sql.config import get_db_config
import sqlalchemy as sqla
from to_sql import table_schemas


db_config = get_db_config('db_config.toml')



def show_tables(conn: sqla.Connection):
    tables = conn.execute(sqla.text('SHOW TABLES'))

    print(tables.all())



def show_packet_size(conn: sqla.Connection):
    packet_size = conn.execute(sqla.text('SHOW VARIABLES LIKE \'max_allowed_packet\''))

    print(packet_size.all())



def show_tables_innodb(mysql_conn: sqla.Connection, db_name: str):
    tables = mysql_conn.execute(sqla.text("""
    SELECT table_name
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE engine = :engine
    AND TABLE_SCHEMA = :database
    """), 
    {
        'engine': 'InnoDB',
        'database': db_name
    })

    print(tables.all())



def create_lens_df(df: pd.DataFrame):
    df_dtypes = df.dtypes

    df_objects_cols = df_dtypes[df_dtypes == 'object'].index.to_list()

    ser_lens = []

    for df_objects_col in df_objects_cols:
        ser_len = df[df_objects_col].fillna('').map(len)

        ser_len.name = f'{df_objects_col}_len'

        ser_lens.append(ser_len)

    if not ser_lens:
        return f'{df.head()}\n\nNo Columns with Objects'

    return pd.concat(ser_lens, axis=1).max()

def analyze_object_lens(conn: sqlite3.Connection):
    sql_comms = ['SELECT * FROM  reviews', 'SELECT * FROM  artists', 'SELECT * FROM  genres', 'SELECT * FROM  labels', 'SELECT * FROM  years', 'SELECT * FROM  content']

    for sql_comm in sql_comms:
        df = pd.read_sql(
            sql=sql_comm,
            con=conn
        )

        print(create_lens_df(df))
        print('\n=========================\n')



def analyze_reviews_date_col(conn: sqlite3.Connection):
    reviews_df = pd.read_sql(
        sql='SELECT * FROM  reviews',
        con=conn
    )

    print(reviews_df['pub_date'].head())
    print('')

    score_str = reviews_df['score'].map(str)
    score_str = score_str.str.split('.', expand=True)

    print(f'Whole Len : {score_str[0].map(len).max()}')
    print(f'Decimal Len : {score_str[1].map(len).max()}')



def data_type_format(value):
    print(f'Is str type: {value} - {isinstance(value, str)}')

def analyze_object_type(conn: sqlite3.Connection):
    reviews_df = pd.read_sql(
        sql='SELECT * FROM  reviews',
        con=conn
    )

    print(len(reviews_df))

    reviews_df['pub_date'].head().apply(data_type_format)



def test_insert(conn: sqlite3.Connection, query_stmt: str):
    reviews_df = pd.read_sql(
        sql=query_stmt,
        con=conn
    )

    # print(commands.generate_insert_header('reviews', reviews_df.columns.to_list()))
    # print('')
    print(commands.generate_values(reviews_df)[:2])



def get_schemas(relative_path: str) -> list[typing.Any]:
    conn = sqlite3.connect(str(Path('.', relative_path).resolve()))
    conn.row_factory = sqlite3.Row

    schemas = None

    try:
        cur = conn.cursor()
        cur.execute('SELECT sql FROM sqlite_master WHERE type=?', ('table', ))

        results = cur.fetchall()

        if not results:
            raise ValueError('No Schemas Found')

        schemas = [dict(schema)['sql'] for schema in results]
        
    except BaseException as e:
        logging.error(traceback.format_exc())
    finally:
        conn.close()

    if schemas is None:
        raise ValueError('Has results but has no schemas generated')
    
    return schemas

def check_schemas():
    table_schemas = get_schemas(db_config['sqlite_db_relpath'])

    for table_schema in table_schemas:
        print(table_schema)
        print('')



def check_sqlite3_version():
    print('')
    print(sqlite3.sqlite_version)



def test_pd_numeric_type():
    import numpy as np
    n_i_32 = np.int32(20)
    n_i_64 = np.int64(30)

    n_f_32 = np.float32(20.2)
    n_f_64 = np.float64(30.5)

    print(pd.api.types.is_integer(n_i_32))
    print(pd.api.types.is_integer(n_i_64))
    print(pd.api.types.is_float(n_i_32))
    print(pd.api.types.is_float(n_i_64))
    print(pd.api.types.is_float(n_f_32))
    print(pd.api.types.is_float(n_f_64))
    print(pd.api.types.is_integer(n_f_32))
    print(pd.api.types.is_integer(n_f_64))



def check_duplicate_reviewids(df: pd.DataFrame):
    count_reviewid = df['reviewid'].value_counts().sort_values(ascending=False)

    duplicate_reviewids = count_reviewid[count_reviewid > 1]

    print(duplicate_reviewids)



def test_years_data(
    mysql_conn: sqla.Connection,
    test_mode: typing.Literal['all'] | typing.Literal['sample-valid'] | typing.Literal['sample-null'] = 'all',
    test_mysql: bool = True
):
    from pathlib import Path

    table_name = 'years'

    if test_mysql:
        commands.drop_tables(mysql_conn, table_name)
        commands.create_table(mysql_conn, table_schemas.years_table_schema, f'Successfully created \'{table_name}\' table')

    sqlite_path = str(Path('.', db_config['sqlite_db_relpath']).resolve())
    sqlite_engine = sqla.create_engine(f'sqlite+pysqlite:///{sqlite_path}')

    with sqlite_engine.connect() as sqlite_conn:
        years_df = pd.read_sql(
            sql=sqla.text(f'SELECT * FROM {table_name}'),
            con=sqlite_conn
        )

        print(f'Test Mode is: \'{test_mode}\'')

        if test_mode == 'sample-valid':
            years_df = years_df[~years_df['year'].isna()].head()
        elif test_mode == 'sample-null':
            years_df = years_df[years_df['year'].isna()].head()


        years_df.info()
        print(years_df.head())
        print('')

        if not test_mysql:
            return
        
        print(f'Testing Inserting to \'{table_name}\' Table')
        mysql_conn.execute(
            sqla.text(commands.generate_insert_header(table_name, years_df.columns.to_list())),
            commands.generate_values(years_df)
        )
        mysql_conn.commit()
        print(f'Successfully inserted rows on {table_name} table')

        db_years_df = pd.read_sql(
            sql=sqla.text(f'SELECT * FROM {table_name}'),
            con=mysql_conn
        )

        # print(len(db_years_df))
        db_years_df.info()
        print(db_years_df.head())   



def test_tables_data_pd(mysql_conn: sqla.Connection, *table_names: str):
    # test_query = 'SELECT * FROM  reviews WHERE title LIKE \'%smoke%\' OR artist LIKE \'%glasvegas%\''

    for table_name in table_names:
        if table_name.strip() == '':
            continue

        table_df = pd.read_sql(
            sql=sqla.text(f'SELECT * FROM {table_name}'),
            con=mysql_conn
        )

        table_df.info()
        print('')
    # print(reviews_df[['title', 'artist', 'author_type']])     
        

pitchfork_conn = sqlite3.connect(str(Path('.', db_config['sqlite_db_relpath']).resolve()))
pitchfork_conn.row_factory = sqlite3.Row
# mysql_engine = commands.create_mysql_engine(db_config)

try:
    # analyze_object_lens(pitchfork_conn)
    # analyze_reviews_date_col(pitchfork_conn)
    # test_insert(pitchfork_conn)
    check_schemas()
    # test_pd_numeric_type()
    # check_sqlite3_version()
    # test_insert(pitchfork_conn, 'SELECT * FROM years WHERE year IS NULL;')


    # with mysql_engine.connect() as mysql_conn:
    #     show_tables(mysql_conn)
    #     show_packet_size(mysql_conn)
    #     show_tables_innodb(mysql_conn, db_config['db_name'])

        # test_tables_data_pd(mysql_conn, 'reviews', 'artists', 'genres', 'labels', 'years', 'content')

        # test_years_data(mysql_conn, 'all', False)
    

except BaseException as e:
    logging.error(traceback.format_exc())
finally:
    pitchfork_conn.close()