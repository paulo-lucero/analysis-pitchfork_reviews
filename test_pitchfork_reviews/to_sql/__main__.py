import sqlalchemy as sqla
import commands
import table_schemas
from config import get_db_config

def populate_database(mysql_conn: sqla.Connection):
    table_images = (
        (table_schemas.reviews_table_schema, 'reviews'),
        (table_schemas.artists_table_schema, 'artists'),
        (table_schemas.genres_table_schema, 'genres'),
        (table_schemas.labels_table_schema, 'labels'),
        (table_schemas.years_table_schema, 'years'),
        (table_schemas.content_table_schema, 'content')
    )

    for table_schema, table_name in table_images:
        commands.create_table(mysql_conn, table_schema, f'Successfully created \'{table_name}\' table')
        commands.insert_rows(mysql_conn, table_name)


mysql_engine = commands.create_mysql_engine(get_db_config('db_config.toml'))
with mysql_engine.connect() as mysql_conn:

    commands.drop_tables(mysql_conn, 'reviews', 'artists', 'genres', 'labels', 'years', 'content')

    populate_database(mysql_conn)