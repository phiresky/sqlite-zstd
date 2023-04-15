from datasette import hookimpl
from . import load


@hookimpl
def prepare_connection(conn):
    conn.enable_load_extension(True)
    load(conn)
    conn.enable_load_extension(False)
