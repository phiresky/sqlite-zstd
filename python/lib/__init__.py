import sqlite3
from importlib.resources import files, as_file

def load(conn: sqlite3.Connection) -> None:
    lib = next(x for x in files(__name__).iterdir() if x.name.startswith('lib'))
    with as_file(lib) as ext:
        conn.load_extension(str(ext))
