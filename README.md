To access the module `db` in any of the parallel directories (e.g `flags`), a `context.py` file must be included to append to the `sys.path`.

Please read this for info:
https://docs.python-guide.org/writing/structure/

- but there are some modifications, for instance `from .context import cnx` does not work, it must be `from context import cnx`
- also, within `context.py` the import must be `db.cnx` - I literally don't understand why.
