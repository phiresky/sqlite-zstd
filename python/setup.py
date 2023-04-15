from setuptools import setup
from setuptools_rust import Binding, RustExtension

setup(
    rust_extensions=[RustExtension('sqlite_zstd.libsqlite_zstd',
        path='../Cargo.toml',
        binding=Binding.NoBinding,
        features=['build_extension'],
        py_limited_api=True,
    )],
)
