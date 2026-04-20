from setuptools import (
    Extension,
    setup,
    find_packages,
)
from setuptools_rust import RustExtension
from Cython.Build import cythonize


extensions = [
    Extension(
        "dbhose_airflow.core.ddl",
        ["src/dbhose_airflow/core/ddl.pyx"],
    ),
]

with open(file="README.md", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="dbhose_airflow",
    version="0.2.0.dev2",
    author="0xMihalich",
    author_email="bayanmobile87@gmail.com",
    description=(
        "airflow class for exchanging data between "
        "DBMSs in native binary formats."
    ),
    url="https://github.com/0xMihalich/dbhose_airflow",
    long_description=long_description,
    long_description_content_type="text/markdown",
    project_urls={
        "Homepage": "https://github.com/0xMihalich/dbhose_airflow",
        "Documentation": "https://0xmihalich.github.io/dbhose_airflow/",
    },
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    ext_modules=cythonize(extensions, language_level="3"),
    rust_extensions=[
        RustExtension(
            "dbhose_airflow.core.ddl_core",
            path="src/dbhose_airflow/core/ddl_core/Cargo.toml",
            debug=False,
        )
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Database",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Programming Language :: Python :: 3.14",
        "Framework :: Apache Airflow",
        "Operating System :: OS Independent",
    ],
    keywords="airflow, database, etl, clickhouse, postgresql, greenplum",
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        "apache-airflow>=2.4.3",
        "native-dumper==0.3.7.dev3",
        "pgpack-dumper==0.3.7.dev3",
        "dr-herriot==0.1.0.dev0",
    ],
    python_requires=">=3.10",
)
