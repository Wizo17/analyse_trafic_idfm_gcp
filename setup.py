from setuptools import setup, find_packages

setup(
    name="analytics_trafic_idfm_gcp",
    version="0.1.1",
    author="William ZOUNON",
    author_email="williamzounon@gmail.com",
    description="IDF public transport traffic analyses with python, pyspark and gcp or postgres.",
    packages=find_packages(),
    package_dir={'': 'src'},
    install_requires=[
        "psycopg2"
        "psycopg2-binary"
        "SQLAlchemy"
        "python-dotenv"
        "setuptools"
    ],
)

