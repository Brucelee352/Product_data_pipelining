from setuptools import setup, find_packages

setup(
    name="portfolio_app",
    version="0.3",
    packages=find_packages(),
    install_requires=[
        'dbt-core',
        'dbt-duckdb',
        'pandas',
        'dash',
        'dash-bootstrap-components',
        'plotly',
        'minio',
        'duckdb',
        'python-dotenv'
    ]
)
