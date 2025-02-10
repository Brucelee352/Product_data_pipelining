from setuptools import setup, find_packages

setup(
    name="portfolio_app",
    version="0.3",
    packages=find_packages(),
    install_requires=[
        'dbt-core',
        'dbt-duckdb',
        'pandas',
        'streamlit',
        'plotly',
        'minio',
        'duckdb',
        'python-dotenv'
    ]
)
