def upload_to_duckdb(df, db_path):
    """Load cleaned data into DuckDB."""
    conn = None
    try:
        conn = connect_to_duckdb(db_path)

        # Register the DataFrame as a table in DuckDB
        conn.register('df_table', df)

        # Create user_activity table from the registered DataFrame
        conn.execute("""
            CREATE TABLE IF NOT EXISTS user_activity AS 
            SELECT * FROM df_table
        """)
        log.info("Data successfully loaded into DuckDB")

    except duckdb.Error as e:
        log.error("Error loading data into DuckDB: %s", str(e))
        raise
    finally:
        if conn:
            conn.close()
            log.info("DuckDB connection closed")