import duckdb

# Connect to DuckDB
con = duckdb.connect('vancouver_data.db')

# Check if the table exists
tables = con.execute("SHOW TABLES;").fetchall()
print("Tables:", tables)


# Run a sample query
df = con.execute("SELECT * FROM property_tax limit 10").df()

# Print the result
print(df)