from database_handler import (
    get_database_access, DatabaseHandler, load_query
)

# Get database credentials from json file
database_access = get_database_access('databases_access.json')

# Instantiate the DatabaseHandler class
database_handler = DatabaseHandler(database_access["database-test"])

# Load a query file
query = load_query('query.sql')

# Fetch results
results = database_handler.fetch(query)

# Fetch results and bring them to a Pandas DataFrame
df_results = database_handler.query_to_df(query)
