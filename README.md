## Salesforce SOQL Spark Connector

This connector enables users to easily retrieve data from Salesforce and materialize it in a Spark Temporary View.

### Authentication

To use this code in your environment, you need to have your Salesforce username, password, and security token stored as secrets with the names `salesforce/username`, `salesforce/password`, and `salesforce/token`, respectively.

This code uses Databricks Secrets to securely access your Salesforce credentials. If you are not using Databricks, you can still use this code by substituting your own credentials directly in the code instead of using Databricks Secrets. To do this, replace the calls to `dbutils.secrets.get()` with the appropriate code to retrieve your Salesforce credentials.

### Functionality

This code defines a class called `Salesforce_SOQL_Spark_Connector` with the following methods:

- `__init__(self, username, password, security_token)`: 
  - Initializes the Salesforce_SOQL_Spark_Connector object with the given Salesforce credentials.
- `auth(self)`: 
  - Authenticates the user with Salesforce using the given credentials.
- `get_fields_and_object(self, query)`: 
  - Parses the SELECT statement in a SOQL query to return a list of fields and the object being queried.
- `build_query(self, fields_a, fields_b, object, where, group_by, limit)`: 
  - Builds one or two SOQL queries based on the given fields, object, and WHERE, GROUP BY, and LIMIT clauses.
- `run_query(self, query, include_deleted=False)`: 
  - Executes a SOQL query and returns the data as a dictionary.
- `process_df(self, df)`: 
  - Cleans up a Pandas DataFrame obtained from Salesforce and returns a Spark DataFrame.
- `get_where(self, query)`: 
  - Extracts the WHERE clause from a SOQL query.
- `get_group_by(self, query)`: 
  - Extracts the GROUP BY clause from a SOQL query.
- `get_limit(self,query)`: 
  - Extracts the LIMIT clause from a SOQL query.
- `run_query_with_backoff(self, query, include_deleted=False)`: 
  - Executes a SOQL query and automatically retries if there is an error.
- `get_query_lists(self, data, select_star)`: 
  - Separates a long list of fields into two shorter lists if the length of the list exceeds 400.
- `build_query_fields(self, fields, object, include_deleted)`: 
  - Builds a SELECT statement for a SOQL query based on the given fields and object.
- `create_temp_view_from_salesforce_object(self, query, temp_view, include_deleted=False, print_soql=True)`: 
  - Retrieves data from Salesforce using a SOQL query, cleans up the data, and stores it in a temporary Spark SQL view.

### Usage

Here's an example of how to use this connector:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Salesforce SOQL Spark Connector').getOrCreate()

# Initialize the Salesforce_SOQL_Spark_Connector object
sf_to_spark = Salesforce_SOQL_Spark_Connector(dbutils.secrets.get("salesforce", "username"), dbutils.secrets.get("salesforce", "password"), dbutils.secrets.get("salesforce", "token"))
sf_to_spark.auth()

# Define the SOQL query to retrieve data
query     = "SELECT owner.name,* FROM opportunity limit 1"
temp_view = "temp_view"

# Create a temporary view of the Salesforce object in a Spark dataframe
sf_to_spark.create_temp_view_from_salesforce_object(query, temp_view)

# Query the Spark dataframe to retrieve the data
data = spark.sql(f"SELECT * FROM {temp_view}")

# Show the data
display(data)
