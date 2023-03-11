## Salesforce Spark Connector

The Salesforce Spark Connector is a Python library that makes it easy to retrieve data from Salesforce and materialize it in a Spark Temporary View. This library provides two main functionalities:

#### 1. Querying Salesforce Data with SOQL
- The first main functionality is the ability to easily query Salesforce data using Salesforce Object Query Language (SOQL) and store the data in a Spark Temporary View

#### 2. Exporting Salesforce Report Data to Spark
- The second main functionality is the ability to export data from Salesforce reports and store it in a Spark Temporary View

### Authentication

To use this code in your environment, you need to have your Salesforce username, password, and security token stored as secrets with the names `salesforce/username`, `salesforce/password`, and `salesforce/token`, respectively.

This code uses Databricks Secrets to securely access your Salesforce credentials. If you are not using Databricks, you can still use this code by substituting your own credentials directly in the code instead of using Databricks Secrets. To do this, replace the calls to `dbutils.secrets.get()` with the appropriate code to retrieve your Salesforce credentials.

### Functionality

This code defines a class called `Salesforce_Spark_Connector` with the following methods:

- `__init__(self, username, password, security_token)`: 
  - Initializes the Salesforce_Spark_Connector object with the given Salesforce credentials.
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
- `get_order_by(self, query)`: 
  - Extracts the ORDER BY clause from a SOQL query.
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
- `export_sfdc_report_into_spark_as_view(self, salesforce_report_id,temp_name)`:
  - Retrieves a Salesforce report in CSV format and stores it in a temporary Spark SQL view, allowing the user to easily analyze the report data using SQL.

### Usage

Here's an example of how to use this connector to query SOQL using Spark:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Salesforce SOQL Spark Connector').getOrCreate()

# Initialize the Salesforce_Spark_Connector object
sf_to_spark = Salesforce_Spark_Connector(dbutils.secrets.get("salesforce", "username"), dbutils.secrets.get("salesforce", "password"), dbutils.secrets.get("salesforce", "token"))
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
```


Here's an example of how to use this connector to export a Salesforce Report ID and query it directly using Spark:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Salesforce SOQL Spark Connector').getOrCreate()

# Create a new instance of the Salesforce_Spark_Connector class, passing in your Salesforce credentials
sf_connector = Salesforce_Spark_Connector(username='your_sf_username', password='your_sf_password', security_token='your_sf_security_token')

# Define the ID of the Salesforce report you want to export (the ID typically starts with `00O`)
report_id = 'insert_your_18_digit_salesforce_id_here'

# Define the name you want to give to the temporary Spark SQL view that will be created from the report data
view_name = 'my_report_data'

# Call the `export_sfdc_report_into_spark_as_view` method on the Salesforce_Spark_Connector instance, passing in the report ID and view name
sf_connector.export_sfdc_report_into_spark_as_view(report_id, view_name)

# Now you can use Spark SQL to query the data in the view
# For example, you could run a SELECT statement to get all the data from the view
spark.sql(f"SELECT * FROM {view_name}").show()

# Behind the scenes, the export_sfdc_report_into_spark_as_view method is doing the following:
# 1. Authenticating with Salesforce using your credentials
# 2. Retrieving the report data in CSV format from Salesforce's servers
# 3. Parsing the CSV data using Pandas
# 4. Converting the Pandas DataFrame to a Spark DataFrame
# 5. Creating a temporary SQL view from the Spark DataFrame
# All of this replicates the process of manually exporting a report from Salesforce, but with the added benefit of being automated and integrated with your Spark environment.
# Keep in mind that this method may not be suitable for very large reports, as it may fail due to the sheer size of the report. Also note that it will only pull the data that is visible in the report, so if you need additional fields (like IDs), you'll need to add those to the report before exporting.
```
