## Salesforce Spark Connector

The Salesforce Spark Connector is a Python library that makes it easy to retrieve data from Salesforce and materialize it in a Spark Temporary View. This library provides two main functionalities:

#### 1. Querying Salesforce Data with SOQL
- Makes it easy to query Salesforce data using Salesforce Object Query Language (SOQL) and store the data in a Spark Temporary View

#### 2. Exporting Salesforce Report Data to Spark
- Simulates the export of a Salesforce Report (using python's requests library) and stores it in a Spark Temporary View


### Shameless Plug
- Related Medium Post
  - Here's a [link]([url](https://medium.com/@liam_clifford/query-salesforce-soql-data-in-spark-5355b95122e5) to a (hopefully) less technical version of this! 

### Prerequisites

#### Credentials:
- To use this code in your environment, you need to have your Salesforce `username`, `password`, and `security token` available.
  - I personally use [Databricks Secrets](https://docs.databricks.com/security/secrets/index.html) for storing secrets. If you are not using Databricks, you can still use this code by substituting your own credentials directly in the code of course.

#### API Enabled Profile Permission:
- The Salesforce credentials that you use will need to be tied to a user that has a profile with the `API Enabled` permission.
  - For more details on how to set this up, see this step-by-step [guide](https://support.geckoboard.com/en/articles/6055614-enable-api-access-in-salesforce).


### Basic Setup
- Clone the repository:
```python
%sh rm -rf Salesforce-Spark-Connector
```
```python
%sh git clone https://github.com/liam-clifford/Salesforce-Spark-Connector.git
```
- Install the library using pip:
```python
%pip install ./Salesforce-Spark-Connector
```



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
- `export_sfdc_report_into_spark_as_view(self, salesforce_report_id, temp_name, domain)`:
  - Retrieves a Salesforce report in CSV format and stores it in a temporary Spark SQL view, allowing the user to easily analyze the report data using SQL.



### Usage

#### Here's how you would `authenticate`:

1. Initialize Spark Session (This depends on your working environment and will not always be needed)
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Salesforce SOQL Spark Connector').getOrCreate() 
```

2. Import the `Salesforce_Spark_Connector` module:
```python
import Salesforce_Spark_Connector
from Salesforce_Spark_Connector import Salesforce_Spark_Connector
```

3. Set up the Salesforce API connection by providing your Salesforce `username`, `password`, and `security token`:
```python
sf_username = 'your_salesforce_username'
sf_password = 'your_salesforce_password'
sf_token    = 'your_salesforce_token'

connect_to_sfdc_api = Salesforce_Spark_Connector(username=sf_username, password=sf_password, security_token=sf_token)
connect_to_sfdc_api.auth()
```



#### Here's an example of how to use this connector to query SOQL using Spark:
```python
# Define the SOQL query to retrieve data
query = "SELECT owner.name,* FROM opportunity limit 1"
temp_view = "temp_view"

# Create a temporary view of the Salesforce object in a Spark dataframe
connect_to_sfdc_api.create_temp_view_from_salesforce_object(query, temp_view)

# Query the Spark dataframe to retrieve the data
data = spark.sql(f"SELECT * FROM {temp_view}")

# Show the data
display(data)
```



#### Here's an example of how to use this connector to export a Salesforce Report ID and query it directly using Spark:
```python
# Define the ID of the Salesforce report you want to export (the ID typically starts with `00O`)
salesforce_report_id = 'insert_your_18_digit_salesforce_report_id'

# Define the name you want to give to the temporary Spark SQL view that will be created from the report data
view_name = 'my_report_data'

# Input your Company's Salesforce Domain (Typically located: `https://{domain}.lightning.force.com/` or `https://{domain}.my.salesforce.com`)
domain = 'your_company_sfdc_domain'

# Call the `export_sfdc_report_into_spark_as_view` method on the Salesforce_Spark_Connector instance, passing in the Salesforce report ID, view name, and your domain
connect_to_sfdc_api.export_sfdc_report_into_spark_as_view(salesforce_report_id, view_name, domain)

# Now you can use Spark SQL to query the data stored in your temp view
display(spark.sql(f"SELECT * FROM {view_name}"))

# Behind the scenes, the export_sfdc_report_into_spark_as_view method is doing the following:
# 1. Authenticating with Salesforce using your credentials
# 2. Retrieving the report data in CSV format from Salesforce's servers
# 3. Parsing the CSV data using Pandas
# 4. Converting the Pandas DataFrame to a Spark DataFrame
# 5. Creating a temporary SQL view from the Spark DataFrame

# Keep in mind that this method may not be suitable for very large reports, as it may fail due to the sheer size of the report.
```

### Limitations
- When you use lookup fields and `limit` together, the REST API returns the first part of the lookup field only
    ```python
    SELECT account.owner.name FROM opportunity LIMIT 1 
    ```
    - Returns: `Account`
- Whereas:
    ```python
    SELECT account.owner.name FROM opportunity
    ```
    - Returns: `Account.Owner.Name`
