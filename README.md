# Salesforce-SOQL-Spark-Connector

This code allows you to extract data from a Salesforce object and convert it into a Spark DataFrame.

## Setup

### Dependencies

You need to have the following packages installed:

- pandas
- numpy
- datetime
- requests
- functools
- re
- time
- json
- simple_salesforce
- pyspark

### Authentication

To use this code in your environment, you need to have your Salesforce username, password, and security token stored as secrets with the names `salesforce/username`, `salesforce/password`, and `salesforce/token`, respectively.

This code uses Databricks Secrets to securely access your Salesforce credentials. If you are not using Databricks, you can still use this code by substituting your own credentials directly in the code instead of using Databricks Secrets. To do this, replace the calls to dbutils.secrets.get() with the appropriate code to retrieve your Salesforce credentials.

### Spark Session

This code creates a Spark session with the name "Python Spark SQL". You can customize the configuration of the Spark session by modifying the `SparkSession.builder` call in the code.

## Usage

To extract data from a Salesforce object and convert it into a Spark DataFrame, call the `create_temp_view_from_salesforce_object` function with the following arguments:

- `query`: A SOQL query string
- `table_name`: A string specifying the name of the temporary view that will be created for the resulting Spark DataFrame
- `include_deleted`: A Boolean indicating whether deleted records should be included in the results (default False)

The resulting Spark DataFrame can be accessed using the specified `table_name`. You can perform Spark SQL operations on the DataFrame, save it to a file, or create a permanent table in a database using the temporary view.

## Functionality

The `create_temp_view_from_salesforce_object` function performs the following steps:

1. Gets the Salesforce credentials from Databricks Secrets and sets up a Salesforce session
2. Extracts the fields and object from the query
3. Builds the query strings for API calls
4. Makes the API call(s) to Salesforce
5. Processes the API response into a Pandas DataFrame
6. Converts the Pandas DataFrame into a Spark DataFrame
7. Creates a temporary view of the Spark DataFrame using the specified `table_name`
