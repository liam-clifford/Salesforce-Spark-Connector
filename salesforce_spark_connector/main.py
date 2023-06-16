import pandas as pd
import numpy as np
import datetime
import requests
import functools
import re
import time
import json

import simple_salesforce
from simple_salesforce import Salesforce

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("Spark") \
    .getOrCreate()
# OPTIONAL IF USING Databricks

def format_string_to_json(input_string):
    # Extract the JSON part from the raw string
    json_string = input_string[input_string.find("[{"):input_string.rfind("}]")+2]

    # Remove single and double quotes
    input_string = json_string.replace("'", "").replace('"', '')

    # Replace keys and string values with double-quoted strings
    pattern = r'([a-zA-Z_]+): ([\s\S]+?)(?=,\s*[a-zA-Z_]+:|})'
    formatted_string = re.sub(pattern, r'"\1": "\2"', input_string)

    # Remove surrounding brackets and replace line breaks
    final = formatted_string[1:-1].replace('\n', 'NEW_LINE')

    # Load the formatted string into a Python object
    data = json.loads(final)

    return data
    
class Salesforce_Spark_Connector:
    def __init__(self, username, password, security_token):
        self.username = username
        self.password = password
        self.security_token = security_token
        self.session = requests.Session()
        self.session.request = functools.partial(self.session.request, timeout=1000)
        self.sf = None

    def auth(self):
        
        import simple_salesforce
        from simple_salesforce import Salesforce
        
        self.sf = Salesforce(
            username=''.join([i for i in self.username]),
            password=''.join([i for i in self.password]),
            security_token=''.join([i for i in self.security_token]),
            session=self.session
        )
        

    def build_query(self, fields_a, fields_b, object, where, group_by, order_by, limit):
        query_a = f'SELECT {fields_a} FROM {object} {where} {group_by} {order_by} {limit}'
        if fields_b:
            query_b = f'SELECT id, {fields_b} FROM {object} {where} {group_by} {order_by} {limit}'
            return query_a, query_b
        else:
            return query_a, None

    def run_query(self, query, include_deleted=False):
        while True:
            try:
                data = self.sf.query_all(query, include_deleted=include_deleted)
                break
            except Exception as e:
                if str(e).lower().find('malformed request')!=-1:
                    code = format_string_to_json(str(e))['errorCode'].replace('NEW_LINE','\n')
                    print(f'Error Message ({code}): ',format_string_to_json(str(e))['message'].replace('NEW_LINE','\n'))
                if 'ConnectionError' in str(e) or 'REQUEST_LIMIT_EXCEEDED' in str(e):
                    print(f'{e}... pausing 10 seconds before re-attempting')
                    time.sleep(10)
                else:
                    raise
        return data

    def process_df(self, df):
        cols = [c for c in df.columns if c.lower().find('.attributes.') == -1 \
                and c.lower().find('attributes.type') == -1 \
                and c.lower().find('attributes.url') == -1]

        df = df[cols]
        if len(df) > 0:
            try:
                spark_df = spark.createDataFrame(df)
            except:
                lst = list(df)
                df[lst] = df[lst].astype(str)
                spark_df = spark.createDataFrame(df)
            return spark_df
        else:
            return None

          
    def get_where(self,query):
          
        import re
        query = re.sub(r'\n',' ',query)
          
        match = re.search(r'where(.+?)(?:(\s+group\s+by)|(\s+order\s+by)|(\s+limit)|$)', query, re.IGNORECASE | re.DOTALL)
        return 'WHERE ' + match.group(1) if match else ''

    
    def get_group_by(self,query):
          
        import re
        query = re.sub(r'\n',' ',query)
          
        match = re.search(r'group\s+by(.+?)(?:(\s+order\s+by)|(\s+limit)|$)', query, re.IGNORECASE | re.DOTALL)
        return 'GROUP BY ' + match.group(1) if match else ''

    
    def get_order_by(self,query):
          
        import re
        query = re.sub(r'\n',' ',query)
          
        match = re.search(r'order\s+by(.+?)(\s+limit|$)', query, re.IGNORECASE | re.DOTALL)
        return 'ORDER BY ' + match.group(1) if match else ''

    
    def get_limit(self,query):
          
        import re
        query = re.sub(r'\n',' ',query)
          
        match = re.search(r'limit(.+?)(?:$)', query, re.IGNORECASE | re.DOTALL)
        return 'LIMIT ' + match.group(1) if match else ''
    

    def get_query_lists(self, data, select_star):
        
        import re
        
        exceeds_field_threshold = False

        if len(data['fields']) > 400 and select_star:
            exceeds_field_threshold = True

        if exceeds_field_threshold:
            query_list_a = [data['fields'][i]['name'] for i in range(400)]
            query_list_b = [data['fields'][i]['name'] for i in range(400, len(data['fields']))]
        else:
            query_list_a = [data['fields'][i]['name'] for i in range(len(data['fields']))]
            query_list_b = []

        return query_list_a, query_list_b, exceeds_field_threshold


    def build_query_fields(self, fields, object, include_deleted):
        
        import re
        import pandas as pd
        
        select_star = '*' in fields
        
        if select_star:
            try:
                rest_query = f'sobjects/{object}/describe/'
                data = self.sf.restful(rest_query, params=None)
            except Exception as e:
                if str(e).lower().find('malformed request')!=-1:
                    code = format_string_to_json(str(e))['errorCode'].replace('NEW_LINE','\n')
                    print(f'Error Message ({code}): ',format_string_to_json(str(e))['message'].replace('NEW_LINE','\n'))
                if 'ConnectionError' in str(e) or 'REQUEST_LIMIT_EXCEEDED' in str(e):
                    print(f'{e}... pausing 10 seconds before re-attempting')
                    time.sleep(10)
                    rest_query = f'sobjects/{object}/describe/'
                    data = self.sf.restful(rest_query, params=None)
                else:
                    data = self.sf.restful(rest_query, params=None)

        else:
            inputted_fields = fields.split(',')
            data = {'fields': []}
            for i in range(len(inputted_fields)):
                data['fields'].append({'name': inputted_fields[i]})
                    
        query_list_a, query_list_b, exceeds_field_threshold = self.get_query_lists(data, select_star)

        additional_fields = [x for x in fields.split(',') if x != '*' \
                             and x.lower() not in [y.lower() for y in query_list_a] \
                             and x.lower() not in [str(y).lower() for y in query_list_b]]

        if len(additional_fields)>0:
          query_list_a = query_list_a + additional_fields

        return ','.join(query_list_a), ','.join(query_list_b)

      
    def create_temp_view_from_salesforce_object(self, query, temp_view, include_deleted=False, print_soql=True):
        
        import re
        import pandas as pd

        # Get fields and object from the query
        assert 'from' in query.lower(), 'FROM is missing from your query (You need to specify an object to query from)'
        fields = re.search(r'select\s+(.*?)\s+from', query, re.IGNORECASE).group(1).replace(' ', '')
        object = query.lower().split('from ')[1].split(' ')[0]

        # Build the query strings for API calls
        where = self.get_where(query)
        group_by = self.get_group_by(query)
        order_by = self.get_order_by(query)
        limit = self.get_limit(query)

        fields_a, fields_b = self.build_query_fields(fields, object, include_deleted)

        # Make the API call(s)
        if print_soql:
          print(f'SELECT {fields_a} FROM {object} {where} {group_by} {order_by} {limit}')
        print(group_by)
        data_a = self.run_query(f'SELECT {fields_a} FROM {object} {where} {group_by} {order_by} {limit}', include_deleted)
        data_b = self.run_query(f'SELECT id, {fields_b} FROM {object} {where} {group_by} {order_by} {limit}', include_deleted) if fields_b else None

        # Process the API response into a Spark DataFrame
        spark_df_a = self.process_df(pd.json_normalize(data_a['records']))
        if spark_df_a is not None:
            spark_df_a.createOrReplaceTempView('spark_df_a')
            if data_b is not None:
                spark_df_b = self.process_df(pd.json_normalize(data_b['records']))
                spark_df_b.createOrReplaceTempView('spark_df_b')

            # Combine the two DataFrames if there were two queries
            if fields_b:
                master_df = spark.sql(f'select *, current_timestamp as processdate from spark_df_a join spark_df_b using (id)')
            else:
                master_df = spark.sql(f'select *, current_timestamp as processdate from spark_df_a')

            master_pandas_df = master_df.toPandas()

            # include/exclude columns that contain ALL null values
            mask = master_pandas_df.applymap(lambda x: x is None)
            cols = master_pandas_df.columns[(mask).any()]
            for col in master_pandas_df[cols]:
              master_pandas_df.loc[mask[col], col] = ''
            master_df = spark.createDataFrame(master_pandas_df)

            # Create a temp view of the combined DataFrame
            master_df.createOrReplaceTempView(temp_view)
        else:
            print(f'0 rows returned for query "{query}"')

        
    def export_sfdc_report_into_spark_as_view(self, salesforce_report_id, temp_name, domain):

        import pandas as pd
        import requests
        import base64
        import json
        import io
        
        self.auth()
        headers  = {'Authorization': self.sf.session_id}
        cookie   = {'sid': self.sf.session_id}
        response = requests.get(f"https://{domain}.my.salesforce.com/{salesforce_report_id}?isdtp=p1&export=1&enc=UTF-8&xf=csv", headers=headers, cookies=cookie)

        urlData  = response.content
        rawData  = pd.read_csv(io.StringIO(urlData.decode('utf-8')))

        # convert all columns to strings
        lst = list(rawData)
        rawData[lst] = rawData[lst].astype(str)

        spark = SparkSession.builder.getOrCreate()
        spark_df = spark.createDataFrame(rawData)

        updated_columns = spark_df.columns
        spark_df_final  = spark_df.toDF(*updated_columns)
        spark_df_final.createOrReplaceTempView(f'{temp_name}')
