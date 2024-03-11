import boto3
import os
import time
import json
import re

def lambda_handler(event, context):
    # Extract database name from input parameters
    database_name = event['database_name']
    
    # Initialize Athena client
    client = boto3.client('athena')
    
    # Query Amazon Athena to get the list of tables for the given database
    query = f"SHOW TABLES IN {database_name}"
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database_name
        },
        WorkGroup="athena2",
        ResultConfiguration={
            'OutputLocation': event['athena_query_results_bucket_name']  
        }
    )
    query_execution_id = response['QueryExecutionId']
    
    
    # Get query status (Show tables query)
    while True:
        query_execution_state = client.get_query_execution(QueryExecutionId=query_execution_id)
        status = query_execution_state['QueryExecution']['Status']['State']
    
        if status in ['FAILED', 'CANCELLED', 'SUCCEEDED']:
            break
            
            
        time.sleep(0.5)
    
    # Get Query results - These are the table names
    query_results = client.get_query_results(QueryExecutionId=query_execution_id)
    
    data_list = query_results['ResultSet']['Rows']
    
    table_name_list =  [d['Data'][0]['VarCharValue'] for d in data_list]
    
    
    
    
    for table_name in table_name_list:
        # Get table DDL
        ddl_query = f"SHOW CREATE TABLE {database_name}.{table_name}"
        ddl_response = client.start_query_execution(
            QueryString=ddl_query,
            QueryExecutionContext={
                'Database': database_name
            },
            ResultConfiguration={
                'OutputLocation': event['athena_query_results_bucket_name']  # Update with your S3 bucket
            }
        )
        ddl_query_execution_id = ddl_response['QueryExecutionId']
        
        
        
        # Get query status (create tables query)
        while True:
            query_execution_state = client.get_query_execution(QueryExecutionId=ddl_query_execution_id)
            status = query_execution_state['QueryExecution']['Status']['State']
            
            if status in ['FAILED', 'CANCELLED']:
                break
            elif status in ['SUCCEEDED']:
                print("The queryID: " + ddl_query_execution_id +" is for table "+ table_name)
                s3 = boto3.client('s3')
                copy_source = {
                'Bucket': '300289082521-athena-query-results',
                'Key': ddl_query_execution_id+".txt"}
                
                s3.copy_object(CopySource=copy_source, Bucket=event['athena_new_ddl_bucket_name'], Key=database_name+'/original_ddl/'+table_name+'.ddl')
                
                # Read the file content from S3
                response = s3.get_object(Bucket=event['athena_new_ddl_bucket_name'], Key=database_name+'/original_ddl/'+table_name+'.ddl')
                ddl_content = response['Body'].read().decode('utf-8')
                
                # Extract table name and location from DDL using regex
                table_name_match = re.search(r'CREATE EXTERNAL TABLE `([^`]+)`', ddl_content)
                table_name1 = table_name_match.group(1) if table_name_match else None
        
                location_match = re.search(r"LOCATION\s+'([^']+)'", ddl_content)
                location = location_match.group(1) if location_match else None
                
                # Rewrite the DDL
                rewritten_ddl = f"CREATE EXTERNAL TABLE {table_name1}\n" \
                                f"LOCATION '{location}'\n" \
                                f"TBLPROPERTIES (\n" \
                                f"  'table_type'='DELTA'\n" \
                                f");"
                
                key_new = "updated_ddl/"+database_name+"/"+table_name+'.ddl_updated'
                
                s3.put_object(Bucket=event['athena_new_ddl_bucket_name'], Key=key_new, Body=rewritten_ddl.encode('utf-8'))
                
                break

            time.sleep(0.5)

        print("This file has the generated DDL")


    return "DDL statements stored successfully"

