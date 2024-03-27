from collections import defaultdict
from datetime import datetime

from botocore.exceptions import ClientError
from flask import Flask, request
import json
import boto3

app = Flask(__name__)
@app.route('/')
def hello():
    return 'Hello World!'


AWS_BUCKET_NAME = 'mw-code-tester'
AWS_ACCESS_KEY_ID="AKIAVQHKED5NDIVEAKU6"
AWS_REGION = 'ap-south-1'
AWS_SECRET_ACCESS_KEY="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"
S3_CLIENT = boto3.client('s3',aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)


@app.route('/ingest/', methods=['POST',])
def ingest():
    """
    1. split data, 2. get directory name, and 3. get s3 file , 4. count and store it
        request :
        {
            "logs: [
                "2023-10-11T10:31:00Z INFO [apache] Received GET request from 192.168.0.1 for /index.html",
                "2023-10-11T10:32:15Z INFO [apache] Request from 10.0.0.2 failed with status code 404 for /page-not-found.html",
                "2023-10-11T11:33:30Z WARN [nginx] Received POST request from 192.168.0.3 for /submit-form",
                "2023-10-11T11:34:45Z WARN [nginx] Timeout warning for request from 192.168.0.4 to /api/data",
                "2023-10-11T11:35:45Z WARN [nginx]  Timeout warning for request from 192.168.0.4 to /api/data"
            ]
        }
    """
    if request.method == 'POST':
        logs = (json.loads(request.data)).get("logs", [])
        log_dict = defaultdict(list)

        for log in logs:

            log_split_data = log.split()
            log_time = datetime.strptime(log_split_data[0], '%Y-%m-%dT%H:%M:%SZ')
            log_timestemp = log_time.strftime('%Y-%m-%dT%H:00:00')  # need 10-11 kind of formatting
            sev_level = log_split_data[1]
            source = log_split_data[2]
            message = ' '.join(log_split_data[3:])

            s3_key = f"ushank2/{log_timestemp}/{source}/{sev_level}/summary.log"

            try:
                S3_CLIENT.head_object(Bucket=AWS_BUCKET_NAME, Key=s3_key)
            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    # Object doesn't exist, create new summary log
                    summary_log = {}
            else:
                summary_log_response = S3_CLIENT.get_object(Bucket=AWS_BUCKET_NAME, Key=s3_key)
                summary_log = json.loads(summary_log_response['Body'].read().decode('utf-8'))
            summary_log[message] = summary_log.get(message, 0) + 1

            # Upload updated summary log to S3
            S3_CLIENT.put_object(
                Bucket=AWS_BUCKET_NAME,
                Key=s3_key,
                Body=json.dumps(summary_log),
                ContentType='application/json'
            )
        print(log_dict)

        return 'Logs ingested successfully'

    print(request.data)
    return 'Done'
# See PyCharm help at https://www.jetbrains.com/help/pycharm/

@app.route('/get_s3_logs/', methods=['GET',])

def get_s3_logs():
    summary_log_response = S3_CLIENT.get_object(Bucket=AWS_BUCKET_NAME, Key=f"ushank/2023-10-11T10:31:00Z/[apache]/INFO/summary.log")
    summary_log = json.loads(summary_log_response['Body'].read().decode('utf-8'))
    return summary_log

@app.route('/delete_all_files_in_prefix/', methods=['GET',])
def delete_all_files_in_prefix():

    # List all objects in the specified prefix
    response = S3_CLIENT.list_objects_v2(Bucket=AWS_BUCKET_NAME, Prefix='ushank')

    # Check if there are any objects to delete
    if 'Contents' in response:
        objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
        print(objects_to_delete)
        # Perform the delete operation
        S3_CLIENT.delete_objects(Bucket=AWS_BUCKET_NAME, Delete={'Objects': objects_to_delete})
    return  ''

@app.route('/top_error_logs/', methods=['POST',])
def top_error_logs():
    top_error_log = None
    max_count = 0

    paginator = S3_CLIENT.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=AWS_BUCKET_NAME, Prefix="ushank2"):
        for obj in page.get('Contents', []):
            key = obj['Key']
            print(key)
            if key.endswith('summary.log'):
                # Retrieve summary log from S3
                summary_log_response = S3_CLIENT.get_object(Bucket=AWS_BUCKET_NAME, Key=key)
                summary_log = json.loads(summary_log_response['Body'].read().decode('utf-8'))

                for log_message, count in summary_log.items():
                    if count > max_count:
                        top_error_log = log_message
                        max_count = count
                        top_error_log_key = key

    if top_error_log:
        # Extract service name and severity from the error log message
        name, log_timestemp, source, sev_level, file_name = top_error_log_key.split('/')
        return {
            'top-error': top_error_log,
            'count': max_count,
            'source': source,
            'sev_level': sev_level
        }
    else:
        return ''
