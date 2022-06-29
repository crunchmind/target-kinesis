import boto3
import json


def kinesis_setup_client():
    return boto3.client('kinesis')


def kinesis_deliver(client, stream_name, partition_key, records):

    if len(records) == 0:
        raise Exception("Record list is empty")

    if isinstance(records, dict):
        raise Exception("Single record given, array is required")

    for record in records:
        response = client.put_record(
            DeliveryStreamName=stream_name,
            Record={'Data': json.dumps(record)},
            PartitionKey=records[0][partition_key]
        )
    return response
