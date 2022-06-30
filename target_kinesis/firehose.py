import boto3
import json


def firehose_setup_client():
    return boto3.client('firehose')


def firehose_deliver(client, stream_name, records):

    if len(records) == 0:
        raise Exception("Record list is empty")

    if isinstance(records, dict):
        raise Exception("Single record given, array is required")

    records = list(map(lambda x: {'Data': json.dumps(x)}, records))

    response = client.put_record_batch(DeliveryStreamName=stream_name, Records=records)
    return response
