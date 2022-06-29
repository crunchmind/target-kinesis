import boto3
import json


def firehose_setup_client():
    return boto3.client('firehose')


def firehose_deliver(client, stream_name, records):

    if len(records) == 0:
        raise Exception("Record list is empty")

    if isinstance(records, dict):
        raise Exception("Single record given, array is required")

    encoded_records = map(lambda x: json.dumps(x), records)
    payload = ("\n".join(encoded_records) + "\n")

    response = client.put_record(
        DeliveryStreamName=stream_name,
        Record={'Data': payload}
    )
    return response
