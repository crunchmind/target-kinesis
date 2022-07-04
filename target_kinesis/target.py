import argparse
import io
import os
import sys
import json
import threading
import http.client
import urllib
import datetime
import collections
from decimal import Decimal

import pkg_resources
from jsonschema.validators import Draft4Validator
import singer

from .kinesis import *
from .firehose import *

DEFAULT_RECORD_CHUNKS = 10
DEFAULT_DATA_CHUNKS = 1000

logger = singer.get_logger()
RECORDS = []


def add_metadata_columns_to_schema(schema_message):
    """Metadata _sdc columns according to the stitch documentation at
    https://www.stitchdata.com/docs/data-structure/integration-schemas#sdc-columns
    Metadata columns gives information about data injections"""

    schema_message['schema']['properties'].update(
        _sdc_batched_at={'type': ['null', 'string'], 'format': 'date-time'},
        _sdc_deleted_at={'type': ['null', 'string']},
        _sdc_extracted_at={'type': ['null', 'string'], 'format': 'date-time'},
        _sdc_primary_key={'type': ['null', 'string']},
        _sdc_received_at={'type': ['null', 'string'], 'format': 'date-time'},
        _sdc_sequence={'type': ['integer']},
        _sdc_table_version={'type': ['null', 'string']})

    return schema_message


def add_metadata_values_to_record(record_message, schema_message, timestamp):
    """Populate metadata _sdc columns from incoming record message
    The location of the required attributes are fixed in the stream"""
    utcnow = timestamp.astimezone(datetime.timezone.utc).replace(tzinfo=None).isoformat()
    record_message['record'].update(
        _sdc_batched_at=utcnow,
        _sdc_deleted_at=record_message.get('record', {}).get('_sdc_deleted_at'),
        _sdc_extracted_at=record_message.get('time_extracted'),
        _sdc_primary_key=schema_message.get('key_properties'),
        _sdc_received_at=utcnow,
        _sdc_sequence=int(timestamp.timestamp() * 1e3),
        _sdc_table_version=record_message.get('version'))

    return record_message['record']


def remove_metadata_values_from_record(record_message):
    """Removes every metadata _sdc column from a given record message"""
    for key in {
        '_sdc_batched_at',
        '_sdc_deleted_at',
        '_sdc_extracted_at',
        '_sdc_primary_key',
        '_sdc_received_at',
        '_sdc_sequence',
        '_sdc_table_version'
    }:

        record_message['record'].pop(key, None)

    return record_message['record']


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


def float_to_decimal(value):
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, list):
        return [float_to_decimal(child) for child in value]
    if isinstance(value, dict):
        return {k: float_to_decimal(v) for k, v in value.items()}
    return value


def decode_line(line):
    try:
        o = json.loads(line)
    except json.decoder.JSONDecodeError:
        logger.error("Unable to parse:\n{}".format(line))
        raise
    return o


def get_line_type(decode_line, line):
    if 'type' not in decode_line:
        raise Exception(
            "Line is missing required key 'type': {}".format(line))
    return decode_line['type']


def handle_record(o, schemas, line, config, validators, ts):
    if 'stream' not in o:
        raise Exception(
            "Line is missing required key 'stream': {}".format(line))
    if o['stream'] not in schemas:
        raise Exception(
            "A record for stream {} was encountered before a corresponding schema".format(o['stream']))
    validate_record(o['stream'], o['record'], schemas, validators)

    if config.get('add_metadata_columns'):
        o['record'] = add_metadata_values_to_record(o, {}, ts)
    else:
        o['record'] = remove_metadata_values_from_record(o)

    o['record']['stream'] = o['stream']
    buffer_record(o['record'])


def handle_state(o):
    logger.debug('Setting state to {}'.format(o['value']))
    return o['value']


def handle_schema(o, schemas, validators, key_properties, line, config):
    if 'stream' not in o:
        raise Exception(
            "Line is missing required key 'stream': {}".format(line))
    stream = o['stream']

    if config.get('add_metadata_columns'):
        schemas[stream] = add_metadata_columns_to_schema(o)
    else:
        schemas[stream] = float_to_decimal(o['schema'])

    schemas[stream] = o['schema']
    validators[stream] = Draft4Validator(o['schema'])
    if 'key_properties' not in o:
        raise Exception("key_properties field is required")
    key_properties[stream] = o['key_properties']


    return schemas, validators, key_properties


def persist_lines(config, lines):

    global RECORDS
    RECORDS = []

    state = None
    schemas = {}
    key_properties = {}
    validators = {}

    lines_counter = 0

    timezone = datetime.timezone(datetime.timedelta(hours=config.get('timezone_offset'))) if config.get('timezone_offset') is not None else None
    now = datetime.datetime.now(timezone)

    for line in lines:
        lines_counter += 1

        # default to smallest between 10 records or 1kB
        record_chunks = config["record_chunks"] if "record_chunks" in config else DEFAULT_RECORD_CHUNKS
        data_chunks = config["data_chunks"] if "data_chunks" in config else DEFAULT_DATA_CHUNKS

        o = decode_line(line)
        t = get_line_type(o, line)

        if t == 'RECORD':
            handle_record(o, schemas, line, config, validators, now)
            state = None
        elif t == 'STATE':
            state = handle_state(o)
        elif t == 'SCHEMA':
            handle_schema(o, schemas, validators, key_properties, line, config)
        else:
            raise Exception(
                "Unknown message type {} in message {}".format(o['type'], o))

        enough_records = len(RECORDS) > record_chunks

        # approximate message size is calculated using stringified
        # version of the array. This is the faster way to get the 
        # approximated size of the data but require a +3 to skip the 
        # "empty array" special case
        enough_data = len(str(RECORDS)) > (data_chunks + 3) 

        if enough_records or enough_data:
            deliver_records(config, RECORDS)
            RECORDS = []

    # deliver pending records after last line
    if len(RECORDS) > 0:
        deliver_records(config, RECORDS)

    return state


def validate_record(stream, record, schemas, validators):
    pass
    # schema = schemas[stream]
    # validators[stream].validate(record)


def buffer_record(record):
    RECORDS.append(record)


def deliver_records(config, records):
    is_firehose = config.get("is_firehose", False)
    if is_firehose:
        client = firehose_setup_client()
        stream_name = config.get("stream_name", "missing-stream-name")
        firehose_deliver(client, stream_name, records)
    else:
        client = kinesis_setup_client()
        stream_name = config.get("stream_name", "missing-stream-name")
        partition_key = config.get("partition_key", "id")
        kinesis_deliver(client, stream_name, partition_key, records)


def load_config(config_filename):
    if config_filename:
        with open(config_filename) as input:
            config = json.load(input)
    else:
        config = {}
    return config


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    args = parser.parse_args()

    config = load_config(args.config)

    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = persist_lines(config, input)
    emit_state(state)

    logger.debug("Exiting normally")
