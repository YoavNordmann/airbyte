#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import time
from socket import socket
from typing import Any, Dict, List, Mapping

import docker
import pytest
from airbyte_cdk import AirbyteLogger
from airbyte_cdk.models import (
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStateMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    Status,
    SyncMode,
    Type,
)
from destination_rest import DestinationRest


@pytest.fixture(scope="module")
def docker_client():
    return docker.from_env()


@pytest.fixture(name="config", scope="module")
def config_fixture(docker_client):
    with socket() as s:
        s.bind(("", 0))
        available_port = s.getsockname()[1]

    config = {
        'host': f'http://localhost',
        'port': available_port,
        'path': "/test",
        'data_key': 'conf',
        'static_config': {
            'sc1': 'smaple_static_conf'
        }
    }
    container = docker_client.containers.run(
        "ealen/echo-server",
        name="echo_server",
        ports={80:config['port']},
        detach=True,
    )
    time.sleep(20)
    yield config
    container.kill()
    container.remove()    


@pytest.fixture(name="configured_catalog")
def configured_catalog_fixture() -> ConfiguredAirbyteCatalog:
    stream_schema = {
        "type": "object",
        "properties": {"string_col": {"type": "str"}, "int_col": {"type": "integer"}},
    }

    overwrite_stream = ConfiguredAirbyteStream(
        stream=AirbyteStream(name="overwrite_stream", json_schema=stream_schema, supported_sync_modes=[SyncMode.incremental]),
        sync_mode=SyncMode.incremental,
        destination_sync_mode=DestinationSyncMode.overwrite,
    )

    return ConfiguredAirbyteCatalog(streams=[overwrite_stream])



def _state(data: Dict[str, Any]) -> AirbyteStateMessage:
    return AirbyteMessage(type=Type.STATE, state=AirbyteStateMessage(data=data))


def _record(stream: str, str_value: str, int_value: int) -> AirbyteRecordMessage:
    return AirbyteMessage(
        type=Type.RECORD,
        record=AirbyteRecordMessage(
            stream=stream,
            data={"str_col": str_value, "int_col": int_value},
            emitted_at=0,
        ),
    )


def test_check_valid_config(config: Mapping):
    outcome = DestinationSftpJson().check(AirbyteLogger(), config)
    assert outcome.status == Status.SUCCEEDED


def test_send(config: Mapping, configured_catalog: ConfiguredAirbyteCatalog):

    overwrite_stream = configured_catalog.streams[0].stream.name,
    streams = [overwrite_stream]
    first_state_message = _state({"state": "1"})
    first_record_chunk = [_record(overwrite_stream, str(i), i) for i in range(5)] + [_record(overwrite_stream, str(i), i) for i in range(5)]

    second_state_message = _state({"state": "2"})
    second_record_chunk = [_record(overwrite_stream, str(i), i) for i in range(5, 10)] + [
        _record(overwrite_stream, str(i), i) for i in range(5, 10)
    ]

    destination = DestinationRest()

    expected_states = [first_state_message, second_state_message]
    output_states = list(
        destination.write(
            config,
            configured_catalog,
            [
                *first_record_chunk,
                first_state_message,
                *second_record_chunk,
                second_state_message,
            ],
        )
    )
    assert expected_states == output_states, "Checkpoint state messages were expected from the destination"

    expected_records = [_record(overwrite_stream, str(i), i) for i in range(10)] + [_record(overwrite_stream, str(i), i) for i in range(10)]
    records_in_destination = retrieve_all_records(client, streams)
    assert _sort(expected_records) == records_in_destination, "Records in destination should match records expected"

    # After this sync we expect the append stream to have 15 messages and the overwrite stream to have 5
    third_state_message = _state({"state": "3"})
    third_record_chunk = [_record(overwrite_stream, str(i), i) for i in range(10, 15)] + [
        _record(overwrite_stream, str(i), i) for i in range(10, 15)
    ]

    output_states = list(destination.write(config, configured_catalog, [*third_record_chunk, third_state_message]))
    assert [third_state_message] == output_states

    records_in_destination = retrieve_all_records(client, streams)
    expected_records = [_record(overwrite_stream, str(i), i) for i in range(15)] + [
        _record(overwrite_stream, str(i), i) for i in range(10, 15)
    ]
    assert _sort(expected_records) == records_in_destination
