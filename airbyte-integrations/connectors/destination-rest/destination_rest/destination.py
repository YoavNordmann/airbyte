#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from typing import Any, Iterable, Mapping

from airbyte_cdk import AirbyteLogger
from airbyte_cdk.destinations import Destination
from airbyte_cdk.models import AirbyteConnectionStatus, AirbyteMessage, ConfiguredAirbyteCatalog, Status, Type
import requests, json



class DestinationRest(Destination):
    def write(
        self, config: Mapping[str, Any], configured_catalog: ConfiguredAirbyteCatalog, input_messages: Iterable[AirbyteMessage]
    ) -> Iterable[AirbyteMessage]:

        rest_endpoint = config.get("host") + ":" + config.get("port") + config.get("path")

        for message in input_messages:
            if message.type == Type.STATE:
                yield message
            elif message.type == Type.RECORD:
                record = message.record
                r = requests.post(rest_endpoint, json=self.prepare_message(record.data, config))
                print(f"Data sent to {rest_endpoint}. Return Status Code: {r.status_code}")
            else:
                # ignore other message types for now
                continue


    def prepare_message(self, data: json, config: Mapping[str, Any]):
        static_config = config.get("static_config")
        if(static_config):
            try:
                add_data = json.loads(static_config)
            except ValueError as e:
                print("Static Config values not in JSON format")
            else:
                data.update(add_data)

        key_name = config.get("key_name")
        if(key_name):
            return {
                key_name: data
            }

        return data
        

    def check(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        try:
            rest_endpoint = config.get("host") + ":" + config.get("port") + config.get("path")
            response = requests.get(rest_endpoint)
            response.raise_for_status()
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            return AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {repr(e)}")
