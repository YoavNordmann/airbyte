#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from destination_rest import DestinationRest


def test_prepare_message1():
    dr = DestinationRest();

    data = {
        "entry1": "data1"
    }

    config = {
        "something1": "something2"
    }    

    result = dr.prepare_message(data, config)
    assert result["entry1"]
    assert result.len == 1



def test_prepare_message2():
    dr = DestinationRest();

    data = {
        "entry1": "data1"
    }

    config = {
        "static_config": {
            "something1": "something2"
        },
        "data_key": "farunkel"
    }    

    result = dr.prepare_message(data, config)
    assert result["data_key"]
    assert result['farunkel'].len == 2




