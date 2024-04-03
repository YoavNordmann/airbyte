#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from destination_rest import DestinationRest

if __name__ == "__main__":
    DestinationRest().run(sys.argv[1:])
