#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

docker exec namenode bash -c "bash $(cat $SCRIPT_DIR/load-data.sh)"