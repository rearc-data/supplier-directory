#!/bin/sh

### below just disabled for lambda layer
python3 -m pip uninstall --yes rearc_data_utils

### below just disabled for lambda layer
pip install ../rearc-data-utils/dist/rearc_data_utils-0.0.1-py3-none-any.whl
