#!/usr/bin/env python3
import json
import pandas as pd
from lsms_library.local_tools import get_dataframe

#converts the unit conversion table csv to a json
# NOTE: does NOT work as the json generated is invalid, had to be manually reformatted

units = get_dataframe('../1997/Data/unittable.csv')

units.to_json('units.json', orient='records', lines=True)
