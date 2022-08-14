s3_keys = [
  {'name': 'immigration',
   'key': 'sas_data',
   'file_format': 'parquet',
   'sep': '',
   'quality_check': []
  },
  {'name': 'us_cities_demographics',
   'key': 'data/us-cities-demographics.csv',
   'file_format': 'csv',
   'sep': ';',
   'quality_check': []
  },
  {'name': 'airport_codes',
   'key': 'data/airport-codes_csv.csv',
   'file_format': 'csv',
   'sep': ',',
   'quality_check': []
  },
  {'name': 'world_temperature',
   'key': 'data/GlobalLandTemperaturesByCity.csv',
   'file_format': 'csv',
   'sep': ',',
   'quality_check': []
  },
]
sas_data = [
  {'name': 'i94cit_res',
   'value': 'i94cntyl',
   'columns': ['code', 'country'],
   'quality_check': [{'check_sql_query': "SELECT COUNT(*) FROM i94cit_res WHERE code is null", 'expected_result': 0}]
  },
  {'name': 'i94port',
   'value': 'i94prtl',
   'columns': ['code', 'port'],
   'quality_check': [{'check_sql_query': "SELECT COUNT(*) FROM i94port WHERE code is null", 'expected_result': 0}]
  },
  {'name': 'i94mode',
   'value': 'i94model',
   'columns': ['code', 'mode'],
   'quality_check': [{'check_sql_query': "SELECT COUNT(*) FROM i94mode WHERE code is null", 'expected_result': 0}]
  },
  {'name': 'i94addr',
   'value': 'i94addrl',
   'columns': ['code', 'addr'],
   'quality_check': [{'check_sql_query': "SELECT COUNT(*) FROM i94addr WHERE code is null", 'expected_result': 0}]
  },
  {'name': 'i94visa',
   'value': 'I94VISA',
   'columns': ['code', 'type'],
   'quality_check': [{'check_sql_query': "SELECT COUNT(*) FROM i94visa WHERE code is null", 'expected_result': 0}]
  }
]
