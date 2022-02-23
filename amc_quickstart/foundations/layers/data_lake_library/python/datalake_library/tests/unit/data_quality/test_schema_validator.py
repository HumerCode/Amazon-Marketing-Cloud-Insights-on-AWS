# Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import botocore

from python.datalake_library.data_quality.schema_validator import ParquetSchemaValidator


class TestParquetSchemaValidator:

    @staticmethod
    def test_validation_off(mocker):
        mocker.patch('botocore.client.BaseClient._make_api_call', return_value={
            'Table': {
                'Parameters': {
                    'validate_schema': 'false'
                }
            }
        })
        mocker.patch('awswrangler.s3.read_parquet_metadata',
                     return_value=({}, None))

        assert ParquetSchemaValidator().validate('', [], None, None)

    @staticmethod
    def test_validation_failure_missing_columns(mocker):
        mocker.patch('botocore.client.BaseClient._make_api_call', return_value={
            'Table': {
                'Parameters': {
                    'validate_schema': 'true'
                },
                'StorageDescriptor': {
                    'Columns': [
                        {'Name': 'id', 'Type': 'string'}
                    ]
                }
            }
        })
        mocker.patch('awswrangler.s3.describe_objects', return_value={})
        mocker.patch('awswrangler.s3.read_parquet_metadata',
                     return_value=({}, None))

        assert not ParquetSchemaValidator().validate('', [], None, None)

    @staticmethod
    def test_validation_failure_different_types(mocker):
        mocker.patch('botocore.client.BaseClient._make_api_call', return_value={
            'Table': {
                'Parameters': {
                    'validate_schema': 'true'
                },
                'StorageDescriptor': {
                    'Columns': [
                        {'Name': 'id', 'Type': 'int'},
                        {'Name': 'op', 'Type': 'string'},
                        {'Name': 'last_modified_at', 'Type': 'timestamp'}
                    ]
                }
            }
        })
        mocker.patch('awswrangler.s3.describe_objects', return_value={})
        mocker.patch('awswrangler.s3.read_parquet_metadata', return_value=({
            'Op': 'string',
            'last_modified_at': 'timestamp',
            'id': 'string'}, None))

        assert not ParquetSchemaValidator().validate('', [], None, None)

    @staticmethod
    def test_validation_success(mocker):
        mocker.patch('botocore.client.BaseClient._make_api_call', return_value={
            'Table': {
                'Parameters': {
                    'validate_schema': 'true'
                },
                'StorageDescriptor': {
                    'Columns': [
                        {'Name': 'op', 'Type': 'string'},
                        {'Name': 'last_modified_at', 'Type': 'timestamp'},
                        {'Name': 'id', 'Type': 'string'}
                    ]
                }
            }
        })
        mocker.patch('awswrangler.s3.describe_objects', return_value={})
        mocker.patch('awswrangler.s3.read_parquet_metadata', return_value=({
            'Op': 'string',
            'last_modified_at': 'timestamp',
            'id': 'string'}, None))

        assert ParquetSchemaValidator().validate('', [], None, None)

    @staticmethod
    def test_validation_success_unordered(mocker):
        mocker.patch('botocore.client.BaseClient._make_api_call', return_value={
            'Table': {
                'Parameters': {
                    'validate_schema': 'true'
                },
                'StorageDescriptor': {
                    'Columns': [
                        {'Name': 'id', 'Type': 'string'},
                        {'Name': 'op', 'Type': 'string'},
                        {'Name': 'last_modified_at', 'Type': 'timestamp'}
                    ]
                }
            }
        })
        mocker.patch('awswrangler.s3.describe_objects', return_value={})
        mocker.patch('awswrangler.s3.read_parquet_metadata', return_value=({
            'Op': 'string',
            'last_modified_at': 'timestamp',
            'id': 'string'}, None))

        assert ParquetSchemaValidator().validate('', [], None, None)
