import asyncio
import gzip
import importlib
import json
import os
import sys
from unittest.mock import Mock,patch
import pytest
from app.config import Settings

@pytest.fixture
def parser():
    modules=('app.main','app.db','app.s3_client','app.csv_processor','app.ndjson_processor','app.retention_worker')
    for name in modules:sys.modules.pop(name,None)
    config=Settings('storage.invalid','synthetic-access','synthetic-secret','synthetic-bucket',False,'database.invalid',5432,'synthetic','synthetic','synthetic-password')
    with patch('app.config.get_settings',return_value=config):main=importlib.import_module('app.main')
    yield main
    for name in modules:sys.modules.pop(name,None)

def test_csv_parser_keeps_valid_invalid_counters_and_audit(parser):
    from app import csv_processor
    payload=b'timestamp,deviceId,latitude,longitude\n2026-01-01T00:00:00Z,synthetic,12,13\nbad,synthetic,12,13\n'
    with patch.object(csv_processor,'insert_soft_data_rows') as write:
        result=csv_processor.process_csv_bytes(payload)
    assert result=={'rows_total':2,'rows_inserted':1,'rows_failed':1}
    row=write.call_args.args[0][0];assert row['source']=='s3-open' and row['deviceid']=='synthetic'
    assert row['raw_payload']['deviceId']=='synthetic' and row['raw_payload_text']

@pytest.mark.parametrize('compressed',[False,True])
def test_existing_ndjson_version_and_gzip_kept(parser,compressed):
    from app import ndjson_processor
    payload=(json.dumps({'EN':{'TP':'T2.2','DI':'synthetic','SQ':1,'TS':'2026-01-01T00:00:00Z'},'GP':{'f':['LA','LO'],'r':[[12,13]]}})+'\n').encode()
    with patch.object(ndjson_processor,'insert_soft_data_rows') as write:
        result=ndjson_processor.process_ndjson_bytes(gzip.compress(payload) if compressed else payload,'ndjson_gz_file' if compressed else 'ndjson_file')
    assert result=={'rows_total':1,'rows_inserted':1,'rows_failed':0}
    assert write.call_args.args[0][0]['deviceid']=='synthetic'

def test_already_processed_object_is_not_downloaded(parser):
    with patch.object(parser,'is_object_processed',return_value=True),patch.object(parser,'download_object_to_bytes') as download,patch.object(parser,'mark_object_processing_started') as start:
        parser.handle_object('synthetic','file.csv')
    download.assert_not_called();start.assert_not_called()

def test_download_failure_records_type_not_private_exception(parser,caplog):
    with patch.object(parser,'is_object_processed',return_value=False),patch.object(parser,'mark_object_processing_started'),patch.object(parser,'download_object_to_bytes',side_effect=RuntimeError('synthetic-sensitive-error')),patch.object(parser,'mark_object_processed_error') as record:
        parser.handle_object('synthetic','file.csv')
    assert record.call_args.kwargs['error_code']=='DOWNLOAD_ERROR'
    assert record.call_args.kwargs['error_message']=='RuntimeError'
    assert 'synthetic-sensitive-error' not in caplog.text

def test_health_failure_does_not_reveal_exception(parser,caplog):
    with patch.object(parser,'ensure_processed_table',side_effect=RuntimeError('synthetic-sensitive-error')):
        response=asyncio.run(parser.health_db())
    assert response.status_code==500 and b'synthetic-sensitive-error' not in response.body
    assert 'synthetic-sensitive-error' not in caplog.text

def test_retention_parameters_are_not_changed_or_executed(parser):
    from app import retention_worker
    with patch.dict(os.environ,{'RETENTION_DAYS':'37','RETENTION_LIMIT':'42','RETENTION_DELETE_ENABLED':'false'}):
        assert retention_worker._get_retention_days()==37
        assert retention_worker._get_retention_limit()==42
        assert retention_worker._is_delete_enabled() is False
