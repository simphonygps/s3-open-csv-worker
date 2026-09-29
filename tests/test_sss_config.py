import os
from unittest.mock import patch
import pytest
from app.config import get_settings
from app.sss_reader import SecretReadError

PUBLIC={'S3_BUCKET':'synthetic','PG_DB':'synthetic','PG_USER':'synthetic','PG_HOST':'database.invalid'}

def test_settings_read_three_files_without_environment_mutation_or_repr():
    with patch.dict(os.environ,PUBLIC,clear=True),patch('app.config.secret_text',side_effect=lambda key:'SYNTHETIC-PRIVATE-'+key) as read:
        before=dict(os.environ);settings=get_settings()
        assert {call.args[0] for call in read.call_args_list}=={'S3_ACCESS_KEY','S3_SECRET_KEY','PG_PASSWORD'}
        assert settings.pg_password=='SYNTHETIC-PRIVATE-PG_PASSWORD'
        assert 'SYNTHETIC-PRIVATE' not in repr(settings)
        assert dict(os.environ)==before

@pytest.mark.parametrize('name',('DB_DSN','DATABASE_URL','POSTGRES_DSN','PG_DSN','PGPASSWORD','PGPASSFILE'))
def test_secondary_authority_rejected(name):
    with patch.dict(os.environ,{**PUBLIC,name:'synthetic'},clear=True),pytest.raises(SecretReadError,match='legacy_database_authority_rejected'):
        get_settings()

def test_settings_cannot_fall_back_to_literals():
    with patch.dict(os.environ,{**PUBLIC,'S3_ACCESS_KEY':'synthetic'},clear=True),pytest.raises(SecretReadError,match='secret_reference_required'):
        get_settings()
