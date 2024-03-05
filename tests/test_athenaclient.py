# coding:utf-8
# ---------------------------------------------------------------------------
# author = 'Satoshi Imai'
# credits = ['Satoshi Imai']
# version = '0.9.1'
# ---------------------------------------------------------------------------

import copy
import logging
import shutil
import tempfile
import time
from logging import Logger, StreamHandler
from pathlib import Path
from typing import Dict, Generator
from unittest.mock import Mock, patch

import boto3
import localstack_client.session
import pandas as pd
import pytest
from botocore.config import Config
from moto.athena import mock_athena

from src.pyawswrapper import (AthenaCallException, AthenaClient, s3client,
                              s3path)

mock_s3_path = 's3://localstack-bucket/athena'
MOTO_ACCOUNT_ID = '123456789012'


@pytest.fixture(scope='session', autouse=True)
def setup_and_teardown(tempdir: Path):
    # setup

    yield

    # teardown
    # end def


@pytest.fixture(scope='module')
def logger() -> Generator[Logger, None, None]:
    log = logging.getLogger(__name__)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s : %(message)s')
    s_handler = StreamHandler()
    s_handler.setLevel(logging.INFO)
    s_handler.setFormatter(formatter)
    log.addHandler(s_handler)

    yield log
    # end def


@pytest.fixture(scope='session')
def tempdir() -> Generator[Path, None, None]:

    tempdir = Path(tempfile.mkdtemp())
    yield tempdir
    if tempdir.exists():
        shutil.rmtree(tempdir)
        # end if
    # end def


@pytest.fixture(scope='session')
def test_df() -> Generator[pd.DataFrame, None, None]:

    test_df = pd.DataFrame([[1, 2, 3], [2, 3, 4], [5, 6, 7]], columns=[
                           'column_a', 'column_b', 'column_c'])

    yield test_df
    # end def


@pytest.mark.run(order=10)
def test_init(logger: Logger):

    logger.info('test init')

    my_athena = AthenaClient(
        profile='default',
        region='us-east-1',
        database='dummy_database',
        workplace=mock_s3_path,
        workgroup='dummy_workgroup',
        polling_time=20,
        connect_timeout=80,
        read_timeout=80,
        max_attempts=10,
        error_as_exception=False,
        non_query_massage_as_exception=False
    )

    assert my_athena.profile == 'default'
    assert my_athena.region == 'us-east-1'
    assert my_athena.database == 'dummy_database'
    assert my_athena.workplace == mock_s3_path
    assert my_athena.polling_time == 20
    assert my_athena.connect_timeout == 80
    assert my_athena.read_timeout == 80
    assert my_athena.max_attempts == 10
    assert my_athena.error_as_exception is False
    assert my_athena.non_query_massage_as_exception is False
    # end def


@pytest.mark.run(order=20)
def test_property_profile(logger: Logger):

    logger.info('test property: profile')

    my_athena = AthenaClient(logger=logger)

    my_athena.profile = 'dummy'
    assert my_athena.profile == 'dummy'
    # end def


@pytest.mark.run(order=30)
def test_property_region(logger: Logger):

    logger.info('test property: region')

    my_athena = AthenaClient(logger=logger)

    my_athena.region = 'dummy'
    assert my_athena.region == 'dummy'
    # end def


@pytest.mark.run(order=40)
def test_property_config(logger: Logger):

    logger.info('test property: config')

    my_athena = AthenaClient(logger=logger)

    my_config = Config(connect_timeout=100,
                       read_timeout=110,
                       retries={'max_attempts': 5})

    my_athena.config = my_config
    assert my_athena.config is my_config
    assert my_athena.connect_timeout == 100
    assert my_athena.read_timeout == 110
    assert my_athena.max_attempts == 5
    # end def


@pytest.mark.run(order=50)
def test_property_database(logger: Logger):

    logger.info('test property: database')

    my_athena = AthenaClient(logger=logger)

    my_athena.database = 'dummy'
    assert my_athena.database == 'dummy'
    # end def


@pytest.mark.run(order=60)
def test_property_workplace(logger: Logger):

    logger.info('test property: workplace')

    my_athena = AthenaClient(logger=logger)

    my_athena.workplace = 's3://dummy'
    assert my_athena.workplace == 's3://dummy'
    # end def


@pytest.mark.run(order=70)
def test_property_workgroup(logger: Logger):

    logger.info('test property: workgroup')

    my_athena = AthenaClient(logger=logger)

    my_athena.workgroup = 'my_workgroup'
    assert my_athena.workgroup == 'my_workgroup'
    # end def


@pytest.mark.run(order=80)
def test_property_polling_time(logger: Logger):

    logger.info('test property: polling_time')

    my_athena = AthenaClient(logger=logger)

    my_athena.polling_time = 23
    assert my_athena.polling_time == 23
    # end def


@pytest.mark.run(order=90)
def test_property_connect_timeout(logger: Logger):

    logger.info('test property: connect_timeout')

    my_athena = AthenaClient(logger=logger)

    my_athena.connect_timeout = 120
    assert my_athena.connect_timeout == 120
    assert my_athena.config.connect_timeout == 120
    # end def


@pytest.mark.run(order=100)
def test_property_read_timeout(logger: Logger):

    logger.info('test property: read_timeout')

    my_athena = AthenaClient(logger=logger)

    my_athena.read_timeout = 150
    assert my_athena.read_timeout == 150
    assert my_athena.config.read_timeout == 150
    # end def


@pytest.mark.run(order=110)
def test_property_error_as_exception(logger: Logger):

    logger.info('test property: error_as_exception')

    my_athena = AthenaClient(logger=logger)

    my_athena.error_as_exception = False
    assert my_athena.error_as_exception is False
    # end def


@pytest.mark.run(order=120)
def test_property_non_query_massage_as_exception(logger: Logger):

    logger.info('test property: non_query_massage_as_exception')

    my_athena = AthenaClient(logger=logger)

    my_athena.non_query_massage_as_exception = False
    assert my_athena.non_query_massage_as_exception is False
    # end def


@mock_athena
@pytest.mark.run(order=200)
def test_run_query(tempdir: Path, test_df: pd.DataFrame, logger: Logger):

    logger.info('run_query')

    # generate mock stuff
    my_client = boto3.client('athena', region_name='ap-northeast-1')
    query = 'SELECT stuff'
    location = mock_s3_path
    database = 'dummy'

    start_result = my_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': location},
    )
    exec_id = start_result['QueryExecutionId']

    dump_to = tempdir.joinpath('Athena', f'{exec_id}.csv')
    dump_to.parent.mkdir(parents=True, exist_ok=True)
    test_df.to_csv(dump_to, header=True, index=False)

    my_s3client = s3client(use_local=True)
    my_s3client.UpTos3(dump_to, mock_s3_path)

    time.sleep(2)

    get_result = my_client.get_query_execution(
        QueryExecutionId=exec_id
    )

    # patch wrong OutputLocation
    get_result['QueryExecution']['ResultConfiguration']['OutputLocation'] = s3path.join(
        mock_s3_path, f'{exec_id}.csv')

    localstack_session = localstack_client.session.Session()

    mock_athena_client = Mock()
    mock_athena_client.start_query_execution.return_value = start_result
    mock_athena_client.get_query_execution.return_value = get_result

    mock_session = Mock()
    mock_session.client.return_value = mock_athena_client

    my_athena = AthenaClient(
        database='dummy',
        workplace=mock_s3_path,
        workgroup='dummy',
        logger=logger)

    type_def = {'column_a': int, 'column_b': int, 'column_c': int}

    with patch.object(boto3, 'Session', return_value=mock_session):
        with patch.object(boto3.session, 'Session', return_value=localstack_session):
            result_df = my_athena.run_query('SELECT dummy', dtype=type_def)
            # end with
        # end with

    sub_df = result_df - test_df
    assert sub_df.values.sum() == 0

    with patch.object(boto3, 'Session', return_value=mock_session):
        with patch.object(boto3.session, 'Session', return_value=localstack_session):
            result_df_2 = my_athena.run_query(
                'SELECT dummy', dtype=type_def, chunksize=1)
            # end with
        # end with

    chunk_count = 0
    for _ in result_df_2:
        chunk_count += 1
        # end for
    assert chunk_count == 3
    # end def


@mock_athena
@pytest.mark.run(order=210)
def test_run_queries(tempdir: Path, test_df: pd.DataFrame, logger: Logger):

    logger.info('run_query')

    # generate mock stuff
    my_client = boto3.client('athena', region_name='ap-northeast-1')
    query = 'SELECT stuff'
    location = mock_s3_path
    database = 'dummy'

    start_result = my_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': location},
    )
    exec_id = start_result['QueryExecutionId']

    get_result = my_client.get_query_execution(
        QueryExecutionId=exec_id
    )

    start_result_2 = my_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': location},
    )
    exec_id_2 = start_result_2['QueryExecutionId']

    get_result_2 = my_client.get_query_execution(
        QueryExecutionId=exec_id_2
    )

    # patch wrong OutputLocation
    get_result['QueryExecution']['ResultConfiguration']['OutputLocation'] = s3path.join(
        mock_s3_path, f'{exec_id}.csv')
    get_result_2['QueryExecution']['ResultConfiguration']['OutputLocation'] = s3path.join(
        mock_s3_path, f'{exec_id_2}.csv')

    get_result_3 = copy.deepcopy(get_result)
    get_result_3['QueryExecution']['Status']['State'] = 'RUNNING'

    localstack_session = localstack_client.session.Session()

    mock_athena_client = Mock()
    mock_athena_client.start_query_execution.side_effect = [
        start_result, start_result_2]
    mock_athena_client.get_query_execution.side_effect = [
        get_result_3, get_result_2, get_result, get_result_2]

    mock_session = Mock()
    mock_session.client.return_value = mock_athena_client

    my_athena = AthenaClient(
        database='dummy',
        workplace=mock_s3_path,
        workgroup='dummy',
        logger=logger)

    with patch.object(boto3, 'Session', return_value=mock_session):
        with patch.object(boto3.session, 'Session', return_value=localstack_session):
            results = my_athena.run_queries(
                ['SELECT dummy1', 'SELECT dummy2'], return_paths=True)
            # end with
        # end with

    logger.info(results)

    assert results[0] == get_result['QueryExecution']['ResultConfiguration']['OutputLocation']
    assert results[1] == get_result_2['QueryExecution']['ResultConfiguration']['OutputLocation']
    # end def


@mock_athena
@pytest.mark.run(order=220)
def test_run_nonquery(tempdir: Path, logger: Logger):

    logger.info('run_nonquery')

    # generate mock stuff
    my_client = boto3.client('athena', region_name='ap-northeast-1')
    query = 'MSCK REPAIR TABLE stuff'
    location = mock_s3_path
    database = 'dummy'

    start_result = my_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': location},
    )
    exec_id = start_result['QueryExecutionId']

    dump_to = tempdir.joinpath('Athena', f'{exec_id}.txt')
    dump_to.parent.mkdir(parents=True, exist_ok=True)
    dump_to.touch()

    my_s3client = s3client(use_local=True)
    my_s3client.UpTos3(dump_to, mock_s3_path)

    time.sleep(2)

    get_result = my_client.get_query_execution(
        QueryExecutionId=exec_id
    )

    # patch wrong OutputLocation
    get_result['QueryExecution']['ResultConfiguration']['OutputLocation'] = s3path.join(
        mock_s3_path, f'{exec_id}.txt')

    localstack_session = localstack_client.session.Session()

    mock_athena_client = Mock()
    mock_athena_client.start_query_execution.return_value = start_result
    mock_athena_client.get_query_execution.return_value = get_result

    mock_session = Mock()
    mock_session.client.return_value = mock_athena_client

    my_athena = AthenaClient(
        database='dummy',
        workplace=mock_s3_path,
        workgroup='dummy',
        logger=logger)

    with patch.object(boto3, 'Session', return_value=mock_session):
        with patch.object(boto3.session, 'Session', return_value=localstack_session):
            result = my_athena.run_nonquery('MSCK REPAIR TABLE stuff')
            # end with
        # end with

    assert result == ''
    # end def


@mock_athena
@pytest.mark.run(order=230)
def test_run_nonqueries(tempdir: Path, test_df: pd.DataFrame, logger: Logger):

    logger.info('run_nonqueries')

    # generate mock stuff
    my_client = boto3.client('athena', region_name='ap-northeast-1')
    query = 'MSCK REPAIR TABLE stuff'
    location = mock_s3_path
    database = 'dummy'

    start_result = my_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': location},
    )
    exec_id = start_result['QueryExecutionId']

    get_result = my_client.get_query_execution(
        QueryExecutionId=exec_id
    )

    start_result_2 = my_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': location},
    )
    exec_id_2 = start_result_2['QueryExecutionId']

    get_result_2 = my_client.get_query_execution(
        QueryExecutionId=exec_id_2
    )

    dump_to = tempdir.joinpath('Athena', f'{exec_id}.txt')
    dump_to.parent.mkdir(parents=True, exist_ok=True)
    dump_to.touch()

    dump_to_2 = tempdir.joinpath('Athena', f'{exec_id_2}.txt')
    dump_to_2.touch()

    my_s3client = s3client(use_local=True)
    my_s3client.UpTos3(dump_to.parent, mock_s3_path, recursive=True)

    time.sleep(2)

    # patch wrong OutputLocation
    get_result['QueryExecution']['ResultConfiguration']['OutputLocation'] = s3path.join(
        mock_s3_path, f'{exec_id}.txt')
    get_result_2['QueryExecution']['ResultConfiguration']['OutputLocation'] = s3path.join(
        mock_s3_path, f'{exec_id_2}.txt')

    get_result_3 = copy.deepcopy(get_result_2)
    get_result_3['QueryExecution']['Status']['State'] = 'QUEUED'

    localstack_session = localstack_client.session.Session()

    mock_athena_client = Mock()
    mock_athena_client.start_query_execution.side_effect = [
        start_result, start_result_2]
    mock_athena_client.get_query_execution.side_effect = [
        get_result, get_result_3, get_result, get_result_2]

    mock_session = Mock()
    mock_session.client.return_value = mock_athena_client

    my_athena = AthenaClient(
        database='dummy',
        workplace=mock_s3_path,
        workgroup='dummy',
        logger=logger)

    with patch.object(boto3, 'Session', return_value=mock_session):
        with patch.object(boto3.session, 'Session', return_value=localstack_session):
            results = my_athena.run_nonqueries(
                ['MSCK REPAIR TABLE stuff1', 'MSCK REPAIR TABLE stuff2'])
            # end with
        # end with

    logger.info(results)

    assert results == ['', '']
    # end def


@mock_athena
@pytest.mark.run(order=240)
def test_run_nonquery_exception(tempdir: Path, logger: Logger):

    logger.info('run_nonquery')

    # generate mock stuff
    my_client = boto3.client('athena', region_name='ap-northeast-1')
    query = 'MSCK REPAIR TABLE stuff'
    location = mock_s3_path
    database = 'dummy'

    start_result = my_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': location},
    )
    exec_id = start_result['QueryExecutionId']

    dump_to = tempdir.joinpath('Athena', f'{exec_id}.txt')
    dump_to.parent.mkdir(parents=True, exist_ok=True)

    with open(dump_to, 'w') as file:
        file.write('Some messages')
        # end with

    my_s3client = s3client(use_local=True)
    my_s3client.UpTos3(dump_to, mock_s3_path)

    time.sleep(2)

    get_result = my_client.get_query_execution(
        QueryExecutionId=exec_id
    )

    # patch wrong OutputLocation
    get_result['QueryExecution']['ResultConfiguration']['OutputLocation'] = s3path.join(
        mock_s3_path, f'{exec_id}.txt')

    localstack_session = localstack_client.session.Session()

    mock_athena_client = Mock()
    mock_athena_client.start_query_execution.return_value = start_result
    mock_athena_client.get_query_execution.return_value = get_result

    mock_session = Mock()
    mock_session.client.return_value = mock_athena_client

    my_athena = AthenaClient(
        database='dummy',
        workplace=mock_s3_path,
        workgroup='dummy',
        logger=logger)

    with pytest.raises(AthenaCallException):
        with patch.object(boto3, 'Session', return_value=mock_session):
            with patch.object(boto3.session, 'Session', return_value=localstack_session):
                my_athena.run_nonquery('MSCK REPAIR TABLE stuff')
                # end with
            # end with
        # end with

    my_athena.non_query_massage_as_exception = False
    with patch.object(boto3, 'Session', return_value=mock_session):
        with patch.object(boto3.session, 'Session', return_value=localstack_session):
            result = my_athena.run_nonquery(
                'MSCK REPAIR TABLE stuff')
            # end with
        # end with

    assert result == 'Some messages'
    # end def


@mock_athena
@pytest.mark.run(order=250)
def test_run_query_exception(logger: Logger):

    logger.info('run_query')

    # generate mock stuff
    my_client = boto3.client('athena', region_name='ap-northeast-1')
    query = 'SELECT stuff'
    location = mock_s3_path
    database = 'dummy'

    start_result = my_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': location},
    )
    exec_id = start_result['QueryExecutionId']

    get_result = my_client.get_query_execution(
        QueryExecutionId=exec_id
    )

    # patch status
    get_result['QueryExecution']['Status']['State'] = 'FAILED'

    localstack_session = localstack_client.session.Session()

    mock_athena_client = Mock()
    mock_athena_client.start_query_execution.return_value = start_result
    mock_athena_client.get_query_execution.return_value = get_result

    mock_session = Mock()
    mock_session.client.return_value = mock_athena_client

    my_athena = AthenaClient(
        database='dummy',
        workplace=mock_s3_path,
        workgroup='dummy',
        logger=logger)

    with pytest.raises(AthenaCallException):
        with patch.object(boto3, 'Session', return_value=mock_session):
            with patch.object(boto3.session, 'Session', return_value=localstack_session):
                my_athena.run_query('SELECT dummy')
                # end with
            # end with
        # end with

    my_athena.error_as_exception = False
    with patch.object(boto3, 'Session', return_value=mock_session):
        with patch.object(boto3.session, 'Session', return_value=localstack_session):
            result = my_athena.run_query('SELECT dummy')
            # end with
        # end with

    assert result is None
    # end def


@mock_athena
@pytest.mark.run(order=260)
def test_run_query_exception_02(logger: Logger):

    logger.info('run_query')

    # generate mock stuff
    my_client = boto3.client('athena', region_name='ap-northeast-1')
    query = 'SELECT stuff'
    location = mock_s3_path
    database = 'dummy'

    start_result = my_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': location},
    )
    exec_id = start_result['QueryExecutionId']

    get_result = my_client.get_query_execution(
        QueryExecutionId=exec_id
    )

    # patch status
    get_result['QueryExecution']['Status']['State'] = 'STOPPED'

    localstack_session = localstack_client.session.Session()

    mock_athena_client = Mock()
    mock_athena_client.start_query_execution.return_value = start_result
    mock_athena_client.get_query_execution.return_value = get_result

    mock_session = Mock()
    mock_session.client.return_value = mock_athena_client

    my_athena = AthenaClient(
        database='dummy',
        workplace=mock_s3_path,
        workgroup='dummy',
        logger=logger)

    with pytest.raises(AthenaCallException):
        with patch.object(boto3, 'Session', return_value=mock_session):
            with patch.object(boto3.session, 'Session', return_value=localstack_session):
                my_athena.run_query('SELECT dummy')
                # end with
            # end with
        # end with

    my_athena.error_as_exception = False
    with patch.object(boto3, 'Session', return_value=mock_session):
        with patch.object(boto3.session, 'Session', return_value=localstack_session):
            result = my_athena.run_query('SELECT dummy')
            # end with
        # end with

    assert result is None
    # end def


@pytest.mark.run(order=500)
@pytest.mark.parametrize('states,expected',
                         [({0: 'RUNNING', 1: 'RUNNING', 2: 'RUNNING', 3: 'RUNNING'}, True),
                          ({0: 'QUEUED', 1: 'RUNNING',
                           2: 'RUNNING', 3: 'RUNNING'}, True),
                          ({0: 'QUEUED', 1: 'QUEUED', 2: 'QUEUED', 3: 'QUEUED'}, True),
                          ({0: 'STOPPED', 1: 'STOPPED',
                           2: 'STOPPED', 3: 'RUNNING'}, True),
                          ({0: 'QUEUED', 1: 'RUNNING', 2: None, 3: None}, True),
                          ({0: 'STOPPED', 1: 'ERROR',
                           2: 'STOPPED', 3: 'STOPPED'}, False),
                          ({0: 'FAILED', 1: 'ERROR', 2: 'FAILED', 3: 'FAILED'}, False),
                          ({0: None, 1: None, 2: None, 3: None}, True)])
def test__keep_polling(states: Dict[int, str], expected: bool, logger: Logger):

    logger.info('_keep_polling')

    my_athena = AthenaClient(
        database='dummy',
        workplace=mock_s3_path,
        workgroup='dummy',
        logger=logger)

    assert my_athena._keep_polling(states) == expected
    # end def
