# coding:utf-8
# ---------------------------------------------------------------------------
# author = 'Satoshi Imai'
# credits = ['Satoshi Imai']
# version = "0.9.0"
# ---------------------------------------------------------------------------

import logging
from logging import Logger, StreamHandler
from typing import Generator, List

import pytest

from src.pyawswrapper import s3path


@pytest.fixture(scope='session', autouse=True)
def setup_and_teardown():
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


@pytest.mark.run(order=10)
@pytest.mark.parametrize('args,expected',
                         [(['prefix1', 'prefix2', 'file1.txt'], 's3://test.com/prefix1/prefix2/file1.txt'),
                          (['prefix1/', 'prefix2/', 'file1.txt'],
                           's3://test.com/prefix1/prefix2/file1.txt'),
                          (['prefix1/prefix2/', 'file1.txt'], 's3://test.com/prefix1/prefix2/file1.txt')])
def test_join_01(args: List[str], expected: str, logger: Logger):

    result = s3path.join('s3://test.com', args)
    logger.info(result)
    assert expected == result
    # end def


@pytest.mark.run(order=20)
def test_join_02(logger: Logger):
    excepted = 's3://test.com/prefix1/prefix2/file1.txt'

    result = s3path.join('s3://test.com', 'prefix1', ['prefix2', 'file1.txt'])
    logger.info(result)
    assert excepted == result

    result = s3path.join('s3://test.com', 'prefix1/',
                         ['prefix2/', 'file1.txt'])
    logger.info(result)
    assert excepted == result

    result = s3path.join('s3://test.com', 'prefix1/',
                         ['prefix2/prefix3', 'file1.txt'])
    logger.info(result)
    assert 's3://test.com/prefix1/prefix2/prefix3/file1.txt' == result
    # end def


@pytest.mark.run(order=30)
def test_join_03(logger: Logger):
    excepted = 's3://test.com/prefix1/prefix2/file1.txt'

    result = s3path.join('s3://test.com', 'prefix1', 'prefix2', 'file1.txt')
    logger.info(result)
    assert excepted == result

    result = s3path.join('s3://test.com', 'prefix1/', 'prefix2/', 'file1.txt')
    logger.info(result)
    assert excepted == result

    result = s3path.join('s3://test.com', 'prefix1/prefix2/', 'file1.txt')
    logger.info(result)
    assert excepted == result

    result = s3path.join('s3://test.com', 'prefix1/prefix2', 'file1.txt')
    logger.info(result)
    assert excepted == result
    # end def


@pytest.mark.run(order=40)
def test_split(logger: Logger):
    excepted = ('s3://test.com/prefix1/prefix2', 'file1.txt')

    result = s3path.split('s3://test.com/prefix1/prefix2/file1.txt')
    logger.info(result)
    assert excepted == result
    # end def


@pytest.mark.run(order=50)
def test_basename(logger: Logger):
    excepted = 'file1.txt'

    result = s3path.basename('s3://test.com/prefix1/prefix2/file1.txt')
    logger.info(result)
    assert excepted == result
    # end def


@pytest.mark.run(order=60)
def test_dirname(logger: Logger):
    excepted = 's3://test.com/prefix1/prefix2'

    result = s3path.dirname('s3://test.com/prefix1/prefix2/file1.txt')
    logger.info(result)
    assert excepted == result
    # end def


@pytest.mark.run(order=70)
def test_root_path(logger: Logger):
    excepted = 's3://test.com'

    result = s3path.root_path('s3://test.com/prefix1/prefix2/file1.txt')
    logger.info(result)
    assert excepted == result
    # end def


@pytest.mark.run(order=80)
def test_bucket_name(logger: Logger):
    excepted = 'test.com'

    result = s3path.bucket_name('s3://test.com/prefix1/prefix2/file1.txt')
    logger.info(result)
    assert excepted == result
    # end def


@pytest.mark.run(order=90)
def test_to_list(logger: Logger):
    excepted = ['s3://test.com', 'prefix1', 'prefix2', 'file1.txt']

    result = s3path.to_list('s3://test.com/prefix1/prefix2/file1.txt')
    logger.info(result)
    assert excepted == result
    # end def
