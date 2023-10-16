# coding:utf-8
# ---------------------------------------------------------------------------
# author = 'Satoshi Imai'
# credits = ['Satoshi Imai']
# version = "0.9.0"
# ---------------------------------------------------------------------------

import logging
import shutil
import tempfile
from logging import Logger, StreamHandler
from pathlib import Path
from typing import Generator
from unittest.mock import patch

import pyshellutil
import pytest

from src.pyawswrapper import ClientErrorException, s3client, s3path

mock_s3_path = 's3://localstack-bucket'


@pytest.fixture(scope='session', autouse=True)
def setup_and_teardown(file1: Path,
                       file2: Path, file3: Path, file4: Path):
    # setup

    file3.parent.mkdir(parents=True, exist_ok=True)

    with open(file1, 'w') as file:
        file.write('file1')
        # end with

    with open(file2, 'w') as file:
        file.write('file2')
        # end with

    with open(file3, 'w') as file:
        file.write('file3')
        # end with

    with open(file4, 'w') as file:
        file.write('file4')
        # end with

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
def file1(tempdir: Path) -> Generator[Path, None, None]:

    yield tempdir.joinpath('file1.txt')
    # end def


@pytest.fixture(scope='session')
def file2(tempdir: Path) -> Generator[Path, None, None]:

    yield tempdir.joinpath('file2.txt')
    # end def


@pytest.fixture(scope='session')
def file3(tempdir: Path) -> Generator[Path, None, None]:

    yield tempdir.joinpath('dir1', 'file3.txt')
    # end def


@pytest.fixture(scope='session')
def file4(tempdir: Path) -> Generator[Path, None, None]:

    yield tempdir.joinpath('dir1', 'file4.txt')
    # end def


@pytest.mark.run(order=10)
def test_init(logger: Logger):

    logger.info('test init')

    my_s3client = s3client(
        profile='dummy',
        logger=logger,
        error_as_exception=True,
        use_local=True)

    assert my_s3client.profile == 'dummy'
    assert my_s3client.logger is logger
    assert my_s3client.error_as_exception is True
    # end def


@pytest.mark.run(order=20)
def test_property_profile(logger: Logger):

    logger.info('test property: profile')

    my_s3client = s3client(use_local=True)

    my_s3client.profile = 'dummy'
    assert my_s3client.profile == 'dummy'
    # end def


@pytest.mark.run(order=30)
def test_property_logger(logger: Logger):

    logger.info('test property: logger')

    my_s3client = s3client(use_local=True)

    my_s3client.logger = logger
    assert my_s3client.logger is logger
    # end def


@pytest.mark.run(order=40)
def test_property_error_as_exception(logger: Logger):

    logger.info('test property: error_as_exception')

    my_s3client = s3client(use_local=True)

    my_s3client.error_as_exception = True
    assert my_s3client.error_as_exception is True
    # end def


@pytest.mark.run(order=50)
def test_UpTos3_01(file1: Path, logger: Logger):

    logger.info('UpTos3')

    my_s3client = s3client(use_local=True)
    test_prefix = 'Up01'

    my_s3client.UpTos3(file1, s3path.join(mock_s3_path, test_prefix))

    logger.info(
        f'exit_code: {my_s3client.exit_code} / {my_s3client.command_line}')

    _, files = my_s3client.ls(s3path.join(mock_s3_path, test_prefix) + '/')
    assert file1.name in files
    # end def


@pytest.mark.run(order=60)
def test_UpTos3_02(file1: Path, logger: Logger):

    logger.info('UpTos3')

    my_s3client = s3client(profile='default', use_local=True)
    test_prefix = 'Up02'

    my_s3client.UpTos3(
        file1,
        s3path.join(
            mock_s3_path,
            test_prefix),
        include='file*.txt')

    logger.info(
        f'exit_code: {my_s3client.exit_code} / {my_s3client.command_line}')

    _, files = my_s3client.ls(s3path.join(mock_s3_path, test_prefix) + '/')
    assert file1.name in files
    # end def


@pytest.mark.run(order=70)
def test_UpTos3_03(file1: Path, logger: Logger):

    logger.info('UpTos3')

    my_s3client = s3client(use_local=True)
    test_prefix = 'Up03'

    my_s3client.UpTos3(
        file1,
        s3path.join(
            mock_s3_path,
            test_prefix),
        profile_overwrite='default')

    logger.info(
        f'exit_code: {my_s3client.exit_code} / {my_s3client.command_line}')

    _, files = my_s3client.ls(s3path.join(mock_s3_path, test_prefix) + '/')
    assert file1.name in files
    # end def


@pytest.mark.run(order=80)
def test_UpTos3_04(tempdir: Path, logger: Logger):

    logger.info('UpTos3')

    my_s3client = s3client(profile='default', use_local=True)
    test_prefix = 'Up04'

    my_s3client.UpTos3(
        tempdir,
        s3path.join(
            mock_s3_path,
            test_prefix),
        recursive=True)

    logger.info(
        f'exit_code: {my_s3client.exit_code} / {my_s3client.command_line}')

    prefixes, files = my_s3client.ls(
        s3path.join(mock_s3_path, test_prefix) + '/')
    assert ['file1.txt', 'file2.txt'] == files
    assert ['dir1/'] == prefixes
    # end def


@pytest.mark.run(order=90)
def test_UpTos3_05(tempdir: Path, logger: Logger):

    logger.info('UpTos3')

    my_s3client = s3client(use_local=True)
    test_prefix = 'Up05'

    my_s3client.UpTos3(
        tempdir,
        s3path.join(
            mock_s3_path,
            test_prefix),
        recursive=True, exclude='*', include='**/file3*',
        profile_overwrite='default')

    logger.info(
        f'exit_code: {my_s3client.exit_code} / {my_s3client.command_line}')

    prefixes, files = my_s3client.ls(
        s3path.join(mock_s3_path, test_prefix) + '/')
    assert [] == files
    assert ['dir1/'] == prefixes
    # end def


@pytest.mark.run(order=100)
def test_GetFroms3_01(tempdir: Path, logger: Logger):

    logger.info('GetFroms3')

    my_s3client = s3client(profile='default', use_local=True)
    test_prefix = 'Get01'

    my_s3client.UpTos3(
        tempdir,
        s3path.join(
            mock_s3_path,
            test_prefix),
        recursive=True, exclude='Get*')
    get_to = tempdir.joinpath(test_prefix)
    get_to.mkdir(parents=True, exist_ok=True)

    my_s3client.GetFroms3(
        s3path.join(
            mock_s3_path,
            test_prefix),
        get_to.joinpath('file1.txt'))

    logger.info(
        f'exit_code: {my_s3client.exit_code} / {my_s3client.command_line}')

    assert get_to.joinpath('file1.txt').exists()
    # end def


@pytest.mark.run(order=110)
def test_GetFroms3_02(tempdir: Path, logger: Logger):

    logger.info('GetFroms3')

    my_s3client = s3client(use_local=True)
    test_prefix = 'Get02'

    my_s3client.UpTos3(
        tempdir,
        s3path.join(
            mock_s3_path,
            test_prefix),
        recursive=True, exclude='Get*')
    get_to = tempdir.joinpath(test_prefix)
    get_to.mkdir(parents=True, exist_ok=True)

    my_s3client.GetFroms3(
        s3path.join(
            mock_s3_path,
            test_prefix),
        get_to.joinpath('file1.txt'),
        profile_overwrite='default',
        include="*")

    logger.info(
        f'exit_code: {my_s3client.exit_code} / {my_s3client.command_line}')

    assert get_to.joinpath('file1.txt').exists()
    # end def


@pytest.mark.run(order=120)
def test_GetFroms3_03(tempdir: Path, logger: Logger):

    logger.info('GetFroms3')

    my_s3client = s3client(profile='default', use_local=True)
    test_prefix = 'Get03'

    my_s3client.UpTos3(
        tempdir,
        s3path.join(
            mock_s3_path,
            test_prefix),
        recursive=True, exclude='Get*')
    get_to = tempdir.joinpath(test_prefix)
    get_to.mkdir(parents=True, exist_ok=True)

    my_s3client.GetFroms3(
        s3path.join(
            mock_s3_path,
            test_prefix),
        get_to,
        recursive=True)

    logger.info(
        f'exit_code: {my_s3client.exit_code} / {my_s3client.command_line}')

    assert get_to.joinpath('file1.txt').exists()
    assert get_to.joinpath('dir1', 'file4.txt').exists()
    # end def


@pytest.mark.run(order=130)
def test_GetFroms3_04(tempdir: Path, logger: Logger):

    logger.info('GetFroms3')

    my_s3client = s3client(use_local=True)
    test_prefix = 'Get04'

    my_s3client.UpTos3(
        tempdir,
        s3path.join(
            mock_s3_path,
            test_prefix),
        recursive=True, exclude='Get*')
    get_to = tempdir.joinpath(test_prefix)
    get_to.mkdir(parents=True, exist_ok=True)

    my_s3client.GetFroms3(
        s3path.join(
            mock_s3_path,
            test_prefix),
        get_to,
        recursive=True,
        exclude='*', include='dir1/file*.txt',
        profile_overwrite='default')

    logger.info(
        f'exit_code: {my_s3client.exit_code} / {my_s3client.command_line}')

    assert not get_to.joinpath('file1.txt').exists()
    assert get_to.joinpath('dir1', 'file4.txt').exists()
    # end def


@pytest.mark.run(order=140)
def test_GetFroms3_05(tempdir: Path, logger: Logger):

    logger.info('GetFroms3')

    my_s3client = s3client(use_local=True)
    test_prefix = 'Get05'
    get_to = tempdir.joinpath(test_prefix)

    my_s3client.GetFroms3(
        s3path.join(
            mock_s3_path,
            test_prefix, 'nofile.txt'),
        get_to)

    assert my_s3client.exit_code != 0

    my_s3client.error_as_exception = True
    with pytest.raises(ClientErrorException):
        my_s3client.GetFroms3(
            s3path.join(
                mock_s3_path,
                test_prefix, 'nofile.txt'),
            get_to)
        # end with
    # end def


@pytest.mark.run(order=150)
def test_UpTos3_06(tempdir: Path, logger: Logger):

    logger.info('UpTos3')

    my_s3client = s3client(use_local=True)
    test_prefix = 'Up05'

    with patch.object(pyshellutil.ShellCaller, 'call_subprocess', side_effect=pyshellutil.SubprocessErrorException):
        my_s3client.UpTos3(
            tempdir,
            s3path.join(
                mock_s3_path,
                test_prefix),
            recursive=True, exclude='*', include='**/file3*',
            profile_overwrite='default')

        assert my_s3client.exit_code != 0

        my_s3client.error_as_exception = True
        with pytest.raises(ClientErrorException):
            my_s3client.UpTos3(
                tempdir,
                s3path.join(
                    mock_s3_path,
                    test_prefix),
                recursive=True, exclude='*', include='**/file3*',
                profile_overwrite='default')
            # end with
        # end with
    # end def


@pytest.mark.run(order=160)
def test_ls_01(tempdir: Path, logger: Logger):

    logger.info('ls')

    my_s3client = s3client(use_local=True)
    test_prefix = 'ls01'

    my_s3client.UpTos3(
        tempdir,
        s3path.join(
            mock_s3_path,
            test_prefix),
        recursive=True, exclude='Get*')
    get_to = tempdir.joinpath(test_prefix)
    get_to.mkdir(parents=True, exist_ok=True)

    prefixes, files = my_s3client.ls(
        s3path.join(
            mock_s3_path,
            test_prefix + '/'),
        recursive=True,
        profile_overwrite='default')

    logger.info(
        f'exit_code: {my_s3client.exit_code} / {my_s3client.command_line}')

    assert prefixes == []
    diff = set(files) - set([
        'ls01/file1.txt',
        'ls01/file2.txt',
        'ls01/dir1/file3.txt',
        'ls01/dir1/file4.txt'])
    assert len(diff) == 0
    # end def


@pytest.mark.run(order=170)
def test_ls_02(tempdir: Path, logger: Logger):

    logger.info('ls')

    my_s3client = s3client(use_local=True)
    test_prefix = 'ls01'

    my_s3client.UpTos3(
        tempdir,
        s3path.join(
            mock_s3_path,
            test_prefix),
        recursive=True, exclude='Get*')
    get_to = tempdir.joinpath(test_prefix)
    get_to.mkdir(parents=True, exist_ok=True)

    with patch.object(pyshellutil.ShellCaller, 'call_subprocess', side_effect=pyshellutil.SubprocessErrorException):
        prefixes, files = my_s3client.ls(
            s3path.join(
                mock_s3_path,
                test_prefix + '/'),
            recursive=True)

        assert my_s3client.exit_code != 0
        assert prefixes == []
        assert files == []

        my_s3client.error_as_exception = True
        with pytest.raises(ClientErrorException):
            prefixes, files = my_s3client.ls(
                s3path.join(
                    mock_s3_path,
                    test_prefix + '/'),
                recursive=True)
            # end with
        # end with

    # end def
