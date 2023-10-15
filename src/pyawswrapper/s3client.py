# coding:utf-8
# ---------------------------------------------------------------------------
# __author__ = 'Satoshi Imai'
# __credits__ = ['Satoshi Imai']
# __version__ = '0.9.0'
# ---------------------------------------------------------------------------

import logging
import os
import re
import warnings
from os import path
from typing import List, Tuple

from pyshellutil import ShellCaller, SubprocessErrorException

from .s3path import s3path


class ClientErrorException(Exception):
    pass


class s3client(object):

    def __init__(self, profile: str = None,
                 logger: logging.Logger = None, error_as_exception: bool = None, use_local: bool = False):
        super(s3client, self).__init__()

        self.__base_format = 'aws s3 cp "{0:}" "{1:}"'
        self.__use_local = use_local
        if self.__use_local:
            self.__base_format = 'awslocal s3 cp "{0:}" "{1:}"'
            # end if
        self.__newline = '\n'

        self.profile = None
        self.logger = None
        self.error_as_exception = False
        self.__exit_code = None
        self.__command_line = None

        if profile is not None:
            self.profile = profile
            # end if

        if logger is not None:
            self.logger = logger
            # end if

        if error_as_exception is not None:
            self.error_as_exception = error_as_exception
            # end if
        # end def

    @property
    def exit_code(self):
        # get only property
        return self.__exit_code
        # end def

    @property
    def command_line(self):
        # get only property
        return self.__command_line
        # end def

    def get_profile(self):
        return self.__profile
        # end def

    def set_profile(self, value: str):
        self.__profile = value
        # end def

    profile = property(get_profile, set_profile)

    def get_logger(self):
        return self.__logger if self.__logger is not None else logging.getLogger(
            __name__)
        # end def

    def set_logger(self, value: str):
        self.__logger = value
        # end def

    logger = property(get_logger, set_logger)

    def get_error_as_exception(self):
        return self.__error_as_exception
        # end def

    def set_error_as_exception(self, value: bool):
        self.__error_as_exception = value
        # end def

    error_as_exception = property(
        get_error_as_exception,
        set_error_as_exception)

    def GetFroms3(self, s3target: str, target: str, recursive: bool = None,
                  exclude: str = None, include: str = None, profile_overwrite: str = None) -> str:

        result = ''
        command = ''

        if not recursive:
            if exclude is not None or include is not None:
                warning_string = 'WARNING: Keywords `exclude` or `include` should be used in `recursive` mode. These options are ignored.'
                result += warning_string + self.__newline
                self.logger.warning(warning_string)
                warnings.warn(warning_string, UserWarning)
                # end if

            command = self.__base_format.format(
                s3path.join(s3target, path.basename(target)), target)

            if profile_overwrite is not None:
                # profile overwrite
                command += f' --profile={profile_overwrite}'
            else:
                if self.profile is not None:
                    # default profile
                    command += f' --profile={self.profile}'
                    # end if
                # end if
            # end if

        if recursive:
            command = self.__base_format.format(s3target, target)
            command += ' --recursive'

            if profile_overwrite is not None:
                # profile overwrite
                command += f' --profile={profile_overwrite}'
            else:
                if self.profile is not None:
                    # default profile
                    command += f' --profile={self.profile}'
                    # end if
                # end if

            if exclude is not None:
                command += f' --exclude="{exclude}"'
                # end if
            if include is not None:
                command += f' --include="{include}"'
                # end if
            # end if

        self.__command_line = command
        shell = ShellCaller()
        try:
            shell_result = shell.call_subprocess(command)
            result += shell.parse_result(shell_result, self.logger)
            self.__exit_code = shell_result[0]
        except SubprocessErrorException as e:
            if self.error_as_exception:
                raise ClientErrorException(str(e))
            else:
                self.__exit_code = 1
                # end if
            # end try
        return result
        # end def

    def UpTos3(self, target: str, s3target: str, recursive: bool = None,
               exclude: str = None, include: str = None, profile_overwrite: str = None) -> str:

        result = ''
        command = ''

        if not recursive:
            if exclude is not None or include is not None:
                warning_string = 'WARNING: Keywords `exclude` or `include` should be used in `recursive` mode. These options are ignored.'
                result += warning_string + self.__newline
                self.logger.warning(warning_string)
                warnings.warn(warning_string, UserWarning)
                # end if

            command = self.__base_format.format(
                target, s3path.join(
                    s3target, path.basename(target)))

            if profile_overwrite is not None:
                # profile overwrite
                command += f' --profile={profile_overwrite}'
            else:
                if self.profile is not None:
                    # default profile
                    command += f' --profile={self.profile}'
                    # end if
                # end if
            # end if

        if recursive:
            command = self.__base_format.format(target, s3target)
            command += ' --recursive'

            if profile_overwrite is not None:
                # profile overwrite
                command += f' --profile={profile_overwrite}'
            else:
                if self.profile is not None:
                    # default profile
                    command += f' --profile={self.profile}'
                    # end if
                # end if

            if exclude is not None:
                command += f' --exclude="{exclude}"'
                # end if
            if include is not None:
                command += f' --include="{include}"'
                # end if
            # end if

        self.__command_line = command
        shell = ShellCaller()
        try:
            shell_result = shell.call_subprocess(command)
            result += shell.parse_result(shell_result, self.logger)
            self.__exit_code = shell_result[0]
        except SubprocessErrorException as e:
            if self.error_as_exception:
                raise ClientErrorException(str(e))
            else:
                self.__exit_code = 1
                # end if
            # end try
        return result
        # end def

    def ls(self, s3target: str, recursive: bool = None,
           profile_overwrite: str = None) -> Tuple[List[str]]:

        command = ''
        prefix_pattern = r'PRE\s*(?P<name>.*)$'
        file_pattern = r'(Bytes|KiB|MiB|GiB|TiB)\s*(?P<name>.*)$'
        prefix_prog = re.compile(prefix_pattern)
        file_prog = re.compile(file_pattern)

        ls_base_format = 'aws s3 ls "{0}" --human-readable'
        if self.__use_local:
            ls_base_format = 'awslocal s3 ls "{0}" --human-readable'
            # end if
        if recursive:
            ls_base_format += ' --recursive'
            # end if

        command = ls_base_format.format(s3target)
        if profile_overwrite is not None:
            # profile overwrite
            command += f' --profile={profile_overwrite}'
        else:
            if self.profile is not None:
                # default profile
                command += f' --profile={self.profile}'
                # end if
            # end if

        self.__command_line = command

        prefixes = []
        files = []

        result_string = ''
        shell = ShellCaller()
        try:
            shell_result = shell.call_subprocess(command)
            result_string = shell.parse_result(shell_result, self.logger)
            self.__exit_code = shell_result[0]
        except SubprocessErrorException as e:
            if self.error_as_exception:
                raise ClientErrorException(str(e))
            else:
                self.__exit_code = 1
                # end if
            # end try

        if self.__exit_code == 0:
            candidates = result_string.split(os.linesep)
            for this_one in candidates:
                # prefix check
                if this_one != '':
                    prefix_match = prefix_prog.search(this_one)
                    file_match = file_prog.search(this_one)
                    if prefix_match is not None:
                        name = prefix_match.group('name')
                        if name != '':
                            prefixes.append(name)
                            # end if
                    elif file_match is not None:
                        name = file_match.group('name')
                        if name != '' and not name.endswith('/'):
                            files.append(name)
                            # end if
                        # end if
                    # end if
                # end for
            # end if

        return (prefixes, files)
        # end def

    # end class
