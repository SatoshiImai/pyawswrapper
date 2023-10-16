# coding:utf-8
# ---------------------------------------------------------------------------
# __author__ = 'Satoshi Imai'
# __credits__ = ['Satoshi Imai']
# __version__ = '0.9.0'
# ---------------------------------------------------------------------------

import posixpath
from typing import Any, List, Tuple


class s3path(object):
    _s3_protocol = 's3://'

    @classmethod
    def join(cls, base_path: str, *keys: Any) -> str:
        args = [base_path]
        for this_value in list(keys):
            if isinstance(this_value, List):
                args = args + this_value
            else:
                args = args + [this_value]
                # end if
            # end for
        return posixpath.join(*args)
        # end def

    @classmethod
    def split(cls, target: str) -> Tuple[str]:
        return posixpath.split(target)
        # end def

    @classmethod
    def basename(cls, target: str) -> str:
        return s3path.split(target)[1]
        # end def

    @classmethod
    def dirname(cls, target: str) -> str:
        return s3path.split(target)[0]
        # end def

    @classmethod
    def root_path(cls, target: str) -> str:
        return s3path.to_list(target)[0]
        # end def

    @classmethod
    def bucket_name(cls, target: str) -> str:
        return s3path.to_list(target)[0].lstrip(cls._s3_protocol)
        # end def

    @classmethod
    def to_list(cls, target: str) -> str:
        result = target.lstrip(cls._s3_protocol).split('/')
        result[0] = cls._s3_protocol + result[0]
        return result
        # end def

    # end class
