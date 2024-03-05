# coding:utf-8
# ---------------------------------------------------------------------------
# __author__ = 'Satoshi Imai'
# __credits__ = ['Satoshi Imai']
# __version__ = "0.9.1"
# ---------------------------------------------------------------------------

import json
import logging
import time
from typing import Any, Dict, List, Union

import boto3
import pandas as pd
from botocore.config import Config
from pycodehelper.json import CustomJsonEncoder

from .s3path import s3path


class AthenaCallException(Exception):
    pass


class AthenaClient(object):
    def __init__(self,
                 profile: str = None,
                 region: str = 'ap-northeast-1',
                 config: Config = None,
                 database: str = None,
                 workplace: str = None,
                 workgroup: str = None,
                 polling_time: float = 10,
                 connect_timeout: float = 60,
                 read_timeout: float = 60,
                 max_attempts: int = 0,
                 logger: logging.Logger = None,
                 error_as_exception: bool = True,
                 non_query_massage_as_exception: bool = True):

        super(AthenaClient, self).__init__()

        if logger is None:
            logger = logging.getLogger(__name__)
            # end if
        self.logger = logger

        self.__profile = profile
        self.__region = region
        self.__config = config
        self.__database = database
        self.__workplace = workplace
        self.__workgroup = workgroup
        self.__polling_time = polling_time
        self.__connect_timeout = connect_timeout
        self.__read_timeout = read_timeout
        self.__max_attempts = max_attempts
        self.__error_as_exception = error_as_exception
        self.__non_query_massage_as_exception = non_query_massage_as_exception

        self.__config_refresh()
        # end def

    def __config_refresh(self):
        if self.config is None:
            config = Config(connect_timeout=self.connect_timeout,
                            read_timeout=self.read_timeout,
                            retries={'max_attempts': self.max_attempts})
        else:
            config = self.config
            config.connect_timeout = self.connect_timeout
            config.read_timeout = self.read_timeout
            # end if
        self.__config = config
        # end def

    def get_profile(self) -> float:
        return self.__profile
        # end def

    def set_profile(self, value: float):
        self.__profile = value
        # end def

    profile = property(get_profile, set_profile)

    def get_region(self) -> float:
        return self.__region
        # end def

    def set_region(self, value: float):
        self.__region = value
        # end def

    region = property(get_region, set_region)

    def get_config(self) -> float:
        return self.__config
        # end def

    def set_config(self, value: float):
        self.__config = value
        self.__connect_timeout = value.connect_timeout
        self.__read_timeout = value.read_timeout
        if 'max_attempts' in value.retries:
            self.__max_attempts = value.retries['max_attempts']
            # end if
        # end def

    config = property(get_config, set_config)

    def get_database(self) -> str:
        return self.__database
        # end def

    def set_database(self, value: str):
        self.__database = value
        # end def

    database = property(get_database, set_database)

    def get_workplace(self) -> str:
        return self.__workplace
        # end def

    def set_workplace(self, value: str):
        self.__workplace = value
        # end def

    workplace = property(get_workplace, set_workplace)

    def get_workgroup(self) -> float:
        return self.__workgroup
        # end def

    def set_workgroup(self, value: float):
        self.__workgroup = value
        # end def

    workgroup = property(get_workgroup, set_workgroup)

    def get_polling_time(self) -> float:
        return self.__polling_time
        # end def

    def set_polling_time(self, value: float):
        self.__polling_time = value
        # end def

    polling_time = property(get_polling_time, set_polling_time)

    def get_connect_timeout(self) -> float:
        return self.__connect_timeout
        # end def

    def set_connect_timeout(self, value: float):
        self.__connect_timeout = value
        self.__config_refresh()
        # end def

    connect_timeout = property(get_connect_timeout, set_connect_timeout)

    def get_read_timeout(self) -> float:
        return self.__read_timeout
        # end def

    def set_read_timeout(self, value: float):
        self.__read_timeout = value
        self.__config_refresh()
        # end def

    read_timeout = property(get_read_timeout, set_read_timeout)

    def get_max_attempts(self) -> int:
        return self.__max_attempts
        # end def

    max_attempts = property(get_max_attempts)

    def get_error_as_exception(self) -> float:
        return self.__error_as_exception
        # end def

    def set_error_as_exception(self, value: float):
        self.__error_as_exception = value
        # end def

    error_as_exception = property(
        get_error_as_exception,
        set_error_as_exception)

    def get_non_query_massage_as_exception(self) -> bool:
        return self.__non_query_massage_as_exception
        # end def

    def set_non_query_massage_as_exception(self, value: bool):
        self.__non_query_massage_as_exception = value
        # end def

    non_query_massage_as_exception = property(get_non_query_massage_as_exception,
                                              set_non_query_massage_as_exception)

    def run_query(self,
                  query: str,
                  database: str = None,
                  dtype: Dict = None,
                  return_path: bool = False,
                  **kwargs: Any) -> Union[pd.DataFrame, str]:

        return self.__execute([query],
                              database=database,
                              is_data_query=True,
                              dtypes=[dtype],
                              return_paths=return_path,
                              **kwargs)[0]
        # end def

    def run_queries(self,
                    queries: List[str],
                    database: str = None,
                    dtypes: List[Dict] = None,
                    return_paths: bool = False,
                    **kwargs: Any) -> List[Union[pd.DataFrame, str]]:

        return self.__execute(queries,
                              database=database,
                              is_data_query=True,
                              dtypes=dtypes,
                              return_paths=return_paths,
                              **kwargs)
        # end def

    def run_nonquery(self,
                     query: str,
                     database: str = None) -> str:

        return self.__execute([query],
                              database=database,
                              is_data_query=False)[0]
        # end def

    def run_nonqueries(self,
                       queries: List[str],
                       database: str = None) -> List[str]:

        return self.__execute(queries,
                              database=database,
                              is_data_query=False)
        # end def

    def __execute(self,
                  queries: List[str],
                  database: str,
                  is_data_query: bool = True,
                  dtypes: List[Dict] = None,
                  return_paths: bool = False,
                  **kwargs: Any) -> List[Any]:

        if database is None:
            database = self.database
            # end if

        output_to = self.workplace
        if not output_to.endswith('/'):
            output_to += '/'
            # end if

        my_session = boto3.Session(
            region_name=self.region,
            profile_name=self.profile)

        my_client = my_session.client(
            'athena',
            region_name=self.region,
            config=self.config)

        responses = {}
        query_ids = {}
        query_states = {x: None for x in range(len(queries))}
        results: Dict[int, Any] = {}
        if dtypes is None:
            dtypes = [None for _ in range(len(queries))]
            # end if

        for index in range(len(queries)):
            additional_args = {}
            if self.__workgroup is not None:
                additional_args['WorkGroup'] = self.__workgroup
                # end if
            responses[index] = my_client.start_query_execution(
                QueryString=queries[index],
                QueryExecutionContext={'Database': database},
                ResultConfiguration={'OutputLocation': output_to},
                **additional_args
            )
            self.logger.info(
                json.dumps(
                    responses[index],
                    cls=CustomJsonEncoder))
            query_ids[index] = responses[index]['QueryExecutionId']
            # end for

        while self._keep_polling(query_states):
            for index in range(len(queries)):
                query_status = my_client.get_query_execution(
                    QueryExecutionId=query_ids[index])
                query_states[index] = query_status['QueryExecution']['Status'][
                    'State']

                if query_states[index] == 'SUCCEEDED':
                    if index in results:
                        # wait
                        pass
                    else:
                        query_output_path = query_status['QueryExecution']['ResultConfiguration']['OutputLocation']

                        self.logger.info(
                            json.dumps(query_status, cls=CustomJsonEncoder))
                        if is_data_query:
                            if return_paths:
                                results[index] = query_output_path
                            else:
                                results[index] = self.__obtain_data(
                                    query_output_path, dtypes[index], **kwargs)
                                # end if
                        else:
                            # non-query
                            results[index] = self.__check_result(
                                query_output_path)
                            if results[index] != '' and self.error_as_exception and self.non_query_massage_as_exception:
                                raise AthenaCallException(
                                    f'Athena query {query_states[index]}, {query_ids[index]}: {queries[index]}\nResult has a message: {results[index]}'
                                )
                                # end if
                            # end if
                        # end if
                elif query_states[index] == 'QUEUED' or query_states[
                        index] == 'RUNNING':
                    # wait
                    pass
                elif query_states[index] == 'FAILED' or query_states[
                        index] == 'CANCELLED':
                    message = f'Athena query {query_states[index]}, {query_ids[index]}: {queries[index]}\n{json.dumps(query_status, cls=CustomJsonEncoder)}'
                    if self.error_as_exception:
                        raise AthenaCallException(
                            message
                        )
                    else:
                        self.logger.debug(message)
                        results[index] = None
                        # end if
                else:
                    message = f'Athena query {query_states[index]}, {query_ids[index]}: {queries[index]}\n{json.dumps(query_status, cls=CustomJsonEncoder)}'
                    if self.error_as_exception:
                        raise AthenaCallException(
                            message
                        )
                    else:
                        self.logger.debug(message)
                        results[index] = None
                        # end if
                    # end if
                # end for

            time.sleep(self.polling_time)
            # end while

        return [results[x] for x in range(len(results))]
        # end def

    def __obtain_data(self,
                      output_to: str,
                      dtype: Dict = None,
                      **kwargs: Any) -> pd.DataFrame:

        my_session = boto3.session.Session(
            region_name=self.region,
            profile_name=self.profile)

        my_client = my_session.client(
            's3',
            region_name=self.region,
            config=self.config)

        bucket = s3path.bucket_name(output_to)
        key = '/'.join(s3path.to_list(output_to)[1:])
        obj = my_client.get_object(Bucket=bucket, Key=key)
        result = pd.read_csv(obj['Body'], dtype=dtype, **kwargs)
        return result
        # end def

    def __check_result(self, output_to: str) -> str:

        my_session = boto3.session.Session(
            region_name=self.region,
            profile_name=self.profile)

        my_client = my_session.client(
            's3',
            region_name=self.region,
            config=self.config)

        bucket = s3path.bucket_name(output_to)
        key = '/'.join(s3path.to_list(output_to)[1:])
        obj = my_client.get_object(Bucket=bucket, Key=key)
        result = obj['Body'].read().decode()
        return result
        # end def

    def _keep_polling(self, states: Dict) -> bool:
        for _, this_item in states.items():
            if this_item == 'QUEUED' or this_item == 'RUNNING' or this_item is None:
                return True
                # end if
            # end for
        return False
        # end def

    # end class
