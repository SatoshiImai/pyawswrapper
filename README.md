# pyawswrapper

A simple AWS services wrapper for Python project.

## s3path

It uses `posixpath`.

```python
>>> from pyawswrapper import s3path
>>> s3path.join('s3://your_bucket', 'A', 'B', 'test.txt')
's3://your_bucket/A/B/test.txt'
>>> s3path.split('s3://your_bucket/A/B/test.txt')
('s3://your_bucket/A/B', 'test.txt')
>>> s3path.basename('s3://your_bucket/A/B/test.txt')
'test.txt'
>>> s3path.dirname('s3://your_bucket/A/B/test.txt')
's3://your_bucket/A/B'
>>> s3path.root_path('s3://your_bucket/A/B/test.txt')
's3://your_bucket'
>>> s3path.bucket_name('s3://your_bucket/A/B/test.txt')
'your_bucket'
>>> s3path.to_list('s3://your_bucket/A/B/test.txt')
['s3://your_bucket', 'A', 'B', 'test.txt']
```

## s3client

s3client requires `awscli`. It is a wrapper of `aws s3`.

```python
from pyawswrapper import s3client

my_s3client = s3client()

my_s3client.UpTos3('{your file path}', 's3://{Up to path}')

prefixes, files = my_s3client.ls('s3://{Up to path}' + '/')

my_s3client.GetFroms3('s3://{Up to path}', '{your file path}')
```

## AthenaClient

Call Athena and load result into pandas.DataFrame.
It supports `run_nonquery`, too.

```python
from pyawswrapper import AthenaClient

my_athena = AthenaClient(
    database='{your database}',
    workplace='s3://{your workplace}',
    workgroup='{your workgroup}')

type_def = {'column_a': int, 'column_b': int, 'column_c': int}

result_df = my_athena.run_query(
    'SELECT column_a, column_b, column_c FROM your_table;', 
    dtype=type_def)
```

## Change log

### 0.9.1

* `athenaclient.run_query` and `athenaclient.run_queries` support `**kwargs` parameter that is passed to read_csv.

## LICENSE

I inherited BSD 3-Clause License from [pandas](https://pypi.org/project/pandas/)
