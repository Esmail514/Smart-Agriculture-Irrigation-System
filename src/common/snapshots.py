"""Immutable Parquet snapshots selected by one atomic MinIO/S3 manifest PUT.

Single writer per dataset is required. Old versions are intentionally retained:
readers may still use the previous manifest after a newer one is published.
"""
import json
import re
from uuid import uuid4

MANIFEST_NAME = '_current.json'
_VERSION = re.compile(r'^[0-9a-f]{32}$')


def _filesystem(spark, path):
    jvm = spark.sparkContext._jvm
    target = jvm.org.apache.hadoop.fs.Path(path)
    fs = target.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    return fs, jvm.org.apache.hadoop.fs.Path


def _read_manifest(spark, fs, target):
    jvm = spark.sparkContext._jvm
    stream = fs.open(target)
    reader = None
    try:
        reader = jvm.java.io.BufferedReader(jvm.java.io.InputStreamReader(stream, 'UTF-8'))
        lines = []
        while True:
            line = reader.readLine()
            if line is None:
                break
            lines.append(str(line))
        return json.loads('\n'.join(lines))
    finally:
        if reader is not None:
            reader.close()
        else:
            stream.close()


def _legacy_dataset_files(fs, path_cls, path):
    target = path_cls(path)
    if not fs.exists(target):
        return []
    files = fs.listFiles(target, True)
    root = path.rstrip('/') + '/'
    parquet_files = []
    while files.hasNext():
        file_path = str(files.next().getPath().toString())
        relative = file_path[len(root):] if file_path.startswith(root) else file_path
        # An interrupted first publication can leave a complete orphan version.
        # It must never be mistaken for a legacy dataset and unioned as history.
        if any(part == 'versions' or part.startswith('_') or part.startswith('.')
               for part in relative.split('/')[:-1]):
            continue
        if relative.endswith('.parquet'):
            parquet_files.append(file_path)
    return parquet_files


def read_latest_snapshot(spark, path, schema=None):
    """Read a pinned version; return None only for an unpublished dataset.

    Legacy Parquet is read only when actual Parquet files exist
    outside reserved version/temporary directories. All other errors propagate.
    """
    path = path.rstrip('/')
    fs, path_cls = _filesystem(spark, path)
    manifest_path = path_cls(f'{path}/{MANIFEST_NAME}')
    if fs.exists(manifest_path):
        manifest = _read_manifest(spark, fs, manifest_path)
        if not isinstance(manifest, dict) or manifest.get('format_version') != 1:
            raise ValueError(f'Unsupported snapshot manifest at {path}')
        version = manifest.get('version')
        if not isinstance(version, str) or not _VERSION.fullmatch(version):
            raise ValueError(f'Invalid snapshot version at {path}')
        dataset = f'{path}/versions/{version}'
        if not fs.exists(path_cls(dataset)):
            raise FileNotFoundError(f'Manifest points to missing snapshot {dataset}')
    else:
        legacy_files = _legacy_dataset_files(fs, path_cls, path)
        if not legacy_files:
            return None
        # Explicit files prevent Spark from discovering orphan version folders
        # beside an old flat snapshot after a failed first migration.
        reader = spark.read.schema(schema) if schema is not None else spark.read
        return reader.option('basePath', path).parquet(*legacy_files)
    reader = spark.read.schema(schema) if schema is not None else spark.read
    return reader.parquet(dataset)


def publish_snapshot(spark, df, path):
    """Write all Parquet successfully, then select it with a single-object PUT.

    S3A directory rename is copy/delete and cannot safely publish datasets.
    FSDataOutputStream.close commits this small manifest object in one S3 PUT.
    This atomic publication guarantee targets the project's MinIO/S3A storage;
    an overwrite stream on a local filesystem has different visibility rules.
    """
    path = path.rstrip('/')
    version = uuid4().hex
    dataset = f'{path}/versions/{version}'
    df.write.mode('errorifexists').parquet(dataset)
    manifest = json.dumps({'format_version': 1, 'version': version}, separators=(',', ':'))
    fs, path_cls = _filesystem(spark, path)
    stream = fs.create(path_cls(f'{path}/{MANIFEST_NAME}'), True)
    try:
        # JSON contains ASCII only; writeBytes avoids Py4J byte-array ambiguity.
        stream.writeBytes(manifest)
    except Exception:
        # Never close a partially written manifest: close would publish it.
        # Newer S3A streams expose Abortable; retain the original write error
        # if the installed Hadoop version does not implement abort.
        try:
            stream.getWrappedStream().abort()
        except Exception:
            pass
        raise
    stream.close()
    return dataset
