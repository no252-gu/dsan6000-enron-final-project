"""
Ingest the raw Enron email corpus straight from CMU into your public S3 bucket (the "Data Lake").

Nothing is saved to disk: the tarball is streamed from CMU, unpacked on the fly, and the raw
email files are bundled into Parquet files of 25,000 emails each, written straight to S3.
Each row is one untouched email: (path, raw_text). No parsing or cleaning happens here.

Usage (on your EC2 instance):
    python3 ingest_enron.py YOUR-DATA-LAKE-BUCKET

Result:
    s3://YOUR-DATA-LAKE-BUCKET/enron/raw/part-0000.parquet
    s3://YOUR-DATA-LAKE-BUCKET/enron/raw/part-0001.parquet
    ...  (about 21 files)
"""
import io
import sys
import tarfile

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import requests

SOURCE_URL = "https://www.cs.cmu.edu/~enron/enron_mail_20150507.tar.gz"
PREFIX = "enron/raw"
ROWS_PER_FILE = 25_000


def upload_chunk(s3, bucket, part_num, paths, texts):
    table = pa.table({"path": paths, "raw_text": texts})
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    key = f"{PREFIX}/part-{part_num:04d}.parquet"
    s3.upload_fileobj(buf, bucket, key)
    print(f"  uploaded s3://{bucket}/{key} ({len(paths):,} emails)", flush=True)


def main(bucket, source=SOURCE_URL):
    s3 = boto3.client("s3")
    paths, texts, part_num, total = [], [], 0, 0

    if source.startswith("http"):
        print(f"Streaming {source} ...", flush=True)
        resp = requests.get(source, stream=True, timeout=60)
        resp.raise_for_status()
        fileobj = resp.raw
    else:  # local file, only used for testing
        fileobj = open(source, "rb")

    # "r|gz" = read the gzipped tar as a stream, one file at a time, without saving it
    with tarfile.open(fileobj=fileobj, mode="r|gz") as tar:
        for member in tar:
            if not member.isfile():
                continue
            f = tar.extractfile(member)
            if f is None:
                continue
            paths.append(member.name)                      # e.g. maildir/lay-k/inbox/28.
            texts.append(f.read().decode("latin-1"))
            total += 1
            if len(paths) == ROWS_PER_FILE:
                upload_chunk(s3, bucket, part_num, paths, texts)
                paths, texts, part_num = [], [], part_num + 1

    if paths:
        upload_chunk(s3, bucket, part_num, paths, texts)
        part_num += 1

    print(f"Done: {total:,} emails in {part_num} files at s3://{bucket}/{PREFIX}/")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("Usage: python3 ingest_enron.py YOUR-DATA-LAKE-BUCKET")
    main(*sys.argv[1:3])
