#!/usr/bin/env python
# -*- coding: utf-8 -*-
# This program is dedicated to the public domain under the MIT license.

# For MIME types
import magic
import mimetypes
import time

import threading
import queue

import io
import boto3
import botocore

from os import getenv, getpid
import argparse
import logging
import sys
import warnings

from builtins import ConnectionRefusedError


def warning_on_one_line(message, category, filename, lineno, file=None, line=None):
    return "%s:%s: %s:%s\n" % (filename, lineno, category.__name__, message)


warnings.formatwarning = warning_on_one_line


def iterate_bucket_items(bucket, start_with=""):
    """
    Generator that iterates over all objects in a given s3 bucket

    See http://boto3.readthedocs.io/en/latest/reference/services/s3.html#S3.Client.list_objects_v2
    for return data format
    :param bucket: name of s3 bucket
    :return: dict of metadata for an object
    """

    client = boto3.client("s3")
    StartingToken = None

    with open("token.txt", "r") as f:
        StartingToken = f.read()
    paginator = client.get_paginator("list_objects_v2")
    operation_parameters = {"Bucket": bucket, "PaginationConfig": {"MaxKeys": 500}}

    if StartingToken:
        operation_parameters["PaginationConfig"]["StartingToken"] = StartingToken
    page_iterator = paginator.paginate(**operation_parameters)

    for page in page_iterator:
        if page["KeyCount"] > 0:
            for item in page["Contents"]:
                if not start_with:
                    yield item
                else:
                    if item["Key"].startswith(start_with):
                        logging.info("Found %s", start_with)
                        start_with = None
                        yield item
                    else:
                        continue
            logging.info("Go to next page")
            if page.get("NextContinuationToken"):
                with open("token.txt", "w") as f:
                    f.write(page["NextContinuationToken"])


class S3File(io.RawIOBase):
    """
    Author: Alex Chan
    Source: https://alexwlchan.net/2019/02/working-with-large-s3-objects/
    """

    def __init__(self, s3_object):
        self.s3_object = s3_object
        self.position = 0

    def __repr__(self):
        return "<%s s3_object=%r>" % (type(self).__name__, self.s3_object)

    @property
    def size(self):
        return self.s3_object.content_length

    def tell(self):
        return self.position

    def seek(self, offset, whence=io.SEEK_SET):
        if whence == io.SEEK_SET:
            self.position = offset
        elif whence == io.SEEK_CUR:
            self.position += offset
        elif whence == io.SEEK_END:
            self.position = self.size + offset
        else:
            raise ValueError(
                "invalid whence (%r, should be %d, %d, %d)"
                % (whence, io.SEEK_SET, io.SEEK_CUR, io.SEEK_END)
            )

        return self.position

    def seekable(self):
        return True

    def read(self, size=-1):
        if size == -1:
            # Read to the end of the file
            range_header = "bytes=%d-" % self.position
            self.seek(offset=0, whence=io.SEEK_END)
        else:
            new_position = self.position + size

            # If we're going to read beyond the end of the object, return
            # the entire object.
            if new_position >= self.size:
                return self.read()

            range_header = "bytes=%d-%d" % (self.position, new_position - 1)
            self.seek(offset=size, whence=io.SEEK_CUR)

        return self.s3_object.get(Range=range_header)["Body"].read()

    def readable(self):
        return True


def proccessing(key, source_bucket_name, target_bucket_name, q):
    s3 = boto3.resource("s3")
    bytes_buffer = io.BytesIO()

    mime = magic.Magic(mime=True)

    s3_object = s3.Object(bucket_name=source_bucket_name, key=key["Key"])

    s3_file = S3File(s3_object)

    if key["Size"] > 1024:
        buff = s3_file.read(1024)
    elif key["Size"] == 0:
        logging.info("%(Key)s is empty - do nothing", key)
        q.task_done()
        return
    else:
        buff = s3_file.read()

    logging.info("Parse %(Key)s file", key)

    content_type = mime.from_buffer(buff)  # 'application/pdf'

    old_datetime = key["LastModified"]

    logging.debug("Detect content-type: %s", content_type)
    logging.debug("Detect last update: %s", old_datetime)

    s3_object.metadata.update({"last_modified": old_datetime.isoformat()})

    if content_type != "application/octet-stream":
        extension = mimetypes.guess_all_extensions(content_type)
        if extension:
            extension.sort()
            ext = extension[-1][1:]
        elif content_type == "application/gzip":
            ext = "gz"
        elif content_type == "application/CDFV2":
            ext = "doc"
        elif content_type == "text/x-shellscript":
            ext = "sh"
        elif content_type == "video/mp4":
            ext = "mp4"

        item_name = "%s.%s" % (key["Key"], ext)
    else:
        logging.info("Unknown content-type: %s", content_type)
        item_name = key["Key"]

    new_object = s3.Object(
        bucket_name=target_bucket_name,
        key="recovery/%s/%s" % (old_datetime.strftime("%Y/%m/%d"), item_name),
    )

    try:
        new_object.load()
    except botocore.exceptions.ClientError as ex:
        if ex.response["Error"]["Code"] == "404":
            # The object does not exist.
            new_object.copy_from(
                CopySource={
                    "Bucket": source_bucket_name,
                    "Key": key["Key"],
                },
                MetadataDirective="REPLACE",
                ContentType=content_type,
                Metadata=s3_object.metadata,
            )
            logging.info("Done")
            q.task_done()
            return True
        else:
            logging.error("Something else broken on key %(Key)s", key)
            # Something else has gone wrong.
            logging.exception(ex)
            q.task_done()
            return True
    else:
        # The object does exist.
        logging.info("File %(Key)s already present", key)
        q.task_done()
        return True


def møve(q):
    logging.info("PID: %s, working", getpid())
    from __main__ import proccessing

    while True:
        data = q.get()
        if not data:
            q.task_done()
            break

        source_bucket_name, target_bucket_name, key = data

        # set max retries
        retries = 10
        for attempt in range(retries):
            try:
                if proccessing(key, source_bucket_name, target_bucket_name, q):
                    break
            except (
                botocore.vendored.requests.exceptions.ConnectionError,
                botocore.vendored.requests.exceptions.ReadTimeout,
                botocore.exceptions.EndpointConnectionError,
                ConnectionRefusedError,
            ) as e:
                backoff = (2 ** attempt) * 10
                logging.info("Go to sleep %s", backoff)
                time.sleep(backoff)


def transfer(args):
    source_bucket_name = args.source
    target_bucket_name = args.target
    start_with = args.start_with
    num_worker_threads = args.threads

    q = queue.Queue()

    threads = []

    for i in range(num_worker_threads):
        t = threading.Thread(target=møve, args=(q,))
        t.start()
        threads.append(t)

    logging.debug("Source bucket name: %s", source_bucket_name)
    logging.debug("Target bucket name: %s", target_bucket_name)
    logging.debug("Start iterate from: %s", start_with)

    if not source_bucket_name or not target_bucket_name:
        logging.error("Args not defined")
        raise Exception("Args for processing not defined")

    for iter_key in iterate_bucket_items(source_bucket_name, start_with=start_with):
        q.put((source_bucket_name, target_bucket_name, iter_key))

    # block until all tasks are done
    q.join()

    logging.info("stopping workers!")

    # stop workers
    for i in range(num_worker_threads):
        q.put(None)

    for t in threads:
        t.join()


if __name__ == "__main__":

    formatter = "[%(asctime)s][%(levelname)s] %(name)s %(filename)s:%(funcName)s:%(lineno)d %(threadName)s | %(message)s"

    logging.basicConfig(level=logging.INFO, format=formatter)

    parser = argparse.ArgumentParser(
        prog=__file__,
        usage="%(prog)s [options]",
        description="""Mime-type parser""",
    )

    subparsers = parser.add_subparsers()

    transfer_parser = subparsers.add_parser("transfer")

    # set the default function to hello
    transfer_parser.set_defaults(func=transfer)
    transfer_parser.add_argument(
        "--source",
        nargs="?",
        required=False,
        default=getenv("SOURCE_BUCKET"),
    )

    transfer_parser.add_argument(
        "--target",
        nargs="?",
        required=False,
        default=getenv("TARGET_BUCKET"),
    )

    transfer_parser.add_argument(
        "--start_with",
        nargs="?",
        required=False,
        default=getenv("START_OBJECT_ID"),
    )

    transfer_parser.add_argument(
        "--threads",
        nargs="?",
        required=False,
        default=getenv("THREADS", 4),
    )

    args = parser.parse_args()
    if "func" not in args:
        parser.print_help()
    else:
        args.func(args)
