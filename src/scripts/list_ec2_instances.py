import os
import boto3
import json
import datetime
import threading

import logging
from logging.handlers import RotatingFileHandler
import logmatic  # type: ignore

#
# helper functions
#


def os_touch(file):
    """
    creates a file if it doesn't already exist.

    :param file: the full path to the file to be removed
    :returns: None
    :rtype: void
    """
    try:
        with open(file, 'a'):
            os.utime(file, None)
    except Exception as e:
        logging.exception(e)


def convert_dt(dt):
    """
    converts a datetime field into a string for JSON serialization.

    :param dt: datetime value to be converted
    :returns: date as string
    :rtype: string
    """
    if isinstance(dt, datetime.datetime):
        return dt.__str__()


def write_to_firehose(firehose_client, stream_name, instances, snapshot_at):
    """
    writes a record to a kinesis firehose stream.

    :param firehose_client: an instance of a firehose client
    :param stream_name: the name of the stream to which we will write
    :param instances: the data to write to firehose
    :param snapshot_at: the time the snapshot was made
    :returns: none
    :rtype: void
    """
    for i in range(len(instances['Reservations'])):
        data = json.dumps(
            {"snapshot_at": snapshot_at, "data": instances['Reservations'][i]},
            default=convert_dt,
        )
        firehose_client.put_record(
            DeliveryStreamName=stream_name, Record={'Data': data + '\n'}
        )


def get_ec2_client(parent_arn, child_arn, child_session_name, aws_service, aws_region):
    """
    assumes a role and fires up a client for the aws service specified.

    :param parent_arn: arn associated with the role that has access to the service of interest
    :param child_arn: arn associated with the role that has access to the service of interest
    :param child_session_name: just give the session a name
    :param aws_service: the aws service needed
    :returns: aws client
    :rtype: AWS<A>
    """

    parent_assume_role_response = (
        boto3.Session()
        .client('sts')
        .assume_role(RoleArn=parent_arn, RoleSessionName="parent_role")
    )

    parent_credentials = parent_assume_role_response['Credentials']

    parent_session = boto3.Session(
        aws_access_key_id=parent_credentials['AccessKeyId'],
        aws_secret_access_key=parent_credentials['SecretAccessKey'],
        aws_session_token=parent_credentials['SessionToken'],
    )

    child_assume_role_response = parent_session.client('sts').assume_role(
        RoleArn=child_arn, RoleSessionName=child_session_name
    )

    child_credentials = child_assume_role_response['Credentials']

    return boto3.client(
        aws_service,
        aws_region,
        aws_access_key_id=child_credentials['AccessKeyId'],
        aws_secret_access_key=child_credentials['SecretAccessKey'],
        aws_session_token=child_credentials['SessionToken'],
    )


def get_service_client(arn, session_name, aws_service, aws_region):
    """
    assumes a role and fires up a client for the aws service specified.

    :param arn: arn associated with the role that has access to the service of interest
    :param session_name: just give the session a name
    :param aws_service: the aws service needed
    :param aws_region: the region of the aws service
    :returns: aws client
    :rtype: AWS<A>
    """

    assume_role_response = (
        boto3.Session()
        .client('sts')
        .assume_role(RoleArn=arn, RoleSessionName=session_name)
    )

    temp_credentials = assume_role_response['Credentials']

    return boto3.client(
        aws_service,
        aws_region,
        aws_access_key_id=temp_credentials['AccessKeyId'],
        aws_secret_access_key=temp_credentials['SecretAccessKey'],
        aws_session_token=temp_credentials['SessionToken'],
    )


def worker(thread_num, firehose_client, snapshot_at):
    """
    worker function for each thread.

    :param thread_num: the number of the thread (also acts as arn list index)
    :param firehose_client: an instance of a firehose client
    :param snapshot_at: the time the snapshot was made
    :returns: none
    :rtype: void
    """
    ec2 = get_ec2_client(
        CONFIG['parent_arn'],
        CONFIG['ec2_arn'][thread_num]['arn'],
        'DescribeEC2',
        'ec2',
        CONFIG['ec2_arn'][thread_num]['region'],
    )

    key = 'NextToken'
    kwargs = {'MaxResults': 1000, key: ''}

    while True:
        instances = ec2.describe_instances(**kwargs)
        write_to_firehose(
            firehose_client, 'instance-data-delivery', instances, snapshot_at
        )
        try:
            kwargs[key] = instances[key]
        except KeyError:
            break


#
# main procedure
#


def main():

    # load config file
    global CONFIG
    CONFIG = json.loads(open('/root/ec2_snapshot/config.json').read())

    # set up logging
    log_location = CONFIG['log_location']
    logger = logging.getLogger()
    os_touch(log_location)
    handler = RotatingFileHandler(filename=log_location, maxBytes=8000000)
    handler.setFormatter(logmatic.JsonFormatter())
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    # generate snapshot_at timestamp
    snapshot_at = convert_dt(datetime.datetime.now())

    # fire up firehose client
    firehose = boto3.client('firehose', 'us-west-2')

    # for each arn/aws account, create a thread to perform the describe-instances work
    threads = []
    for i in range(len(CONFIG['ec2_arn'])):
        t = threading.Thread(target=worker(i, firehose, snapshot_at))
        threads.append(t)
        t.start()


#
# run it on a cron schedule
#

if __name__ == '__main__':
    main()
