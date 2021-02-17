import boto3
import datetime


def cloudwatch_metric(
    namespace, name, dimensions, value, unit=None, timestamp=None, region='us-west-2'
):
    boto3.client('cloudwatch', region).put_metric_data(
        Namespace=namespace,
        MetricData=[
            {
                'MetricName': name,
                'Dimensions': dimensions,
                'Timestamp': timestamp or datetime.datetime.utcnow(),
                'Unit': unit,
                'Value': value,
            }
        ],
    )
