import boto3
import json
import os
from datetime import datetime, timezone


def lambda_handler(event, context):
    cloudwatch = boto3.client('cloudwatch')
    sns = boto3.client('sns')

    sns_topic_arn = os.environ['SNS_TOPIC_ARN']
    alarm_prefix = os.environ.get('ALARM_PREFIX', 'rds-')
    account_id = context.invoked_function_arn.split(':')[4]
    region = os.environ.get('AWS_REGION', 'eu-central-1')

    paginator = cloudwatch.get_paginator('describe_alarms')
    active_alarms = []

    for page in paginator.paginate(StateValue='ALARM', AlarmNamePrefix=alarm_prefix):
        active_alarms.extend(page.get('MetricAlarms', []))

    if not active_alarms:
        print("No alarms currently in ALARM state")
        return {'statusCode': 200, 'body': 'No active alarms'}

    for alarm in active_alarms:
        state_time = alarm.get('StateUpdatedTimestamp', datetime.now(timezone.utc))
        state_time_str = (
            state_time.strftime('%Y-%m-%dT%H:%M:%S.000+0000')
            if hasattr(state_time, 'strftime')
            else str(state_time)
        )

        message = {
            "AlarmName": alarm['AlarmName'],
            "AlarmDescription": alarm.get('AlarmDescription', ''),
            "AWSAccountId": account_id,
            "NewStateValue": "ALARM",
            "NewStateReason": f"[REPEAT ALERT] {alarm.get('StateReason', 'Alarm is still active')}",
            "StateChangeTime": state_time_str,
            "Region": region,
            "OldStateValue": "ALARM",
            "AlarmArn": alarm.get('AlarmArn', ''),
            "Trigger": {
                "MetricName": alarm.get('MetricName', ''),
                "Namespace": alarm.get('Namespace', 'AWS/RDS'),
                "StatisticType": "Statistic",
                "Statistic": alarm.get('Statistic', 'Average').upper(),
                "Dimensions": [
                    {"name": d['Name'], "value": d['Value']}
                    for d in alarm.get('Dimensions', [])
                ],
                "Period": alarm.get('Period', 60),
                "EvaluationPeriods": alarm.get('EvaluationPeriods', 1),
                "ComparisonOperator": alarm.get('ComparisonOperator', ''),
                "Threshold": alarm.get('Threshold', 0),
            },
        }

        sns.publish(
            TopicArn=sns_topic_arn,
            Subject=f"ALARM: {alarm['AlarmName']}",
            Message=json.dumps(message),
        )
        print(f"Repeat alert published for: {alarm['AlarmName']}")

    return {
        'statusCode': 200,
        'body': f"Published {len(active_alarms)} repeat alerts",
    }
