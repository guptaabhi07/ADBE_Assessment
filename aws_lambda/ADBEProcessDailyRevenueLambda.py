
import os
import logging
from urllib.parse import unquote, unquote_plus
import boto3
import re

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    Lambda Function getting invoked by S3 Events and pass glue param based on events to a glue job
    """

    try:
        glueJobName = os.environ['glueJobName']
        glueJobregion = os.environ['glueJobregion']

        # getting bucket name and key from S3 events
        bucketname = str(unquote_plus(event.get("Records")[0].get('s3').get('bucket').get('name')))
        bucketkey = str(unquote_plus(event.get("Records")[0].get('s3').get('object').get('key')))

        logger.info(f"Bucket Name {bucketname} and Key from the S3 Event {bucketkey}")

        # getting date from filename and pass to glue job for partitioning the data
        startdate = re.search("([0-9]{4}\-[0-9]{2}\-[0-9]{2})", bucketkey)[0]

        logger.info(f"File Date: {startdate}")

        # glue params to a glue job
        glue_params = {"--src_s3_bucket": bucketname,
                       "--startdate_YYYY_mm_dd": str(startdate),
                       "--enddate_YYYY_mm_dd": str(startdate),
                       "--daily_revenue_crawler": os.environ['daily_revenue_crawler'],
                       "--is_backfill": os.environ['is_backfill'],
                       "--dst_s3_bucket": os.environ['dst_s3_bucket']
                       }

        logger.info(f"Passed Glue Params to job {glue_params}")

        # glue client and triggering glue job
        client = boto3.client('glue', region_name=glueJobregion)
        response = client.start_job_run(JobName=glueJobName, Arguments=glue_params)
        logger.info(f"Glue Job run ID {response['JobRunId']}")

        return_response = f"Glue Job Started with {response['JobRunId']}"

    except Exception as e:
        print(f"Glue Job Invoke Failed with Exception occured {e} ")


