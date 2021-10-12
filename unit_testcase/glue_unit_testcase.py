import os
import logging
from urllib.parse import unquote, unquote_plus
import boto3
import re
from pyspark.sql import SparkSession
import time
import unittest
from pandas.util.testing import assert_frame_equal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

spark = SparkSession.builder.master("local[1]").appName('UnitTest').getOrCreate()

class RevenueUnitTest(unittest.TestCase):
    def test_case(self):
        """
        Test Case which invoke glue job for final data and then do assert
        """
        try:
            glueJobName = "ProcessDailyRevenueGlueJob"
            glueJobregion = "us-east-1"

            bucketname = "daily-revenue-bucket/testdata/unittestdata"
            # bucketname = "daily-revenue-bucket"
            bucketkey = "Test-2021-10-04.tsv"
            # bucketkey = "2021-10-04.tsv"

            print(f"Bucket Name {bucketname} and Key from the S3 Event {bucketkey}")

            startdate = re.search("([0-9]{4}\-[0-9]{2}\-[0-9]{2})", bucketkey)[0]
            print(f"File Date: {startdate}")

            glue_params = {"--src_s3_bucket": bucketname,
                       "--startdate_YYYY_mm_dd": str(startdate),
                       "--enddate_YYYY_mm_dd": str(startdate),
                       "--daily_revenue_crawler": os.environ['daily_revenue_crawler'],
                       "--is_backfill": "0",
                       "--dst_s3_bucket": "daily-revenue-bucket-test"
                       }

            logger.info(f"Passed Glue Params to job {glue_params}")

            client = boto3.client('glue', region_name=glueJobregion)
            response = client.start_job_run(JobName=glueJobName, Arguments=glue_params)
            logger.info(f"Glue Job run ID {response['JobRunId']}")

            return_response = response['JobRunId']

            df = {}
            if len(return_response) > 5:
                time.sleep(600)
                src_s3path = "s3://" + bucketname + "/" + str(startdate) + ".tsv"
                df = spark.read.option("header", "true").option("delimiter", r"\t").csv(src_s3path)

            data = {'Query': 'IPOD', 'Host': 'https://google.com', 'TotalRevenue': '290', 'HitDate': '2021-09-27'}
            assert_frame_equal(df, data)

        except Exception as e:
            print(f"Glue Job Invoke Failed with Exception occured {e} ")


if __name__ == '__main__':
    RevenueUnitTest.main()
