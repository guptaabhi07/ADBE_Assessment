from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.types import ArrayType, StringType, StructType, DateType
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import datetime, sys, logging, boto3

# Loading Glue Job Args
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'is_backfill', 'dst_s3_bucket', 'src_s3_bucket',
                                     'startdate_YYYY_mm_dd', 'enddate_YYYY_mm_dd', 'daily_revenue_crawler'])

# Creating Spark, Glue Context and job init
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


class RevenueJob:
    def __init__(self):
        """
        |--------------------------------------------------------------------------
        | Job Parameters
        |--------------------------------------------------------------------------
        """
        self.is_backfill = args["is_backfill"]
        self.dataset_start_date_yyyymmddhh = args["startdate_YYYY_mm_dd"]
        self.dataset_end_date_yyyymmddhh = args["enddate_YYYY_mm_dd"]
        self.dst_s3_bucket = args["dst_s3_bucket"]
        self.src_s3_bucket = args["src_s3_bucket"]
        self.dst_glue_crawler = args["daily_revenue_crawler"]
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)

    def transform_write(self, startdate):
        src_s3path = "s3://" + self.src_s3_bucket + "/" + str(startdate) + ".tsv"

        self.logger.info(f"Loading data from file {src_s3path}")
        df = spark.read.option("header", "true").option("delimiter", r"\t").csv(src_s3path)
        df = self.process_agg_data(df)

        # Loading the data at daily partition for scalability and performance
        dst_s3path = "s3://" + self.dst_s3_bucket + "/" + str(startdate) + "/" + "SearchKeywordPerformance"
        df.write.option("header", "true").mode('overwrite').csv(dst_s3path)
        self.logger.info(f"Df File Successfully Written at {dst_s3path}")

        # dst_s3path = "s3://" + args['dst_s3_bucket'] + "/" + + str(startdate) + "/" + str(startdate) +
        # "_SearchKeywordPerformance"

        # Write to single file in csv
        # df.repartition(1).write.option("header", "true").mode('overwrite').csv(dst_s3path)
        # df.toPandas().to_csv(dst_s3path+".csv")

    def process_agg_data(self, df):
        """
        Processing Dataframe with different functions to parse Product Events and Calculate Revenue
        """

        def process_data(df):
            """
            Handling Nulls and Blank Records on Revenue and Query to clean the data
            """
            self.logger.info(f"Processing dataframe in Process_Data func")

            # making nulls and blank records to 0 in Total Revenue
            df = df.withColumn('TotalRevenue',
                               F.when((F.col('TotalRevenue') == '') | (F.col('TotalRevenue').isNull()), 0).otherwise(
                                   F.col('TotalRevenue')))

            # Parsing Referrer field to get Host and Query Parameters
            df = df.withColumn("query", F.upper(F.expr("parse_url(referrer, 'QUERY', 'q')"))) \
                .withColumn("Host", F.upper(F.expr("parse_url(referrer, 'HOST')")))

            # Assigning NA to null and blank records for query field
            df = df.withColumn('query', F.when((F.col('query') == '') | (F.col('query').isNull()), "NA").otherwise(
                F.col('query')))

            self.logger.info(f"Processing Completed at Process_Data func")

            return df

        def agg_Revenue(df):
            """
            Calculating Revenue from different Referrer Host directing to ESHOPZILLA
            """
            self.logger.info(f"Processing dataframe in Agg_Revenue func ")

            # Since different events lies from referrer site to actual order Complete for a single user(IP addr)
            # We will Window aggregate the data at IP addr first and then at the Host & Query to have the actual revenue

            df = df.select("Host", "query", "TotalRevenue", "ip", F.col("date_time").cast(DateType()).alias("Hitdate")) \
                .withColumn("RevenueOverIP", F.sum(F.col("TotalRevenue")).over(Window().partitionBy("Hitdate", "ip"))) \
                .filter(F.col("Host") != "WWW.ESSHOPZILLA.COM").groupby("Hitdate", "Host", "Query").agg(
                F.sum("RevenueOverIP").alias("RevenueOverHostQuery")) \
                .select("Hitdate", "Host", "query", "RevenueOverHostQuery")

            self.logger.info(f"Processing Completed at Agg_Revenue func")

            return df

        def parse_product_events(df, proddict={0: "Category", 1: "ProductName", 2: "NoItems", 3: "TotalRevenue",
                                               4: "CustomEvent", 5: "MerchanteVar"}):
            """
            Parsing and splitting Values from Product List
            """
            self.logger.info(f"Processing dataframe in Parse_Product_Events func ")

            for key, value in proddict.items():
                df = df.withColumn(value, F.split(F.col("product_list"), ';').getItem(key))

            self.logger.info(f"Processing Completed at Parse_Product_Events func")

            return df

        df = parse_product_events(df)
        df = process_data(df)
        df = agg_Revenue(df)

        return df

    def run_crawler(self):
        """
        Running the Crawler after processing the data so that Athena Table is upto date for QuickSight
        """
        glue_client = boto3.client('glue', region_name='us-east-1')
        glue_client.start_crawler(Name=self.dst_glue_crawler)
        self.logger.info(f"Started {self.dst_glue_crawler} Crawler")


def main():
    # Class Instance created
    dly_revenue = RevenueJob()

    try:
        if dly_revenue.is_backfill == "1":

            # If manual backfill required - use startdate and enddate to process incrementally all files
            startdate = datetime.datetime.strptime(dly_revenue.dataset_start_date_yyyymmddhh, "%Y-%m-%d").date()
            enddate = datetime.datetime.strptime(dly_revenue.dataset_start_date_yyyymmddhh, "%Y-%m-%d").date()

            if enddate < startdate:
                raise Exception("EndDate provided can't be less than StartDate")
            else:
                dly_revenue.logger.info(f"Backfill will be for Period from {startdate} to {enddate}")

            for i in range((enddate - startdate).days + 1):
                filedate = startdate + datetime.timedelta(days=i)
                dly_revenue.transform_write(filedate)
        else:
            # Daily Single File Processing (comes from Lambda)
            startdate = datetime.datetime.strptime(dly_revenue.dataset_start_date_yyyymmddhh, "%Y-%m-%d").date()
            dly_revenue.transform_write(startdate)

        dly_revenue.run_crawler()

    except Exception as e:
        dly_revenue.logger.info(f"Exception is {e}")


if __name__ == "__main__":
    main()
    job.commit()
