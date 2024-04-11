import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1712856619972 = glueContext.create_dynamic_frame.from_catalog(database="de_haguz_raw", push_down_predicate="region IN ('ca')", table_name="rawstatistics", transformation_ctx="AmazonS3_node1712856619972")

# Script generated for node Change Schema
ChangeSchema_node1712856621845 = ApplyMapping.apply(frame=AmazonS3_node1712856619972, mappings=[("video_id", "string", "video_id", "bigint"), ("trending_date", "string", "trending_date", "string"), ("title", "string", "title", "string"), ("channel_title", "string", "channel_title", "string"), ("category_id", "long", "category_id", "bigint"), ("publish_time", "string", "publish_time", "string"), ("tags", "string", "tags", "string"), ("views", "long", "views", "bigint"), ("likes", "long", "likes", "bigint"), ("dislikes", "long", "dislikes", "bigint"), ("comment_count", "long", "comment_count", "bigint"), ("thumbnail_link", "string", "thumbnail_link", "string"), ("comments_disabled", "boolean", "comments_disabled", "boolean"), ("ratings_disabled", "boolean", "ratings_disabled", "boolean"), ("video_error_or_removed", "boolean", "video_error_or_removed", "boolean"), ("description", "string", "description", "string"), ("region", "string", "region", "string")], transformation_ctx="ChangeSchema_node1712856621845")

# Script generated for node Amazon S3
AmazonS3_node1712856658813 = glueContext.getSink(path="s3://de-haguz-cleaned-useast1-dev/youtube/cleaned_statistics/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=["region"], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1712856658813")
AmazonS3_node1712856658813.setCatalogInfo(catalogDatabase="de_haguz_cleaned",catalogTableName="cleaned_statistics")
AmazonS3_node1712856658813.setFormat("glueparquet", compression="snappy")
AmazonS3_node1712856658813.writeFrame(ChangeSchema_node1712856621845)
job.commit()