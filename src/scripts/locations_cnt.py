from datetime import datetime, timedelta
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql import Window 
import os 

def input_event_paths(date, depth, path):
    dt = datetime.strptime(date, '%Y-%m-%d')
    return [f"{path}/date={(dt-timedelta(days=x)).strftime('%Y-%m-%d')}" for x in range(depth)]

def registration_set(df):
    window_spec = Window.partitionBy("user_id").orderBy(F.asc("date"))
    df_with_first_message = df.withColumn(
        "first_message",
        F.when(F.row_number().over(window_spec) == 1, True).otherwise(False)
    )

    df_updated = df_with_first_message.withColumn(
        "event_type",
        F.when(F.col("first_message") == True, "registration").otherwise(F.col("event_type"))
    ).drop("first_message")
    
    return df_updated

def add_location_las_msg(df):
    window_spec = Window.partitionBy("user_id").orderBy(F.desc("date")) 
    df_with_last_city = df.withColumn("last_city", F.first("city").over(window_spec))
    return df_with_last_city

def find_event_location(df,spark):
    r = 6371
    df_geo = spark.read.csv("/user/valentinat/geo.csv", header=True, sep=';')
    df_geo = df_geo.withColumnRenamed("lat", "geo_lat")\
                   .withColumnRenamed("lng", "geo_lon")\
                   .withColumn("geo_lat", F.trim(F.col("geo_lat")))\
                   .withColumn("geo_lat", F.regexp_replace(F.col("geo_lat"), ",", "."))\
                   .withColumn("geo_lon", F.trim(F.col("geo_lon")))\
                   .withColumn("geo_lon", F.regexp_replace(F.col("geo_lon"), ",", "."))
                          
    df_combined = df.crossJoin(df_geo)
    df_with_distance = df_combined.withColumn(
        "distance", F.lit(2) * r * F.asin(
            F.pow(F.pow(F.sin((F.col('geo_lat') - F.col('lat')) / F.lit(2)), 2) +
            F.cos(F.col('lat')) * F.cos(F.col('geo_lat')) *
            F.pow(F.sin((F.col('geo_lon') - F.col('lon')) / F.lit(2)), 2), 0.5)
        )
    )

    df_min_distance = df_with_distance.select(F.col("event_type"),F.col("event_id"), F.col("user_id"),
            F.col("city"), F.col("distance"), F.col("date"))\
    .withColumn("rank", F.row_number().over(Window.partitionBy("event_id")\
    .orderBy(F.asc("distance"))))\
    .where("rank = 1")\
    .select("event_type", "event_id", "user_id", "city", "distance", "date")\
    
    return df_min_distance

def locations_cnt(date, depth, spark, events_path):
    message_paths = input_event_paths(date, depth, events_path)

    df = spark.read\
        .option("basePath", events_path)\
        .parquet(*message_paths)

    df_users = df.withColumn(
        "user_id",
        F.when(F.col("event_type") == "message", F.col("event.message_from"))
         .when(F.col("event_type") == "reaction", F.col("event.reaction_from"))
         .when(F.col("event_type") == "subscription", F.col("event.user"))
         .otherwise(F.lit(None))
    ).withColumn(
        "event_id",
        F.when(F.col("event_type") == "message", F.col("event.message_id"))
         .when(F.col("event_type") == "reaction", F.col("event.message_id"))
         .when(F.col("event_type") == "subscription", F.col("event.subscription_channel"))
         .otherwise(F.lit(None))).select("user_id", "event_id", "date", "event_type", "lon", "lat") 
    
    
    df_users_all_events = registration_set(df_users)
    df_users_all_events.show()
    df_location = find_event_location(df_users_all_events, spark)
    df_location.show(truncate=False)
    
    add_locations_msg = add_location_las_msg(df_location).withColumn(
        "city",
        F.when(F.col("city").isNull(), F.col("last_city")).otherwise(F.col("city")) 
    )
    add_locations_msg.orderBy(F.asc("user_id")).show()
    event_df = add_locations_msg.withColumn("month", F.month("date")) \
                                 .withColumn("week", F.weekofyear("date"))


    result_df = event_df.groupBy("month", "week", "city").agg(
        F.count(F.when(F.col("event_type") == "message", True)).alias("week_message"),
        F.count(F.when(F.col("event_type") == "reaction", True)).alias("week_reaction"),
        F.count(F.when(F.col("event_type") == "subscription", True)).alias("week_subscription"),
        F.count(F.when(F.col("event_type") == "registration", True)).alias("week_user")
    )
    
    result_df.show()

    result_df_mnth = result_df.select(F.col("month"),F.col("week"), F.col("city"),
            F.col("week_message"), F.col("week_reaction"), F.col("week_subscription"), F.col("week_user"))\
    .withColumn("month_message", F.sum("week_message").over(Window.partitionBy("month", "city")))\
    .withColumn("month_reaction", F.sum("week_reaction").over(Window.partitionBy("month", "city")))\
    .withColumn("month_subscription", F.sum("week_subscription").over(Window.partitionBy("month", "city")))\
    .withColumn("month_user", F.sum("week_user").over(Window.partitionBy("month", "city")))

    result_df_mnth.show(truncate=False)
    
    return result_df_mnth

def main():
    date = sys.argv[1]
    days_count = sys.argv[2]
    events_base_path = sys.argv[3]
    output_base_path = sys.argv[4]

    conf = SparkConf().setAppName("EventsPartitioningJob")
    sc = SparkContext.getOrCreate(conf) 
    spark = SQLContext(sc)

    locations_cnt(date, days_count, spark, events_base_path).write.parquet(f"{output_base_path}/date={date}")
    
if __name__ == "__main__":
    
    main()