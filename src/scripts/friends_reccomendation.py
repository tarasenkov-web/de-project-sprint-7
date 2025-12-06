from datetime import datetime, timedelta
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql import Window 
import os 

def input_event_paths(date, depth, path):
    dt = datetime.strptime(date, '%Y-%m-%d')
    return [f"{path}/date={(dt-timedelta(days=x)).strftime('%Y-%m-%d')}" for x in range(depth)]

def get_contacts(direct_messages):
    return direct_messages\
    .select(F.col("event.message_from").alias("from"),
        F.col("event.message_to").alias("to"),
        F.explode(F.array(F.col("event.message_from"), F.col("event.message_to"))).alias("user_id"))\
    .withColumn("contact_id", F.when(F.col("user_id") == F.col("from"), F.col("to")).otherwise(F.col("from")))\
    .select("user_id", "contact_id")\
    .orderBy(F.asc("user_id"))\
    .distinct()

def distance(df):
    r = 6371
    df_with_distance = df.withColumn(
    "distance", F.lit(2)*r*F.asin(
    F.pow(F.pow(F.sin((F.col('lat_left') - F.col('lat_right'))/F.lit(2)),2)\
        +F.cos(F.col('lat_right'))*F.cos(F.col('lat_left'))\
        *F.pow(F.sin((F.col('lon_left') - F.col('lon_right'))/F.lit(2)),2), 0.5)
    ))
    
    return df_with_distance

def find_last_location(df):
    r = 6371
    df_geo = spark.read.csv("/user/valentinat/geo.csv", header=True, sep=';')
    df_geo = df_geo.withColumnRenamed("lat", "geo_lat")\
    .withColumnRenamed("lng", "geo_lon")\
    .withColumn("geo_lat", F.trim(F.col("geo_lat")))\
    .withColumn("geo_lat", F.regexp_replace(F.col("geo_lat"), ",", "."))\
    .withColumn("geo_lon", F.trim(F.col("geo_lon")))\
    .withColumn("geo_lon", F.regexp_replace(F.col("geo_lon"), ",", "."))\
                          
    df_combined = df.crossJoin(df_geo)
    df_with_distance = df_combined.withColumn(
    "distance", F.lit(2)*r*F.asin(
    F.pow(F.pow(F.sin((F.col('geo_lat') - F.col('lat'))/F.lit(2)),2)\
        +F.cos(F.col('lat'))*F.cos(F.col('geo_lat'))\
        *F.pow(F.sin((F.col('geo_lon') - F.col('lon'))/F.lit(2)),2), 0.5)
    )
    )
    df_min_distance = df_with_distance.select(F.col("event_type"),F.col("message_id"),
            F.col("TIME_UTC"),F.col("message_to"),
            F.col("city"),F.col("distance"),F.col("date"),F.col("user_id"),F.col("lon"),F.col("lat"))\
    .withColumn("rank", F.row_number().over(Window.partitionBy("message_id")\
    .orderBy(F.asc("distance"))))\
    .where("rank = 1")\
    .select("event_type", "message_id", "message_to", "city", "lon","lat", "date", "user_id", "TIME_UTC")
        

    window_spec = Window.partitionBy("user_id").orderBy(F.desc("TIME_UTC"))
    df_with_act_city = df_min_distance.withColumn("act_city", F.first("city").over(window_spec))\
            .withColumn("act_lon", F.first("lon").over(window_spec))\
            .withColumn("act_lat", F.first("lat").over(window_spec))\
        .select( "act_lat", "act_lon", "act_city", "user_id").distinct()
    
    return df_with_act_city

def get_timezone(city):
    valid_timezones = [
        'Australia/Sydney', 'Australia/Melbourne', 
        'Australia/Brisbane', 'Australia/Perth', 
        'Australia/Adelaide', 'Australia/Hobart', 
        'Australia/Darwin'
    ]
    
    valid_timezones_str = ', '.join(f"'{tz}'" for tz in valid_timezones)
    
    return F.when(F.col("city").isNull() | (F.col("city") == ""), "Australia/Sydney")\
             .otherwise(F.when(F.expr(f"city NOT IN ({valid_timezones_str})"), "Australia/Sydney")\
             .otherwise(F.col("city")))

def friends_reccomendation(date, depth, spark, events_path):
    events_base_path = input_event_paths(date, depth, events_path)
    messages = spark.read\
    .option("basePath", events_path)\
    .parquet(*events_base_path)\
    .where("event_type='message'")

    all_messages = messages.where("event.message_to is not null")
    direct_messages = all_messages.select(F.col("event_type"),F.col("event.message_id").alias("message_id"),
            F.col("event.message_ts").alias("TIME_UTC"),F.col("event.message_to").alias("message_to"),
            F.col("lat"),F.col("lon"),F.col("date"),F.col("event.message_from").alias("user_id"))
    
    df_users_locations=find_last_location(direct_messages)
    df_users_locations.show()
    
    print(df_users_locations.count())
    subscriptions = spark.read\
        .option("basePath", events_path)\
        .parquet(*events_base_path)\
        .where("event_type='subscription'")\
        .where("event.subscription_channel is not null")\
        .where("event.user is not null")\
        .where("event.user != 0")\
        .select(F.col("event.user").alias("user_id"),F.col("event.subscription_channel").alias("subscription_channel"))
    
    df_pairs = subscriptions.alias("s1").join(
        subscriptions.alias("s2"),
        (F.col("s1.subscription_channel") == F.col("s2.subscription_channel")) & 
        (F.col("s1.user_id") < F.col("s2.user_id")),
        "inner"
    ).select(F.col("s1.user_id").alias("user_left"), F.col("s2.user_id").alias("user_right")).orderBy(F.asc("user_left"))
    df_contacts = get_contacts(all_messages)
    df_no_messages = df_pairs.join(
        df_contacts,
        (
            (df_pairs.user_left == df_contacts.user_id) & (df_pairs.user_right == df_contacts.contact_id)
        ),
        "left_anti"
    )
    
    df_act_location_prev = df_no_messages.join(
        df_users_locations,
        df_no_messages.user_left == df_users_locations.user_id, "left")\
        .withColumn("city_left", F.col("act_city"))\
        .withColumn("lon_left", F.col("act_lon"))\
        .withColumn("lat_left", F.col("act_lat"))\
        .select("city_left", "lon_left", "lat_left", "user_left", "user_right")
    df_act_location = df_act_location_prev.join(
        df_users_locations,
        df_no_messages.user_right == df_users_locations.user_id, "left")\
        .withColumn("city_right", F.col("act_city"))\
        .withColumn("lon_right", F.col("act_lon"))\
        .withColumn("lat_right", F.col("act_lat"))
    
    df_with_distance = distance(df_act_location).withColumn(
    "distance",
    F.col("distance").cast("double")
).where(F.col("distance") < 1000).withColumnRenamed("act_city", "city")
    df_with_distance.show()
    df_with_time = df_with_distance.withColumn(
        "last_TIME_UTC", F.current_timestamp())\
        .withColumn("timezone", F.concat(F.lit("Australia/"), F.col("city")))
    final_df = df_with_time.withColumn(
        "local_time",
        F.from_utc_timestamp(
            F.col("last_TIME_UTC"),
            get_timezone(F.col("city"))
        )
    ).withColumn(
        "processed_dttm",
        F.current_date()
        ).select(F.col("user_left"),F.col("user_right"),F.col("processed_dttm"),F.col("city_right").alias("zona_id"),F.col("local_time"))
    final_df.show()
    return final_df

def main():
    date = sys.argv[1]
    days_count = sys.argv[2]
    events_base_path = sys.argv[3]
    output_base_path = sys.argv[4]

    conf = SparkConf().setAppName("EventsPartitioningJob")
    sc = SparkContext.getOrCreate(conf) 
    spark = SQLContext(sc)

    friends_reccomendation(date, days_count, spark, events_base_path).write.parquet(f"{output_base_path}/date={date}")
    

if __name__ == "__main__":
    
    main()
