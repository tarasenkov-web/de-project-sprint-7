from datetime import datetime, timedelta
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql import Window 
import os 

def input_event_paths(date, depth, path):
    dt = datetime.strptime(date, '%Y-%m-%d')
    return [f"{path}/date={(dt-timedelta(days=x)).strftime('%Y-%m-%d')}" for x in range(depth)]

def messages_coordinates(date, depth, spark, events_path):
    message_paths = input_event_paths(date, depth, events_path)
    r = 6371
    df_geo = spark.read.csv("/user/valentinat/geo.csv", header=True, sep=';')
    df_geo = df_geo.withColumnRenamed("lat", "geo_lat")\
    .withColumnRenamed("lng", "geo_lon")\
    .withColumn("geo_lat", F.trim(F.col("geo_lat")))\
    .withColumn("geo_lat", F.regexp_replace(F.col("geo_lat"), ",", "."))\
    .withColumn("geo_lon", F.trim(F.col("geo_lon")))\
    .withColumn("geo_lon", F.regexp_replace(F.col("geo_lon"), ",", "."))\
    
    df_geo.show()

    result_df = spark.read\
    .option("basePath", events_path)\
    .parquet(*message_paths)\
    .where("lat is not null")\
    .where("lon is not null")\
    .where("event.message_from is not null")\
                          
    df_combined = result_df.crossJoin(df_geo)
    df_with_distance = df_combined.withColumn(
    "distance", F.lit(2)*r*F.asin(
    F.pow(F.pow(F.sin((F.col('geo_lat') - F.col('lat'))/F.lit(2)),2)\
        +F.cos(F.col('lat'))*F.cos(F.col('geo_lat'))\
        *F.pow(F.sin((F.col('geo_lon') - F.col('lon'))/F.lit(2)),2), 0.5)
    )
    )
    df_min_distance = df_with_distance.select(F.col("event.message_id").alias("message_id"),
            F.col("event.message_ts").alias("TIME_UTC"),
            F.col("city").alias("act_city"),F.col("distance"),F.col("date"),F.col("event.message_from").alias("user_id"))\
    .withColumn("rank", F.row_number().over(Window.partitionBy("message_id")\
    .orderBy(F.asc("distance"))))\
    .where("rank = 1")\
    .select("message_id", "act_city", "distance", "date", "user_id", "TIME_UTC")
    df_min_distance.orderBy(F.col("user_id").asc()).show()

    df_grouped = df_min_distance.select("user_id", "act_city", "date").distinct()
    window_spec = Window.partitionBy("user_id", "act_city").orderBy("date")
    df_dates = df_grouped.withColumn("day_num", F.row_number().over(window_spec))

    df_continuous = df_dates.withColumn("date_diff", 
        F.datediff(F.col("date"), F.min("date").over(window_spec))
    )

    home_city_df = df_continuous.groupBy("user_id", "act_city").agg(
        F.countDistinct("date").alias("days_count")
    ).filter(F.col("days_count") >= 27).withColumnRenamed("act_city", "home_city")

    act_city = df_min_distance.join(home_city_df, 
                       on="user_id", 
                       how="left")
    
    df_sorted = df_min_distance.select("user_id", "act_city", "date").distinct().orderBy("user_id", "date")

    travel_city = df_sorted.groupBy("user_id").agg(
        F.count("act_city").alias("travel_count"), 
        F.collect_list("act_city").alias("travel_array") 
    )
    
    result_df = act_city.join(travel_city, 
                       on="user_id", 
                       how="left").withColumnRenamed("act_city", "city")
    result_df.show()
    
    df = result_df.withColumn("TIME_UTC", F.to_timestamp(F.col("TIME_UTC"))).withColumn("timezone", F.concat(F.lit("Australia/"), F.col("city")))

    # Определение окна для получения последнего события
    window_spec = Window.partitionBy("user_id").orderBy(F.desc("TIME_UTC"))

    # Добавление столбца с актуальным городом
    df_with_act_city = df.withColumn("act_city", F.first("city").over(window_spec))

    # Получение последнего TIME_UTC и timezone для каждого пользователя
    last_event_df = df_with_act_city.withColumn("last_TIME_UTC", F.first("TIME_UTC").over(window_spec))\
                                      .withColumn("last_timezone", F.first("timezone").over(window_spec))

    # Удаление дубликатов для получения одного события на пользователя
    last_event_df = last_event_df.dropDuplicates(["user_id"])

    # Расчет местного времени
    final_df = last_event_df.withColumn(
        "local_time",
        F.from_utc_timestamp(
            F.col("last_TIME_UTC"),
            F.when(F.col("last_timezone").isNull() | (F.col("last_timezone") == ""), "Australia/Sydney")
             .otherwise(F.when(F.expr("last_timezone NOT IN ('Australia/Sydney', 'Australia/Melbourne', 'Australia/Brisbane', 'Australia/Perth', 'Australia/Adelaide', 'Australia/Hobart', 'Australia/Darwin')"), "Australia/Sydney")
                        .otherwise(F.col("last_timezone")))
        )
    )

    # Показать результаты
    final_df.select("user_id", "local_time", "act_city","home_city","travel_count","travel_array").distinct().show()
    
    return final_df

def main():
    date = sys.argv[1]
    days_count = sys.argv[2]
    events_base_path = sys.argv[3]
    output_base_path = sys.argv[4]

    conf = SparkConf().setAppName("EventsPartitioningJob")
    sc = SparkContext.getOrCreate(conf) 
    spark = SQLContext(sc)

    messages_coordinates(date, 7, spark, events_base_path).write.parquet(f"{output_base_path}/date={date}")
    
if __name__ == "__main__":
    
    main()
    