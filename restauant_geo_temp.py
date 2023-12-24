from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType
import requests
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType, StructType, StructField
import random
import geohash
from pyspark.sql import functions as F

# Create a Spark session
spark = SparkSession.builder.appName("ETLJob").config("spark.executor.cores", "4").config("spark.executor.memory", "8g").getOrCreate()
spark

# Read csv data 
restaurant_data = spark.read.csv('rest_data')
# I like format of pandas tables so used this, spark format is not good when data contains many columns, it is unreadable
restaurant_data.toPandas().head(4)

# Tested Api jon
# api_url = "https://api.opencagedata.com/geocode/v1/json"
# api_key = "43d7b440eba049d79cd87f0bc8699f8a"

# query = 'The Golden Spoon, Decatur'
# response = requests.get(api_url, params={"q": f"{query}", "key": api_key})
# result = response.json()
# print(result)
# print(result["results"][0]["geometry"]["lat"], result["results"][0]["geometry"]["lng"])

# Task 1: Check and fill incorrect values using OpenCage API
# Function to map lat and lng using OpenCage API
def map_coordinates(lat,lng,city,franchise_name):
    if lat is None or lng is None:
        ## Checked with data and lng and lat is more accurate with only two parametrs, without country
        query = franchise_name +', '+ city
        # Call OpenCage API to get lat and lng
        api_url = "https://api.opencagedata.com/geocode/v1/json"
        api_key = "43d7b440eba049d79cd87f0bc8699f8a"
        
        response = requests.get(api_url, params={"q": f"{query}", "key": api_key})
        result = response.json()
        if "results" in result and len(result["results"]) > 0:
            return (result["results"][0]["geometry"]["lat"], result["results"][0]["geometry"]["lng"])
    
    # Return values if no update is needed
    return (lat,lng)
# Convert to spark udf
map_coordinates_udf = udf(map_coordinates, StructType([StructField("lat", DoubleType()), StructField("lng", DoubleType())]))
# Apply the mapping function
mapped_data = restaurant_data.withColumn("new_coord", map_coordinates_udf("lat","lng","city","franchise_name")).selectExpr("id", "COALESCE(lat, new_coord.lat) as lat", "COALESCE(lng, new_coord.lng) as lng")
mapped_data.show(10)

# Task 2: Generate geohash
def generate_geohash(lat, lng):
    return geohash.encode(lat, lng, precision=4)

geohash_udf = udf(generate_geohash, StringType())

mapped_data_geohash = mapped_data.withColumn("geohash", geohash_udf(col("lat"), col("lng")))
mapped_data_geohash.show(3)

# I uploaded my weather data by one month, from my understandin of data in weather files it is coordinates and temp by different period of time
# Also what i also like in spark that it by itself search directories for parquet files
weather_data = spark.read.parquet("/FileStore/tables/weather/weather")
# Use same udf to get geohash data
weather_data = weather_data.withColumn("geohash", geohash_udf(col("lat"), col("lng")))
# I noticed that precision 4 is not enough and uniqness of geohash is broken, so
# I modifyed weather data by removing lat and lng, because our Pk is geohash and lng and lat is already in restaurant data
weather_data = weather_data.select("geohash","avg_tmpr_f","avg_tmpr_c","wthr_date")\
                            .groupBy("geohash","wthr_date")\
                            .agg(F.round(F.avg("avg_tmpr_c"),3).alias("avg_tmpr_c"),F.round(F.avg("avg_tmpr_f"),3).alias("avg_tmpr_f"))
weather_data.show(3)


# Task 3: Left-join weather and restaurant data using the geohash
# Here i used 
enriched_data = mapped_data_geohash.join(weather_data.filter(col("wthr_date") == "2017-09-01"), on="geohash", how="left")
enriched_data.show(5)
# enriched_data.filter(col("wthr_date").isNull()).count() 
# We can see that 32 rows is not joined, because there is no data in weather set
# Task 4: Store the enriched data in the local file system in the parquet format
enriched_data.repartition("geohash").write.mode("overwrite").parquet("/FileStore/tables/weather/enriched_data.parquet")

# Stop spark session
spark.stop()
