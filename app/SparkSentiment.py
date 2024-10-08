from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StringType

# Basis Spark session
spark = SparkSession.builder \
    .appName("KafkaTweetSentimentAnalysis") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Schema for the data
schema = StructType() \
    .add("textID", StringType()) \
    .add("text", StringType()) \
    .add("selected_text", StringType()) \
    .add("sentiment", StringType())

# Read settings for Kafka
tweets_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9093") \
    .option("subscribe", "tweets_topic") \
    .option("startingOffsets", "earliest") \
    .load()

# JSON Type
tweets_df = tweets_df.select(from_json(col("value").cast("string"), schema).alias("tweet")).select("tweet.*")

# Simple sentiment analysis function
def analyze_sentiment(text):
    try:
        positive_words = ['happy', 'love', 'great', 'good', 'fantastic', 'awesome']
        negative_words = ['sad', 'angry', 'bad', 'terrible', 'awful', 'hate']
        
        text = text.lower()
        
        if any(word in text for word in positive_words):
            return 'positive'
        elif any(word in text for word in negative_words):
            return 'negative'
        else:
            return 'neutral'
    except Exception as e:
        print(f"Error processing text: {text} - {e}")
        return 'neutral'

sentiment_udf = udf(analyze_sentiment, StringType())

# Apply sentiment analysis to the 'text'
predictions = tweets_df.withColumn('prediction', sentiment_udf(col('text')))

# sentiment predictions to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    jdbc_url = "jdbc:postgresql://postgres:5432/mydatabase"
    db_properties = {
        "user": "myuser",
        "password": "mypassword",
        "driver": "org.postgresql.Driver"
    }
    
    
    print(f"Displaying batch {batch_id}:")
    batch_df.show(10, truncate=False)  # Display data before writing to PostgreSQL for to be sure

    #  DF to PostgreSQL
    try:
        batch_df.select("textID", "text", "selected_text", "sentiment","prediction") \
                .write \
                .jdbc(jdbc_url, "sentiment_analysis_results", mode="append", properties=db_properties)
        print(f"Batch {batch_id} written to PostgreSQL successfully.")
    except Exception as e:
        print(f"Error writing batch {batch_id} to PostgreSQL: {e}")

# Output of the sentiment predictions and writing to PostgreSQL
query = predictions.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_postgres) \
    .start()

query.awaitTermination()
