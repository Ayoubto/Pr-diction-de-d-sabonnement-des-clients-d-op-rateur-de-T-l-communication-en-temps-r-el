import findspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, IntegerType, StringType, StructType, FloatType
from pyspark.sql.functions import col, from_json, regexp_replace, lower, when
from pyspark.ml.classification import GBTClassificationModel
from pyspark.ml.feature import VectorAssembler

def preprocess_data(df):
    # Remove punctuation
    df = df.withColumn("Churn", regexp_replace(col("Churn"), "[^\w\s]", ""))
    # Convert text to lowercase
    df = df.withColumn("Churn", lower(col("Churn")))
    # Convert 'yes'/'no' to binary for 'International plan' and 'Voice mail plan'
    df = df.withColumn("International plan", when(col("International plan") == "yes", 1).otherwise(0))
    df = df.withColumn("Voice mail plan", when(col("Voice mail plan") == "yes", 1).otherwise(0))
    # Drop rows with null values
    df = df.dropna()
    return df

if __name__ == "__main__":
    findspark.init()
    pretrained_model = '../gbt_model'
    spark = SparkSession.builder.master("local[*]").appName("ChurnPrediction")\
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")\
        .getOrCreate()

    # Schema for reading data from Kafka
    schema = StructType([
        StructField("State", StringType(), True),
        StructField("Account length", IntegerType(), True),
        StructField("Area code", IntegerType(), True),
        StructField("International plan", StringType(), True),
        StructField("Voice mail plan", StringType(), True),
        StructField("Number vmail messages", IntegerType(), True),
        StructField("Total day minutes", FloatType(), True),
        StructField("Total day calls", IntegerType(), True),
        StructField("Total day charge", FloatType(), True),
        StructField("Total eve minutes", FloatType(), True),
        StructField("Total eve calls", IntegerType(), True),
        StructField("Total eve charge", FloatType(), True),
        StructField("Total night minutes", FloatType(), True),
        StructField("Total night calls", IntegerType(), True),
        StructField("Total night charge", FloatType(), True),
        StructField("Total intl minutes", FloatType(), True),
        StructField("Total intl calls", IntegerType(), True),
        StructField("Total intl charge", FloatType(), True),
        StructField("Customer service calls", IntegerType(), True),
        StructField("Churn", StringType(), True)
    ])

    # Read data from Kafka
    data = spark.readStream.format("kafka")\
        .option("kafka.bootstrap.servers", "kafka2:29092")\
        .option("subscribe", "churn_topic")\
        .option("startingOffsets", "latest")\
        .option("header", "true")\
        .load()\
        .selectExpr("CAST(value AS STRING) as message")\
        .withColumn("value", from_json("message", schema))\
        .select("value.*")

    # Preprocess the data
    preprocessed_data = preprocess_data(data)

    # Create feature vector
    feature_columns = [
        "Account length", "Area code", "International plan", "Voice mail plan",
        "Number vmail messages", "Total day minutes", "Total day calls", "Total day charge",
        "Total eve minutes", "Total eve calls", "Total eve charge", "Total night minutes",
        "Total night calls", "Total night charge", "Total intl minutes", "Total intl calls",
        "Total intl charge", "Customer service calls"
    ]
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    preprocessed_data = assembler.transform(preprocessed_data)

    # Load the pre-trained model
    loaded_model = GBTClassificationModel.load(pretrained_model)

    # Make predictions
    predictions = loaded_model.transform(preprocessed_data)

    # Translate prediction to churn categories
    predictions = predictions.withColumn("Churn_prediction",
                                         when(col("prediction") == 0, "False")
                                         .when(col("prediction") == 1, "True")
                                         .otherwise("Unknown"))
    
    selected_predictions = predictions.select(
        "State", "Account length", "Area code", "International plan", "Voice mail plan",
        "Number vmail messages", "Total day minutes", "Total day calls", "Total day charge",
        "Total eve minutes", "Total eve calls", "Total eve charge", "Total night minutes",
        "Total night calls", "Total night charge", "Total intl minutes", "Total intl calls",
        "Total intl charge", "Customer service calls", "Churn_prediction"
    )

    def write_to_mongodb(df, epoch_id):
        df.write.format("mongo").mode("append")\
            .option("uri", "mongodb://mongodb2:27017/ChurnPrediction.Predictions").save()
        df.show(truncate=False)

    # Output selected predictions to MongoDB using foreachBatch
    query = selected_predictions.writeStream.foreachBatch(write_to_mongodb)\
        .outputMode("append").start().awaitTermination()