from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

def load(drinks_path="dbfs:/FileStore/mini_project11/drinks.csv", 
         drugs_path="dbfs:/FileStore/mini_project11/drugs.csv"):
    # Initialize Spark session
    spark = SparkSession.builder.appName("Allen Mini Project 11").getOrCreate()

    # Load datasets
    drinks_df = spark.read.csv(drinks_path, header=True, inferSchema=True)
    drugs_df = spark.read.csv(drugs_path, header=True, inferSchema=True)

    return drinks_df, drugs_df


def transform_drinks(drinks_df):
    # Transform drinks data
    drinks_df = drinks_df.withColumnRenamed("country", "country") \
                         .withColumn("total_litres_of_pure_alcohol", 
                                     col("total_litres_of_pure_alcohol").fillna(0)) \
                         .withColumn("alcohol_diversity", 
                                     expr("((beer_servings / (beer_servings + spirit_servings + wine_servings)) ** 2) + "
                                          "((spirit_servings / (beer_servings + spirit_servings + wine_servings)) ** 2) + "
                                          "((wine_servings / (beer_servings + spirit_servings + wine_servings)) ** 2)"))

    return drinks_df


def transform_drugs(drugs_df):
    # Transform drugs data
    drugs_df = drugs_df.select([
        col(c).cast("float").alias(c) if c.endswith("frequency") else col(c)
        for c in drugs_df.columns
    ]).fillna(0)  # Filling nulls with 0 for numerical stability

    return drugs_df


def save_to_db(drinks_df, drugs_df):
    # Save transformed data to Delta tables
    drinks_table_path = "dbfs:/FileStore/mini_project11/transformed_drinks"
    drugs_table_path = "dbfs:/FileStore/mini_project11/transformed_drugs"

    # Write drinks data
    drinks_df.write.format("delta").mode("overwrite").saveAsTable(drinks_table_path)

    # Write drugs data
    drugs_df.write.format("delta").mode("overwrite").saveAsTable(drugs_table_path)


def main():
    # Load datasets
    drinks_df, drugs_df = load()

    # Transform datasets
    drinks_df = transform_drinks(drinks_df)
    drugs_df = transform_drugs(drugs_df)

    # Save datasets to Delta tables
    save_to_db(drinks_df, drugs_df)


if __name__ == "__main__":
    main()
