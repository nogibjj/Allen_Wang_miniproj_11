import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession

def visualize_query_results():
    # Load environment variables
    spark = SparkSession.builder.appName("Allen Query").config("spark.sql.catalogImplementation", "hive").enableHiveSupport().getOrCreate()

    delta_path = "dbfs:/FileStore/Allen_mini_project11/zw_308_transformed_drink"

    # Query the database
    query = (f"""SELECT country, beer_servings, spirit_servings, wine_servings, total_litres_of_pure_alcohol
    FROM delta.`{delta_path}`
    WHERE total_litres_of_pure_alcohol > 5
    ORDER BY total_litres_of_pure_alcohol DESC
    LIMIT 10"""
    )
    df = spark.sql(query)

    # Convert PySpark DataFrame to Pandas DataFrame for visualization
    pandas_df = df.toPandas()

    # Plot 1: Bar Plot of Alcohol Consumption by Country
    plt.figure(figsize=(10, 6))
    sns.barplot(
        data=pandas_df,
        x="country",
        y="total_litres_of_pure_alcohol",
        palette="viridis"
    )
    plt.title("Top 10 Countries by Alcohol Consumption")
    plt.ylabel("Total Litres of Pure Alcohol")
    plt.xlabel("Country")
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig("top_10_countries_alcohol_consumption.png")  # Save the image
    plt.close()  # Close the plot to free memory

    # Plot 2: Stacked Bar Chart of Alcohol Types
    pandas_df_melted = pandas_df.melt(
        id_vars=["country"],
        value_vars=["beer_servings", "spirit_servings", "wine_servings"],
        var_name="Alcohol_Type",
        value_name="Servings"
    )
    plt.figure(figsize=(12, 7))
    sns.barplot(
        data=pandas_df_melted,
        x="country",
        y="Servings",
        hue="Alcohol_Type",
        palette="pastel"
    )
    plt.title("Alcohol Servings by Type for Top 10 Countries")
    plt.ylabel("Servings")
    plt.xlabel("Country")
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig("alcohol_servings_by_type.png")  # Save the image
    plt.close()  # Close the plot to free memory

if __name__ == "__main__":
    visualize_query_results()
