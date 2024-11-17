import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession

def visualize_query_results():
    # Load environment variables
    spark = SparkSession.builder.appName("Allen Query").getOrCreate()

    # Query the database
    query = """
    SELECT country, beer_servings, spirit_servings, wine_servings, total_litres_of_pure_alcohol
    FROM zw308_drink
    WHERE total_litres_of_pure_alcohol > 5
    ORDER BY total_litres_of_pure_alcohol DESC
    LIMIT 10
    """
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
    plt.show()

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
    plt.show()

if __name__ == "__main__":
    visualize_query_results()
