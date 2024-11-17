import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

def visualize_query_results():
    # Load environment variables
    load_dotenv()
    server_h = os.getenv("SERVER_HOSTNAME")
    access_token = os.getenv("ACCESS_TOKEN")
    http_path = os.getenv("HTTP_PATH")

    # Create database engine
    connection_string = f"databricks://token:{access_token}@{server_h}?http_path={http_path}"
    engine = create_engine(connection_string)

    # Query the database
    query = """
    SELECT country, beer_servings, spirit_servings, wine_servings, total_litres_of_pure_alcohol
    FROM zw308_drink
    WHERE total_litres_of_pure_alcohol > 5
    ORDER BY total_litres_of_pure_alcohol DESC
    LIMIT 10
    """
    with engine.connect() as connection:
        df = pd.read_sql_query(text(query), connection)

    # Plot 1: Bar Plot of Alcohol Consumption by Country
    plt.figure(figsize=(10, 6))
    sns.barplot(
        data=df,
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
    df_melted = df.melt(
        id_vars=["country"],
        value_vars=["beer_servings", "spirit_servings", "wine_servings"],
        var_name="Alcohol_Type",
        value_name="Servings"
    )
    plt.figure(figsize=(12, 7))
    sns.barplot(
        data=df_melted,
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
