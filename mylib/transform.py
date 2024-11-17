import pandas as pd
import requests
from io import StringIO
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


def load_data_from_url(url):
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data from {url}. Status code: {response.status_code}")
    return pd.read_csv(StringIO(response.text), delimiter=",")


def transform_drinks(df):
    # Clean and rename columns
    df.columns = df.columns.str.lower().str.replace(" ", "_")
    df['total_litres_of_pure_alcohol'] = df['total_litres_of_pure_alcohol'].fillna(0)
    # Add alcohol diversity index
    total = df['beer_servings'] + df['spirit_servings'] + df['wine_servings']
    df['alcohol_diversity'] = (df['beer_servings'] / total) ** 2 + \
                              (df['spirit_servings'] / total) ** 2 + \
                              (df['wine_servings'] / total) ** 2
    return df


def transform_drugs(df):
    # Clean and rename columns
    df.columns = df.columns.str.lower().str.replace(" ", "_")
    # Replace invalid '-' with None and cast percentages to float
    for col in df.columns:
        df[col] = df[col].replace('-', None)
    df['cocaine_frequency'] = df['cocaine_frequency'].astype(float)
    # Add new column for total drug frequency
    df['total_drug_frequency'] = df[['cocaine_frequency', 'crack_frequency', 'heroin_frequency']].sum(axis=1)
    return df


def save_to_db(drinks_df, drugs_df):
    load_dotenv()
    server_h = os.getenv("SERVER_HOSTNAME")
    access_token = os.getenv("ACCESS_TOKEN")
    http_path = os.getenv("HTTP_PATH")

    connection_string = f"databricks://token:{access_token}@{server_h}?http_path={http_path}"
    engine = create_engine(connection_string)

    with engine.begin() as connection:
        try:
            # Create tables if not exist
            connection.execute(text("""
                CREATE OR REPLACE TABLE zw308_drink (
                    country VARCHAR(255),
                    beer_servings INT,
                    spirit_servings INT,
                    wine_servings INT,
                    total_litres_of_pure_alcohol FLOAT,
                    alcohol_diversity FLOAT
                ) USING DELTA
            """))
            connection.execute(text("""
                CREATE OR REPLACE TABLE zw308_drug_use (
                    age_group VARCHAR(50),
                    number INT,
                    alcohol_use FLOAT,
                    alcohol_frequency FLOAT,
                    marijuana_use FLOAT,
                    marijuana_frequency FLOAT,
                    cocaine_use FLOAT,
                    cocaine_frequency FLOAT,
                    crack_use FLOAT,
                    crack_frequency FLOAT,
                    heroin_use FLOAT,
                    heroin_frequency FLOAT,
                    hallucinogen_use FLOAT,
                    hallucinogen_frequency FLOAT,
                    inhalant_use FLOAT,
                    inhalant_frequency FLOAT,
                    pain_releiver_use FLOAT,
                    pain_releiver_frequency FLOAT,
                    oxycontin_use FLOAT,
                    oxycontin_frequency FLOAT,
                    tranquilizer_use FLOAT,
                    tranquilizer_frequency FLOAT,
                    stimulant_use FLOAT,
                    stimulant_frequency FLOAT,
                    meth_use FLOAT,
                    meth_frequency FLOAT,
                    sedative_use FLOAT,
                    sedative_frequency FLOAT,
                    total_drug_frequency FLOAT
                ) USING DELTA
            """))

            # Insert transformed data into tables
            drinks_df.to_sql('zw308_drink', con=engine, if_exists='append', index=False)
            drugs_df.to_sql('zw308_drug_use', con=engine, if_exists='append', index=False)

        except Exception as e:
            print(f"An error occurred: {e}")


def main():
    # URLs for datasets
    url_drinks = "https://raw.githubusercontent.com/fivethirtyeight/data/master/alcohol-consumption/drinks.csv"
    url_drugs = "https://raw.githubusercontent.com/fivethirtyeight/data/master/drug-use-by-age/drug-use-by-age.csv"
    # Load data
    drinks_df = load_data_from_url(url_drinks)
    drugs_df = load_data_from_url(url_drugs)

    # Transform data
    drinks_df = transform_drinks(drinks_df)
    drugs_df = transform_drugs(drugs_df)

    # Save transformed data to the database
    save_to_db(drinks_df, drugs_df)


if __name__ == "__main__":
    main()
