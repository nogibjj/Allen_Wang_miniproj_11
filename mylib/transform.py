import pandas as pd
import requests
from io import StringIO
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


def csv_to_db(url1, url2):
    response1 = requests.get(url1)
    response2 = requests.get(url2)

    if response1.status_code != 200:
        raise Exception(f"Failed to fetch data from {url1}. Status code: {response1.status_code}")
    if response2.status_code != 200:
        raise Exception(f"Failed to fetch data from {url2}. Status code: {response2.status_code}")

    df = pd.read_csv(StringIO(response1.text), delimiter=",")
    df2 = pd.read_csv(StringIO(response2.text), delimiter=",")

    load_dotenv()
    server_h = os.getenv("SERVER_HOSTNAME")
    access_token = os.getenv("ACCESS_TOKEN")
    http_path = os.getenv("HTTP_PATH")

    connection_string = f"databricks://token:{access_token}@{server_h}?http_path={http_path}"
    engine = create_engine(connection_string)

    with engine.begin() as connection:  # Use engine.begin() for transactional connection
        try:
            drink_exists = connection.execute(text("SELECT COUNT(*) FROM zw308_drink")).scalar() > 0
            drug_use_exists = connection.execute(text("SELECT COUNT(*) FROM zw308_drug_use")).scalar() > 0
            # Create new tables
            if not drink_exists:
                connection.execute(text(
                    """
                    CREATE OR REPLACE TABLE zw308_drink (
                        country VARCHAR(255),
                        beer_servings INT,
                        spirit_servings INT,
                        wine_servings INT,
                        total_litres_of_pure_alcohol FLOAT
                    ) USING DELTA
                    """
                ))
            if not drug_use_exists:
                connection.execute(text(
                    """
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
                        sedative_frequency FLOAT
                    ) USING DELTA
                    """
                ))
            if not drink_exists:
                for _, row in df.iterrows():
                    insert_query = text("""
                        INSERT INTO default.zw308_drink (
                            country, beer_servings, spirit_servings, wine_servings, total_litres_of_pure_alcohol
                        )
                        VALUES (:country, :beer_servings, :spirit_servings, :wine_servings, :total_litres_of_pure_alcohol)
                    """)
                    connection.execute(insert_query, {
                        'country': row['country'],
                        'beer_servings': row['beer_servings'],
                        'spirit_servings': row['spirit_servings'],
                        'wine_servings': row['wine_servings'],
                        'total_litres_of_pure_alcohol': row['total_litres_of_pure_alcohol']
                    })
            if not drug_use_exists:
                for _, row in df2.iterrows():
                    for key in ['cocaine_frequency', 'crack_frequency', 'heroin_frequency', 'inhalant_frequency', 'oxycontin_frequency', 'meth_frequency']:
                        if row[key] == '-':
                            row[key] = None
                        else:
                            row[key] = float(row[key])
                    insert_query = text("""
                        INSERT INTO default.zw308_drug_use (
                            age_group, number, alcohol_use, alcohol_frequency, marijuana_use, marijuana_frequency,
                            cocaine_use, cocaine_frequency, crack_use, crack_frequency, heroin_use, heroin_frequency,
                            hallucinogen_use, hallucinogen_frequency, inhalant_use, inhalant_frequency, 
                            pain_releiver_use, pain_releiver_frequency, oxycontin_use, oxycontin_frequency,
                            tranquilizer_use, tranquilizer_frequency, stimulant_use, stimulant_frequency, 
                            meth_use, meth_frequency, sedative_use, sedative_frequency
                        )
                        VALUES (:age_group, :number, :alcohol_use, :alcohol_frequency, :marijuana_use, :marijuana_frequency,
                                :cocaine_use, :cocaine_frequency, :crack_use, :crack_frequency, :heroin_use, :heroin_frequency,
                                :hallucinogen_use, :hallucinogen_frequency, :inhalant_use, :inhalant_frequency, 
                                :pain_releiver_use, :pain_releiver_frequency, :oxycontin_use, :oxycontin_frequency,
                                :tranquilizer_use, :tranquilizer_frequency, :stimulant_use, :stimulant_frequency, 
                                :meth_use, :meth_frequency, :sedative_use, :sedative_frequency)
                    """)
                    connection.execute(insert_query, {
                        'age_group': row['age'],
                        'number': row['n'],
                        'alcohol_use': row['alcohol_use'],
                        'alcohol_frequency': row['alcohol_frequency'],
                        'marijuana_use': row['marijuana_use'],
                        'marijuana_frequency': row['marijuana_frequency'],
                        'cocaine_use': row['cocaine_use'],
                        'cocaine_frequency': row['cocaine_frequency'],
                        'crack_use': row['crack_use'],
                        'crack_frequency': row['crack_frequency'],
                        'heroin_use': row['heroin_use'],
                        'heroin_frequency': row['heroin_frequency'],
                        'hallucinogen_use': row['hallucinogen_use'],
                        'hallucinogen_frequency': row['hallucinogen_frequency'],
                        'inhalant_use': row['inhalant_use'],
                        'inhalant_frequency': row['inhalant_frequency'],
                        'pain_releiver_use': row['pain_releiver_use'],
                        'pain_releiver_frequency': row['pain_releiver_frequency'],
                        'oxycontin_use': row['oxycontin_use'],
                        'oxycontin_frequency': row['oxycontin_frequency'],
                        'tranquilizer_use': row['tranquilizer_use'],
                        'tranquilizer_frequency': row['tranquilizer_frequency'],
                        'stimulant_use': row['stimulant_use'],
                        'stimulant_frequency': row['stimulant_frequency'],
                        'meth_use': row['meth_use'],
                        'meth_frequency': row['meth_frequency'],
                        'sedative_use': row['sedative_use'],
                        'sedative_frequency': row['sedative_frequency']
                    })

        except Exception as e:
            print(f"An error occurred: {e}")

# Usage
#csv_to_db("https://raw.githubusercontent.com/fivethirtyeight/data/master/alcohol-consumption/drinks.csv",
#           "https://raw.githubusercontent.com/fivethirtyeight/data/master/drug-use-by-age/drug-use-by-age.csv")
