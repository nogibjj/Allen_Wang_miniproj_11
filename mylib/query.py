import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def create_row(country, beer_servings, spirit_servings, wine_servings, total_litres_of_pure_alcohol):
    load_dotenv()
    HOST = os.getenv("SERVER_HOSTNAME")
    ACCESS_TOKEN = os.getenv("ACCESS_TOKEN") 
    HTTP_PATH = os.getenv("HTTP_PATH")
    
    # Create Databricks engine
    connection_string = f"databricks://token:{ACCESS_TOKEN}@{HOST}?http_path={HTTP_PATH}"
    engine = create_engine(connection_string)

    try:
        with engine.connect() as connection:
            sql = '''INSERT INTO default.zw308_drink (country, beer_servings, spirit_servings, wine_servings, total_litres_of_pure_alcohol) VALUES (?, ?, ?, ?, ?)'''
            connection.execute(text(sql), (country, beer_servings, spirit_servings, wine_servings, total_litres_of_pure_alcohol))
            return True
    except Exception as e:
        print(f"Error: {e}")
        return False


def read_all():
    load_dotenv()
    HOST = os.getenv("SERVER_HOSTNAME")
    ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
    HTTP_PATH = os.getenv("HTTP_PATH")
    
    # Create Databricks engine
    connection_string = f"databricks://token:{ACCESS_TOKEN}@{HOST}?http_path={HTTP_PATH}"
    engine = create_engine(connection_string)

    try:
        with engine.connect() as connection:
            sql = '''SELECT * FROM default.zw308_drug_use'''
            results = connection.execute(text(sql)).fetchall()
            return results
    except Exception as e:
        print(f"Error: {e}")
        return None


def update_row(country, beer_servings):
    load_dotenv()
    HOST = os.getenv("SERVER_HOSTNAME")
    ACCESS_TOKEN = os.getenv("ACCESS_TOKEN") 
    HTTP_PATH = os.getenv("HTTP_PATH")
    
    # Create Databricks engine
    connection_string = f"databricks://token:{ACCESS_TOKEN}@{HOST}?http_path={HTTP_PATH}"
    engine = create_engine(connection_string)

    try:
        with engine.connect() as connection:
            sql = '''UPDATE default.zw308_drink SET beer_servings = ? WHERE country = ?'''
            connection.execute(text(sql), (beer_servings, country))
            return True
    except Exception as e:
        print(f"Error: {e}")
        return False
    
def delete_row(country):
    load_dotenv()
    HOST = os.getenv("SERVER_HOSTNAME")
    ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
    HTTP_PATH = os.getenv("HTTP_PATH")
    
    # Create Databricks engine
    connection_string = f"databricks://token:{ACCESS_TOKEN}@{HOST}?http_path={HTTP_PATH}"
    engine = create_engine(connection_string)

    try:
        with engine.connect() as connection:
            sql = '''DELETE FROM default.zw308_drink WHERE country = ?'''
            connection.execute(text(sql), (country,))
            return True
    except Exception as e:
        print(f"Error: {e}")
        return False


def general(query):
    load_dotenv()
    HOST = os.getenv("SERVER_HOSTNAME")
    ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
    HTTP_PATH = os.getenv("HTTP_PATH")
    
    # Create Databricks engine
    connection_string = f"databricks://token:{ACCESS_TOKEN}@{HOST}?http_path={HTTP_PATH}"
    engine = create_engine(connection_string)

    try:
        with engine.connect() as connection:
            result = connection.execute(text(query))
            if query.strip().lower().startswith("select"):
                results = result.fetchall()
                return results
            return True  # For non-select queries
    except Exception as e:
        print(f"Error: {e}")
        return None
    
#general("SELECT tc.country, tc.total_beer_servings, u.age_group, u.alcohol_use, u.alcohol_frequency FROM (SELECT country, SUM(beer_servings) AS total_beer_servings FROM zw308_drink GROUP BY country ORDER BY total_beer_servings DESC LIMIT 5) AS tc JOIN zw308_drug_use u ON u.alcohol_use = (SELECT MAX(alcohol_use) FROM zw308_drug_use) ORDER BY tc.total_beer_servings DESC, u.alcohol_use DESC;")