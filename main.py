from mylib.extract import extract
from mylib.transform_load import load,transform_drinks,transform_drugs,save_to_db
from mylib.query_visual import visualize_query_results

if __name__ == "__main__":
    extract()
    drinks_df, drugs_df = load()
    # Transform datasets
    drinks_df = transform_drinks(drinks_df)
    drugs_df = transform_drugs(drugs_df)
    # Save datasets to Delta tables
    save_to_db(drinks_df, drugs_df)
    visualize_query_results()