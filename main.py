from extract import Extract
from transform import Transform
from load import Load
from plotting import Plot

def main():
    # Extract the raw datasets into dataframes
    extractor = Extract()
    membership_df, logs_df, transactions_df = extractor()

    # Transform the dataframes
    transformer = Transform(membership_df, logs_df, transactions_df, extractor.extract_currency_exchange_rate())
    transformed_membership, transformed_logs, transformed_transactions = transformer()

    # Load the data into .csv
    loader = Load(transformed_membership, transformed_logs, transformed_transactions)
    main_df = loader.main_dataframe()

    # Plot the data
    plotter = Plot(main_df)
    plotter.boxplot()

if __name__ == "__main__":
    main()
