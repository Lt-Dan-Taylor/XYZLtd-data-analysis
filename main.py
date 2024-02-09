from extract import Extract
from transform import Transform
from load import Load

# Extract the raw datasets into dataframes
extractor = Extract()
membership_df, logs_df, transactions_df = extractor()

# Transform the dataframes
transformer = Transform(membership_df, logs_df, transactions_df, extractor.extract_currency_exchange_rate())
transformed_membership, transformed_logs, transformed_transactions = transformer()

# Load the data into .csv
loader = Load(transformed_membership, transformed_logs, transformed_transactions)
loader.main_dataframe()
