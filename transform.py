import pandas as pd
import numpy as np

class Transform:
    """
    A class to handle data trasformation from `Extract` class extracted DataFrames.
    
    This class takes extracted DataFrames from the `Extract` class and applies
    transformations to prepare them for analysis. It handles data cleaning,
    feature engineering, and aggregation tasks.
    
    Methods:
        + `__init__`: Initializes the object with extracted DataFrames.
        + `__call__`: Returns transformed DataFrames.
        + `transformed_membership`: Transforms and cleans membership data.
        + `transformed_logs`: Transforms, filters, and aggregates logs data.
        + `transform_transactions`: Transforms, aggregates, and converts currency in transactions data.
    """
    def __init__(self, membership_df, logs_df, transactions_df, currency_exchange_rate):
        self.membership_df = membership_df
        self.logs_df = logs_df
        self.transactions_df = transactions_df
        self.currency_exchange_rate = currency_exchange_rate

    def __call__(self):
        transformed_membership = self.transform_membership(self.membership_df)
        transformed_logs = self.transform_logs(self.logs_df)
        transformed_transactions = self.transform_transactions(self.transactions_df)

        return transformed_membership, transformed_logs, transformed_transactions

    def transform_membership(self, df):
        df = df.copy()

        # Drop rows with NaN values if any
        df = df.dropna(subset=['membership_id'])

        # Transform columns
        date_columns = [col for col in df.columns if any(substring in col.lower() for substring in ['date', 'time'])]
        df[date_columns] = df[date_columns].apply(pd.to_datetime, errors='coerce')
    
        df['membership_id'] = df['membership_id'].astype('int32')
        df['country_state'] = df['billing_address'].str.split(',').str[4]
    
        # Filter the columns
        main_columns = ['membership_id', 'creation_date', 'company', 'country_state', 'key_account_manager', 'animation_team', 'membership_plan', 'membership_amount', 'currency']
        df = df[main_columns]
    
        return df

    def transform_logs(self, df):
        df = df.copy()

        # Transform columns
        date_columns = [col for col in self.logs_df.columns if any(substring in col.lower() for substring in ['date', 'time'])]
        self.logs_df[date_columns] = self.logs_df[date_columns].apply(pd.to_datetime, errors='coerce')

        # Replace empty strings and NaN with pd.NA
        self.logs_df['new_plan'] = self.logs_df['new_plan'].replace({'': pd.NA, np.nan: pd.NA})

        # Filter logs
        mask = ((self.logs_df['churn_date'] >= '2019-01-01') & (self.logs_df['churn_date'] < '2023-01-01')) | \
               ((self.logs_df['cancellation_date'] >= '2019-01-01') & (self.logs_df['cancellation_date'] < '2023-01-01')) | \
               self.logs_df['new_plan'].notnull()
        df = self.logs_df[mask]

        # Group by membership_id and aggregate last new_plan, min churn_date, min cancellation_date
        df = df.groupby('membership_id').agg({'new_plan': 'last', 'churn_date': 'min', 'cancellation_date': 'min'}).reset_index()

        return df

    def transform_transactions(self, df):
        df = df.copy()

        # Transform columns
        date_columns = [col for col in self.transactions_df.columns if any(substring in col.lower() for substring in ['date', 'time'])]
        self.transactions_df[date_columns] = self.transactions_df[date_columns].apply(pd.to_datetime, errors='coerce')

        # Convert charge_amount to numeric
        self.transactions_df['charge_amount'] = pd.to_numeric(self.transactions_df['charge_amount'], errors='coerce')

        # Merge with membership_df to get currency
        df = pd.merge(self.transactions_df, self.membership_df[['membership_id', 'currency']], on='membership_id', how='left', suffixes=('', '_membership'))
        df['currency'] = df['currency'].fillna(df['currency_membership'])
        df.drop(columns=['currency_membership'], inplace=True)

        # Convert charge_amount to USD
        def convert_to_usd(row):
            currency = row['currency']
            date = row['transaction_date']
            amount = row['charge_amount']

            if currency == 'USD':
                return amount

            if not currency:
                return None

            exchange_rate = self.currency_exchange_rate.loc[self.currency_exchange_rate['Date'] == date, currency].values

            if len(exchange_rate) > 0:
                return amount * exchange_rate[0]
            else:
                return None

        df['charge_amount(USD)'] = df.apply(convert_to_usd, axis=1)

        # Create new columns in the original DataFrame based on unique values in description_event
        agg = df.groupby('description_event')['charge_amount(USD)'].apply(list).reset_index()

        for event in agg['description_event']:
            df[event.replace(' ', '_')] = df.apply(lambda x: round(x['charge_amount(USD)'], 2) if x['description_event'] == event else 0, axis=1)

        df.drop(columns='description_event', inplace=True)

        df = df.groupby('membership_id')[['membership_payment', 'charge_for_specific_project', 'membership_additional_service']].sum().reset_index()

        df['charge_total'] = round(df[['membership_payment', 'charge_for_specific_project', 'membership_additional_service']].sum(axis=1), 2)

        df = df[df['charge_total'] != 0]

        return df
