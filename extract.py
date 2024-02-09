import pandas as pd
import numpy as np
import sqlite3

class Extract:
    def __init__(self):
        self.membership_df = self.extract_membership()
        self.logs_df = self.extract_logs()
        self.transactions_df = self.extract_transactions()

    def __call__(self):
        return self.membership_df, self.logs_df, self.transactions_df

    def extract_membership(self):
        file_path = 'raw_data/Membership.csv'
        membership_df = pd.read_csv(file_path, sep=',') 
        return membership_df

    def extract_logs(self):
        file_path = 'raw_data/Membership_log.json'
        logs_df = pd.read_json(file_path, encoding='utf-8-sig')
        logs_df = logs_df[['event_name', 'membership_id', 'new_plan', 'churn_date', 'cancellation_date', 'log_creation_time']]
        return logs_df

    def extract_transactions(self):
        file_path = 'raw_data/membership_transactions.sql'
        conn = sqlite3.connect('raw_data/transactionsDB.db')
        cursor = conn.cursor()

        # Create the 'membership_transactions' table if not exists
        with open(file_path, 'r', encoding='utf-8-sig') as sql_file:
            create_table_sql = """
                CREATE TABLE IF NOT EXISTS membership_transactions (
                    charge_amount DECIMAL(10, 2),
                    currency VARCHAR(3),
                    membership_id INTEGER,
                    description_event VARCHAR(255),
                    discount DOUBLE,
                    status VARCHAR(10),
                    message VARCHAR(255),
                    transaction_date TEXT,
                    triggered_by VARCHAR(50),
                    payment_method VARCHAR(50)
                );
            """
            cursor.executescript(create_table_sql + sql_file.read())

        conn.commit()

        # Read data from the 'membership_transactions' table
        transactions_df = pd.read_sql('SELECT * FROM membership_transactions;', conn)
        conn.close()

        return transactions_df

    def extract_currency_exchange_rate(self):
        currency_dict = {
            'DKK': 'raw_data/exchange_rate/DKK_USD Historical Data.csv',
            'EUR': 'raw_data/exchange_rate/EUR_USD Historical Data.csv',
            'GBP': 'raw_data/exchange_rate/GBP_USD Historical Data.csv'
        }

        currency_exchange_rate = pd.DataFrame()

        for currency, file_path in currency_dict.items():
            exchange_rate = pd.read_csv(file_path, usecols=['Date', 'Price'])
            exchange_rate = exchange_rate.rename(columns={'Price': currency})

            if currency_exchange_rate.empty:
                currency_exchange_rate = exchange_rate
            else:
                currency_exchange_rate = pd.merge(currency_exchange_rate, exchange_rate, on='Date', how='inner')

        currency_exchange_rate['Date'] = pd.to_datetime(currency_exchange_rate['Date'])

        return currency_exchange_rate
