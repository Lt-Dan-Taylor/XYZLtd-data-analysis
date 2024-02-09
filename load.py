import pandas as pd

class Load:
    def __init__(self, transformed_membership, transformed_logs, transformed_transactions):
        self.transformed_membership = transformed_membership
        self.transformed_logs = transformed_logs
        self.transformed_transactions = transformed_transactions

    def main_dataframe(self):
        # Merging the three dataframes
        main_df = pd.merge(self.transformed_membership, self.transformed_logs, on='membership_id', how='left')
        main_df = pd.merge(main_df, self.transformed_transactions, on='membership_id', how='left')

        # Update columns
        main_df['churned'] = main_df['churn_date'].notnull() | main_df['cancellation_date'].notnull()
        main_df['membership_plan'] = main_df['new_plan'].where(main_df['new_plan'].notnull(), main_df['membership_plan'])
        
		# Rename some columns name and change their order
        main_df = main_df.rename(columns={'charge_total': 'charged_total', 
                                          'membership_payment': 'membership_payment_total',
                                          'charge_for_specific_project': 'project_payment_total',
                                          'membership_additional_service': 'additional_service_total'})

        main_df = main_df[['membership_id', 'creation_date', 'churned', 'company', 'country_state',
                           'key_account_manager', 'animation_team', 'membership_plan',
                           'membership_payment_total', 'project_payment_total',
                           'additional_service_total', 'charged_total']]

        # Save the dataframe to CSV
        main_df.to_csv('XYZ_Customer_Analysis_2019-2022.csv', sep=',', index=False)
