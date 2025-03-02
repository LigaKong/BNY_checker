from merge_paris_data import MergeParisData
import pandas as pd
import os

class BNYChecker:

    """
    Compares the BNY data with Paris Returns Audits data to identify discrepancies.

    Requirements:
        1. Statement_of_Change_in_Net_Assets_{client_name}.xls (BNY website)
        2. (Optional) Asset_Detail_{client_name}.xls (BNY website, for comparing Commingle Fund market values)
        3. (Optional) Transaction_Detail_{client_name}.xls (BNY website, for comparing Commingle Fund cash flows)
    """

    def __init__(self, client_name):
        
        self.client_name = client_name
        self.soc = pd.read_excel(f"./input/Statement_of_Change_in_Net_Assets_{self.client_name}.xls",header=0)
        self.ad_filepath = f"./input/Asset_Detail_{self.client_name}.xls"
        self.td_filepath = f"./input/Transaction_Detail_{self.client_name}.xls"
        
        self.ctr = ['RECEIPTS:_CONTRIBUTIONS:',
                    'RECEIPTS:_RECEIVED FROM',
                    'RECEIPTS:_MISCELLANEOUS RECEIPTS']
        self.dtr = ['DISBURSEMENTS:_DISTRIBUTION',
                    'DISBURSEMENTS:_PAYMENTS TO INSURANCE CARRIERS:_PREMIUMS',
                    'DISBURSEMENTS:_DISBURSED TO PARTICIPATING ACCOUNTS']
        self.ti =  ['RECEIPTS:_TRANSFERS IN:',
                    'RECEIPTS:_DIRECT ROLLOVER TRANSFER IN',
                    'RECEIPTS:_PARTICIPANT TRANSFER IN']
        self.to =  ['DISBURSEMENTS:_TRANSFERS OUT:',
                    'DISBURSEMENTS:_DIRECT ROLLOVER TRANSFER OUT',
                    'DISBURSEMENTS:_PARTICIPANT TRANSFER OUT',]
        self.fe =  ['DISBURSEMENTS:_ADMINISTRATIVE EXPENSES:_INVESTMENT ADVISORY FEES',
                    'DISBURSEMENTS:_ADMINISTRATIVE EXPENSES:_INVESTMENT MANAGEMENT',
                    'DISBURSEMENTS:_ADMINISTRATIVE EXPENSES:_SERVICE FEES',
                    'RECEIPTS:_MT ALL INVESTMENT MANAGER FEES',
                    'DISBURSEMENTS:_ADMINISTRATIVE EXPENSES:_MANAGEMENT FEE - COMMITMENT']
        self.exp = ['DISBURSEMENTS:_ADMINISTRATIVE EXPENSES:',
                    'RECEIPTS:_MASTER TRUST ALLOCATED EXPENSES',
                    'RECEIPTS:_MASTER TRUST CONSULTING FEES',
                    'RECEIPTS:_MASTER TRUST SEC LENDING REBATE',
                    'RECEIPTS:_MASTER TRUST STOCK LOAN FEES',
                    'RECEIPTS:_MT ALL TRUST/CUSTODIAN FEES']
        self.ignore = ['DISBURSEMENTS:_ADMINISTRATIVE EXPENSES:_COMMISSION ON FUTURES CONTRACTS']

    def generate_description(self, row):
        desc1 = str(row['Description 1']) if pd.notna(row['Description 1']) else ''
        desc2 = str(row['Description 2']) if pd.notna(row['Description 2']) else ''
        desc3 = str(row['Description 3']) if pd.notna(row['Description 3']) else ''

        if desc3:
            return f"{desc1}_{desc2}_{desc3}"
        if desc2:
            return f"{desc1}_{desc2}"
        else:
            return desc1

    def assign_transaction(self, description):
        transaction_mapping = {'CTR': self.ctr,'DTR': self.dtr,'TI': self.ti,'TO': self.to,'FE': self.fe,'EXP': self.exp}
        if any(ignore_item in description for ignore_item in self.ignore): 
            return None
        for transaction, values in transaction_mapping.items():
            if any(value in description for value in values):
                return transaction
        return None 

    def get_statement_of_change_in_net_assets_data(self):

        df_soc = self.soc[['Reporting Account Number','End Date', 'Description 1', 'Description 2', 'Description 3', 'Local/Base Value', 'Acctg Status Update (EST)', 'Accounting Status']].copy()
        df_soc['Description'] = df_soc.apply(self.generate_description, axis=1)    
        df_soc['Transaction'] = df_soc['Description'].apply(self.assign_transaction)

        grouped = df_soc.groupby('Reporting Account Number')
        df = grouped.apply(lambda x: pd.Series({
            'Beginning Balance': x.loc[x['Description'] == 'NET ASSETS - BEGINNING OF PERIOD', 'Local/Base Value'].sum(),
            'Ending Balance': (
                x.loc[x['Description'] == 'NET ASSETS - BEGINNING OF PERIOD', 'Local/Base Value'].sum() +
                x.loc[x['Description 1'] == 'RECEIPTS:', 'Local/Base Value'].sum() -
                x.loc[x['Description 1'] == 'DISBURSEMENTS:', 'Local/Base Value'].sum()),
            'CTR': x.loc[x['Transaction'] == 'CTR', 'Local/Base Value'].sum(),
            'DTR': x.loc[x['Transaction'] == 'DTR', 'Local/Base Value'].sum(),
            'TI': x.loc[x['Transaction'] == 'TI', 'Local/Base Value'].sum(),
            'TO': x.loc[x['Transaction'] == 'TO', 'Local/Base Value'].sum(),
            'EXP': x.loc[x['Transaction'] == 'EXP', 'Local/Base Value'].sum(),
            'FE': x.loc[x['Transaction'] == 'FE', 'Local/Base Value'].sum()})).reset_index()

        df['End Date'] = grouped['End Date'].first().values
        df['Acctg Status Update (EST)'] = grouped['Acctg Status Update (EST)'].first().values
        df['Accounting Status'] = grouped['Accounting Status'].first().values
        
        df.rename(columns={'Reporting Account Number': 'CustodianAcct'}, inplace=True)

        return df
    
    def get_asset_detail_data(self):
        df_ad = pd.read_excel(self.ad_filepath, header=0)[['Reporting Account Number', 'Mellon Security ID', 'Asset Type', 'Base Market Value']].copy()
        df_ad.rename(columns={'Reporting Account Number': 'CustodianAcct', 'Mellon Security ID': 'CustodianSecurityID'}, inplace=True)
        df_ad['CustodianSecurityID'] = df_ad['CustodianSecurityID'].astype(str)
        
        return df_ad
    
    def get_transaction_detail_data(self):
        df_td = pd.read_excel(self.td_filepath, header=0)[['Reporting Account Number', 'Mellon Security ID', 'Transaction Category', 'Asset-Type/Sub-Category', 'Base Txn Amount']].copy()
        df_td.rename(columns={'Reporting Account Number': 'CustodianAcct', 'Mellon Security ID': 'CustodianSecurityID'}, inplace=True)
        df_td['CustodianSecurityID'] = df_td['CustodianSecurityID'].astype(str)
        
        grouped = df_td.groupby(['CustodianAcct','CustodianSecurityID'])
        df_td = grouped.apply(lambda x: pd.Series({
            'Commingle Fund TI':  -x.loc[(x['Transaction Category'] == 'PURCHASES')&(x['Asset-Type/Sub-Category'] == 'UNIT OF PARTICIPATION'), 'Base Txn Amount'].sum(),
            'Commingle Fund TO': -x.loc[(x['Transaction Category'] == 'SALES')&(x['Asset-Type/Sub-Category'] == 'UNIT OF PARTICIPATION'), 'Base Txn Amount'].sum()})).reset_index()

        return df_td


    def bny_checker(self):
        
        df_paris = MergeParisData(self.client_name).merge_paris_data()
        df_soc = BNYChecker(self.client_name).get_statement_of_change_in_net_assets_data()
        df = pd.merge(df_paris, df_soc, on='CustodianAcct', how="left")
        
        for id in df.index:
            account_id = str(df.loc[id,'CustodianAcct'])
            df.loc[id,'Beginning MV_diff'] = df.loc[id,'Beginning Balance'] - sum(df.loc[(df['CustodianAcct']==account_id)]['Prior Market Value'])
            df.loc[id,'MV_diff'] = df.loc[id,'Ending Balance'] - sum(df.loc[(df['CustodianAcct']==account_id)]['Total Market Value'])
            df.loc[id,'EXP_diff'] = df.loc[id,'EXP'] - sum(df.loc[(df['CustodianAcct']==account_id)]['Expenses'])
            df.loc[id,'FE_diff'] = df.loc[id,'FE'] - sum(df.loc[(df['CustodianAcct']==account_id)]['Fees'])
            df.loc[id,'CTR_diff'] = df.loc[id,'CTR'] - sum(df.loc[(df['CustodianAcct']==account_id)]['Contributions'])
            df.loc[id,'DTR_diff'] = df.loc[id,'DTR'] - sum(df.loc[(df['CustodianAcct']==account_id)]['Distributions'])
            df.loc[id,'TI_diff'] = df.loc[id,'TI'] - sum(df.loc[(df['CustodianAcct']==account_id)]['Transfers in'])
            df.loc[id,'TO_diff'] = df.loc[id,'TO'] - sum(df.loc[(df['CustodianAcct']==account_id)]['Transfers out'])
            
        result_columns = ['ParisID','CustodianAcct','CustodianSecurityID','AccountDescription','Beginning MV_diff', 'MV_diff','EXP_diff','FE_diff','TO_diff','TI_diff','CTR_diff','DTR_diff','Commingle Fund']
        
        if os.path.isfile(self.ad_filepath):
            df_ad = BNYChecker(self.client_name).get_asset_detail_data()
            df = pd.merge(df, df_ad, on=['CustodianAcct', 'CustodianSecurityID'], how='left')
            
            for id in df.index:
                account_id = str(df.loc[id,'CustodianAcct'])
                account_security_id = str(df.loc[id,'CustodianSecurityID'])
                if df.loc[id,'Commingle Fund'] == 'Yes':
                    df.loc[id,'Commingle Fund MV_diff'] = df.loc[id,'Base Market Value'] - df.loc[(df['CustodianAcct']==account_id)&(df['CustodianSecurityID']== account_security_id)]['Total Market Value'].values
            result_columns = result_columns + ['Commingle Fund MV_diff']
        else:
            print("The MV of Commingle Fund will not be checked this time since Asset_Detail file is not deposited.")
        
        if os.path.isfile(self.td_filepath):
            df_td = BNYChecker(self.client_name).get_transaction_detail_data()
            df = pd.merge(df, df_td, on=['CustodianAcct', 'CustodianSecurityID'], how='left')
            
            for id in df.index:
                account_id = str(df.loc[id,'CustodianAcct'])
                account_security_id = str(df.loc[id,'CustodianSecurityID'])
                if df.loc[id,'Commingle Fund'] == 'Yes':
                    df.loc[id,'Commingle Fund TI_diff'] = df.loc[id,'Commingle Fund TI'] - df.loc[(df['CustodianAcct']==account_id)&(df['CustodianSecurityID']== account_security_id)]['Transfers in'].values
                    df.loc[id,'Commingle Fund TO_diff'] = df.loc[id,'Commingle Fund TO'] + df.loc[(df['CustodianAcct']==account_id)&(df['CustodianSecurityID']== account_security_id)]['Transfers out'].values
            result_columns = result_columns + ['Commingle Fund TI_diff','Commingle Fund TO_diff']
        else:
            print("The cash flows of Commingle Fund will not be checked this time since Asset_Detail file is not deposited.")        
        
        df = df[result_columns]
        
        return df  
    
    
    def check_and_save_output(self, df):
        try:
            df.set_index('ParisID').to_csv(f'./output/{self.client_name}_result.csv')
            print(df)
        except FileNotFoundError as e:
            print(e)

if __name__ == '__main__':
    
    client_name = 'FSM01'
    bny_checker = BNYChecker(client_name)
    df = bny_checker.bny_checker()
    bny_checker.check_and_save_output(df)