import os
import pandas as pd
import re

class MergeParisData:
      """
      Converts ReturnsAudit.xlsx to a DataFrame and matches it with AccountSetupAudit.xlsx for further processing.
            
      Requirements:
            1. ReturnsAudit.xlsx (Paris - Returns Audit)
            2. AccountSetupAudit.xlsx (Paris - Management Reports - Regular updates required)
      """ 
      
      def __init__(self, client_name):
            
            self.client_name = client_name
            self.returnsaudit_filepath = f"./input/{client_name}.xlsx"
            self.returnsaudit_useful_columns = ['ParisID', 'Total Market Value', 'Prior Market Value', 'Distributions', 'Contributions', 'Transfers out', 'Transfers in', 'Expenses', 'Fees']
            self.accountsetupaudit_filepath = "./input/AccountSetupAudit.xlsx"
            self.accountsetupaudit_useful_columns = ['AccountId','ClientName','GroupName','AccountDescription','Custodian','CustodianAcct','CustodianSecurityID','AccountType']
            self.float_columns = ['Total Market Value','Prior Market Value','Distributions','Contributions','Transfers out','Transfers in','Expenses','Fees']
            self.str_columns = ['ClientName','GroupName','AccountDescription','Custodian','CustodianAcct','CustodianSecurityID','AccountType']
            
            # Check if ReturnsAudit file exists
            if not os.path.exists(self.returnsaudit_filepath):
                  raise FileNotFoundError(f"File {self.returnsaudit_filepath} not found.")
            
      def extract_parisid(self, x):
            matches = re.findall(r"\((\d+)\)$", str(x))
            return int(matches[-1]) if matches else None
      
      def process_returns_audit(self, df):
            df['ParisID'] = df['Plan'].apply(self.extract_parisid)
            df.rename(columns={'Prior\nMarket Value': 'Prior Market Value'}, inplace=True)
            df = df[self.returnsaudit_useful_columns].dropna(axis=0, how='all') # Keep useful columns and delete rows where all values are na
            return df

      def process_account_setup_audit(self, df_info):
            df_info = df_info[self.accountsetupaudit_useful_columns].copy()
            df_info.rename(columns={"AccountId": 'ParisID'}, inplace=True)
            df_info['ParisID'] = df_info['ParisID'].astype(int)
            return df_info
      
      def merge_paris_data(self):

            df = pd.read_excel(self.returnsaudit_filepath, skiprows=4, header=0).fillna(method='backfill', axis=1)
            df = self.process_returns_audit(df)
            
            df_info = pd.read_excel(self.accountsetupaudit_filepath, skiprows=1, header=0)
            df_info = self.process_account_setup_audit(df_info)
            
            df = pd.merge(df, df_info, on='ParisID', how='left').drop_duplicates(subset=['ParisID'], keep='first')
            df = df[(df['AccountType'] == 'Atomic') | (df['AccountType'].isnull())]
            df[['CustodianAcct', 'CustodianSecurityID']] = df[['CustodianAcct', 'CustodianSecurityID']].apply(lambda x: x.str.upper())
      
            # Remove commas from floating columns
            for col in self.float_columns:
                  df[col] = df[col].astype(str).apply(lambda x: float(x.replace(',', '')))
            
            # Strip leading and trailing whitespaces from string columns
            for col in self.str_columns:
                  df[col] = df[col].astype(str).str.strip()
            
            # Supplement security ID with leading zeros
            df['CustodianSecurityID'] = df['CustodianSecurityID'].apply(lambda x: x if x == 'nan' else x.zfill(9)) 

            # Judge the Commingle Fund by CustodianAcct 
            df['Commingle Fund'] = df['CustodianAcct'].apply(lambda x: 'Yes' if df['CustodianAcct'].eq(x).sum() > 1 else 'No' if x != 'nan' else '')  
            
            return df

if __name__ == '__main__':

      client_name = "FSM01"
      try:
            df = MergeParisData(client_name).merge_paris_data()
            print(df)
            # df.set_index('ParisID').to_csv(f'./output/{client_name}.csv')
      except FileNotFoundError as e:
            print(e)
