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
            
      # Define a function to extract the number inside the last parentheses and convert it to an integer
      def extract_parisid(x):
            matches = re.findall(r"\((\d+)\)$", x)  # Match the number inside the last parentheses
            if len(matches) > 0:
                  return int(matches[-1])  # Get the last matched number
            else:
                  return None

      def merge_paris_data(self):

            df = pd.read_excel(self.returnsaudit_filepath, skiprows=4, header=0)

            # Organize Returns Audit data
            df = df.fillna(method='backfill',axis=1)
            df['ParisID'] = df['Plan'].apply(MergeParisData.extract_parisid)
            df.rename(columns={'Prior\nMarket Value': 'Prior Market Value'}, inplace=True)
            df = df[self.returnsaudit_useful_columns].dropna(axis=0, how='all') # Keep useful columns and delete rows where all values are na
                  
            df_info = pd.read_excel(self.accountsetupaudit_filepath, skiprows=1, header=0)
            
            # Organize Account Setup Audit data.
            df_info = df_info[self.accountsetupaudit_useful_columns]
            df_info.rename(columns={"AccountId": 'ParisID'}, inplace=True)
            df_info['ParisID'] = df_info['ParisID'].apply(lambda x: int(x))
            
            # Merge two dataframes.
            df = pd.merge(df, df_info, on='ParisID', how='left')
            
            # Organize merge data.
            df.drop_duplicates(subset=['ParisID'], keep='first', inplace=True)
            df = df[(df['AccountType'] == 'Atomic') | (df['AccountType'].isnull())]

      
            # Remove commas from floating columns
            for col in self.float_columns:
                  df[col] = df[col].astype(str)
                  df[col] = df[col].apply(lambda x: float(x.replace(',', '')))
            
            # Strip leading and trailing whitespaces from string columns
            for col in self.str_columns:
                  df[col] = df[col].astype(str).str.strip()
            
            # Supplement security ID with leading zeros
            df['CustodianSecurityID'] = df['CustodianSecurityID'].apply(lambda x: x if x == 'nan' else x.zfill(9)) 

            # Judge the Commingle Fund by CustodianAcct 
            for num in df.index:
                  if df.loc[num,'CustodianAcct'] == 'nan':
                        pass
                  elif list(df['CustodianAcct']).count(df.loc[num,'CustodianAcct']) > 1:
                        df.loc[num,'Commingle Fund'] = 'Yes'
                  else:
                        df.loc[num,'Commingle Fund'] = 'No'    
            
            return df

if __name__ == '__main__':

      client_name = "NEXCOM01"
      df = MergeParisData(client_name).merge_paris_data()
      print(df)
      # df.set_index('ParisID').to_csv(f'./output/{client_name}.csv')
