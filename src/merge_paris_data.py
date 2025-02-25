import os
import pandas as pd
import re

class ParisExport:
      """
      Converts ReturnsAudit.xlsx to a DataFrame and matches it with AccountSetupAudit.xlsx for further processing.
            
      Requirements:
            1. ReturnsAudit.xlsx (Paris - Returns Audit)
            2. AccountSetupAudit.xlsx (Paris - Management Reports - Regular updates required)
      """ 
      
      def __init__(self, client_name):
            
            self.client_name = client_name
            self.returnsaudit_filepath = f"./input/{client_name}.xlsx"
            self.accountsetupaudit_filepath = "./input/AccountSetupAudit.xlsx"
            self.useful_columns = ['ParisID', 'Total Market Value', 'Distributions', 'Contributions', 'Transfers out', 'Transfers in', 'Expenses', 'Fees']
            self.float_columns = ['Total Market Value','Prior Market Value','Distributions','Contributions','Transfers out','Transfers in','Expenses','Fees']
            self.str_columns = ['ClientName','GroupName','AccountDescription','Custodian','CustodianAcct','CustodianSecurityID']
            
            # Check if ReturnsAudit file exists
            if not os.path.exists(self.returnsaudit_filepath):
                  raise FileNotFoundError(f"File {self.returnsaudit_filepath} not found.")
            
      # Define a function to extract the number inside the last parentheses and convert it to an integer.
      def extract_parisid(x):
            matches = re.findall(r"\((\d+)\)$", x)  # Match the number inside the last parentheses
            if len(matches) > 0:
                  return int(matches[-1])  # Get the last matched number
            else:
                  return None

      def merge_parisdata(self):

            df = pd.read_excel(self.returnsaudit_filepath, skiprows=4, header=0)

            # Organize Returns Audit data
            df = df.fillna(method='backfill',axis=1)
            df['ParisID'] = df['Plan']
            df = df[self.useful_columns].dropna(axis=0, how='all')
            df['ParisID'] = df['ParisID'].apply(lambda x: int(re.findall(r"\((\d+)\)",x)[0]))
            # df['ParisID'] = df['ParisID'].apply(lambda x:int(str(x)[-9:-1]))
            for col in self.float_columns:
                  df[col] = df[col].astype(str)
                  df[col] = df[col].apply(lambda x: float(x.replace(',','')))
                  
            df_info = pd.read_excel(self.Parisimage_filepath, skiprows=1, header=0)
            df_info = df_info[df_info['AccountType']=='Atomic'][['AccountId','ClientName','GroupName','AccountDescription','Custodian','CustodianAcct','CustodianSecurityID']]
            df_info.rename(columns={"AccountId": 'ParisID'}, inplace=True)
            df_info['ParisID'] = df_info['ParisID'].apply(lambda x: int(x))
            df = pd.merge(df, df_info, on='ParisID')
            df.drop_duplicates(subset=['ParisID'], keep='first', inplace=True)
            
            # Strip whitespace from string columns
            for col in self.str_columns:
                  df[col] = df[col].astype(str)
                  df[col] = df[col].apply(lambda x:str(x.strip()))

            # Check for 'Commingle Fund' based on CustodianAcct
            for num in df.index:
                  if df.loc[num,'CustodianAcct'] == 'nan':
                        pass
                  elif list(df['CustodianAcct']).count(df.loc[num,'CustodianAcct']) > 1:
                        df.loc[num,'Commingle Fund'] = 'Yes'
                  else:
                        df.loc[num,'Commingle Fund'] = 'No'    

            # Supplement security ID with leading zeros
            df['CustodianSecurityID'] = df['CustodianSecurityID'].apply(lambda x: x if x == 'nan' else x.zfill(9))
            
            return df

if __name__ == '__main__':

      client_name = "SBSUSA01"
      df = ParisExport(client_name).merge()
      print(df)
      # df.set_index('ParisID').to_csv(f'./output/{Client}.csv')
