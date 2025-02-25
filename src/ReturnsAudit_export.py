import pandas as pd
import re

class PARisExport:
      """
      Export Return Audit from PARis, use this code to convert to a dataframe format and match the corresponding CustodianID that can be used for further data check.

      Two files are needed:
            1. ReturnAudit.xlsx 
            2. PARisImage.xlsx (Need monthly update)  
      """ 
      def __init__(self, filepath):
            
            self.returnaudit_filepath = filepath
            self.parisimage_filepath = "./input/PARisImage.xlsx"
            self.df = pd.read_excel(self.returnaudit_filepath, skiprows=4, header=0)
            self.useful_columns = ['PARisID', 'Total Market Value', 'Distributions', 'Contributions', 'Transfers out', 'Transfers in', 'Expenses', 'Fees']
            self.float_columns = ['Total Market Value','Distributions','Contributions','Transfers out','Transfers in','Expenses','Fees']
            self.str_columns = ['ClientName','GroupName','AccountDescription','Custodian','CustodianAcct','CustodianSecurityID']

      def get_returnaudit(self):

            df = self.df
            df['PARisID'] = df.fillna(method='backfill',axis=1)['Plan']
            df = df[self.useful_columns].dropna(axis=0, how='all')
            df['PARisID'] = df['PARisID'].apply(lambda x: int(re.findall(r"\((\d+)\)",x)[0]))
            # df['PARisID'] = df['PARisID'].apply(lambda x:int(str(x)[-9:-1]))
            for col in self.float_columns:
                  df[col] = df[col].astype(str)
                  df[col] = df[col].apply(lambda x: float(x.replace(',','')))
                  
            df_info = pd.read_excel(self.parisimage_filepath, skiprows=1, header=0)
            df_info = df_info[df_info['AccountType']=='Atomic'][['AccountId','ClientName','GroupName','AccountDescription','Custodian','CustodianAcct','CustodianSecurityID']]
            df_info.rename(columns={"AccountId": 'PARisID'}, inplace=True)
            df_info['PARisID'] = df_info['PARisID'].apply(lambda x: int(x))
            df = pd.merge(df, df_info, on='PARisID')
            df.drop_duplicates(subset=['PARisID'], keep='first', inplace=True)
            for col in self.str_columns:
                  df[col] = df[col].astype(str)
                  df[col] = df[col].apply(lambda x:str(x.strip()))

            # Add "Commingle Fund" column
            for num in df.index:
                  if df.loc[num,'CustodianAcct'] == 'nan':
                        pass
                  elif list(df['CustodianAcct']).count(df.loc[num,'CustodianAcct']) > 1:
                        df.loc[num,'Commingle Fund'] = 'Yes'
                  else:
                        df.loc[num,'Commingle Fund'] = 'No'    

            # Supplement security ID with 0
            df['CustodianSecurityID'] = df['CustodianSecurityID'].apply(lambda x: x if x == 'nan' else x.zfill(9))
            
            return df

if __name__ == '__main__':

      Client = "Dallas07"
      PARisExport = PARisExport(f"./input/{Client}.xlsx")
      df = PARisExport.get_returnaudit()
      print(df)
      # df.set_index('PARisID').to_csv(f'./output/{Client}.csv')
