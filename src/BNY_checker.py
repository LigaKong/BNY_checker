from merge_paris_data import MergeParisData
import pandas as pd

class BNYChecker:
    
    def __init__(self, client_name):
        
        self.client_name = client_name
        self.soc = pd.read_excel(f"./input/Statement_of_Change_in_Net_Assets_{self.client_name}.xls",header=0)
        
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
        self.result_columns = ['ParisID','CustodianAcct','CustodianSecurityID','AccountDescription','Commingle Fund','MV_diff',
                                'EXP_diff','FE_diff','TO_diff','TI_diff','CTR_diff','DTR_diff']

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

    def get_bny_data(self):

        df_soc = self.soc[['Reporting Account Number','End Date', 'Description 1', 'Description 2', 'Description 3', 'Local/Base Value', 'Acctg Status Update (EST)', 'Accounting Status']].copy()
        df_soc['Description'] = df_soc.apply(self.generate_description, axis=1)    
        df_soc['Transaction'] = df_soc['Description'].apply(self.assign_transaction)
        
        print(df_soc['Description'])

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

        df.rename(columns={'Reporting Account Number': 'CustodianAcct'}, inplace=True)
        
        print(df)

        return df

    def bny_check(self):
        # Merge Paris export data and excel data
        Paris_Export = MergeParisData(self.client_name)
        df = Paris_Export.merge_paris_data()
        df_excel = BNYChecker(self.client_name).get_bny_data()
        df = pd.merge(df, df_excel, on='CustodianAcct', how="left")
        
        # Get commingle fund MV and merge to the dataframe
        # df_commingle = self.commingle[['Account Number', 'Cusip Asset Identifier', 'A-AST-MV-BSE']].copy()
        # df_commingle[['Account Number', 'Cusip Asset Identifier']] = df_commingle[['Account Number', 'Cusip Asset Identifier']].astype(str)
        # df_commingle = df_commingle.rename(columns={"Account Number": 'CustodianAcct',"Cusip Asset Identifier": 'CustodianSecurityID'})
        # df_cf = pd.merge(df.loc[(df['Commingle Fund']=='Yes')], df_commingle, on=['CustodianAcct','CustodianSecurityID'], how="left")
        # df = df.drop(df[df['Commingle Fund']=='Yes'].index).append(df_cf).reset_index(drop=True)
        
        # Add Security Receipts and Security Deliveries data in the dataframe
        # for col in ['Account Number', 'N-TRAN-CATG', 'A-MKT-VAL']:
        #     if col not in list(self.fundanddis):
        #         continue
        #         print("Client has no transactions")

        
        # # df_fundanddis = self.fundanddis[['Account Number', 'N-TRAN-CATG', 'A-MKT-VAL']].copy()
        # df_fundanddis = pd.DataFrame(columns = ['Account Number', 'N-TRAN-CATG', 'A-MKT-VAL'])
        # df_fundanddis['Account Number'] = df_fundanddis['Account Number'].astype(str)
        # for id in df.index:
        #     df_sr = df_fundanddis.loc[(df_fundanddis['Account Number']==df.loc[id,'CustodianAcct'])&(df_fundanddis['N-TRAN-CATG']=='Security Receipts')]
        #     df.loc[id,'Security Receipts'] = sum(df_sr['A-MKT-VAL'])
        #     df_sd = df_fundanddis.loc[(df_fundanddis['Account Number']==df.loc[id,'CustodianAcct'])&(df_fundanddis['N-TRAN-CATG']=='Security Deliveries')]
        #     df.loc[id,'Security Deliveries'] = sum(df_sd['A-MKT-VAL'])
        
        # Calculate the difference
        for id in df.index:
            account_id = str(df.loc[id,'CustodianAcct'])
            df.loc[id,'MV_diff'] = df.loc[id,'Ending Balance'] - sum(df.loc[(df['CustodianAcct']==account_id)]['Total Market Value'])
            df.loc[id,'EXP_diff'] = df.loc[id,'EXP'] - sum(df.loc[(df['CustodianAcct']==account_id)]['Expenses'])
            df.loc[id,'FE_diff'] = df.loc[id,'FE'] - sum(df.loc[(df['CustodianAcct']==account_id)]['Fees'])
            df.loc[id,'CTR_diff'] = df.loc[id,'CTR'] - sum(df.loc[(df['CustodianAcct']==account_id)]['Contributions'])
            df.loc[id,'DTR_diff'] = df.loc[id,'DTR'] - sum(df.loc[(df['CustodianAcct']==account_id)]['Distributions'])
            df.loc[id,'TI_diff'] = df.loc[id,'TI'] - sum(df.loc[(df['CustodianAcct']==account_id)]['Transfers in'])
            df.loc[id,'TO_diff'] = df.loc[id,'TO'] - sum(df.loc[(df['CustodianAcct']==account_id)]['Transfers out'])

        # Filter abnormal data and organize results
        # df_nan = df[df[['MV_diff','EXP_diff','FE_diff','Receipts_diff','Disbursements_diff']].isna().any(axis=1)]
        #df.query(self.check_formula, inplace=True)
        # df = df.drop(df[(df['Receipts_diff']-df['Disbursements_diff']<1)&(df['Receipts_diff']-df['Disbursements_diff']>-1)\
        #                 &(df['MV_diff']<1)&(df['MV_diff']>-1)&(df['EXP_diff']<1)&(df['EXP_diff']>-1)&(df['FE_diff']<1)\
        #                 &(df['FE_diff']>-1)].index)
        # df = df.drop(df[(df['Receipts_diff']<1)&(df['Receipts_diff']>-1)&(df['Disbursements_diff']<1)&(df['Disbursements_diff']>-1)\
        #                 &(df['ComMV_diff']<1)&(df['ComMV_diff']>-1)&(df['EXP_diff']<1)&(df['EXP_diff']>-1)&(df['FE_diff']<1)\
        #                 &(df['FE_diff']>-1)].index)
        # df = df.append(df_nan)
        df = df[self.result_columns]
        
        return df  
        

if __name__ == '__main__':
    
    client_name = 'FSM01'
    df = BNYChecker(client_name).bny_check()
    
    print(df)
    df.set_index('ParisID').to_csv(f'./output/{client_name}_result.csv')