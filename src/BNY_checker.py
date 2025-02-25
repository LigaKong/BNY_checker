from PARis_Export import PARisExport
import pandas as pd

class BNYCheck:
    
    def __init__(self,client_name):
        
        self.client_name = client_name
        self.changeinna = pd.read_excel(f"./input/Statement_of_Change_in_Net_Assets_{self.client_name}.xls",header=0)
        
        self.ctr = ['RECEIPTS:_CONTRIBUTIONS:_COMPANY','RECEIPTS:_CONTRIBUTIONS:_EMPLOYEE','RECEIPTS:_MISCELLANEOUS RECEIPTS',
                    'RECEIPTS:_RECEIVED FROM PLAN ACCOUNTS','RECEIPTS:_RECEIVED FROM PLAN ADMINISTRATOR',
                    'RECEIPTS:_RECEIVED FROM PARTICIPATING ACCOUNTS']
        self.dtr = ['DISBURSEMENTS:_DISTRIBUTION OF BENEFITS:_ANNTY RETIRMNTS/WITHDRWLS','DISBURSEMENTS:_DISTRIBUTION OF BENEFITS:_MEDICAL EXPENSE',
                    'DISBURSEMENTS:_DISTRIBUTION OF BENEFITS:_PAYMENTS - BENEFITS AND SERVICES','DISBURSEMENTS:_DISTRIBUTION OF BENEFITS:_PAYMENTS TO PARTICIPANTS',
                    'DISBURSEMENTS:_DISTRIBUTION OF CASH','DISBURSEMENTS:_DISTRIBUTION TO OTHER BANKS',
                    'DISBURSEMENTS:_DISTRIBUTION TO PLAN ACCOUNTS','DISBURSEMENTS:_DISTRIBUTION TO PLAN ADMINISTRATOR',
                    'DISBURSEMENTS:_PAYMENTS TO INSURANCE CARRIERS:_PREMIUMS','DISBURSEMENTS:_DISBURSED TO PARTICIPATING ACCOUNTS']
        self.ti = ['RECEIPTS:_DIRECT ROLLOVER TRANSFER IN','RECEIPTS:_PARTICIPANT TRANSFER IN',
                    'RECEIPTS:_TRANSFERS IN:_CASH','RECEIPTS:_TRANSFERS IN:_INCOME CASH']
        self.to = ['DISBURSEMENTS:_DIRECT ROLLOVER TRANSFER OUT','DISBURSEMENTS:_PARTICIPANT TRANSFER OUT',
                    'DISBURSEMENTS:_TRANSFERS OUT:_CASH','DISBURSEMENTS:_TRANSFERS OUT:_INCOME CASH']
        self.fe = ['DISBURSEMENTS:_ADMINISTRATIVE EXPENSES:_INVESTMENT ADVISORY FEES','DISBURSEMENTS:_ADMINISTRATIVE EXPENSES:_INVESTMENT MANAGEMENT',
                    'DISBURSEMENTS:_ADMINISTRATIVE EXPENSES:_SERVICE FEES','RECEIPTS:_MT ALL INVESTMENT MANAGER FEES','DISBURSEMENTS:_ADMINISTRATIVE EXPENSES:_MANAGEMENT FEE - COMMITMENT']
        self.exp = ['RECEIPTS:_MASTER TRUST ALLOCATED EXPENSES','RECEIPTS:_MASTER TRUST CONSULTING FEES',
                    'RECEIPTS:_MASTER TRUST SEC LENDING REBATE','RECEIPTS:_MASTER TRUST STOCK LOAN FEES',
                    'RECEIPTS:_MT ALL TRUST/CUSTODIAN FEES']
        self.ignore = ['DISBURSEMENTS:_ADMINISTRATIVE EXPENSES:_COMMISSION ON FUTURES CONTRACTS','DISBURSEMENTS:_ADMINISTRATIVE EXPENSES:_INTEREST EXPENSE']
        self.result_columns = ['PARisID','CustodianAcct','CustodianSecurityID','AccountDescription','Commingle Fund','MV_diff',
                                'EXP_diff','FE_diff','TO_diff','TI_diff','CTR_diff','DTR_diff']
        #self.check_formula = 'MV_diff>1|MV_diff<-1|EXP_diff>1|EXP_diff<-1|FE_diff>1|FE_diff<-1|\CTR_diff>1|CTR_diff<-1|DTR_diff>1|DTR_diff<-1|TI_diff>1|TI_diff<-1|TO_diff>1|TO_diff<-1'

    def get_excel_data(self):

        df_changeinna = self.changeinna[['Reporting Account Number', 'Description 1', 'Description 2', 'Description 3', 'Local/Base Value']].copy()
        for id in df_changeinna.index:
            if len(str(df_changeinna.loc[id,'Description 3'])) != 3:
                df_changeinna.loc[id,'Description'] = str(df_changeinna.loc[id,'Description 1']) + '_' + str(df_changeinna.loc[id,'Description 2']) + '_' + str(df_changeinna.loc[id,'Description 3'])
            elif len(str(df_changeinna.loc[id,'Description 2'])) != 3:
                df_changeinna.loc[id,'Description'] = str(df_changeinna.loc[id,'Description 1']) + '_' + str(df_changeinna.loc[id,'Description 2'])
            else:
                df_changeinna.loc[id,'Description'] = str(df_changeinna.loc[id,'Description 1'])
        for id in df_changeinna.index:
            if df_changeinna.loc[id,'Description'] in self.ctr:
                df_changeinna.loc[id,'Transaction'] = 'CTR'
            elif df_changeinna.loc[id,'Description'] in self.dtr:
                df_changeinna.loc[id,'Transaction'] = 'DTR'
            elif df_changeinna.loc[id,'Description'] in self.ti:
                df_changeinna.loc[id,'Transaction'] = 'TI'
            elif df_changeinna.loc[id,'Description'] in self.to:
                df_changeinna.loc[id,'Transaction'] = 'TO'
            elif df_changeinna.loc[id,'Description'] in self.fe:
                df_changeinna.loc[id,'Transaction'] = 'FE'
            elif df_changeinna.loc[id,'Description'] in self.exp:
                df_changeinna.loc[id,'Transaction'] = 'EXP'
            elif df_changeinna.loc[id,'Description'] in self.ignore:
                continue
            elif 'DISBURSEMENTS:_ADMINISTRATIVE EXPENSES:' in df_changeinna.loc[id,'Description']:
                df_changeinna.loc[id,'Transaction'] = 'EXP'
                
        df=pd.DataFrame(set(df_changeinna['Reporting Account Number']), columns=['CustodianAcct'])
        df.set_index('CustodianAcct',inplace=True)
        for id in df.index:
            bv = sum(df_changeinna.loc[(df_changeinna['Reporting Account Number']==id)&(df_changeinna['Description'] == 'NET ASSETS - BEGINNING OF PERIOD')]['Local/Base Value'])
            pls = sum(df_changeinna.loc[(df_changeinna['Reporting Account Number']==id)&(df_changeinna['Description 1'] == 'RECEIPTS:')]['Local/Base Value'])
            mis = sum(df_changeinna.loc[(df_changeinna['Reporting Account Number']==id)&(df_changeinna['Description 1'] == 'DISBURSEMENTS:')]['Local/Base Value'])
            df.loc[id,'Ending Balance']= bv + pls - mis
            df_filter = df_changeinna.loc[df_changeinna['Reporting Account Number']==id]
            df.loc[id,'CTR']=sum(df_filter.loc[df_filter['Transaction']=='CTR']['Local/Base Value'])
            df.loc[id,'DTR']=sum(df_filter.loc[df_filter['Transaction']=='DTR']['Local/Base Value'])
            df.loc[id,'TI']=sum(df_filter.loc[df_filter['Transaction']=='TI']['Local/Base Value'])
            df.loc[id,'TO']=sum(df_filter.loc[df_filter['Transaction']=='TO']['Local/Base Value'])
            df.loc[id,'EXP']=sum(df_filter.loc[df_filter['Transaction']=='EXP']['Local/Base Value'])
            df.loc[id,'FE']=sum(df_filter.loc[df_filter['Transaction']=='FE']['Local/Base Value'])

        return df

    def bny_check(self):
        # Merge PARis export data and excel data
        PARis_Export = PARisExport(f"./input/{self.client_name}.xlsx")
        df = PARis_Export.get_returnaudit()
        df_excel = BNYCheck(self.client_name).get_excel_data()
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
    
    client_name = 'SBSUSA01'
    df = BNYCheck(client_name).bny_check()
    
    print(df)
    df.set_index('PARisID').to_csv(f'./output/{client_name}_result.csv')