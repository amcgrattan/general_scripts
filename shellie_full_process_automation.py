import pandas as pd
import numpy as np
import re
import pyodbc
import os
from openpyxl import load_workbook
from datetime import datetime
import cx_Oracle

cwd = os.getcwd()
print(cwd)

server = 'USIDCVSQLUS03.ctl.intranet,7114' 
database = 'MAUS' 
username = 'MAAPPLSA' 
password = 'irwuB1pucLRBG9tO_2025' #'irwuB1pucLRBG9tO'
cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

server_0043 = 'USIDCVSQL0043' 
database_0043 = 'FA_Margin' 
username_0043 = 'SSRSAccount' 
password_0043 = '$$R$Account@2025-000' #'$$R$Account@2024'
cnxn_0043 = pyodbc.connect('DRIVER={SQL Server};SERVER='+server_0043+';DATABASE='+database_0043+';UID='+username_0043+';PWD='+ password_0043)
cursor_0043 = cnxn_0043.cursor()

#lib_dir = r"C:\Oracle\Ora12_2_0_1\bin"
lib_dir = r"C:\Oracle\Ora12_2_0_1_x64\bin"
cx_Oracle.init_oracle_client(lib_dir=lib_dir)

dsn_tns_kenan = cx_Oracle.makedsn('RACORAP28-SCAN.CORP.INTRANET', '1521', service_name='KEN01P') # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
oconn_kenan = cx_Oracle.connect(user=r'MAAPPLSA', password='''M@u5(ra+2024-111''', dsn=dsn_tns_kenan)

#------------to change: output file name, input file name, most recent run of T_STG_NETEX_AIM_202504 in step 1--------------------#

output_file_name = 'june_offnet_tdm_run_v5.xlsx'
#"april_offnet_tdm_report_4.xlsx"

input_file_name = 'Copy of 20250617_june_tdm_ecckt_base_for_rerates.xlsx' #'Copy of 20250520_may_tdm_ecckt_base_for_rerates.xlsx'
input_file_sheet_name = 'Jun_TDM_ECCKT_Base_and_Cost_Bas'

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------#

def make_cuts(piid_list): 
    start = 0
    end = len(piid_list)  
    step = 1000
    cuts = [] #will have a bunch of groups of 100 with some left over 

    for i in range(start, end, step):
        x = i 
        cuts.append(piid_list[x:x+step])
    
    return cuts

def get_check_date(): 
    
    today = datetime.today() #.strftime('%Y-%m-%d')
    check_month = today.month + 3
    check_year = today.year
    
    if check_month > 12:
        check_year = today.year + 1
        check_month = check_month - 12
        
    check_date = datetime(check_year, check_month, 1)
    return check_date

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------#

#step 0: pull the mars and marc data per piid
def pull_og_data(input_ecckts):
    
    og_df = pd.DataFrame() 
    chunks = make_cuts(input_ecckts)
    
    for c in chunks:
        
        ps = '(' + "'" + "','".join(str(x) for x in c) + "'" + ')'
    
        step_1_query = pd.read_sql_query('''
                                      SELECT  m.ECCKT_AML_ID, m.RECORD_ID, m.SAP_ACCOUNT_CD, s.GL_ACCOUNT_DESC, s.GL_ACCOUNT_ESS_TIER3_DESC, m.ECCKT_STRIPPED_NAME, m.ECCKT_NETEX, 
m.REV_PRODUCT_COMPNT_ID, m.REV_MRR, m.SOLUTION_ID_PRIME, m.DNT_EXCLUSION_LEVEL, m.DNT_REASON_CODE, m.LAST_PROJECT, m.LAST_RERATE_DATE, m.JRNL_YR_MNTH_NBR, 
m.BUS_ORG_ID, m.BUS_ORG_NAME, m.ULT_BUS_ORG_ID, m.ULT_BUS_ORG_NAME, m.BAN, FINANCE_ACCOUNT_NBR, m.BILL_SRC_SYS_CD,
m.BILLED_IND, m.GOV_IND, m.REV_REGION, m.OCC_REGION, marc.PRODUCT_CONCAT, marc.BUSINESS_UNIT, marc.REV_REGION_CONCAT, marc.IN_TERM_TDM_RR, marc.CUSTOMER_TERM_END_DT, marc.rev_mrr as MARC_REV_MRR, marc.OCC_REGION_CONCAT
FROM    [EC].[MARGIN_ANALYTICS_REPORTING_SERVICE] m
left join [STAGE].[CODS_FINANCE_GL_SEG5_ACCOUNT] s on s.GL_ACCOUNT_CD = m.SAP_ACCOUNT_CD
left join [EC].[MARGIN_ANALYTICS_REPORTING_CIRCUIT] marc on m.SOLUTION_ID_PRIME = marc.SOLUTION_ID_PRIME
where ECCKT_STRIPPED_NAME in {0}   
                                      '''.format(ps), cnxn)
    
        raw_df = pd.DataFrame(step_1_query)
        og_df = pd.concat([og_df, raw_df], ignore_index=True)
    
    #save results to excel file
    og_df.to_excel(output_file_name, sheet_name='Data', index=False)
    #fill all nan with '' for consistency
    og_df = og_df.fillna('')

    return og_df

#print(raw_df.head(10))

#start options
input_df = pd.read_excel(input_file_name, sheet_name=input_file_sheet_name)
input_ecckts = input_df['ECCKT_STRIPPED_NAME'].unique().tolist()
mars_marc_cods_df = pull_og_data(input_ecckts)

#since not all ecckts are returned/found in mars/marc/cods, need to join on the input ecckt df
input_df.rename(columns={'ECCKT_AML_ID': 'input_ECCKT_AML_ID', 'ECCKT_STRIPPED_NAME': 'input_ECCKT_STRIPPED_NAME'}, inplace=True)
df = pd.merge(left = input_df, right = mars_marc_cods_df, left_on='input_ECCKT_STRIPPED_NAME', right_on='ECCKT_STRIPPED_NAME', how='left')


#these columns will need to be rolled up and finalized at piid level but keep as just ecckt level for now
#need all piids to have the same high level status and if there's a status that makes one of the ecckts on a piid actionable but another ecckt is not actionable, call the piid actionable
df['NonActionable Reason'] = ''
df['Actionable/NonActionable'] = ''

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------#

#step 1: ECCKT does not exist in (month) AIM: NonActionable
#Check does STRIPPED_EC_CIRCUIT_ID (ECCKT) exist in T_STG_NETEX_AIM_202504 (date for most recent month)

#step 2: Expense Not Active in (month) AIM: NonActionable
#Check does STRIPPED_EC_CIRCUIT_ID (ECCKT) show Active_Ind = N in T_STG_NETEX_AIM_202504 (date for most recent month)

#step 3: Not Lumen Expense (DW Secure Company <> 1): NonActionable
#Check does STRIPPED_EC_CIRCUIT_ID (ECCKT) show DW_SECURE_COMPANY_NBR <> 1 in T_STG_NETEX_AIM_202504 (date for most recent month)

#important: the name of this table has to be updated with the most recent run
#ex: running in May so most recent data is from 202504

#this only applies to ecckts that were not found in mars in the first step
def check_ecckt_existance_and_active_status(ecckts): 
    
    big_df = pd.DataFrame() 
    chunks = make_cuts(ecckts)
    
    for c in chunks:
        
        ps = '(' + "'" + "','".join(str(x) for x in c) + "'" + ')'
    
        query = pd.read_sql_query('''select distinct STRIPPED_EC_CIRCUIT_ID, Active_Ind, DW_SECURE_COMPANY_NBR from dbo.T_STG_NETEX_AIM_202505
where STRIPPED_EC_CIRCUIT_ID in {0}'''.format(ps), cnxn)
    
        lil_df = pd.DataFrame(query)
        big_df = pd.concat([big_df, lil_df], ignore_index=True)
    
    with pd.ExcelWriter(output_file_name, mode='a') as writer:  
        big_df.to_excel(writer, sheet_name='T_STG_NETEX_AIM_Data', index=False)

    #fill all nan with '' for consistency
    big_df = big_df.fillna('')

    return big_df


mars_marc_cods_ecckts = mars_marc_cods_df['ECCKT_STRIPPED_NAME'].unique().tolist()
not_found_in_mars_ecckts = [ecckt for ecckt in input_ecckts if ecckt not in mars_marc_cods_ecckts]

#ecckts = df['input_ECCKT_STRIPPED_NAME'].unique().tolist()
t_stg_netex_aim_df = check_ecckt_existance_and_active_status(not_found_in_mars_ecckts)

#do another vlookup on comp grp val to latis active to see if anything returns
not_found_in_mars_subset = df.loc[(df['ECCKT_STRIPPED_NAME'] == '') | (pd.isnull(df['ECCKT_STRIPPED_NAME']) == True)]

not_found_in_mars_subset = pd.merge(left = not_found_in_mars_subset, right = t_stg_netex_aim_df, left_on='input_ECCKT_STRIPPED_NAME', right_on='STRIPPED_EC_CIRCUIT_ID', how='left')

#step 1, 2, 3
for index, row in not_found_in_mars_subset.iterrows():
    
    if (row['STRIPPED_EC_CIRCUIT_ID'] == None) | (row['STRIPPED_EC_CIRCUIT_ID'] == ''): 
        
        not_found_in_mars_subset['NonActionable Reason'] = np.where(not_found_in_mars_subset['input_ECCKT_STRIPPED_NAME'] == row['input_ECCKT_STRIPPED_NAME'], "ECCKT does not exist in (month) AIM", not_found_in_mars_subset['NonActionable Reason'])
        not_found_in_mars_subset['Actionable/NonActionable'] = np.where(not_found_in_mars_subset['input_ECCKT_STRIPPED_NAME'] == row['input_ECCKT_STRIPPED_NAME'], "NonActionable", not_found_in_mars_subset['Actionable/NonActionable'])
        
        df['NonActionable Reason'] = np.where(df['input_ECCKT_STRIPPED_NAME'] == row['input_ECCKT_STRIPPED_NAME'], "ECCKT does not exist in (month) AIM", df['NonActionable Reason'])
        df['Actionable/NonActionable'] = np.where(df['input_ECCKT_STRIPPED_NAME'] == row['input_ECCKT_STRIPPED_NAME'], "NonActionable", df['Actionable/NonActionable'])
        
    if ( (row['NonActionable Reason'] == None) | (row['NonActionable Reason'] == '') ) & (row['STRIPPED_EC_CIRCUIT_ID'] != '') & (pd.isnull(row['STRIPPED_EC_CIRCUIT_ID']) == False) & (row['Active_Ind'] == 'N'): 
        
        not_found_in_mars_subset['NonActionable Reason'] = np.where(not_found_in_mars_subset['input_ECCKT_STRIPPED_NAME'] == row['input_ECCKT_STRIPPED_NAME'], "Expense Not Active in (month) AIM", not_found_in_mars_subset['NonActionable Reason'])
        not_found_in_mars_subset['Actionable/NonActionable'] = np.where(not_found_in_mars_subset['input_ECCKT_STRIPPED_NAME'] == row['input_ECCKT_STRIPPED_NAME'], "NonActionable", not_found_in_mars_subset['Actionable/NonActionable'])
        
        df['NonActionable Reason'] = np.where(df['input_ECCKT_STRIPPED_NAME'] == row['input_ECCKT_STRIPPED_NAME'], "Expense Not Active in (month) AIM", df['NonActionable Reason'])
        df['Actionable/NonActionable'] = np.where(df['input_ECCKT_STRIPPED_NAME'] == row['input_ECCKT_STRIPPED_NAME'], "NonActionable", df['Actionable/NonActionable'])

    if ( (row['NonActionable Reason'] == None) | (row['NonActionable Reason'] == '') ) & (row['STRIPPED_EC_CIRCUIT_ID'] != '') & (pd.isnull(row['STRIPPED_EC_CIRCUIT_ID']) == False) & (row['DW_SECURE_COMPANY_NBR'] != 1): 
        
        not_found_in_mars_subset['NonActionable Reason'] = np.where(not_found_in_mars_subset['input_ECCKT_STRIPPED_NAME'] == row['input_ECCKT_STRIPPED_NAME'], "Not Lumen Expense (DW Secure Company <> 1)", not_found_in_mars_subset['NonActionable Reason'])
        not_found_in_mars_subset['Actionable/NonActionable'] = np.where(not_found_in_mars_subset['input_ECCKT_STRIPPED_NAME'] == row['input_ECCKT_STRIPPED_NAME'], "NonActionable", not_found_in_mars_subset['Actionable/NonActionable'])
        
        df['NonActionable Reason'] = np.where(df['input_ECCKT_STRIPPED_NAME'] == row['input_ECCKT_STRIPPED_NAME'], "Not Lumen Expense (DW Secure Company <> 1)", df['NonActionable Reason'])
        df['Actionable/NonActionable'] = np.where(df['input_ECCKT_STRIPPED_NAME'] == row['input_ECCKT_STRIPPED_NAME'], "NonActionable", df['Actionable/NonActionable'])



# null_latis_active_subset = pd.merge(left = null_latis_active_subset, right = latis_active_df, left_on='component_grp_val_id', right_on='COMPONENT_GRP_VAL', how='left')
# null_latis_active_subset['latis active'] = null_latis_active_subset['COMPONENT_GRP_VAL']
# #drop the columns of latis_active_df
# null_latis_active_subset = null_latis_active_subset.iloc[:, :-latis_active_df.shape[1]]

# for index, row in not_found_in_mars_subset.iterrows():
        
#     if (row['latis active'] != '') & (pd.isnull(row['latis active']) == False):
        
#         full_df['latis active'] = np.where( (full_df['serv_comp_id'] == row['serv_comp_id']) & ((full_df['latis active'] == '') | (pd.isnull(full_df['latis active']) == True)), row['latis active'], full_df['latis active'])


# df = pd.merge(left = df, right = t_stg_netex_aim_df, left_on='input_ECCKT_STRIPPED_NAME', right_on='STRIPPED_EC_CIRCUIT_ID', how='left')

# #step 1
# df['NonActionable Reason'] = np.where( df["STRIPPED_EC_CIRCUIT_ID"].isnull() | (df["STRIPPED_EC_CIRCUIT_ID"] == ''), "ECCKT does not exist in (month) AIM", df['NonActionable Reason'] ) 
# df['Actionable/NonActionable'] = np.where( df["STRIPPED_EC_CIRCUIT_ID"].isnull() | (df["STRIPPED_EC_CIRCUIT_ID"] == ''), "NonActionable", df['Actionable/NonActionable'] )

# #step 2
# df['NonActionable Reason'] = np.where( ( df["NonActionable Reason"].isnull() | (df["NonActionable Reason"] == '') ) & (df['STRIPPED_EC_CIRCUIT_ID'] != '') 
#                                       & (pd.isnull(df['STRIPPED_EC_CIRCUIT_ID']) == False) & (df['Active_Ind'] == 'N'), "Expense Not Active in (month) AIM", df['NonActionable Reason'] ) 
# df['Actionable/NonActionable'] = np.where( ( df["Actionable/NonActionable"].isnull() | (df["Actionable/NonActionable"] == '') ) & (df['STRIPPED_EC_CIRCUIT_ID'] != '') & (pd.isnull(df['STRIPPED_EC_CIRCUIT_ID']) == False) & (df['Active_Ind'] == 'N'), 
#                                       "NonActionable", df['Actionable/NonActionable'] ) 

# #step 3
# df['NonActionable Reason'] = np.where( ( df["NonActionable Reason"].isnull() | (df["NonActionable Reason"] == '') ) & 
#                                       (df['STRIPPED_EC_CIRCUIT_ID'] != '') & (pd.isnull(df['STRIPPED_EC_CIRCUIT_ID']) == False) & (df['DW_SECURE_COMPANY_NBR'] != 1), 
#                                       "Not Lumen Expense (DW Secure Company <> 1)", df['NonActionable Reason'] ) 
# df['Actionable/NonActionable'] = np.where( ( df["Actionable/NonActionable"].isnull() | (df["Actionable/NonActionable"] == '') ) & 
#                                           (df['STRIPPED_EC_CIRCUIT_ID'] != '') & (pd.isnull(df['STRIPPED_EC_CIRCUIT_ID']) == False) & (df['DW_SECURE_COMPANY_NBR'] != 1), 
#                                       "NonActionable", df['Actionable/NonActionable'] ) 

# #cut off STRIPPED_EC_CIRCUIT_ID and Active_Ind --> saved for ref on the T_STG_NETEX_AIM_Data sheet
# df = df.iloc[:, :-t_stg_netex_aim_df.shape[1]]

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------#

print("here")


#step 4: Not Direct Netex (Fixed or Opex): NonActionable
#Check SAP_ACCOUNT_CD in MARS for ECCKT in under GL_ACCOUNT_CD in [STAGE].[CODS_FINANCE_GL_SEG5_ACCOUNT], and if GL_ACCOUNT_ESS_TIER3_DESC <> Direct Netex, then put in this category if Fixed Netex or if GL_ACCOUNT_DESC like OPEX.

df['NonActionable Reason'] = np.where( ( df["NonActionable Reason"].isnull() | (df["NonActionable Reason"] == '') ) & (df['GL_ACCOUNT_ESS_TIER3_DESC'] != 'Direct Netex') 
                                      & ( (df['GL_ACCOUNT_ESS_TIER3_DESC'] == 'Fixed Netex') | (df["GL_ACCOUNT_DESC"].str.contains('OPEX', regex=False))  ),
                                      "Not Direct Netex (Fixed or Opex)", df['NonActionable Reason'] ) 

df['Actionable/NonActionable'] = np.where( ( df["Actionable/NonActionable"].isnull() | (df["Actionable/NonActionable"] == '') ) & (df['GL_ACCOUNT_ESS_TIER3_DESC'] != 'Direct Netex') 
                                      & ( (df['GL_ACCOUNT_ESS_TIER3_DESC'] == 'Fixed Netex') | (df["GL_ACCOUNT_DESC"].str.contains('OPEX', regex=False))  ), 
                                      "NonActionable", df['Actionable/NonActionable'] ) 

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------#

#step 5: Disco Shelf Life or Held for Term: NonActionable
#Check If GL_ACCOUNT_DESC like Disco Shelf or Held for Term and Rev MRR is NULL

df['NonActionable Reason'] = np.where( ( df["NonActionable Reason"].isnull() | (df["NonActionable Reason"] == '') ) & ( (df['REV_MRR'] == '') | (pd.isnull(df['REV_MRR']) == True) ) 
                                      & ( (df["GL_ACCOUNT_DESC"].str.contains('Disco Shelf', regex=False))  | (df["GL_ACCOUNT_DESC"].str.contains('Held for Term', regex=False))  ),
                                      "Disco Shelf Life or Held for Term", df['NonActionable Reason'] ) 
df['Actionable/NonActionable'] = np.where( ( df["Actionable/NonActionable"].isnull() | (df["Actionable/NonActionable"] == '') ) & ( (df['REV_MRR'] == '') | (pd.isnull(df['REV_MRR']) == True) ) 
                                      & ( (df["GL_ACCOUNT_DESC"].str.contains('Disco Shelf', regex=False))  | (df["GL_ACCOUNT_DESC"].str.contains('Held for Term', regex=False))  ), 
                                      "NonActionable", df['Actionable/NonActionable'] ) 

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------#

#step 6: Cost is matched to Component/Service which does not bill/invoice: NonActionable
#If Rev MRR is NULL and none of the above apply.

df['NonActionable Reason'] = np.where( ( df["NonActionable Reason"].isnull() | (df["NonActionable Reason"] == '') ) & ( (df['REV_MRR'] == '') | (pd.isnull(df['REV_MRR']) == True) ),
                                      "Cost is matched to Component/Service which does not bill/invoice", df['NonActionable Reason'] ) 
df['Actionable/NonActionable'] = np.where( ( df["Actionable/NonActionable"].isnull() | (df["Actionable/NonActionable"] == '') ) & ( (df['REV_MRR'] == '') | (pd.isnull(df['REV_MRR']) == True) ), 
                                      "NonActionable", df['Actionable/NonActionable'] ) 

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------#

#step 7: Cost not matched in Tail Matching: NonActionable
#If Rev MRR is NULL and Rev Product Component ID is UNMATCHED or UNKNOWN and none of the above apply.

df['NonActionable Reason'] = np.where( ( df["NonActionable Reason"].isnull() | (df["NonActionable Reason"] == '') ) & ( (df['REV_MRR'] == '') | (pd.isnull(df['REV_MRR']) == True) )
                                      & ( (df["REV_PRODUCT_COMPNT_ID"].str.contains('UNMATCHED', regex=False))  | (df["REV_PRODUCT_COMPNT_ID"].str.contains('UNKNOWN', regex=False)) ),
                                      "Cost is matched to Component/Service which does not bill/invoice", df['NonActionable Reason'] ) 
df['Actionable/NonActionable'] = np.where( (df["NonActionable Reason"] == 'Cost is matched to Component/Service which does not bill/invoice') & ( (df['REV_MRR'] == '') | (pd.isnull(df['REV_MRR']) == True) )
                                      & ( (df["REV_PRODUCT_COMPNT_ID"].str.contains('UNMATCHED', regex=False))  | (df["REV_PRODUCT_COMPNT_ID"].str.contains('UNKNOWN', regex=False)) ), 
                                      "NonActionable", df['Actionable/NonActionable'] ) 

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------#

#step 8: Revenue Disconnect: NonActionable
#If all Rev Product Component IDs found in MARS for the Solution ID Prime have a bill stop date in Kenan.  Add max bill stop date as Disco Date.

def get_kenan_billing_inactive_date(df_extra):

    #need to chunk up the requests because there's a limit to how many can be sent through the query
    piids = df_extra['SOLUTION_ID_PRIME'].to_list()
    scids = df_extra['REV_PRODUCT_COMPNT_ID'].to_list()
    
    #want chunk size of nearly 1000 without breaking up a sub case's primary ids 
    
    chunk_len = 1000
    chunks = []
    num_piids = len(piids)
    df_grouped = df_extra.groupby('SOLUTION_ID_PRIME')
    
    chunk = pd.DataFrame()
    count = 1
    for piid, piid_df in df_grouped:
        ps = piid_df['REV_PRODUCT_COMPNT_ID'].to_list()
        if (len(chunk) + len(ps)) <= chunk_len:
            chunk = pd.concat([chunk, piid_df], ignore_index=True)
        else:
            chunks.append(chunk)
            new_chunk = piid_df
            chunk = new_chunk
            if count == num_piids:
                chunks.append(chunk)
        count += 1
        

    kenan_prod_df = pd.DataFrame() 
    iter_count = 1
    total = len(chunks)
    
    for c in chunks: 
        
        print(iter_count, "/", total)
        iter_count += 1
        
        s = c['REV_PRODUCT_COMPNT_ID'].unique().tolist()        

        ps = '(' + "'" + "','".join(str(x) for x in s) + "'" + ')'

        kenan_query = pd.read_sql_query('''SELECT distinct
                        eieml.subscr_no,
                        cm.account_type,
                        CIAM.EXTERNAL_ID AS BUS_ORG_ID,
                       eiam.external_id as BAN,
                       eiam150.external_id as Invoice_BAN,
                       CM.ACCOUNT_NO,
                       CM.BILL_COMPANY,
                       PDV.DISPLAY_VALUE AS PACKAGE,
                       CDV.DISPLAY_VALUE AS COMPONENT,
                       s.EMF_CONFIG_ID,
                       emfv.DISPLAY_VALUE as EMF_Config_Value,
                       P.PRODUCT_ACTIVE_DT,
                       P.PRODUCT_INACTIVE_DT,
                       S.SERVICE_ACTIVE_DT,
                       S.SERVICE_INACTIVE_DT,
                       P.billing_active_dt, 
                       p.billing_inactive_dt,
                       eiem1.inactive_date e1_inactive_dt,
                       eiem65.inactive_date e65_inactive_dt,
                       eiem5.inactive_date e5_inactive_dt,
                       eiem6.inactive_date e6_inactive_dt,
                       P.NO_BILL as Product_no_bill,
                       CM.NO_BILL as Cust_No_Bill,
                       S.NO_BILL as Service_No_Bill,
                       case when p.BILL_PERIOD is not null then BPV.display_value else bpv1.display_value end as Bill_Period,
                       EIEM1.EXTERNAL_ID AS PRIMARY_TYPE_1,
                       p.tracking_id,
                       EIEM6.EXTERNAL_ID AS SCID_TYPE_6,
                       EIEM5.EXTERNAL_ID AS PIID_TYPE_5,
                       EIEM65.EXTERNAL_ID AS FROMER_PRIM_TYPE_65,
                       PK.CREATE_DT,
                       --S.SERVICE_ADDRESS1||', '||
                       --S.SERVICE_CITY||', '||
                       --S.SERVICE_STATE as SERVICE_ADDRESS,
                       RCV.DISPLAY_VALUE as Currency_value,
                       (PRO.OVERRIDE_RATE)/power(10, rcr.implied_decimal) AS Bill_RATE,
                       S.CHG_WHO as Last_changed_by,
                       S.CHG_DT as Last_changed_on,
                       (select param_value from product_ext_data ped where ped.param_id = '29' and ped.view_id = p.view_id) description_override,
                       (select param_value from product_ext_data ped where ped.param_id = '28' and ped.view_id = p.view_id) description_override1,
                       (select param_value from service_ext_data sed where sed.param_id = '107' and sed.view_id = s.view_id) CIRCUIT_ID,
                       (select param_value from product_ext_data ped where ped.param_id = '166' and ped.view_id = p.view_id) PRCMP_DESC,
                       (select param_value from product_ext_data ped where ped.param_id = '165' and ped.view_id = p.view_id) PRCMP_CODE
                FROM CUSTOMER_ID_ACCT_MAP CIAM,
                     CMF CM,
                     CMF_PACKAGE_COMPONENT CPC,
                     PACKAGE_DEFINITION_VALUES PDV,
                     COMPONENT_DEFINITION_VALUES CDV,
                     CMF_COMPONENT_ELEMENT CCE,
                     PRODUCT P,
                     EXTERNAL_ID_EQUIP_MAP EIEM1,
                     EXTERNAL_ID_EQUIP_MAP EIEM5,
                     EXTERNAL_ID_EQUIP_MAP EIEM6,
                     EXTERNAL_ID_EQUIP_MAP EIEM65,
                     EXTERNAL_ID_ACCT_MAP eiam,
                     external_id_acct_map eiam150,
                     arbor.rate_currency_ref rcr,
                     /*Place the list of PIIDs, SCIDs or PSCIDs into the folloing statments where clause*/
                     (select distinct subscr_no, subscr_no_resets from EXTERNAL_ID_EQUIP_MAP where external_id in {0} ) EIEML, --enter serv_comp_ids here
                     PRODUCT_KEY PK,
                     SERVICE S,
                     PRODUCT_RATE_OVERRIDE PRO,
                     bill_period_values BPV,
                     bill_period_values BPV1,
                     RATE_CURRENCY_VALUES RCV,
                     emf_config_id_values emfv
                WHERE CIAM.EXTERNAL_ID_TYPE(+)= 80 --and p.billing_inactive_dt is null and eiem1.inactive_date is null
                and ciam.IS_CURRENT(+) = 1
                AND CIAM.ACCOUNT_NO(+) = CM.ACCOUNT_NO
                AND CM.ACCOUNT_NO= P.PARENT_ACCOUNT_NO
                and eiam.account_no (+) = cm.account_no
                and eiam.external_id_type = 1
                and eiam150.account_no (+) = cm.account_no
                and eiam150.external_id_type = 150
                and s.EMF_CONFIG_ID = emfv.EMF_CONFIG_ID
                and emfv.LANGUAGE_CODE = 1
                AND CPC.PACKAGE_ID=PDV.PACKAGE_ID(+) 
                AND PDV.LANGUAGE_CODE(+)= 1
                AND P.COMPONENT_ID= CDV.COMPONENT_ID(+)
                AND CDV.LANGUAGE_CODE= 1
                AND CCE.COMPONENT_INST_ID= CPC.COMPONENT_INST_ID(+)
                AND CCE.COMPONENT_INST_ID_SERV= CPC.COMPONENT_INST_ID_SERV(+)
                AND CCE.ASSOCIATION_TYPE(+) = 1
                AND P.TRACKING_ID= CCE.ASSOCIATION_ID(+)
                AND P.TRACKING_ID_SERV= CCE.ASSOCIATION_ID_SERV(+)
                AND EIEML.SUBSCR_NO= P.PARENT_SUBSCR_NO
                AND EIEML.SUBSCR_NO_RESETS= P.PARENT_SUBSCR_NO_RESETS
                AND EIEM1.SUBSCR_NO(+)= P.PARENT_SUBSCR_NO
                AND EIEM1.SUBSCR_NO_RESETS(+)= P.PARENT_SUBSCR_NO_RESETS
                AND EIEM1.EXTERNAL_ID_TYPE(+)= 1
                AND EIEM6.SUBSCR_NO(+)= P.PARENT_SUBSCR_NO
                AND EIEM6.SUBSCR_NO_RESETS(+)= P.PARENT_SUBSCR_NO_RESETS
                AND EIEM6.EXTERNAL_ID_TYPE(+)= 6
                AND EIEM5.SUBSCR_NO(+)= P.PARENT_SUBSCR_NO
                AND EIEM5.SUBSCR_NO_RESETS(+)= P.PARENT_SUBSCR_NO_RESETS
                AND EIEM5.EXTERNAL_ID_TYPE(+)= 5
                AND EIEM65.SUBSCR_NO(+)= P.PARENT_SUBSCR_NO
                AND EIEM65.SUBSCR_NO_RESETS(+)= P.PARENT_SUBSCR_NO_RESETS
                AND EIEM65.EXTERNAL_ID_TYPE(+)= 65
                AND PK.TRACKING_ID(+)= P.TRACKING_ID
                AND PK.TRACKING_ID_SERV(+)= P.TRACKING_ID_SERV
                AND S.SUBSCR_NO= P.PARENT_SUBSCR_NO
                AND S.SUBSCR_NO_RESETS= P.PARENT_SUBSCR_NO_RESETS
                AND PRO.TRACKING_ID(+)= P.TRACKING_ID
                AND PRO.TRACKING_ID_SERV(+)= P.TRACKING_ID_SERV
                and PRO.INACTIVE_DT is null
                AND RCV.CURRENCY_CODE(+)= CM.CURRENCY_CODE
                AND RCV.LANGUAGE_CODE(+)= 1
                and bpv.bill_period(+) = p.bill_period
                and bpv1.bill_period(+) = CM.bill_period
                and rcr.currency_code(+)= CM.currency_code
                --and sed.view_id = s.view_id
                order by EIEM1.EXTERNAL_ID desc, p.billing_inactive_dt desc
                                        '''.format(ps), oconn_kenan)
    
        lil_kenan_df = pd.DataFrame(kenan_query) 
        lil_kenan_df = lil_kenan_df.fillna('')

        kenan_prod_df = pd.concat([kenan_prod_df, lil_kenan_df], ignore_index=True)
        

    kenan_prod_df['BILLING_INACTIVE_DT'] = kenan_prod_df["BILLING_INACTIVE_DT"].apply(lambda x: pd.to_datetime(x, errors = 'coerce') if x != '' else '') #if out of bounds timestamp, convert to NaT then fill with ''
    kenan_prod_df = kenan_prod_df.fillna({'BILLING_INACTIVE_DT': ''})

    #since it's ordered by primary id and inactive date desc, grab the first row and that will tell you the status
    final = kenan_prod_df.groupby('PRIMARY_TYPE_1').nth(0)
    
    dates_df = final[['PRIMARY_TYPE_1', 'BILLING_INACTIVE_DT']]
    
    with pd.ExcelWriter(output_file_name, mode='a') as writer:
        dates_df.to_excel(writer, sheet_name='DISCO_DATE_INFO', index=False)
    
    return dates_df
   

dates_df = get_kenan_billing_inactive_date(df)

dates_df.rename(columns={'BILLING_INACTIVE_DT': 'KENAN_BILLING_INACTIVE_DT'}, inplace=True)

df = pd.merge(left = df, right = dates_df, left_on='REV_PRODUCT_COMPNT_ID', right_on='PRIMARY_TYPE_1', how='left')

df['Disco Date'] = ''

#update revenue disconnect status and actionablility
# for index, row in df.iterrows():
#     if (row['Disco Date'] != '') & (row['Disco Date'] != 0) & (pd.isnull(row['Disco Date']) == False):

#        #check the other current/last rerate date in the case where it's no in progress
#        if (pd.isnull(row['Last Completed/In Progress Rerate Date']) == False) & (row['Last Completed/In Progress Rerate Date'] != ''): 
           
#            if row['Disco Date'] <= row['Last Completed/In Progress Rerate Date']: 
               
#                df_extra.at[index,'LNM Status'] = "Revenue Disconnect"

piid_list = []
disco_date_list = []

df_piid_grouped = df.groupby("SOLUTION_ID_PRIME")

for piid, piid_df in df_piid_grouped: 
    
    #pd.isna() or pd.isnull() will return a boolean mask where True indicates the presence of NaT
    NaT_mask = pd.isna(piid_df['KENAN_BILLING_INACTIVE_DT'])
    dates = piid_df['KENAN_BILLING_INACTIVE_DT'].unique().tolist()

    if (len(dates) > 0) & (NaT_mask.sum() == 0): 
            
        if not all(c == '' for c in dates): 
            
            if '' in dates:
                dates.remove('')

            print(dates)
            disco_date_list.append(max(dates))
            piid_list.append(piid)

disco_df = pd.DataFrame({'piid': piid_list, 'stop_date': disco_date_list})

df = pd.merge(left = df, right = disco_df, left_on='SOLUTION_ID_PRIME', right_on='piid', how='left')
df['Disco Date'] = df['stop_date']
df = df.iloc[:, :-disco_df.shape[1]]

df['NonActionable Reason'] = np.where( ( df["NonActionable Reason"].isnull() | (df["NonActionable Reason"] == '') ) & ( (df['Disco Date'] != '') & (pd.isnull(df['Disco Date']) == False) ),
                                      "Revenue Disconnect", df['NonActionable Reason'] ) 
df['Actionable/NonActionable'] = np.where( (df["NonActionable Reason"] == 'Revenue Disconnect') & ( (df['Disco Date'] != '') & (pd.isnull(df['Disco Date']) == False) ), 
                                      "NonActionable", df['Actionable/NonActionable'] ) 

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------#

#step 9: Fed Govt: NonActionable
#Where Gov_Ind = Y

df['NonActionable Reason'] = np.where( ( df["NonActionable Reason"].isnull() | (df["NonActionable Reason"] == '') ) & (df["GOV_IND"].str.contains('Y', regex=False)),
                                      "Fed Govt", df['NonActionable Reason'] ) 
df['Actionable/NonActionable'] = np.where( (df["NonActionable Reason"] == 'Fed Govt') & (df["GOV_IND"].str.contains('Y', regex=False)), 
                                      "NonActionable", df['Actionable/NonActionable'] ) 

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------#

#step 10: ENSEMBLE: NonActionable
#Where Bill_Source_Sys_Cd = ENS

df['NonActionable Reason'] = np.where( ( df["NonActionable Reason"].isnull() | (df["NonActionable Reason"] == '') ) & (df["BILL_SRC_SYS_CD"].str.contains('ENS', regex=False)),
                                      "ENSEMBLE", df['NonActionable Reason'] ) 
df['Actionable/NonActionable'] = np.where( (df["NonActionable Reason"] == 'ENSEMBLE')  & (df["BILL_SRC_SYS_CD"].str.contains('ENS', regex=False)), 
                                      "NonActionable", df['Actionable/NonActionable'] ) 

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------#

#step 11: VANTIVE: NonActionable
#Where Rev Product Component ID like '%Vantive'

df['NonActionable Reason'] = np.where( ( df["NonActionable Reason"].isnull() | (df["NonActionable Reason"] == '') ) & (df["REV_PRODUCT_COMPNT_ID"].str.contains('VANTIVE', case=False, regex=False)),
                                      "VANTIVE", df['NonActionable Reason'] ) 
df['Actionable/NonActionable'] = np.where( (df["NonActionable Reason"] == 'VANTIVE') & (df["REV_PRODUCT_COMPNT_ID"].str.contains('VANTIVE', case=False, regex=False)), 
                                      "NonActionable", df['Actionable/NonActionable'] ) 

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------#

#step 12: CABS: NonActionable
#Where Bill_Source_Sys_Cd = CABS

df['NonActionable Reason'] = np.where( ( df["NonActionable Reason"].isnull() | (df["NonActionable Reason"] == '') ) & (df["BILL_SRC_SYS_CD"].str.contains('CABS', regex=False)),
                                      "CABS", df['NonActionable Reason'] ) 
df['Actionable/NonActionable'] = np.where( (df["NonActionable Reason"] == 'CABS') & (df["BILL_SRC_SYS_CD"].str.contains('CABS', regex=False)), 
                                      "NonActionable", df['Actionable/NonActionable'] ) 

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------#

#step 13: Enterprise Broadband: NonActionable
#Where PRODUCT_CONCAT like '%Enterprise Broadband%'

df['NonActionable Reason'] = np.where( ( df["NonActionable Reason"].isnull() | (df["NonActionable Reason"] == '') ) 
                                      & (df["PRODUCT_CONCAT"].str.contains('enterprise broadband', case=False, regex=False)),
                                      "Enterprise Broadband", df['NonActionable Reason'] ) 
df['Actionable/NonActionable'] = np.where( (df["NonActionable Reason"] == 'Enterprise Broadband')
                                          & (df["PRODUCT_CONCAT"].str.contains('enterprise broadband', case=False, regex=False)), 
                                      "NonActionable", df['Actionable/NonActionable'] ) 

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------#

#step 14: Non-North America: NonActionable
#Where OCC_REGION_CONCAT is not null and not equal to NA

df['NonActionable Reason'] = np.where( ( df["NonActionable Reason"].isnull() | (df["NonActionable Reason"] == '') ) 
                                      & ( (df['OCC_REGION_CONCAT'] != '') & (pd.isnull(df['OCC_REGION_CONCAT']) == False) ) & (df['OCC_REGION_CONCAT'] != 'NA'),
                                      "Non-North America", df['NonActionable Reason'] ) 
df['Actionable/NonActionable'] = np.where( (df["NonActionable Reason"] == 'Non-North America')
                                          & ( (df['OCC_REGION_CONCAT'] != '') & (pd.isnull(df['OCC_REGION_CONCAT']) == False) ) & (df['OCC_REGION_CONCAT'] != 'NA'), 
                                      "NonActionable", df['Actionable/NonActionable'] ) 

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------#

#step 15: OE Process Needs to be Defined: NonActionable
#Where Solution ID Prime is all numeric >= 10 digits or where solution id prime like **LCC

df['NonActionable Reason'] = np.where( ( df["NonActionable Reason"].isnull() | (df["NonActionable Reason"] == '') ) 
                                      & ( ( (df['SOLUTION_ID_PRIME'].str.isdigit()) & (df['SOLUTION_ID_PRIME'].str.len() > 10) ) | (df["SOLUTION_ID_PRIME"].str.contains('**LCC', case=False, regex=False)) ),
                                      "OE Process Needs to be Defined", df['NonActionable Reason'] ) 
df['Actionable/NonActionable'] = np.where( (df["NonActionable Reason"] == 'OE Process Needs to be Defined')
                                          & ( ( (df['SOLUTION_ID_PRIME'].str.isdigit()) & (df['SOLUTION_ID_PRIME'].str.len() > 10) ) | (df["SOLUTION_ID_PRIME"].str.contains('**LCC', case=False, regex=False)) ), 
                                      "NonActionable", df['Actionable/NonActionable'] ) 

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------#

#step 16: In Term: NonActionable
#Where MARC cust contract term end dt > current month + 3 and the 1st of the month....so if data is gathered in May, In Term would be cust contract term end dt beyond 8/1/2025.   
#if data is gathered in June, In Term would be cust contract term end dt beyond 9/1/2025, etc.

check_date = get_check_date()
df['CUSTOMER_TERM_END_DT'] = df["CUSTOMER_TERM_END_DT"].apply(lambda x: pd.to_datetime(x, errors = 'coerce') if x != '' else '')

for index, row in df.iterrows(): 
    if ( (row["NonActionable Reason"] == None) | (row["NonActionable Reason"] == '') ) & (row['CUSTOMER_TERM_END_DT'] != '') & (pd.isnull(row['CUSTOMER_TERM_END_DT']) == False): 
                
        if row['CUSTOMER_TERM_END_DT'] > check_date: 
            
            df.at[index,'NonActionable Reason'] = 'In Term'
            df.at[index,'Actionable/NonActionable'] = 'NonActionable'

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------#

#step 17: In Term TDM - Reratable: Actionable
#Where In Term per guidelines above, but IN_TERM_TDM_RR = Y

df['NonActionable Reason'] = np.where( (df['NonActionable Reason'] == 'In Term') & (df['IN_TERM_TDM_RR'] == 'Y'), "In Term TDM - Reratable", df['NonActionable Reason'] ) 
df['Actionable/NonActionable'] = np.where( (df['NonActionable Reason'] == 'In Term') & (df['IN_TERM_TDM_RR'] == 'Y'), "Actionable", df['Actionable/NonActionable'] )         

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------#

#step 18: DNTL: NonActionable
#Where DNTL Type = Contractual or Bankruptcy and DNTL Expiration > current month + 3 and the 1st of the month
#so if data is gathered in May, DNTL Expiration would need to be beyond 8/1/2025.

def get_dntl_info(df):
    
    dntl_data_df = pd.DataFrame() 
    
    piids = df['SOLUTION_ID_PRIME'].unique().tolist()
    chunks = make_cuts(piids)
    
    for c in chunks: 
        
        ps = '(' + "'" + "','".join(str(x) for x in c) + "'" + ')'
        
        query =  pd.read_sql_query('''SELECT  distinct SOLUTION_ID_PRIME, DNT_REASON_CODE, HOLD_EXPIRATION_DATE, DNT_EXCLUSION_LEVEL
FROM    [EC].[MARGIN_ANALYTICS_REPORTING_SERVICE] (nolock)
where SOLUTION_ID_PRIME in {0} and DNT_REASON_CODE in ('contractual','bankruptcy')'''.format(ps), cnxn_0043)
        
        lil_dntl_df = pd.DataFrame(query) 
        lil_dntl_df = lil_dntl_df.fillna('')
        
        dntl_data_df = pd.concat([dntl_data_df, lil_dntl_df], ignore_index=True)
        
    if not dntl_data_df.empty: 
        
        dntl_data_df.rename(columns={'SOLUTION_ID_PRIME': 'piid'}, inplace=True)
        dntl_data_df['HOLD_EXPIRATION_DATE'] = pd.to_datetime(dntl_data_df['HOLD_EXPIRATION_DATE'])
                
        with pd.ExcelWriter(output_file_name, mode='a') as writer:
            dntl_data_df.to_excel(writer, sheet_name='DNTL_INFO', index=False)
        
    return dntl_data_df


dntl_data_df = get_dntl_info(df)

check_date = get_check_date()

if not dntl_data_df.empty: 

    dntl_data_df.rename(columns={'SOLUTION_ID_PRIME': 'dntl_SOLUTION_ID_PRIME', 'DNT_REASON_CODE': 'dntl_DNT_REASON_CODE', 'HOLD_EXPIRATION_DATE': 'dntl_HOLD_EXPIRATION_DATE', 'DNT_EXCLUSION_LEVEL': 'dntl_DNT_EXCLUSION_LEVEL'}, inplace=True)
    df = pd.merge(left = df, right = dntl_data_df, left_on='SOLUTION_ID_PRIME', right_on='piid', how='left')
    
    for index, row in df.iterrows(): 
        
        if ((row["NonActionable Reason"] == None) | (row["NonActionable Reason"] == '')) & (row['dntl_HOLD_EXPIRATION_DATE'] != '') & (pd.isnull(row['dntl_HOLD_EXPIRATION_DATE']) == False): 
        
            if (row['dntl_HOLD_EXPIRATION_DATE'] > check_date) & ( (row['dntl_DNT_REASON_CODE'] == 'Contractual') | (row['dntl_DNT_REASON_CODE'] == 'Bankruptcy') ): 
            
                df.at[index,'NonActionable Reason'] = 'DNTL'
                df.at[index,'Actionable/NonActionable'] = 'NonActionable'
                
    df = df.iloc[:, :-dntl_data_df.shape[1]]

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------#

#step 19: Rerate In Progress within next 30 days: NonActionable
#Where there is an active rerate for the Solution ID Prime or any of it's rev product component ids and the Workflowstatus = In Progress and the rerate date is getdate + 30 days.

#need to pull in the workflow status
def pull_workflow_status(piids):
    
    status_df = pd.DataFrame() 
    chunks = make_cuts(piids)
    
    for c in chunks:
        
        ps = '(' + "'" + "','".join(str(x) for x in c) + "'" + ')'
    
        query = pd.read_sql_query('''select a.product_inst_id as wf_product_inst_id, a.[SERV_COMP_ID] as wf_SERV_COMP_ID, a.piid_meid as wf_piid_meid, a.SubProjName as wf_SubProjName, 
                                  a.rerate_date as wf_rerate_date, a.Increase as wf_increase, a.WorkflowStatus as wf_WorkflowStatus
from [STAGE].[RERATES_GLOBAL_ALL_DATA_RAW_SCID] a
where a.WorkflowStatus <> 'Canceled'
and  a.rerate_date = (select max(rerate_date) from [STAGE].[RERATES_GLOBAL_ALL_DATA_RAW_SCID] b where b.PRODUCT_INST_ID = a.PRODUCT_INST_ID and WorkflowStatus <> 'Canceled')
and PRODUCT_INST_ID in {0}'''.format(ps), cnxn)
    
        s_df = pd.DataFrame(query)
        status_df = pd.concat([status_df, s_df], ignore_index=True)
    
    #save results to excel file
    with pd.ExcelWriter(output_file_name, mode='a') as writer:
        status_df.to_excel(writer, sheet_name='WorkFlowStatus', index=False)
    #fill all nan with '' for consistency
    status_df = status_df.fillna('')

    return status_df

piids = df['SOLUTION_ID_PRIME'].unique().tolist()
wf_status_df = pull_workflow_status(piids)       

df['Workflowstatus'] = ''
df = pd.merge(left = df, right = wf_status_df, left_on='SOLUTION_ID_PRIME', right_on='wf_product_inst_id', how='left')
df['Workflowstatus'] = df['wf_WorkflowStatus']
df = df.iloc[:, :-wf_status_df.shape[1]]

df['LAST_RERATE_DATE'] = df["LAST_RERATE_DATE"].apply(lambda x: pd.to_datetime(x, errors = 'coerce') if x != '' else '')
next_30_days_check_date = datetime(check_date.year, check_date.month + 1, check_date.day)

for index, row in df.iterrows(): 
    if ( (row["NonActionable Reason"] == None) | (row["NonActionable Reason"] == '') ) & (row['Workflowstatus'] == 'In Progress') & ( (pd.isnull(row['LAST_RERATE_DATE']) == False) & (row['LAST_RERATE_DATE'] != '') ) & ( (row["NonActionable Reason"] == None) | (row["NonActionable Reason"] == '') ):
        
        if row['LAST_RERATE_DATE'] <= next_30_days_check_date: 
            
            df.at[index,'NonActionable Reason'] = 'Rerate In Progress within next 30 days'
            df.at[index,'Actionable/NonActionable'] = 'NonActionable'

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------#

#step 20: Rerated within 3 Months: Potentially Actionable
#Requires Cancel and Reload	Where there is a rerate with any Workflowstatus other than canceled for the Solution ID Prime 
#or any of it's rev product component ids and the rerate date is the same month as JRNL_YR_MNTH_NBR in MARS/MARC or beyond.

for index, row in df.iterrows(): 
    if ( (row["NonActionable Reason"] == None) | (row["NonActionable Reason"] == '') ) & (row['Workflowstatus'] != 'Canceled') & ( (pd.isnull(row['JRNL_YR_MNTH_NBR']) == False) & (row['JRNL_YR_MNTH_NBR'] != '') ) & ( (pd.isnull(row['LAST_RERATE_DATE']) == False) & (row['LAST_RERATE_DATE'] != '') ) & ( (row["NonActionable Reason"] == None) | (row["NonActionable Reason"] == '') ) : 
        
        conversion = str(row['JRNL_YR_MNTH_NBR'])
        check_month = int(conversion[4:6])
        check_year = int(conversion[:4])
        #check_month = pd.to_numeric(conversion[4:6], errors='coerce').astype('Int64')

        if (row['LAST_RERATE_DATE'].month >= check_month) & (row['LAST_RERATE_DATE'].year >= check_year): 
            
            df.at[index,'NonActionable Reason'] = 'Rerated within 3 Months'
            df.at[index,'Actionable/NonActionable'] = 'Potentially Actionable - Requires Cancel and Reload'

#need to check logic here and test#

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------#

#step 21: RevShare: NonActionable
#Where any ECCKT associated to the Solution ID Prime is like '%revshare' and no other status has been applied yet.

df['NonActionable Reason'] = np.where( ( df["NonActionable Reason"].isnull() | (df["NonActionable Reason"] == '') ) & (df["input_ECCKT_STRIPPED_NAME"].str.contains('revshare', regex=False)),
                                      "RevShare", df['NonActionable Reason'] ) 
df['Actionable/NonActionable'] = np.where( (df["NonActionable Reason"] == 'RevShare') & (df["input_ECCKT_STRIPPED_NAME"].str.contains('revshare', regex=False)), 
                                      "NonActionable", df['Actionable/NonActionable'] ) 

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------#

#step 22: Service is $0 Biller: Actionable
#Where MARC Rev MRR = $0

df['NonActionable Reason'] = np.where( ( df["NonActionable Reason"].isnull() | (df["NonActionable Reason"] == '') ) & (df["MARC_REV_MRR"] == 0),
                                      "Service is $0 Biller", df['NonActionable Reason'] ) 
df['Actionable/NonActionable'] = np.where((df["NonActionable Reason"] == 'Service is $0 Biller') & (df["MARC_REV_MRR"] == 0), 
                                      "Actionable", df['Actionable/NonActionable'] ) 

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------#

#step 23: Proceed with Rerate: Actionable
#Default status if none of the above statuses apply.


df['NonActionable Reason'] = np.where( ( df["NonActionable Reason"].isnull() | (df["NonActionable Reason"] == '') ),
                                      "Proceed with Rerate", df['NonActionable Reason'] ) 
df['Actionable/NonActionable'] = np.where( (df["NonActionable Reason"] == 'Proceed with Rerate'), 
                                      "Actionable", df['Actionable/NonActionable'] ) 


#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
#final output
with pd.ExcelWriter(output_file_name, mode='a') as writer:
    df.to_excel(writer, sheet_name='final', index=False)