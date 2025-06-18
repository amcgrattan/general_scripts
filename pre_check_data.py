import pandas as pd
import pyodbc
import os
import numpy as np
import cx_Oracle
from datetime import datetime
from dateutil.relativedelta import relativedelta

cwd = os.getcwd()
print(cwd)
    
server_0043 = 'USIDCVSQL0043'  #'azeus2sqlerpn2.ctl.intranet,7114' #'USIDCVSQL0043' 
database_0043 = 'FA_Margin' 
username_0043 = 'SSRSAccount' 
password_0043 =  '$$R$Account@2025-000' #'$$R$Account@2024'
cnxn_0043 = pyodbc.connect('DRIVER={SQL Server};SERVER='+server_0043+';DATABASE='+database_0043+';UID='+username_0043+';PWD='+ password_0043)
cursor_0043 = cnxn_0043.cursor()

lib_dir = r"C:\Oracle\Ora12_2_0_1_x64\bin"
cx_Oracle.init_oracle_client(lib_dir=lib_dir)

dsn_tns_kenan = cx_Oracle.makedsn('RACORAP28-SCAN.CORP.INTRANET', '1521', service_name='KEN01P') # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
oconn_kenan = cx_Oracle.connect(user=r'MAAPPLSA', password='''M@u5(ra+2024-111''', dsn=dsn_tns_kenan)

dsn_tns_latis = cx_Oracle.makedsn('PRODUX.QINTRA.COM', '1521', sid='PROD') # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
oconn_latis = cx_Oracle.connect(user=r'MAAPPLSA', password='''M@u5(ra+2025-1''', dsn=dsn_tns_latis) # M@u5(ra+2025-0

server_0211 = 'USIDCVSQL0211' 
database_0211 = 'SES_Snapshot' 
username_0211 = 'MAAPPLSA' 
password_0211 = 'f3AvZ688e0J6L852'
cnxn_0211 = pyodbc.connect('DRIVER={SQL Server};SERVER='+server_0211+';DATABASE='+database_0211+';UID='+username_0211+';PWD='+ password_0211)
cursor_0211 = cnxn_0211.cursor()


#update the file names, month, and day depending on bill cycle
validator_update_file = 'June_BC31_Validator_Update.xlsx'
bc_prep_file = 'June_2025_BC31_Prep.xlsx'

month = 6
day = 31
year = 2025


#step 1
def run_validator_query(month, day):
    
    query = pd.read_sql_query('''select distinct l.[PIID_MeID]
        ,l.[PRODUCT_INST_ID]
        ,l.[RedBlue]
        ,l.[SubProjName]
        ,l.[WorkflowStatus]
        ,l.[Rerate_Month]
        ,l.[FiscalYear]
        ,l.[DisconnectDate]
        ,[Disposition]
  	  ,Invoice_Day
  	  , Disconnected
  	  , dteDisconnected
  	  , r.[Non-Opp Reason]
    FROM [FA_Margin].[EC].[RERATE_VALIDATION_LOGS] l
    join dbo.Rerates_All_Data_RAW r on r.PIID_MeID = l.PIID_MeID
    where l.redblue = 'green'
    and l.SubProjName not like '%special access%'
	and Disposition not like '%overlap%'
	and r.WorkflowStatus <> 'canceled'
	and RerateMonth_Actual in (? ) --add rerate month
	and Invoice_Day in (?) --add invoice day
	and r.FiscalYear in (2025) --add fiscalyear
	order by Disposition, l.WorkflowStatus, FiscalYear, Rerate_Month, Invoice_Day''', cnxn_0043, params=(month, day))
    
    validator_df = pd.DataFrame(query)
    validator_df = validator_df.fillna('')
    
    #create an df with just the info needed to update for rawr3
    update_df = validator_df[['PIID_MeID', 'PRODUCT_INST_ID', 'WorkflowStatus', 'Disposition', 'DisconnectDate']]
    # update_df['new'] = ['']*update_df.shape[0]
    # update_df['Non-Opp Reason'] = ['']*update_df.shape[0]
    # update_df['Notes1'] = ['']*update_df.shape[0]
    update_df[['new', 'Non-Opp Reason', 'Notes1']] = ''
    update_df['DisconnectDate'] = update_df['DisconnectDate'].apply(lambda x: pd.to_datetime(x) if x != '' else '')
    
    # print(update_df.shape)
    # update_df.to_excel(validator_update_file, sheet_name='wft', index=False)
    
    # update_df = pd.DataFrame(columns=['PIID_MeID', 'PRODUCT_INST_ID', 'WorkflowStatus', 'Disposition','new', 'Non-Opp Reason', 'Notes1'])
    # update_df['PIID_MeID'] = validator_df['PIID_MeID']
    # update_df['PRODUCT_INST_ID'] = validator_df['PRODUCT_INST_ID']
    # update_df['WorkflowStatus'] = validator_df['WorkflowStatus']
    # update_df['Disposition'] = validator_df['Disposition']
    
    #apply joya's disposition rules for workflow status, non opp reason, and notes1 here
    update_df['new'] = np.where( (update_df['WorkflowStatus'] == 'In Progress') & (update_df['Disposition'] == 'Canceled due to Renewal') , 'Canceled' , update_df['new'])
    update_df['Non-Opp Reason'] = np.where( (update_df['WorkflowStatus'] == 'In Progress') & (update_df['Disposition'] == 'Canceled due to Renewal') , 'Renewal' , update_df['Non-Opp Reason'])
    update_df['Notes1'] = np.where( (update_df['WorkflowStatus'] == 'In Progress') & (update_df['Disposition'] == 'Canceled due to Renewal') , 'Renewal per validator' , update_df['Notes1'])
    
    update_df['new'] = np.where( (update_df['WorkflowStatus'] == 'In Progress') & (update_df['Disposition'] == 'Disconnect') , 'Canceled' , update_df['new'])
    update_df['Non-Opp Reason'] = np.where( (update_df['WorkflowStatus'] == 'In Progress') & (update_df['Disposition'] == 'Disconnect') , 'Disconnect' , update_df['Non-Opp Reason'])
    update_df['Notes1'] = np.where( (update_df['WorkflowStatus'] == 'In Progress') & (update_df['Disposition'] == 'Disconnect') , 'Disconnect per validator' , update_df['Notes1'])
    
    # for index, row in update_df.iterrows():
        
    #     if (row['WorkflowStatus'] == 'In Progress') & (row['Disposition'] == 'Canceled due to Renewal'):
            
    #         update_df.at[index, 'new'] = 'Canceled'
    #         update_df.at[index, 'Non-Opp Reason'] = 'Renewal'
    #         update_df.at[index, 'Notes1'] = 'Renewal per validator'
                
                
    #     elif (row['WorkflowStatus'] == 'In Progress') & (row['Disposition'] == 'Disconnect'): 
            
    #         update_df.at[index, 'new'] = 'Canceled'
    #         update_df.at[index, 'Non-Opp Reason'] = 'Disconnect'
    #         update_df.at[index, 'Notes1'] = 'Disconnect per validator'


    update_df = update_df.drop(['WorkflowStatus', 'Disposition'], axis=1)
    update_df.rename(columns={'new': 'WorkflowStatus'}, inplace=True)
            
    update_df.to_excel(validator_update_file, sheet_name='PIID Changes', index=False)
    
    return update_df
    

validator_df = run_validator_query(month, day) 

#step 2: 
def pull_rerate_detail(month, day):
        
    query = pd.read_sql_query('''SELECT rerate_date, raw.Invoice_Day, subprojname, workflowstatus, meid, serv_comp_id, rr_piid_meid, raw.product_inst_id, scp_current_rate, scp_increase, scp_new_rate, component, component_id, component_grp_val_id, qty as old_qty, cust_contract_term_end_dt, business_unit, raw.bus_org_id, raw.cust_acct_sales_channel_desc, raw.product, invoice_day , inv_ban, prod_Acct_ID, service_element_id, promo_code, prod_cmpnt_cd, pre_billed_ind, reversal_Date, batch, bill_acct_nbr, invoice_kenan_acct_nbr
  FROM    ec.rr_scid scid
  join dbo.Rerates_All_Data_RAW raw on raw.piid_meid = scid.RR_PIID_MeID
  where fiscalyear = 2025--update year
  and RerateMonth_Actual = ?--update month
  and invoice_day  in (?)--update day
  and WorkflowStatus <> 'canceled'
  and RedBlue = 'green'
  and SubProjName not like '%access%'
and Rollup_to_PIID = 'y'
  and SCP_INCREASE <> 0
  and dw_source_system_cd in ('SDP','Orion','Latis')
  and raw.FREQUENCY <> 'NRC'
  order by rerate_date, PRODUCT_INST_ID, SERV_COMP_ID''', cnxn_0043, params=(month, day))
    
    rerate_df = pd.DataFrame(query)
    rerate_df = rerate_df.fillna('')

    return rerate_df

rerate_df = pull_rerate_detail(month, day)

#step 3:
def pull_reversal_detail(month, day, year):
        
    query = pd.read_sql_query('''SELECT rerate_date, raw.Invoice_Day, subprojname, workflowstatus, meid, serv_comp_id, rr_piid_meid, raw.product_inst_id, scp_new_rate as scp_current_rate, scp_increase, scp_current_rate as scp_new_rate, component, component_id, component_grp_val_id, QTY as old_qty, cust_contract_term_end_dt, business_unit, raw.bus_org_id, raw.cust_acct_sales_channel_desc, raw.product, invoice_day , inv_ban, prod_acct_id, service_element_id, promo_code, prod_cmpnt_cd, pre_billed_ind, reversal_date, batch, bill_acct_nbr, reversal_reason
  FROM    ec.rr_scid scid
  join dbo.Rerates_All_Data_RAW raw on raw.piid_meid = scid.RR_PIID_MeID
  where workflowstatus = 'reversal'
  and invoice_day  in (?)--update day
  and RedBlue = 'green'
  and SubProjName not like '%access%'
  and SCP_INCREASE <> 0
and Rollup_to_PIID = 'y'
  and dw_source_system_cd in ('SDP','Orion','Latis')
  and raw.FREQUENCY <> 'NRC'
  order by raw.Invoice_Day, PRODUCT_INST_ID, SERV_COMP_ID''', cnxn_0043, params=(day))
    
    reversal_df = pd.DataFrame(query)
    reversal_df = reversal_df.fillna('')
    
    #sometimes there's missed reversals so need to make sure if there are any dates they're all updated to the bill cycle date
    reversal_df['reversal_date'] = reversal_df['reversal_date'].apply(lambda x: pd.to_datetime(x) if x != '' else '')
    correct_reversal_date = datetime(year, month, day)
    
    for index, row in reversal_df.iterrows():
        
        if (row['reversal_date'] != '') & (pd.isnull(row['reversal_date']) == False):
            if row['reversal_date'] != correct_reversal_date: 
                
                reversal_df.at[index, 'reversal_date'] = correct_reversal_date
        
    return reversal_df

reversal_df = pull_reversal_detail(month, day, year)

#merge rerate and reversal df and convert date column
full_df = pd.concat([rerate_df, reversal_df], ignore_index=True)

#create the unique column (concat)
unique_concat = full_df['component_id'].map(str) + '' + full_df['component_grp_val_id'].map(str) + '' + full_df['component'].map(str)
insert_index = full_df.columns.get_loc('product_inst_id')
full_df.insert(loc = insert_index + 1,column = 'unique', value = unique_concat) 

#new_cols = ['kenan', 'difference', 'status', 'latis active', 'ses order status', 'qty', 'r4 term end date', 'pkg cgv', 'pkg description', 'pkg status', 'piid cancel']
new_cols = ['kenan', 'difference', 'status', 'latis active', 'qty', 'r4 term end date', 'pkg cgv', 'pkg description', 'pkg status', 'piid cancel']
vals = ['']*len(new_cols)

insert_blank_col_index = full_df.columns.get_loc('scp_current_rate')

for i in range(0, len(new_cols)):
    full_df.insert(loc = insert_blank_col_index + 1 + i, column = new_cols[i], value = vals[i])
    

#now the set up for rr_scid is good and can move on to other sheets for the vlookups 
#at this point full_df = base set up for rr_scid sheet    

def make_cuts(l): 
    start = 0
    end = len(l)  
    step = 1000
    cuts = [] #will have a bunch of groups of 1000 with some left over 

    for i in range(start, end, step):
        x = i 
        cuts.append(l[x:x+step])
    
    return cuts


#step 4 --> tstage
def pull_kenan_rates(df): 
    
    scids = df['serv_comp_id'].unique().tolist()
    
    ps = '(' + "'" + "','".join(str(x) for x in scids) + "'" + ')'
    
    query = pd.read_sql_query('''select PRIMARY_ID+PRCMP_DESC as SCID, rate
	  FROM T_STG_ALL_KENAN_RECORDS
	  where PRIMARY_ID in {0}'''.format(ps), cnxn_0043)
    
    kenan_df = pd.DataFrame(query)
    kenan_df = kenan_df.fillna('')
    
    kenan_df.to_excel(bc_prep_file, sheet_name='tstage', index=False)
    
    return kenan_df

tstage_df = pull_kenan_rates(full_df)

#vlookup on scid to full_df
full_df = pd.merge(left = full_df, right = tstage_df, left_on='unique', right_on='SCID', how='left')
full_df['kenan'] = full_df['rate']
# = full_df.drop(['SCID', 'rate'], axis=1)
full_df = full_df.iloc[:, :-tstage_df.shape[1]]


#step 5 --> latis_active (aka look for disconnects)
def pull_latis_active(df): 
    
    comp_grp_vals = df['component_grp_val_id'].unique().tolist()
    
    ps = '(' + "'" + "','".join(str(x) for x in comp_grp_vals) + "'" + ')'
    
    query = pd.read_sql_query('''select COMPONENT_GROUP_CD+COMPONENT_GRP_VAL+PRCMP_DSC as SCID,  COMPONENT_GROUP_CD, COMPONENT_GRP_VAL, MONTHLY_REV_EST, PRCMP_CODE, PRCMP_DSC, PRCMP_ID, PRODUCT_CODE, PRODUCT_CODE+PRCMP_CODE as Quantity_unique
                              from ctl.LATIS_PROD_R where COMPONENT_GRP_VAL in {0}'''.format(ps), cnxn_0043)
    
    latis_active_df = pd.DataFrame(query)
    latis_active_df = latis_active_df.fillna('')
    
    with pd.ExcelWriter(bc_prep_file, mode='a') as writer:
        latis_active_df.to_excel(writer, sheet_name='latis_active', index=False)
    
    return latis_active_df

latis_active_df = pull_latis_active(full_df)

#vlookup on unique to full_df
full_df = pd.merge(left = full_df, right = latis_active_df, left_on='unique', right_on='SCID', how='left')
full_df['latis active'] = full_df['COMPONENT_GRP_VAL']
#drop the columns of latis_active_df
full_df = full_df.iloc[:, :-latis_active_df.shape[1]]

#do another vlookup on comp grp val to latis active to see if anything returns
null_latis_active_subset = full_df.loc[(full_df['latis active'] == '') | (pd.isnull(full_df['latis active']) == True)]

null_latis_active_subset = pd.merge(left = null_latis_active_subset, right = latis_active_df, left_on='component_grp_val_id', right_on='COMPONENT_GRP_VAL', how='left')
null_latis_active_subset['latis active'] = null_latis_active_subset['COMPONENT_GRP_VAL']
#drop the columns of latis_active_df
null_latis_active_subset = null_latis_active_subset.iloc[:, :-latis_active_df.shape[1]]

for index, row in null_latis_active_subset.iterrows():
        
    if (row['latis active'] != '') & (pd.isnull(row['latis active']) == False):
        
        full_df['latis active'] = np.where( (full_df['serv_comp_id'] == row['serv_comp_id']) & ((full_df['latis active'] == '') | (pd.isnull(full_df['latis active']) == True)), row['latis active'], full_df['latis active'])


#do another lookup on latis active df --> if a scid has "-" in it, then check latis becuase they should never be in kenan
#then add the monthly rev to the kenan column to calculate the diff

latis_billing_subset = full_df[full_df['serv_comp_id'].str.contains('-', case=False, na=False)]

latis_billing_subset = pd.merge(left = latis_billing_subset, right = latis_active_df, left_on='unique', right_on='SCID', how='left')
latis_billing_subset['kenan'] = latis_billing_subset['MONTHLY_REV_EST']

#drop the columns of latis_active_df
latis_billing_subset = latis_billing_subset.iloc[:, :-latis_active_df.shape[1]]

for index, row in latis_billing_subset.iterrows():
        
    if (row['kenan'] != '') & (pd.isnull(row['kenan']) == False):
        
        full_df['kenan'] = np.where( (full_df['unique'] == row['unique']) & ((full_df['kenan'] == '') | (pd.isnull(full_df['kenan']) == True)), row['kenan'], full_df['kenan'])


#after checking latis active on comp grp id to fill in gaps, check kenan prod on the same subset used to fill in latis active gaps 
#(aka any null latis active including what was returned with the comp lookup)

def pull_kenan_prod(df): 
    
    scids = df['serv_comp_id'].unique().tolist()
    
    ps = '(' + "'" + "','".join(str(x) for x in scids) + "'" + ')'
    
    query = pd.read_sql_query('''SELECT distinct
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
       EIEM6.EXTERNAL_ID AS SCID_TYPE_6,
       EIEM5.EXTERNAL_ID AS PIID_TYPE_5,
       EIEM65.EXTERNAL_ID AS FROMER_PRIM_TYPE_65,
       PK.CREATE_DT,
       --S.SERVICE_ADDRESS1||', '||
       --S.SERVICE_CITY||', '||
       --S.SERVICE_STATE as SERVICE_ADDRESS,
       RCV.DISPLAY_VALUE as Currency_value,
       (PRO.OVERRIDE_RATE)/100 AS Bill_RATE,
       S.CHG_WHO as Last_changed_by,
       S.CHG_DT as Last_changed_on,
       (select param_value from product_ext_data ped where ped.param_id = '29' and ped.view_id = p.view_id) description_override,
       (select param_value from product_ext_data ped where ped.param_id = '28' and ped.view_id = p.view_id) description_override1,
       (select param_value from service_ext_data sed where sed.param_id = '107' and sed.view_id = s.view_id) CIRCUIT_ID,
       (select param_value from product_ext_data ped where ped.param_id = '166' and ped.view_id = p.view_id) PRCMP_DESC,
       (select param_value from product_ext_data ped where ped.param_id = '165' and ped.view_id = p.view_id) PRCMP_CODE,
       (select param_value from product_ext_data ped where ped.param_id = '164' and ped.view_id = p.view_id) PRCMP_ID
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
     /*Place the list of PIIDs, SCIDs or PSCIDs into the folloing statments where clause*/
     (select distinct subscr_no, subscr_no_resets from EXTERNAL_ID_EQUIP_MAP where external_id in {0} ) EIEML, --enter serv_comp_ids here
     PRODUCT_KEY PK,
     SERVICE S,
     PRODUCT_RATE_OVERRIDE PRO,
     bill_period_values BPV,
     bill_period_values BPV1,
     RATE_CURRENCY_VALUES RCV,
     emf_config_id_values emfv
--     service_ext_data sed
WHERE CIAM.EXTERNAL_ID_TYPE(+)= 80 and p.billing_inactive_dt is null and eiem1.inactive_date is null
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
--and sed.view_id = s.view_id
order by product_inactive_dt desc,product_active_dt desc'''.format(ps), oconn_kenan)
    
    kenan_prod_df = pd.DataFrame(query)
    kenan_prod_df = kenan_prod_df.fillna('')
    
    with pd.ExcelWriter(bc_prep_file, mode='a') as writer:
        kenan_prod_df.to_excel(writer, sheet_name='kenan_prod', index=False)
    
    return kenan_prod_df


#check kenan prod with all kenan billing (aka what's not in latis active)
double_check_kenan_subset = full_df.loc[(full_df['latis active'] == '') | (pd.isnull(full_df['latis active']) == True)]
kenan_prod_df = pull_kenan_prod(double_check_kenan_subset)    

#see if you can fill in any missing kenan bill rates or find the matching value for scp current rate in prod
check_kenan_subset = full_df.loc[((full_df['kenan'] == '') | (pd.isnull(full_df['kenan']) == True)) | (full_df['scp_current_rate'] != full_df['kenan'])]

check_kenan_subset = pd.merge(left = check_kenan_subset, right = kenan_prod_df, left_on='serv_comp_id', right_on='PRIMARY_TYPE_1', how='left')
check_kenan_subset['kenan'] = check_kenan_subset['BILL_RATE']
#drop the columns of kenan_prod_df
check_kenan_subset = check_kenan_subset.iloc[:, :-kenan_prod_df.shape[1]]

for index, row in check_kenan_subset.iterrows():
        
    if (row['kenan'] != '') & (pd.isnull(row['kenan']) == False):
        
        full_df['kenan'] = np.where( full_df['serv_comp_id'] == row['serv_comp_id'] , row['kenan'], full_df['kenan'])
        
    #if kenan is still null and the scids is from the reversal df, change status to 'Move back to complete - Reversal cannot be completed due to disconnect'
    else: 
        
        reversal_scids = reversal_df['serv_comp_id'].unique().tolist()
        
        print(reversal_scids)
        
        if row['serv_comp_id'] in reversal_scids: 
            
            full_df['status'] = np.where( full_df['unique'] == row['unique'], 'Move back to complete - Reversal cannot be completed due to disconnect', full_df['status'])



def pull_latis_prod(df): 
    
    piids = df['product_inst_id'].unique().tolist()
    
    ps = '(' + "'" + "','".join(str(x) for x in piids) + "'" + ')'
    
    query = pd.read_sql_query('''select distinct
APP.APPLICATION_CODE
, APP.APPLICATION_CD_DSC
, C.OCCURRENCE_NUMBER
,C.PRODUCT_ACCOUNT_ID
, C.COMMON_LITEL_CKT
, C.COMPONENT_GROUP_CD
, CG.CMP_GROUP_CODE_DSC
, C.COMPONENT_GRP_VAL
, PA.CUSTOMER_ACCT_ID
--, CAI.CUSTOMER_NM
, CTD.CONTRACT_DATE
, CTD.TERM_LENGTH
, CTD.TERMINATION_DATE
, PD.PRCMP_ID
, PAC.PRCMP_CODE
, PD.PRCMP_DSC
, PD.BILLABLE_IND
, PAC.PRCMP_OVERRIDE_DT
, PAC.PROMOTION_BGN_DATE
, PAC.PROMOTION_END_DATE
, PAC.BILL_EFF_BGN_DATE
, PAC.BILL_EFF_END_DATE
, PAC.BILLED_THRU_DATE
, PAC.MONTHLY_REV_EST
, SE.SERVICE_ELEMENT_ID
,cg.*
, pa.PRODUCT_CODE
from
PRD_ACCT_CMP_GRP c
left join prd_acct_contract ctd on CTD.PRODUCT_ACCOUNT_ID = C.PRODUCT_ACCOUNT_ID
left join service_element se on SE.COMPONENT_GRP_VAL = C.COMPONENT_GRP_VAL and SE.COMPONENT_GROUP_CD = C.COMPONENT_GROUP_CD
left join CORP.PRD_ACCT_CMP pac on PAC.PRODUCT_ACCOUNT_ID = C.PRODUCT_ACCOUNT_ID and PAC.OCCURRENCE_NUMBER = C.OCCURRENCE_NUMBER  
left join CORP.ORD_SERVICE_ELMT_ATTR sae on SAE.SERVICE_ELMT_ID = SE.SERVICE_ELEMENT_ID
left join CORP.ATTRIBUTE_DEF a on A.ATTRIBUTE_ID = sae.ATTRIBUTE_ID 
left join CORP.PRODUCT_ACCT pa on PA.PRODUCT_ACCOUNT_ID = C.PRODUCT_ACCOUNT_ID
left join CORP.CUSTOMER_ACCT_INFO cai on cast(CAI.CUSTOMER_ACCT_ID as varchar(255)) = cast(PA.CUSTOMER_ACCT_ID as varchar(255))
--left join circuits ckt on ckt.litel_circ_id = C.COMMON_LITEL_CKT
left join CORP.APPLICATION app on APP.APPLICATION_CODE = c.APPLICATION_CODE
left join corp.PRCMP_DEF pd on PD.PRCMP_CODE = PAC.PRCMP_CODE
left join corp.COMPONENT_GROUP cg on CG.COMPONENT_GROUP_CD = c.COMPONENT_GROUP_CD
left join PRD_COMB PC on pc.prcmp_id = PD.PRCMP_ID
left join PRODUCT P on PC.PRD_ID = P.PRODUCT_ID
where
c.COMMON_LITEL_CKT in {0}
and PAC.BILL_EFF_END_DATE is null'''.format(ps), oconn_latis)
    
    latis_prod_df = pd.DataFrame(query)
    latis_prod_df = latis_prod_df.fillna('')
    
    with pd.ExcelWriter(bc_prep_file, mode='a') as writer:
        latis_prod_df.to_excel(writer, sheet_name='latis_prod', index=False)
    
    return latis_prod_df


double_check_latis_subset = full_df[full_df['serv_comp_id'].str.contains('-', case=False, na=False)]
latis_prod_df = pull_latis_prod(double_check_latis_subset)    

#see if you can fill in any missing latis bill rates or find the matching value for scp current rate in prod
check_latis_subset = full_df.loc[((full_df['kenan'] == '') | (pd.isnull(full_df['kenan']) == True)) | (full_df['scp_current_rate'] != full_df['kenan'])]

check_latis_subset = pd.merge(left = check_latis_subset, right = latis_prod_df, left_on='product_inst_id', right_on='COMMON_LITEL_CKT', how='left')
check_latis_subset['kenan'] = check_latis_subset['MONTHLY_REV_EST']

#drop the columns of latis_prod_df
check_latis_subset = check_latis_subset.iloc[:, :-latis_prod_df.shape[1]]

for index, row in check_latis_subset.iterrows():
        
    if (row['kenan'] != '') & (pd.isnull(row['kenan']) == False):
        
        full_df['kenan'] = np.where( full_df['serv_comp_id'] == row['serv_comp_id'], row['kenan'], full_df['kenan'])
        
    #if kenan is still null and the scids is from the reversal df, change status to 'Move back to complete - Reversal cannot be completed due to disconnect'
    else: 
        
        reversal_scids = reversal_df['serv_comp_id'].unique().tolist()
        
        if row['serv_comp_id'] in reversal_scids: 
            
            full_df['status'] = np.where( (full_df['unique'] == row['unique']) & ( (full_df['status'] == '') | (pd.isnull(full_df['status']) == True) ), 'Move back to complete - Reversal cannot be completed due to disconnect', full_df['status'])


#calculate the diff between kenan and scp current rate
for index, row in full_df.iterrows():
        
    if (row['kenan'] != '') & (pd.isnull(row['kenan']) == False) & (row['scp_current_rate'] != '') & (pd.isnull(row['scp_current_rate']) == False):
        
        diff = float(row['scp_current_rate']) - float(row['kenan'])
        
        if diff < 0.01: 
            
            full_df.at[index, 'difference'] = 0
        
        else: 

            full_df.at[index, 'difference'] = diff


#check ses for disconnects 
def pull_ses_df(df):
    
    serv_comp_id = df['serv_comp_id'].unique().tolist()
    
    ps = '(' + "'" + "','".join(str(x) for x in serv_comp_id) + "'" + ')'
    
    query = pd.read_sql_query('''SELECT o.order_id, o.customer_order_id, s.service_id, o.created_date, s.bill_start_date, o.last_updated_date, n.note_text,
ai.service_alternate_id_value as PIID,
ai1.service_alternate_id_value as TW_CIRCUIT,
e.lookup_value as SERVICE_PROVISIONING_STATUS,
i.lookup_value as SERVICE_ACTION,
j.lookup_value as SERVICE_STATUS,
f.lookup_value as ORDER_ACTION,
g.lookup_value as ORDER_TYPE,
h.lookup_value as ORDER_STATUS
FROM dbo.sbf_order o 
left join dbo.sbf_service s on s.sbf_order_id = o.sbf_order_id
left outer join dbo.sbf_service_alternate_id ai1 with (nolock) on s.sbf_service_id = ai1.sbf_service_id AND ai1.lookup_service_alternate_id_name_id in ('85454', '87879', '85469', '105652')
left join dbo.ses_note n on n.sbf_service_id = s.sbf_service_id
left join dbo.sbf_service_alternate_id ai with (nolock) on s.sbf_service_id = ai.sbf_service_id AND ai.lookup_service_alternate_id_name_id IN ('24256', '21701')
LEFT OUTER JOIN dbo.ses_lookup e with (nolock) on s.lookup_service_provisioning_status_id=e.ses_lookup_id
LEFT OUTER JOIN dbo.ses_lookup f with (nolock) on o.lookup_order_action_id = f.ses_lookup_id
LEFT OUTER JOIN dbo.ses_lookup g with (nolock) on o.lookup_order_type_id = g.ses_lookup_id
LEFT OUTER JOIN dbo.ses_lookup i with (nolock) on s.lookup_service_action_id = i.ses_lookup_id
LEFT OUTER JOIN dbo.ses_lookup j with (nolock) on s.lookup_service_status_id = j.ses_lookup_id
LEFT OUTER JOIN dbo.ses_lookup h with (nolock) on o.lookup_order_status_id = h.ses_lookup_id
where service_id in {0}
order by service_id, last_updated_date desc'''.format(ps), cnxn_0211)
    
    ses_df = pd.DataFrame(query)
    ses_df = ses_df.fillna('')
    
    ses_df['created_date'] = ses_df['created_date'].apply(lambda x: pd.to_datetime(x) if x != '' else '')
    ses_df['bill_start_date'] = ses_df['bill_start_date'].apply(lambda x: pd.to_datetime(x) if x != '' else '')
    ses_df['last_updated_date'] = ses_df['last_updated_date'].apply(lambda x: pd.to_datetime(x) if x != '' else '')
    
    #since it's ordered by service_id and last_updated_date desc, grab the first row and that will tell you the status
    #commented out because a disconnect can show up anywhere and we want to know that 
    #final = ses_df.groupby('service_id').nth(0)
    
    with pd.ExcelWriter(bc_prep_file, mode='a') as writer:
        ses_df.to_excel(writer, sheet_name='ses', index=False)
    
    return ses_df

#use the null latis active subset to check for disconnects in ses
#ses_df = pull_ses_df(null_latis_active_subset)
#assume that by checking active and prod kenan and latis for a 'kenan' value, if there isn't a value then it's a disconnect
possible_disconnect_subset = full_df.loc[(full_df['kenan'] == '') | (pd.isnull(full_df['kenan']) == True)]
ses_df = pull_ses_df(possible_disconnect_subset)

#if there is a ses order action = 'Disconnect' in the serv comp id group, then the status of that serv comp id should be Disconnect
ses_df_grouped = ses_df.groupby('service_id')

disconnect_scids = []

for scid, scid_df in ses_df_grouped: 
        
    stats = scid_df['SERVICE_ACTION'].tolist()
    
    if len(stats) > 0: 
    
        if 'Disconnect' in stats: 
            
            disconnect_scids.append(scid)
            
        
for scid in disconnect_scids: 
    
    full_df['status'] = np.where( (full_df['serv_comp_id'] == scid) & ( (full_df['status'] == '') | (pd.isnull(full_df['status']) == True) ), 'Cancel - Disconnect', full_df['status'] ) 



#add the status where ses order status is a disconnect
#full_df['status'] = np.where( full_df['ses order status'] == 'Disconnect', 'Cancel - Disconnect', full_df['status'])

#full_df['status'] = np.where( (full_df['ses order status'] != 'Disconnect') & ( (full_df['difference'] != 0) | full_df['difference'].isnull() ), 'Cancel for MRR Cost Change', full_df['status'])
full_df['status'] = np.where( (full_df['status'] != 'Cancel - Disconnect') & ( (full_df['difference'] != 0) | full_df['difference'].isnull() ) & ( (full_df['status'] == '') | (pd.isnull(full_df['status']) == True) ), 'Cancel for MRR Cost Change', full_df['status'])


#quantity checks: 

#same lookup table --> just hard code
qty_ref_df = pd.read_excel('Quantity Based Service Guide_my_copy.xlsx', sheet_name='Consolidated View')

with pd.ExcelWriter(bc_prep_file, mode='a') as writer:
    qty_ref_df.to_excel(writer, sheet_name='qty_ref', index=False)

#add a unique column to latis_active_df: 
latis_active_df['unique'] = latis_active_df['PRODUCT_CODE'].map(str) + '' + latis_active_df['PRCMP_CODE'].map(str) 
latis_active_df['qty'] = ''

latis_active_df = pd.merge(left = latis_active_df, right = qty_ref_df, left_on='unique', right_on='unique (product code + prcmp code)', how='left')
latis_active_df['qty'] = latis_active_df['Quantity Status']
latis_active_df = latis_active_df.iloc[:, :-qty_ref_df.shape[1]]

#update spreadsheet with new latis active tab
with pd.ExcelWriter(bc_prep_file, mode='a') as writer:
    latis_active_df.to_excel(writer, sheet_name='latis_active_updated', index=False)


latis_active_df_grouped = latis_active_df.groupby('COMPONENT_GRP_VAL')

comp_grp_val = []
scid_vals = []
comp_grp_val_value = []

for cgv, comp_grp_val_df in latis_active_df_grouped: 
    
    #if any quantity != Always = 1 --> need to change the status for all the scids in that group 
    
    qtys = comp_grp_val_df['qty'].tolist()
    scids = comp_grp_val_df['SCID'].tolist()
   
    if (len(qtys) != 0):
        
        group_qty_val = ''
        
        if 'Always > 1' in qtys: 
            group_qty_val = 'Always > 1'
            
        elif 'Could be > 1 or = 1' in qtys:
            group_qty_val = 'Could be > 1 or = 1'
            
        else:
            group_qty_val = 'Always = 1'

        for i in range(0, len(scids)): 
            
            scid_vals.append(scids[i])
            comp_grp_val.append(cgv)
            comp_grp_val_value.append(group_qty_val)


comp_grp_val_df = pd.DataFrame({'scid': scid_vals, 'comp_grp_val': comp_grp_val, 'comp_grp_val_vals': comp_grp_val_value})

qty_check_subset = full_df.loc[(full_df['component_id'] != 'EQ') & (full_df['component_id'] != 'IQ') & (full_df['component_id'] != 'LL')]

qty_check_subset = pd.merge(left = qty_check_subset, right = comp_grp_val_df, left_on='unique', right_on='scid', how='left')
qty_check_subset['qty'] = qty_check_subset['comp_grp_val_vals']
#drop the columns of latis_active_df
qty_check_subset = qty_check_subset.iloc[:, :-comp_grp_val_df.shape[1]]

for index, row in qty_check_subset.iterrows():
    
    full_df['qty'] = np.where( (full_df['unique'] == row['unique']) & (full_df['component_id'] != 'EQ') & (full_df['component_id'] != 'IQ') & (full_df['component_id'] != 'LL') , row['qty'], full_df['qty'])
    

#-----------add package details here-------------------#

#Checking for packages that do not have package piids
def get_packages_without_package_piids(month, day):
    
    query = pd.read_sql_query('''select *
  from ec.rr_scid scid
  join dbo.Rerates_All_Data_RAW r on r.PIID_MeID = scid.RR_PIID_MeID
  where serv_comp_id like ( 'pk%')
  and r.PRODUCT_INST_ID not like 'pk-%'
  and workflowstatus = 'in progress'
  and RerateMonth_Actual = ?--add reratemonth actual here
  and Invoice_Day = ?--add invoice day here''', cnxn_0043, params=(month, day))
    
    bad_pkg_df = pd.DataFrame(query)
    bad_pkg_df = bad_pkg_df.fillna('')

    return bad_pkg_df

bad_pkg_df = get_packages_without_package_piids(month, day)

#need to update the status for pk scids without a pk piid to be 'Cancel - PIID Mapping Issue - PK SCID does not have proper PK PIID'
if not bad_pkg_df.empty: 
    
    current_scids = full_df['serv_comp_id'].unique().tolist()
    
    for index, row in bad_pkg_df.iterrows(): 
        
        if row['scid'] in current_scids: 
            
            full_df['status'] = np.where( ( (full_df['status'] == '') | (pd.isnull(full_df['status']) == True) ) & (full_df['serv_comp_id'] == row['scid']), 'Cancel - PIID Mapping Issue - PK SCID does not have proper PK PIID',full_df['status'])


#now check for hidden packages 
hidden_pkg_subset = full_df[full_df['product_inst_id'].str.contains('PK-', case=False, na=False) == False]
comp_grp_val_id_no_pkg = hidden_pkg_subset['component_grp_val_id'].unique().tolist()

def get_hidden_packages(comp_grp_val_ids):
    
    ps = '(' + "'" + "','".join(str(x) for x in comp_grp_val_ids) + "'" + ')'
    
    query = pd.read_sql_query('''  select COMPONENT_GROUP_CD, COMPONENT_GRP_VAL, CUSTOMER_ACCT_ID, PRODUCT_ACCOUNT_ID, PKG_ACCT_ID
 from ctl.LATIS_PROD_R
 where COMPONENT_GRP_VAL in {0} -- place component grp val's here
 and PKG_ACCT_ID is not null'''.format(ps), cnxn_0043)
    
    hidden_df = pd.DataFrame(query)
    hidden_df = hidden_df.fillna('')

    return hidden_df

hidden_df = get_hidden_packages(comp_grp_val_id_no_pkg)

#need to update the status for pk scids without a pk piid to be 'Cancel - PIID Mapping Issue - service is an unidentified package'
if not hidden_df.empty: 
    
    for index, row in hidden_df.iterrows(): 
        
        if row['COMPONENT_GRP_VAL'] in comp_grp_val_id_no_pkg: 
            
            full_df['status'] = np.where( ( (full_df['status'] == '') | (pd.isnull(full_df['status']) == True) ) & (full_df['component_grp_val_id'] == row['COMPONENT_GRP_VAL']), 'Cancel - PIID Mapping Issue - service is an unidentified package',full_df['status'])


#3rd package step: 

#fill in 'pkg cgv' column 
full_df['pkg cgv'] = np.where( full_df['product_inst_id'].str.contains('PK-', case=False, na=False), full_df['product_inst_id'].str[3:], full_df['pkg cgv'])

#check 'PK-' piids            
pk_piid_subset = full_df[full_df['product_inst_id'].str.contains('PK-', case=False, na=False)]
pkg_cgvs = pk_piid_subset['pkg cgv'].unique().tolist()       
rr_piid_me_id = pk_piid_subset['rr_piid_meid'].unique().tolist()  

#to find the pkg cgvs, you must take the PK-XXXXXXXX piids and separate the PK- from the numbers. The numbers are the pkg cgvs. 
#This query is set up to subtract the monthly rev est from the piid current rate. However, if PK rerate is being reversed, the monthly rev est should match the piid new rate.

def get_pkg_status(pkg_cgvs, rr_piid_me_id):
    
    # ps_pkg_cgvs = '(' + "'" + "','".join(str(x) for x in pkg_cgvs) + "'" + ')'
    # ps_rr_piid_me_ids = '(' + "'" + "','".join(str(x) for x in rr_piid_me_id) + "'" + ')'
    
    ps_1 = ', '.join(['?'] * len(pkg_cgvs))
    ps_2 = ', '.join(['?'] * len(rr_piid_me_id))
    
    params = tuple(pkg_cgvs + rr_piid_me_id)
    
    query = pd.read_sql_query('''select COMPONENT_GRP_VAL, COMPONENT_GROUP_CD+COMPONENT_GRP_VAL as scid, BILL_ACCT_NBR, Inv_BAN, PIID_MeID, WorkflowStatus, PKG_DSC, MONTHLY_REV_EST, PIID_CURRENT_RATE, MONTHLY_REV_EST-PIID_CURRENT_RATE as DIFF, PIID_NEW_RATE
from ctl.LATIS_PROD_R p
left join dbo.rerates_all_data_raw r on right((product_inst_id),8) = p.COMPONENT_GRP_VAL
where COMPONENT_GRP_VAL in ({ps_1})--pkg cgv
and PIID_MeID in ({ps_2}) --place PIID MeIDs here'''.format(ps_1 = ps_1, ps_2 = ps_2), cnxn_0043, params = params)

    pkg_stat_df = pd.DataFrame(query)
    pkg_stat_df = pkg_stat_df.fillna('')
    
    return pkg_stat_df

pkg_stat_df = get_pkg_status(pkg_cgvs, rr_piid_me_id)

pkg_stat_df['pkg_status'] = ''

#in progress and diff = 0
pkg_stat_df['pkg_status'] = np.where( (pkg_stat_df['DIFF'] == 0) & (pkg_stat_df['WorkflowStatus'] == 'In Progress'), 'Proceed with Rerate', pkg_stat_df['pkg_status'])

with pd.ExcelWriter(bc_prep_file, mode='a') as writer:
    pkg_stat_df.to_excel(writer, sheet_name='pkg_status_df', index=False)

#reversal status, make diff 0
pkg_stat_df['pkg_status'] = np.where( (pkg_stat_df['DIFF'] != 0) & (pkg_stat_df['WorkflowStatus'] == 'Reversal') & (pkg_stat_df['MONTHLY_REV_EST'] == pkg_stat_df['PIID_NEW_RATE']), 'Proceed with Reversal', pkg_stat_df['pkg_status'])
pkg_stat_df['DIFF'] = np.where( (pkg_stat_df['pkg_status'] == 'Proceed with Reversal') & (pkg_stat_df['DIFF'] != 0) & 
                               (pkg_stat_df['WorkflowStatus'] == 'Reversal') & (pkg_stat_df['MONTHLY_REV_EST'] == pkg_stat_df['PIID_NEW_RATE']), 
                               'Proceed with Reversal', pkg_stat_df['pkg_status'])

#in progress amd diff != 0
in_progress_nonzero_diff_subset = pkg_stat_df[(pkg_stat_df['WorkflowStatus'] == 'In Progress') & (pkg_stat_df['DIFF'] != 0)]

check_inv_bans = in_progress_nonzero_diff_subset['Inv_BAN'].unique().tolist()

#get the name abbreviation of the month
def get_date_abbrev(month):
    
    data = {'num': [1,2,3,4,5,6,7,8,9,10,11,12], 'code': ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']}
    df = pd.DataFrame(data)
    code = df[df['num'] == month]['code'].unique().tolist()[0]
    
    return code


def get_pkg_invoice(check_inv_bans, invoice_month):
    
    invoice_date = '1-' + invoice_month + '-2025'
        
    ps_1 = ','.join(['?'] * len(check_inv_bans))
    s = "'" + "','".join(str(x) for x in check_inv_bans) + "'"
    
    query = pd.read_sql_query('''select
decode(bid.billing_level,0,'ACCOUNT','SERVICE') as billing_level,
bi.prep_date,
bi.statement_date,
bi.prep_status,
ciam.external_id bus_org_id,
cmf.bill_company,
eiam.external_id as ban,
to_char(bid.from_date, 'mm/dd/yyyy') from_date,
to_char(bid.to_date,'mm/dd/yyyy') to_date,
pdv.display_value Package,
d.description_text,
rcv.short_display currency,
bid.amount/100 amount,
eiem1.external_id primary_id,
eiem5.external_id piid,
sed.param_value package_id,
bid.type_code,
bid.annotation,
bid.discount,
bid.discount_id,
 bid.subtype_code
from
cmf
left join bill_invoice bi on cmf.account_no = bi.account_no
left join bill_invoice_detail bid on bi.bill_ref_no = bid.bill_ref_no
left join external_id_acct_map eiam on eiam.account_no = bi.account_no and eiam.external_id_type = 150
left join customer_id_acct_map ciam on ciam.external_id_type = 80 and ciam.is_current = 1 and ciam.account_no = bi.account_no
left join rate_currency_values rcv on rcv.currency_code = bid.rate_currency_code and rcv.language_code = 1
left join descriptions d on d.description_code = bid.description_code and d.language_code = 1
left join package_definition_values pdv on pdv.package_id = bid.package_id and pdv.language_code = 1
left join external_id_equip_map eiem1 on eiem1.external_id_type = 1 and eiem1.subscr_no = bid.subscr_no and eiem1.subscr_no_resets = bid.subscr_no_resets
left join external_id_equip_map eiem5 on eiem5.external_id_type = 5 and eiem5.subscr_no = bid.subscr_no and eiem5.subscr_no_resets = bid.subscr_no_resets
left join service s on s.subscr_no = bid.subscr_no and s.subscr_no_resets = bid.subscr_no_resets
left join service_ext_data sed on sed.view_id = s.view_id and sed.param_id = '742' -- Package
where 1=1
and bi.statement_date >= {d} --need this to be most recent, so if it's a June bill cycle, the May invoices have come out already, so use May
and bid.package_id not in (1111)
and sed.param_value is not null
and bid.subtype_code >= 0
and eiam.external_id in ({ps_1}) -- Inv account number/Invoice ban column
and type_code != 5
and prep_status = 1 --1 for pre checks, 4 for post checks                              
'''.format(d =  "'" + invoice_date + "'", ps_1 = s), oconn_kenan)

    pkg_invoices_df = pd.DataFrame(query)
    pkg_invoices_df = pkg_invoices_df.fillna('')
    
    return pkg_invoices_df

#if june, want may
invoice_month = get_date_abbrev(month - 1)
pkg_invoices_df = get_pkg_invoice(check_inv_bans, invoice_month)


if not pkg_invoices_df.empty: 

    #want to get the sum of amount for each package id
    pkg_invoices_df_pivot = pkg_invoices_df.groupby('PACKAGE_ID')['AMOUNT'].sum()
    pkg_invoices_df_pivot_df = pd.DataFrame({"PACKAGE_ID": pkg_invoices_df_pivot.index, "AMOUNT": pkg_invoices_df_pivot.values})


    for index, row in pkg_invoices_df_pivot_df.iterrows():
    
        pkg_stat_df['pkg_status'] = np.where( (pkg_stat_df['scid'] == row['PACKAGE_ID']) & ( pkg_stat_df['PIID_CURRENT_RATE'] == row['AMOUNT'] ), 'Proceed with Rerate', pkg_stat_df['pkg_status'])
        pkg_stat_df['pkg_status'] = np.where( (pkg_stat_df['scid'] == row['PACKAGE_ID']) & ( pkg_stat_df['PIID_CURRENT_RATE'] != row['AMOUNT'] ) 
                                         & ( pkg_stat_df['MONTHLY_REV_EST'] != row['AMOUNT'] ), 'Cancel for MRR Cost Change', pkg_stat_df['pkg_status'])

#update spreadsheet with new pkg status tab
with pd.ExcelWriter(bc_prep_file, mode='a') as writer:
    pkg_stat_df.to_excel(writer, sheet_name='pkg_status', index=False)
    

full_df = pd.merge(left = full_df, right = pkg_stat_df, left_on='pkg cgv', right_on='COMPONENT_GRP_VAL', how='left')
full_df['pkg description'] = full_df['PKG_DSC']
full_df['pkg status'] = full_df['pkg_status']
full_df = full_df.iloc[:, :-pkg_stat_df.shape[1]]

full_df['status'] = np.where( ( (full_df['status'] == '') | (pd.isnull(full_df['status']) == True) ) & ( (full_df['pkg status'] != '') | (pd.isnull(full_df['pkg status']) == False) ), full_df['pkg status'] ,full_df['status'])

#------------------------------------------------------#


#pull r4 term data
    
def pull_r4_term_end_dates(df): 
    
    scids = df['serv_comp_id'].unique().tolist()
    
    ps = '(' + "'" + "','".join(str(x) for x in scids) + "'" + ')'
    
    query = pd.read_sql_query('''select distinct Circuit_SCID, Circuit_PIID, null as MA_PIID, p.R4_TermEndDate
  FROM [FA_Margin].[R4].[CIRCUITS_COMPONENT] c
  left join [FA_Margin].[R4].[CIRCUITS] p on p.PRODUCT_INST_ID = c.Circuit_PIID
  where Circuit_SCID in {0} --place serv_comp_ids here
  order by Circuit_PIID, p.R4_TermEndDate desc'''.format(ps), cnxn_0043)
    
    r4_ted = pd.DataFrame(query)
    r4_ted = r4_ted.fillna('')
    
    r4_ted['R4_TermEndDate'] = r4_ted['R4_TermEndDate'].apply(lambda x: pd.to_datetime(x) if x != '' else '')
    
    return r4_ted

r4_ted = pull_r4_term_end_dates(full_df)

#need to fix ma piid to be prod inst id on the scid
r4_ted = pd.merge(left = r4_ted, right = full_df, left_on='Circuit_SCID', right_on='serv_comp_id', how='left')
r4_ted['MA_PIID'] = r4_ted['product_inst_id']
r4_ted = r4_ted.iloc[:, :-full_df.shape[1]]

#need to sort by MA_PIID and R4_TermEndDate desc becuase it's returning multiple term end dates for each MA_PIID
r4_ted = r4_ted.sort_values(['MA_PIID', 'R4_TermEndDate'], ascending=[True, False]).drop_duplicates('MA_PIID')


with pd.ExcelWriter(bc_prep_file, mode='a') as writer:
    r4_ted.to_excel(writer, sheet_name='r4_ted', index=False)

#now vlookup on ma piid to prod inst id
full_df = pd.merge(left = full_df, right = r4_ted, left_on='product_inst_id', right_on='MA_PIID', how='left')
full_df['r4 term end date'] = full_df['R4_TermEndDate']
# = full_df.drop(['SCID', 'rate'], axis=1)
full_df = full_df.iloc[:, :-r4_ted.shape[1]]


#pull ctd tem data
def pull_ctd_term_end_dates(df): 
    
    piids = df['product_inst_id'].unique().tolist()
    
    ps = '(' + "'" + "','".join(str(x) for x in scids) + "'" + ')'
    
    query = pd.read_sql_query('''SELECT  COMPONENT_GROUP_CD+SCID as SCID, piid, null as MA_PIID, Service_End_Date
FROM    ctd.LATIS_contract_term_date_working
where PIID in {0}--place product_inst_ids here
order by piid, Service_End_Date desc'''.format(ps), cnxn_0043)
    
    ctd_ted = pd.DataFrame(query)
    ctd_ted = ctd_ted.fillna('')
    
    ctd_ted['Service_End_Date'] = ctd_ted['Service_End_Date'].apply(lambda x: pd.to_datetime(x) if x != '' else '')
    
    return ctd_ted

ctd_ted = pull_ctd_term_end_dates(full_df)

#need to fix ma piid to be prod inst id on the piid
ctd_ted = pd.merge(left = ctd_ted, right = full_df, left_on='SCID', right_on='serv_comp_id', how='left')
ctd_ted['MA_PIID'] = ctd_ted['product_inst_id']
ctd_ted = ctd_ted.iloc[:, :-full_df.shape[1]]

with pd.ExcelWriter(bc_prep_file, mode='a') as writer:
    ctd_ted.to_excel(writer, sheet_name='ctd_ted', index=False)

#now vlookup on ma piid to prod inst id
full_df = pd.merge(left = full_df, right = ctd_ted, left_on='product_inst_id', right_on='MA_PIID', how='left')

#don't overwrite r4 term end dates
full_df['r4 term end date'] = np.where( ( (full_df['r4 term end date'] == '') |  (pd.isnull(full_df['r4 term end date']) == True) ), full_df['Service_End_Date'], full_df['r4 term end date']) 
full_df = full_df.iloc[:, :-ctd_ted.shape[1]]

#fix the formatting problem
full_df['r4 term end date'] = full_df['r4 term end date'].apply(lambda x: pd.to_datetime(x) if x != '' else '')


#need to check for cancels becuase of being in term --> also need to apply some rules
#if the 'r4 term end date' is past current year and month, then it's status should be 'Cancel - Circuit in term'
#UNLESS: subprojname = 'In Term TDM Cost Increase Pass Thru' --> should never get canceled
#UNLESS: if subprojname = 'Offnet TDM Discontinuance' and 'business_unit' = 'WHOLESALE' and cust_contract_term_end_dt > 'r4 term end date', --> do not cancel
#UNLESS: if subprojname = 'Offnet TDM Discontinuance' and 'business_unit' = 'WHOLESALE' and cust_contract_term_end_dt < 'r4 term end date', --> cancel

check_date = datetime(year, month, day)

in_term_subset = full_df.loc[(full_df['r4 term end date'] == '') | (pd.isnull(full_df['r4 term end date']) == True)]

for index, row in in_term_subset.iterrows():
    
    #possibly in term
    if row['r4 term end date'] > check_date: 
        
        if (row['subprojname'] != 'In Term TDM Cost Increase Pass Thru'):
           
            if (row['subprojname'] == 'Offnet TDM Discontinuance') & (row['business_unit'] == 'WHOLESALE') & (row['cust_contract_term_end_dt'] < row['r4 term end date']):
            
                full_df['status'] = np.where( full_df['unique'] == row['unique'], 'Cancel - Circuit in term', full_df['status'])
            


#-----------add service element id here-------------------#

#get missing service element ids
no_seid_subset = full_df[ (full_df['service_element_id'] == '') | (pd.isnull(full_df['service_element_id']) == True) ]
no_seid_comp_grp_vals = no_seid_subset['component_grp_val_id'].unique().tolist()
no_seid_components = no_seid_subset['component'].unique().tolist()

def get_missing_attributes(no_seid_comp_grp_vals, no_seid_components): 
    
    ps_1 = ', '.join(['?'] * len(no_seid_comp_grp_vals))
    ps_2 = ', '.join(['?'] * len(no_seid_components))
    
    s_1 = "'" + "','".join(str(x) for x in no_seid_comp_grp_vals) + "'"
    s_2 = "'" + "','".join(str(x) for x in no_seid_components) + "'"
        
    query = pd.read_sql_query('''select distinct
APP.APPLICATION_CODE
, APP.APPLICATION_CD_DSC
, C.OCCURRENCE_NUMBER
,C.PRODUCT_ACCOUNT_ID
, C.COMMON_LITEL_CKT --same as 'prod inst id'
, C.COMPONENT_GROUP_CD
, CG.CMP_GROUP_CODE_DSC
, C.COMPONENT_GRP_VAL --same as 'unique'
, PA.CUSTOMER_ACCT_ID
--, CAI.CUSTOMER_NM
, CTD.CONTRACT_DATE
, CTD.TERM_LENGTH
, CTD.TERMINATION_DATE
, PD.PRCMP_ID
, PAC.PRCMP_CODE
, PD.PRCMP_DSC
, PD.BILLABLE_IND
, PAC.PRCMP_OVERRIDE_DT
, PAC.PROMOTION_BGN_DATE
, PAC.PROMOTION_END_DATE
, PAC.BILL_EFF_BGN_DATE
, PAC.BILL_EFF_END_DATE
, PAC.BILLED_THRU_DATE
, PAC.MONTHLY_REV_EST
, SE.SERVICE_ELEMENT_ID
from
PRD_ACCT_CMP_GRP c
left join prd_acct_contract ctd on CTD.PRODUCT_ACCOUNT_ID = C.PRODUCT_ACCOUNT_ID
left join service_element se on SE.COMPONENT_GRP_VAL = C.COMPONENT_GRP_VAL and SE.COMPONENT_GROUP_CD = C.COMPONENT_GROUP_CD
left join CORP.PRD_ACCT_CMP pac on PAC.PRODUCT_ACCOUNT_ID = C.PRODUCT_ACCOUNT_ID and PAC.OCCURRENCE_NUMBER = C.OCCURRENCE_NUMBER  
left join CORP.ORD_SERVICE_ELMT_ATTR sae on SAE.SERVICE_ELMT_ID = SE.SERVICE_ELEMENT_ID
left join CORP.ATTRIBUTE_DEF a on A.ATTRIBUTE_ID = sae.ATTRIBUTE_ID 
left join CORP.PRODUCT_ACCT pa on PA.PRODUCT_ACCOUNT_ID = C.PRODUCT_ACCOUNT_ID
left join CORP.CUSTOMER_ACCT_INFO cai on cast(CAI.CUSTOMER_ACCT_ID as varchar(255)) = cast(PA.CUSTOMER_ACCT_ID as varchar(255))
--left join circuits ckt on ckt.litel_circ_id = C.COMMON_LITEL_CKT
left join CORP.APPLICATION app on APP.APPLICATION_CODE = c.APPLICATION_CODE
left join corp.PRCMP_DEF pd on PD.PRCMP_CODE = PAC.PRCMP_CODE
left join corp.COMPONENT_GROUP cg on CG.COMPONENT_GROUP_CD = c.COMPONENT_GROUP_CD
left join PRD_COMB PC on pc.prcmp_id = PD.PRCMP_ID
left join PRODUCT P on PC.PRD_ID = P.PRODUCT_ID
where
C.COMPONENT_GRP_VAL in ({ps_1}) --place component_grp_val_ids here
and PD.PRCMP_DSC in ({ps_2})  --place components here
and PAC.BILL_EFF_END_DATE is null'''.format(ps_1 = s_1, ps_2 = s_2), oconn_latis)

    seid_df = pd.DataFrame(query)
    seid_df = seid_df.fillna('')
    
    return seid_df

seid_df = get_missing_attributes(no_seid_comp_grp_vals, no_seid_components)

#make unique column 
seid_df['seid_unique'] = ''
seid_df['seid_unique'] = seid_df['COMPONENT_GROUP_CD'].map(str) + '' + seid_df['COMPONENT_GRP_VAL'].map(str) + '' + seid_df['PRCMP_DSC'].map(str)

with pd.ExcelWriter(bc_prep_file, mode='a') as writer:
    seid_df.to_excel(writer, sheet_name='seid', index=False)
    

#fill in the seid and prod cmpnt cd in full_df
full_df = pd.merge(left = full_df, right = seid_df, left_on='unique', right_on='seid_unique', how='left')
full_df['service_element_id'] = full_df['SERVICE_ELEMENT_ID']
full_df['prod_cmpnt_cd'] = full_df['PRCMP_CODE']
full_df = full_df.iloc[:, :-seid_df.shape[1]]
    

#now check the pre billed indicator
full_df['pre_billed_ind'] = np.where( ( (full_df['pre_billed_ind'] == '') | (pd.isnull(full_df['pre_billed_ind']) == True) | (full_df['pre_billed_ind'] == 'N') ) 
                                     & (full_df['promo_code'].str.contains('PKG', case=False, na=False) == False), 'Y', full_df['pre_billed_ind'])

full_df['pre_billed_ind'] = np.where( ( (full_df['pre_billed_ind'] == '') | (pd.isnull(full_df['pre_billed_ind']) == True) | (full_df['pre_billed_ind'] == 'N') ) 
                                     & (full_df['promo_code'].str.contains('PKG', case=False, na=False)), 'N', full_df['pre_billed_ind'])


#------------------------------------------------------#


#make sure the piid level has the correct status

full_df['piid level status'] = ''

full_df_grouped = full_df.groupby('product_inst_id')

for piid, piid_df in full_df_grouped: 
    
    scid_stats = piid_df['status'].unique().tolist()

    #go through the order of stats; if a high level stat exists in the piid list, then the big piid status gets that high level status
    
    if len(scid_stats) > 0: 
        
        if 'Cancel - Disconnect' in scid_stats: 
            
            full_df['piid level status'] = np.where( full_df['product_inst_id'] == piid , 'Cancel - Disconnect', full_df['piid level status'])
            
        elif 'Move back to complete - Reversal cannot be completed due to disconnect' in scid_stats: 
            
            full_df['piid level status'] = np.where( full_df['product_inst_id'] == piid , 'Move back to complete - Reversal cannot be completed due to disconnect', full_df['piid level status'])

        elif 'Cancel for MRR Cost Change' in scid_stats:
            
            full_df['piid level status'] = np.where( full_df['product_inst_id'] == piid , 'Cancel for MRR Cost Change', full_df['piid level status'])
        

#will have to delete the piid level status later but for now it is helpful for debugging

#need to watch out for duplicate rows

with pd.ExcelWriter(bc_prep_file, mode='a') as writer:
    full_df.to_excel(writer, sheet_name='rr_scid', index=False)