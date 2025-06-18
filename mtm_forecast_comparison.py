import pandas as pd
import pyodbc
import os
import numpy as np
import cx_Oracle
from datetime import datetime
from dateutil.relativedelta import relativedelta

cwd = os.getcwd()
print(cwd)
    
server_0043 = 'USIDCVSQL0043' 
database_0043 = 'FA_Margin' 
username_0043 = 'SSRSAccount' 
password_0043 = '$$R$Account@2025-000'
cnxn_0043 = pyodbc.connect('DRIVER={SQL Server};SERVER='+server_0043+';DATABASE='+database_0043+';UID='+username_0043+';PWD='+ password_0043)
cursor_0043 = cnxn_0043.cursor()

#modify the months here
past_month = 'may'
current_month = 'june'
output_file = past_month + '_vs_' + current_month + '_forecast.xlsx'

#the most recent run should be saved on the 28th of the month
def pull_past_month():
        
    query = pd.read_sql_query('''select initiative, rerate_type, month(rerate_month) as rerate_month, sum(increase) as sum_increase from [EC].[HIST_RERATE_CHURN_MISR_FRONTIER]
where year(rerate_month) = '2025'
and FINALIZED_DATE = (select max(finalized_date) from [FA_Margin].[EC].[HIST_RERATE_CHURN_MISR_FRONTIER])
group by initiative, rerate_type, month(rerate_month)
order by initiative, rerate_type, month(rerate_month) desc''', cnxn_0043)
    
    past_month_df = pd.DataFrame(query)
    past_month_df = past_month_df.fillna('')

    return past_month_df

def pull_current_month():
        
    query = pd.read_sql_query('''select initiative, rerate_type, month(rerate_month) as rerate_month, sum(increase) as sum_increase from [EC].[RERATE_CHURN_MISR_FRONTIER]
where year(rerate_month) = '2025'
group by initiative, rerate_type, month(rerate_month)
order by initiative, rerate_type, month(rerate_month) desc''', cnxn_0043)
    
    current_month_df = pd.DataFrame(query)
    current_month_df = current_month_df.fillna('')

    return current_month_df

#upddate output file 

#uncomment for real run
past_month_df = pull_past_month()
past_month_df.to_excel(output_file, sheet_name=past_month, index=False)

current_month_df = pull_current_month()
with pd.ExcelWriter(output_file, mode='a') as writer:
    current_month_df.to_excel(writer, sheet_name=current_month, index=False)


#now make comparison tab
compare_df = past_month_df
compare_concat = compare_df['initiative'].map(str) + '' + compare_df['rerate_type'].map(str) + '' + compare_df['rerate_month'].map(str)

compare_insert_index = compare_df.columns.get_loc('rerate_month')
compare_df.insert(loc = compare_insert_index + 1, column = 'concat', value = compare_concat) 

past_month_increase = past_month + '_sum_increase'
compare_df.rename(columns={'sum_increase': past_month_increase}, inplace=True)

current_concat = current_month_df['initiative'].map(str) + '' + current_month_df['rerate_type'].map(str) + '' + current_month_df['rerate_month'].map(str)
current_insert_index = current_month_df.columns.get_loc('rerate_month')
current_concat_name = current_month + '_concat'
current_month_df.insert(loc = current_insert_index + 1, column = current_concat_name, value = current_concat) 

current_month_df.rename(columns={'initiative': 'c_initiative', 'rerate_type': 'c_rerate_type', 'rerate_month': 'c_rerate_month', 'sum_increase': 'c_sum_increase'}, inplace=True)

new_curr_increase_name = current_month + '_sum_increase'
compare_df[new_curr_increase_name] = ''
compare_df = pd.merge(left = compare_df, right = current_month_df, left_on='concat', right_on=current_concat_name, how='left')
compare_df[new_curr_increase_name] = compare_df['c_sum_increase']

compare_df = compare_df.iloc[:, :-current_month_df.shape[1]]

#now that the main columns have been set up, add in percent_diff, over 20% increase, and over 20% decrease
compare_df['percent_diff'] = ''
compare_df['over 20% increase'] = ''
compare_df['over 20% decrease'] = ''

for index, row in compare_df.iterrows():
    
    if (row[past_month_increase] != '') & (pd.isnull(row[past_month_increase]) == False): 
        
        if (row[past_month_increase] != 0) & (row[new_curr_increase_name] != '') & (pd.isnull(row[new_curr_increase_name]) == False): 
            
            compare_df.at[index,'percent_diff'] = ((row[new_curr_increase_name] - row[past_month_increase])/row[past_month_increase])*100
            
        else:
            
            past_month_df.at[index,'percent_diff'] = None


for index, row in compare_df.iterrows():
    
    if (row['percent_diff'] != None) & (row['percent_diff'] != ''):
        
        if float(row['percent_diff']) > 20:
            compare_df.at[index,'over 20% increase'] = 1
        else: 
            compare_df.at[index,'over 20% increase'] = 0
        if float(row['percent_diff']) < -20:
            compare_df.at[index,'over 20% decrease'] = 1
        else: 
            compare_df.at[index,'over 20% decrease'] = 0



with pd.ExcelWriter(output_file, mode='a') as writer:
    compare_df.to_excel(writer, sheet_name='compare', index=False)