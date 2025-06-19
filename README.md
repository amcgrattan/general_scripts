# general_scripts

mtm_forecast_comparison.py --> used for generating the forecast comparison report. need to change the prev and current months

pre_check_data.py --> used to generate validator update file and bc prep file. need to change month and day 

shellie_full_process_automation.py --> used for the offnet tdm data. takes in ecckt file from Alex. need to change table name on line 143 for current dbo.T_STG_NETEX_AIM_2025xx. 
ex: if month is June, then the table name to dbo.T_STG_NETEX_AIM_202505 becasue prev month was 05
