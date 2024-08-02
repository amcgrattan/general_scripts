# print("here")

# import pandas as pd
# import pyodbc
# import bcpy
# import os 
# import numpy as np
# import csv

# print("works here")

# shell_df = pd.DataFrame(columns=['test', 'BILLING_LEVEL', 'PREP_DATE', 'STATEMENT_DATE', 'PREP_STATUS', 'BUS_ORG_ID', 'BILL_COMPANY', 'BAN', 'FROM_DATE', 'TO_DATE', 'PACKAGE', 'DESCRIPTION_TEXT', 'CURRENCY', 'AMOUNT', 'PRIMARY_ID', 
#     'PIID', 'PACKAGE_ID', 'TYPE_CODE', 'ANNOTATION', 'DISCOUNT', 'DISCOUNT_ID', 'SUBTYPE_CODE'])


# # lista = [['SERVICE','Timestamp(2024-06-10 20:37:53)',
# #                    'Timestamp(2024-06-08 00:00:00)',1,'3-K5DFJK4SV8','LIVINGSTON LODGING',
# #                    '90103153','05/08/2024','06/08/2024','CenturyLink Master Services Agreement',
# #                    'Fiber+ Enterprise Voice and Data (Broadsoft)**Q.ADVAN M','USD',0.0,'PK20764942',
# #                    '154896719','PK20764942',2,0,0,0.0,22237639 ]]

# test_data = ['0','SERVICE','45328.6941782407','45326','1','3-A90024','METROPOLITAN TEL DBA MANHATTAN TEL','30750037','45326','45355','Wholesale IP Solutions','Loop**WHSL IP','USD','416.1','LL19508714','150904652','0','2','0','0','0','22236965']

# shell_df.loc[len(shell_df)] = test_data
# print(shell_df)
# # df.fillna(0, inplace=True)
# # df = df.astype(str)

# # #df.to_csv('upload_test.csv', index=True)

# # print(df)

# # server = 'USIDCVSQL1009' 
# # database = 'FA_Margin' 
# # username = 'SSRSAccount' 
# # password = '$$R$Account@2024T'
# # cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)

# # cursor = cnxn.cursor()



# # sql_config = {
# #                 'server': 'USIDCVSQL1009',
# #                 'database': 'FA_Margin',
# #                 'username': 'SSRSAccount',
# #                 'password': '$$R$Account@2024T'
# #             }

# # curr_dir = os.getcwd()
# # dir_list = os.listdir(curr_dir)


# # flat_file = bcpy.FlatFile(qualifier='', path='C:/Users/AD33424/source/repos/PythonApplication2/PythonApplication2/oracle_data_0.csv')
# # sql_table = bcpy.SqlTable(sql_config, schema_name='RDCOM', table='green_5_6_7_rerate_invoice_data')
# # #flat_file.to_sql(sql_table)
# # print(type(flat_file))


# # for file in dir_list: 
# #     #upload_dfs = []
# #     if "oracle_data_" in file: 
# #         print(file)
# #         count = 0
# #         for chunk in pd.read_csv(file, chunksize=1000):
                
# #             chunk = chunk.astype(str)
# #             print(chunk.head(10))
            
# #             #upload_dfs.append(chunk)
        
# #             print("upload time at ", count, "step")
# #             count += 1
# #             try: 
# #                 #file_path = os.path.join(os.getcwd(), 'upload_test.csv')
# #                 #print("file path")
# #                 #print(file_path)
# #                 #c = bcpy.FlatFile(qualifier='', path=file_path)
# #                 bdf = bcpy.DataFrame(chunk)
# #             except Exception as e: 
# #                 print("df to bdf conversion fail")
# #                 print(e)
            
# #             try: 
# #                 sql_table = bcpy.SqlTable(sql_config, schema_name='RDCOM', table='green_5_6_7_rerate_invoice_data')
# #             except Exception as e: 
# #                 print("sql config fail")
# #                 print(e)
# #             try: 
# #                 #.to_sql(sql_table)
# #                 bdf.to_sql(sql_table, use_existing_sql_table = True)
# #             except Exception as e: 
# #                 print("df to_sql upload fail")
# #                 print(e)
 
# # for file in dir_list:
# #     count = 0
# #     if "oracle_data_0" in file:
# #         for chunk in pd.read_csv(file, chunksize=1000):
# #             if count == 0: 
# #                 shell_df.append(chunk)
# #             count += 1

# # print(shell_df)
# # shell_df.to_sql('green_5_6_7_rerate_invoice_data', cnxn, schema = 'RDCOM')



# # count = 0
# # for df in upload_dfs: 
# #     print("upload time at ", count, "step")
# #     count += 1
# #     try: 
# #         #file_path = os.path.join(os.getcwd(), 'upload_test.csv')
# #         #print("file path")
# #         #print(file_path)
# #         #c = bcpy.FlatFile(qualifier='', path=file_path)
# #         bdf = bcpy.DataFrame(df)
# #     except Exception as e: 
# #         print("df to bdf conversion fail")
# #         print(e)
            
# #     try: 
# #         sql_table = bcpy.SqlTable(sql_config, schema_name='RDCOM', table='green_5_6_7_rerate_invoice_data_test')
# #     except Exception as e: 
# #         print("sql config fail")
# #         print(e)
# #     try: 
# #         #.to_sql(sql_table)
# #         bdf.to_sql(sql_table, use_existing_sql_table = False)
# #     except Exception as e: 
# #         print("df to_sql upload fail")
# #         print(e)

# # Define your SQL Server connection details
# server = 'usidcvsql1009'
# database = 'fa_margin'
# username = 'AD33424'
# password = 'R3dC!ayF1$h99?'
# schema = 'RDCOM'
# driver = '{ODBC Driver 17 for SQL Server}'  # Change the driver if needed

# # # Define the path to your CSV file
# csv_file_path = 'C:/Users/AD33424/source/repos/PythonApplication2/PythonApplication2/oracle_data_0.csv'

# # # Define the target table name
# table_name = 'green_5_6_7_rerate_invoice_data'

# # # Establish a connection to SQL Server
# conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};SCHEMA={schema};UID={username};PWD={password}"
# cnxn = pyodbc.connect(conn_str, autocommit=True)

# # # Read the CSV file
# with open(csv_file_path, 'r') as f:
#     reader = csv.reader(f)
#     columns = next(reader)  # Get column names
#     columns = [x.strip() for x in columns]  # Remove extra spaces
#     print(type(columns))
#     print(columns)
#     columns[0] = 'test'
#     print(columns)
#     count = 0
#     for row in reader:
#         if count < 5: 
#             print(row)
#         count += 1

#     # Prepare the SQL query for bulk insert
#     #query = '''BULK INSERT {table_name} ({",".join(columns)}) FROM ? WITH (FORMAT = "CSV", FIRSTROW = 2)'''
#     query = '''BULK INSERT green_5_6_7_rerate_invoice_data (",".join(columns)) FROM 'C:/Users/AD33424/source/repos/PythonApplication2/PythonApplication2/oracle_data_0.csv' WITH (FORMAT = "CSV", FIRSTROW = 2)'''
#     # Execute the bulk insert query
#     query_2 = '''bulk insert FA_Margin.RDCOM.green_5_6_7_rerate_invoice_data FROM 'C:/Users/AD33424/source/repos/PythonApplication2/PythonApplication2/oracle_data_0.csv' WITH (FIRSTROW = 2,FIELDTERMINATOR = ',',ROWTERMINATOR='\n' )'''
#     cursor = cnxn.cursor()
#     try:
#         print(query)
#         #cursor.execute(query, csv_file_path)
#         cursor.execute(query_2)
#     except Exception as e:
#         print("problem: ")
#         print(e)

# # Print a success message
# print(f"Data from {csv_file_path} has been successfully inserted into {table_name}.")
     


# # sql_config = {
# #     'server': 'USIDCVSQL1009',
# #     'database': 'FA_Margin',
# #     'schema_name': 'RDCOM',
# #     'username': 'SSRSAccount',
# #     'password': '$$R$Account@2024T'
# # }
# # table_name = 'green_5_6_7_rerate_invoice_data'

# # flat_file = bcpy.FlatFile(qualifier='', path='C:/Users/AD33424/source/repos/PythonApplication2/PythonApplication2/oracle_data_0.csv')
# # sql_table = bcpy.SqlTable(sql_config, table='green_5_6_7_rerate_invoice_data', schema='RDCOM')
# # flat_file.to_sql(sql_table)
# #'C:\Users\AD33424\source\repos\PythonApplication2\PythonApplication2\oracle_data_0.csv'
# #'C:/Users/AD33424/source/repos/PythonApplication2/PythonApplication2/oracle_data_0.csv'
# #flat_file = bcpy.FlatFile(qualifier='', path='C:/Users/AD33424/source/repos/PythonApplication2/PythonApplication2/oracle_data_0.csv')
# # bdf = bcpy.DataFrame(shell_df)
# # sql_table = bcpy.SqlTable(sql_config, table=table_name, schema_name='RDCOM')
# # print(bdf)
# # bdf.to_sql(sql_table)

import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote
from glob import glob
 
 
print("hdhdh")

try: 
    engine = create_engine('mssql+pymssql://SSRSAccount:%s@USIDCVSQL1009/FA_Margin' % quote('$$R$Account@2024T'))
    conn = engine.connect()

    print("hi")
 
    allcsv = [a for a in glob("test_1_green_bans*")]
    print(allcsv)
except Exception as e:
    print(e)
 
#df = pd.read_csv('oracle_data_0.csv')
 
# def csv_upload(csv_list):
#     for l in csv_list:
#         df = pd.read_csv(l)
#         df = df.rename(columns={"Unnamed: 0": "test"})
#         df.to_sql(name="green_5_6_7_rerate_invoice_data", schema='RDCOM', con=conn, index=False, if_exists="append", method="multi", chunksize = 1000)