import jaydebeapi
import os
import gspread
import logging
import pandas as pd
import numpy as np
import traceback
import sys
import re
import datetime
import smtplib
import snowflake.connector
from snowflake.connector import cursor
from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from snowflake.connector.pandas_tools import pd_writer 
from dotenv import load_dotenv
from googleapiclient.discovery import build
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
load_dotenv(dotenv_path='config_file.env')
logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)
logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s',
                    filename=os.path.join('.log', 'npi_load.log'),
                    level=logging.INFO)
def add_stdout():
    root = logging.getLogger()
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(threadName)s %(levelname)s:%(message)s')
    handler.setFormatter(formatter)
    root.addHandler(handler)

#Contains all snowflake, google and denodo connections    
def get_snowflake_conn():
    engine = create_engine(URL(
         user=os.getenv('SNOWFLAKE_USER')
        ,password=os.getenv('SNOWFLAKE_PASSWORD')
        ,account=os.getenv('SNOWFLAKE_ACCOUNT')
        ,database=os.getenv('SNOWFLAKE_DATABASE')
        ,schema=os.getenv('SNOWFLAKE_SCHEMA')
    ))
    return engine.connect()

def insert_to_snowflake(df, table_name):
    # Dataframe has be upper case before snowflake insertion
    df.columns = map(str.upper, df.columns)
    df.to_sql(table_name , con = get_snowflake_conn(), index = False, if_exists = 'append', method = pd_writer)

def send_email(npi_dict):    
    FROM = 'EMAIL_FROM'
    TO = os.getenv('EMAIL_TO_EMAIL') #insert your email
    SUBJECT = 'NPI Run Complete'
    file_num=len(npi_dict)
    skus=""
    for key in npi_dict:
        skus=skus+str(key) + " "
    TEXT = 'Please find npi files in data science snowflake sandbox, total '+str(file_num) +' files processed. The SKUs are: ' + skus
    # Prepare actual message
    message = """From: %s\nTo: %s\nSubject: %s\n\n%s
    """ % (FROM, TO, SUBJECT, TEXT)

    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.ehlo()
    server.starttls()
    server.login('ada@logitech.com', os.getenv('EMAIL_FROM_PWD'))#read it from configuration file
    server.sendmail(FROM, TO, message)
    server.close()

try: 
    logging.info("Executing NPI".format(datetime.datetime.now()))

    path = "/home/datainsights/denodo-vdp-jdbcdriver.jar" 
    #"./denodo-vdp-jdbcdriver.jar"

    denodo_con = jaydebeapi.connect(
                        "com.denodo.vdp.jdbc.Driver",
                        "jdbc:vdb://aws13dndpr02:9999/edw", {
                            'user': os.getenv('DENODO_USER'),
                            'password': os.getenv('DENODO_PASSWORD'),
                            'other_property': "foobar"
                        }, path)



    scope = ['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive.file','https://www.googleapis.com/auth/drive']
    creds =  ServiceAccountCredentials.from_json_keyfile_name('client_secret.json',scope) # configuration file from google drive api 
    client = gspread.authorize(creds)


    service = build('drive', 'v3', credentials=creds)

    # Define the folder ID of the shared Google Drive, within the URL
    folder_id = 'INSERT_GOOGLE_SHEET_ID'

    query = "mimeType='application/vnd.google-apps.spreadsheet' and trashed=false and '{0}' in parents".format(folder_id)

    sheet_count=0
    npi_dict = {}
    def get_seasonality(product_type):
        sql = '''
        SELECT * FROM "DATASCIENCE_DB"."CDM_ANALYTICS_PRD"."SEASONALITY_PROD_TYPE_ALL_YEARS"
        where PRODUCT_TYPE_DESC = '{product_type}'
        '''.format(product_type = product_type)
        seasonality_df = pd.read_sql(sql,  get_snowflake_conn())
        return seasonality_df

    def get_npi_df(sh):
        sh_len=len(sh.get_all_values())
        sheet=sh.get('A45:T'+str(sh_len))
        df=pd.DataFrame(sheet,columns=[sheet[0]])
        df=df.drop(0)
        df.index=pd.RangeIndex(len(df.index))
        columns=['Country','US Accounts','ASP','Account Weekly FCST', 'Instore Date','Store Count FCST','Store Count Confirmation','Total Load Qty FCST','Load Quantity Confirmation']
        npi_df=df[columns]
        sh_mkt=sh.get('A10:G10')
        dfsh=pd.DataFrame(sh_mkt)
        mkt_name=dfsh.iloc[0][1]
        npi_df['MKTG Name']=mkt_name
        npi_df['SKU']=sh.title
        dfsh=npi_df[['Total Load Qty FCST']]
        npi_df['load month'] = dfsh
        sku_df=pd.DataFrame(sh.get('B8'))
        replaced_sku=sku_df.iloc[0][0]
        npi_df['replaced sku']=replaced_sku
        sh1=sh.get('A19:B21')
        df1=pd.DataFrame(sh1)
        product_type=df1.iloc[0][1]
        type=df1.iloc[2][1]
        ss_df = get_seasonality(product_type)
        type_lower=type.lower()
        type_name=re.findall('[a-zA-Z]', type_lower)
        type_final=''.join(type_name[0:2]+['_']+type_name[2:8])
        columns_ss=['country','product_type_desc','month_number',type_final]
        ss_df_wk=ss_df[columns_ss]
        ss_df_wk['week_count'] = np.where(ss_df_wk['month_number'].isin([3,6,9,12]),5,4)
        ss_df_wk = ss_df_wk.loc[ss_df_wk["month_number"] != 0]
        ss_df_wk.index=pd.RangeIndex(len(ss_df_wk.index))
        countryDict = {'US':'United States','CA':'Canada'}
        # countryDict should have more pairs of values. 

        instore_date=npi_df[['Instore Date']].iloc[0][0]
        instore_month=instore_date[0:2]
        instore_year=instore_date[6:]
        monthDict = {'1':'Jan', '2':'Feb', '3':'Mar', '4':'Apr', '5':'May', '6':'Jun', 
                    '7':'Jul', '8':'Aug', '9':'Sep', '10':'Oct', '11':'Nov', '12':'Dec',
                    '13':'Jan', '14':'Feb', '15':'Mar', '16':'Apr', '17':'May', '18':'Jun', 
                    '19':'Jul', '20':'Aug', '21':'Sep', '22':'Oct', '23':'Nov', '24':'Dec',
                    '25': 'Jan'}
        dictMonth={'1':'1','2':'2','3':'3','4':'4','5':'5','6':'6',
                    '7':'7','8':'8','9':'9','10':'10','11':'11','12':'12',
                    '13':'1','14':'2','15':'3','16':'4','17':'5','18':'6',
                    '19':'7','20':'8','21':'9','22':'10','23':'11','24':'12',
                    '25':'1'}
        for j in range(1,14):
            npilist=list()
            if  (int(instore_month)+j)<=12:
                    col_year=instore_year
            else:
                col_year=str(int(instore_year)+1)
            col_month_num=str(int(instore_month)+j)
            if int(col_month_num)>24:
                col_year=str(int(col_year)+1)

            col_name=monthDict[col_month_num]+'-'+col_year 

            for i in range(0,len(npi_df)):
                ctry=npi_df.iloc[i][0]
                ss_sum=ss_df_wk.loc[ss_df_wk['country']==countryDict[ctry]]
                ss_mnth=ss_sum.loc[ss_sum['month_number']==int(dictMonth[col_month_num])]
                ss_mnth.index=pd.RangeIndex(len(ss_mnth.index))
                if len(ss_mnth)==1:
                    ss=ss_mnth.iloc[0][type_final]
                else: 
                    ss=0
                npilist.append(float(npi_df.iloc[i][3])*float(ss))

            npi_df[col_name]=npilist
        return (npi_df)

    sheets_all = service.files().list(q=query,fields="nextPageToken, files(id, name)").execute()
    current_timestamp_str = datetime.datetime.now().strftime("%Y-%m-%d")

    for sheet in sheets_all.get('files', []):
        sh_name=sheet.get('name')
        query = "mimeType='application/vnd.google-apps.spreadsheet' and trashed = false and name='" + sh_name + "'"
        results = service.files().list(q=query, fields="nextPageToken, files(id, name, owners)").execute()
        items = results.get('files', [])
        if len(items) == 1:
            sheet_id = items[0]['id']
            sheet_owner = items[0]['owners'][0]['emailAddress']

        output=client.open(sh_name)
        sh_list=output.worksheets()
        sh_index_lst=list()
        for i in range (0,len(sh_list)):
            sh_str=str(sh_list[i])[12:]
            if re.search('^I_9',sh_str)!=None:
                sh_index_lst.append(i) 
                sheet_count=sheet_count+1


        for j in range(0,len(sh_index_lst)):
            sh_index=sh_index_lst[j]
            sh=output.get_worksheet(sh_index)
            sh_str=str(sh)[12:24]
            npi_df=get_npi_df(sh)
            npi_df['timestamp']=format(datetime.datetime.now())
            npi_df['owner email']=sheet_owner

            column_names_prev = npi_df.columns.tolist()
            column_names=[t[0] for t in column_names_prev]
            npi_df.columns=column_names
            npi_df.to_csv('npi_df.csv', index = False)
           

            ids=npi_df.columns[0:13].append(npi_df.columns[26:])
            vals=npi_df.columns[13:26]
            npi_df_final=pd.melt(npi_df, id_vars=ids, value_vars=vals) 
            npi_df_final=npi_df_final.rename(columns={"variable": "fiscal month alias"})
            npi_df_final['timestamp']=current_timestamp_str
            npi_dict[sh_str]=npi_df_final

            logging.info("Insert Into Snowflake - " + "SKU: " + sh_str .format(datetime.datetime.now()))
            insert_to_snowflake(npi_df_final, table_name = 'npi_etl_tracking')



    logging.info("script complete".format(datetime.datetime.now()))

    send_email(npi_dict)

except:
    exc_type, exc_value, exc_traceback = sys.exc_info()
    print(traceback.format_exception(exc_type, exc_value,exc_traceback))
