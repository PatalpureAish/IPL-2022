#!/usr/bin/env python
# coding: utf-8

# In[1]:


import psycopg2
import pandas as pd
from pandas import DataFrame
import snowflake.connector
import sqlite3
import os
import numpy as np

# Gets the version
conn = snowflake.connector.connect(
    user="vinayak.laxmeshwar@zocdoc.com",
    password="do not enter your password. it will authenticate with the browser, however this parameter cannot be an empty string.",
    account="zocdoc_001.us-east-1",
    authenticator="externalbrowser",
    database="cistern"
    )

con = sqlite3.connect('rhcp10.db')


# In[2]:


from datetime import datetime, date, timedelta

# Get the current date
current_date = date.today()

# Calculate the first date of the prior month
if current_date.month == 1:
    first_date = date(current_date.year - 1, 12, 1)
else:
    first_date = date(current_date.year, current_date.month - 1, 1)

# Calculate the last date of the prior month
last_date = current_date.replace(day=1) 

# Print the first and last dates
print("First Date:", first_date)
print("Last Date:", last_date)


# In[3]:


# Step 2: Read the Excel file and extract the unique IDs from the two columns
data = pd.read_excel(r'C:\Users\Vinayak.Laxmeshwar\Documents\Files\Review\LPG_Review_file.xlsx')
##filtered_data = data[data['parent'].isnull()]  # Filter rows where Column1 is blank
client_name = data[str('Report Name')]
unique_ids_parent = data['PARENT_ENTITY_ID']
data['Child_ids'] = data['Child_ids'].fillna('')
unique_ids_child = data['Child_ids'].astype(str)# Assuming the file is in the same directory


# In[4]:


unique_ids_child


# In[5]:


import datetime
today = datetime.date.today()
first = today.replace(day=1)
last_month = first - datetime.timedelta(days=1)
Last_month = last_month.strftime("%B %Y")


# In[6]:


Last_month


# In[7]:


for unique_id, child_unique_id, name in zip(unique_ids_parent, unique_ids_child, client_name):
    unique_id_str = str(unique_id)
    child_unique_id_str = str(child_unique_id)
    name_str = str(name)
    if child_unique_id_str == '':
        sql_query_parent_template = f"""select distinct a.PROVIDER_FIRST_NAME || ' ' || a.PROVIDER_Last_NAME as Provider_name
                                    ,a.practice_id,a.practice_name
                                    ,date(r.CREATION_DATE_UTC) as Day_of_REVIEW_CREATION_DATE
                                    ,r.REVIEW_ID,r.OVERALL_RATING
                                    ,r.WAITTIME_RATING,r.BEDSIDE_RATING
                                    ,r.COMMENT as Zocdcoc_review
                                    from patient.reviews r
                                    inner join APPOINTMENT.appointment_summary_commercial_vw as a
                                        on a.appointment_id = r.appointment_id   
                                    left JOIN PROVIDER_ANALYTICS.practice_parent_entity_mapping_vw as e
                                        ON a.monolith_provider_id = e.MONOLITH_PROVIDER_ID
                                    where (r.REVIEW_ID, r.CREATION_DATE_UTC) IN (SELECT REVIEW_ID, Max(CREATION_DATE_UTC) FROM patient.reviews GROUP BY REVIEW_ID)
                                    and r.IS_ZOCDOC_REVIEW = 'TRUE'
                                    and r.CREATION_DATE_UTC between '{first_date.strftime('%Y-%m-%d')}' and '{last_date.strftime('%Y-%m-%d')}'
                                    and a.is_created_appointment = 'TRUE'
                                    AND procedure_id NOT IN ('pc_3BvV0dlIbkC_Hj_HQ8V4nB', 'pc_m459Tgq50kGv-faPIAb_3h','pc_wloLfj5vbUmMiwvX709pXx','pc_m0Wv6TPsc0iu-5Ju3vj7wh')
                                    --parent IDs
                                    AND e.SALESFORCE_ULTIMATE_PARENT_ACCOUNT_ID like '%{unique_id_str}%'
                                    --child ids
                                    --and a.MONOLITH_PROVIDER_ID in  ('11038', '9057')
                                    order by a.PROVIDER_FIRST_NAME || ' ' || a.PROVIDER_Last_NAME;"""# Example SQL query template for parent_id
        df = pd.read_sql(sql_query_parent_template, conn)
        directory = r'C:\Users\Vinayak.Laxmeshwar\Documents\LPG Review Downloads'
        filename = os.path.join(directory, f"{name_str} Review Download - {Last_month}.csv")
                ##for row in resultList:
        df.to_csv(filename, index=False,header=True)
    else:
        sql_query_child_template = f"""select distinct a.PROVIDER_FIRST_NAME || ' ' || a.PROVIDER_Last_NAME as Provider_name
                                    ,a.practice_id,a.practice_name
                                    ,date(r.CREATION_DATE_UTC) as Day_of_REVIEW_CREATION_DATE
                                    ,r.REVIEW_ID,r.OVERALL_RATING
                                    ,r.WAITTIME_RATING,r.BEDSIDE_RATING
                                    ,r.COMMENT as Zocdcoc_review
                                    from patient.reviews r
                                    inner join APPOINTMENT.appointment_summary_commercial_vw as a
                                        on a.appointment_id = r.appointment_id   
                                    left JOIN PROVIDER_ANALYTICS.practice_parent_entity_mapping_vw as e
                                        ON a.monolith_provider_id = e.MONOLITH_PROVIDER_ID
                                    where (r.REVIEW_ID, r.CREATION_DATE_UTC) IN (SELECT REVIEW_ID, Max(CREATION_DATE_UTC) FROM patient.reviews GROUP BY REVIEW_ID)
                                    and r.IS_ZOCDOC_REVIEW = 'TRUE'
                                    and r.CREATION_DATE_UTC between '{first_date.strftime('%Y-%m-%d')}' and '{last_date.strftime('%Y-%m-%d')}'
                                    and a.is_created_appointment = 'TRUE'
                                    AND procedure_id NOT IN ('pc_3BvV0dlIbkC_Hj_HQ8V4nB', 'pc_m459Tgq50kGv-faPIAb_3h','pc_wloLfj5vbUmMiwvX709pXx','pc_m0Wv6TPsc0iu-5Ju3vj7wh')
                                    --parent IDs
                                    --AND e.SALESFORCE_ULTIMATE_PARENT_ACCOUNT_ID like ()
                                    --child ids
                                    and a.MONOLITH_PROVIDER_ID in ({child_unique_id_str})
                                    order by a.PROVIDER_FIRST_NAME || ' ' || a.PROVIDER_Last_NAME;"""
        df = pd.read_sql(sql_query_child_template, conn)
                # List of date column name
        directory =r'C:\Users\Vinayak.Laxmeshwar\Documents\LPG Review Downloads'
        filename = os.path.join(directory, f"{name_str} Review Download - {Last_month}.csv")
                ##for row in resultList:
        df.to_csv(filename, index=False,header=True)  

conn.close()


# In[8]:


import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import os.path

email = 'vinayak.laxmeshwar@zocdoc.com'
password = 'rubzzqumlkcrzagk'
to = "vinayak.laxmeshwar@zocdoc.com"
cc = "akshay.kumar@zocdoc.com,tanvi.malik@zocdoc.com"
subject = 'LPG Review Download- ' + Last_month + '.'
message = """Hi All,

The LPG Client Review Downloads has ran successfully.

Thanks & Regards,
Vinayak R Laxmeshwar"""

rcpt = cc.split(",") + [to]
msg = MIMEMultipart()
msg['From'] = email
msg['To'] = to
msg['Cc'] = cc
msg['Subject'] = subject

msg.attach(MIMEText(message, 'plain'))


server = smtplib.SMTP('smtp.gmail.com', 587)
server.starttls()
server.login(email, password)
text = msg.as_string()
server.sendmail(email, rcpt, text)
server.quit()


# In[ ]:





# In[ ]:




