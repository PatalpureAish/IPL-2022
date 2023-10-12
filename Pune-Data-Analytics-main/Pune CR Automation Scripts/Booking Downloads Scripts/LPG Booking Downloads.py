#!/usr/bin/env python
# coding: utf-8

# In[9]:


# Step 1: Install the required libraries: pandas and psycopg2
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

con = sqlite3.connect('rhcp7.db')




# In[10]:


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


# In[11]:


# Step 2: Read the Excel file and extract the unique IDs from the two columns
data = pd.read_excel(r'C:\Users\Vinayak.Laxmeshwar\Documents\Files\BD\LPG_CLients.xlsx')
##filtered_data = data[data['parent'].isnull()]  # Filter rows where Column1 is blank
client_name = data[str('Report Name')]
unique_ids_parent = data['PARENT_ENTITY_ID']
data['Child_ids'] = data['Child_ids'].fillna('')
unique_ids_child = data['Child_ids'].astype(str)# Assuming the file is in the same directory


# In[12]:


unique_ids_parent


# In[13]:


import datetime
today = datetime.date.today()
first = today.replace(day=1)
last_month = first - datetime.timedelta(days=1)
Last_month = last_month.strftime("%B %Y")


# In[14]:


Last_month


# In[15]:


for unique_id, child_unique_id, name in zip(unique_ids_parent, unique_ids_child, client_name):
    unique_id_str = str(unique_id)
    child_unique_id_str = str(child_unique_id)
    name_str = str(name)
    if child_unique_id_str == '':
        sql_query_parent_template = f"""SELECT
                        distinct a.appointment_id,
                        e.parent_entity_id,
                        e.parent_entity_name,
                        a.provider_npi as Provider_NPI
                       ,monolith_professional_id
                       , a.provider_first_name as Provider_First_Name
                       , a.provider_last_name as Provider_Last_Name
                       , a.booking_specialty_name as Specialty
                       , a.procedure_name as Procedure_Name
                       , a.insurance_carrier_name as Insurance_Carrier
                       , a.insurance_plan_name as Insurance_Plan
                       , a.insurance_plan_type as Insurance_Type
                       , CASE WHEN a.is_new_to_provider_from_pe_vw = TRUE then 'New' else 'Existing' END as New_Or_Existing_Patient
                       , a.appointment_created_timestamp_local as Booking_Time
                       , CASE WHEN a.is_appointment_created_time_after_hours_local = TRUE then 'After Hours' else 'Business Hours' END as After_Hours_vs_Business_Hours_Booking
                       , a.platform as Device_Type
                       , a.referrer_type_category as Booking_Source
                       , CASE
                                   WHEN a.is_non_chargeable_cancellation_from_pe_vw = TRUE then 'Non_Chargable_Patient_Cancellation'
                                   WHEN a.cancellation_reason like 'Patient_%' then 'Cancelled_by_Patient'
                                   WHEN a.cancellation_reason like 'Provider_%' then 'Cancelled_by_Provider'
                                   WHEN a.appointment_outcome = 'RealizedAppointment' then 'Confirmed'
                                   WHEN a.appointment_outcome = 'BookingFailed' then 'Rescheduling Error'
                                   WHEN a.appointment_outcome is null then 'Upcoming Appointment'
                                       else a.appointment_outcome end as Booking_Outcome
                       , CASE
                                   WHEN a.cancellation_reason like 'Patient_%' then 'Patient'
                                   WHEN a.cancellation_reason like 'Provider_%' then 'Provider'
                                   else null end as Cancellation_Initiator
                       , a.cancellation_reason as Cancellation_Reason
                       , (a.hours_between_creation_time_and_initial_appointment_time / 24.00) as Booking_Lead_Time_in_Days
                       , a.latest_appointment_time_local as Appointment_Time
                       , a.latest_event_timestamp_utc as Latest_Appointment_Change_Time
                                   ,a.APPOINTMENT_OUTCOME_TIMESTAMP_UTC
                                   ,convert_timezone('America/New_York', a.APPOINTMENT_OUTCOME_TIMESTAMP_UTC) as APPOINTMENT_OUTCOME_TIMESTAMP_EASTERN
                       , datediff(minute,booking_time,appointment_time)/60 as hours_between_booking_time_and_appointment_time 
                       , datediff(minute,a.appointment_created_timestamp_utc, a.appointment_outcome_timestamp_utc)/60 as hours_between_booking_time_and_appointment_outcome_time
                       , a.realized_cost as Booking_Cost -- is this the correct field?
                       , CASE WHEN a.is_premium_booking = TRUE then 'Premium Booking' else null END as Premium_Booking
                       , CASE WHEN a.is_spo_booking = TRUE then 'SPO Booking' else null END as SPO_Booking
                       , CASE WHEN a.is_virtual_location = TRUE then 'Video Visit' else 'In Person Visit' end as Visit_Type
                       , a.practice_name as Practice_Name
                       , a.MONOLITH_PROVIDER_LOCATION_ID
                       ,l.Name as Location_Name
                       , a.provider_location_address as Street_Address
                       , l.address2 as Suite_Number
                       , a.provider_location_city as City
                       , z.county as County
                       , a.provider_location_state as State
                       , a.provider_location_zip_code as Zip_Code
                       , a.strategic_name as Client_Name
                       , a.session_id  as Session_ID
                       , CASE WHEN a.referrer_type_category = 'Marketplace' then null else s.white_label_directory_id end as White_Label_ID
                       , CASE WHEN a.referrer_type_category = 'Marketplace' then null else s.landing_page_url end as Landing_Page_URL
                       , CASE WHEN a.referrer_type_category = 'Marketplace' then null else s.landing_page_url_network_location end as Landing_Page_URL_Network_Location
                       , CASE WHEN a.referrer_type_category = 'Marketplace' then null else s.landing_page_url_path_component_1 end as Landing_Page_URL_Component_1
                       , CASE WHEN a.referrer_type_category = 'Marketplace' then null else s.landing_page_url_path_component_2 end as Landing_Page_URL_Component_2
                       , CASE WHEN a.referrer_type_category = 'Marketplace' then null else s.referrer_url end as Referrer_URL
                       , CASE WHEN a.referrer_type_category = 'Marketplace' then null else s.platform end as Platform_Type
                       , CASE WHEN a.referrer_type_category = 'Marketplace' then null else s.platform_detail end as Platform_Category
                       , CASE WHEN a.referrer_type_category = 'Marketplace' then null else s.session_start_timestamp_utc end as Session_Start_Timestamp
                       , CASE WHEN a.referrer_type_category = 'Marketplace' then null else s.tracking_id end as Tracking_ID
                       , CASE WHEN a.referrer_type_category = 'Marketplace' then null else s.utm_campaign end as UTM_Campaign
                       , CASE WHEN a.referrer_type_category = 'Marketplace' then null else s.utm_medium end as UTM_Medium
                       , CASE WHEN a.referrer_type_category = 'Marketplace' then null else s.utm_source end as UTM_Source
                       --, a.strategic_id
                       --, a.monolith_strategic_id
                       --, a.parent_strategic_id
                       --, a.monolith_parent_strategic_id
                    FROM APPOINTMENT.appointment_summary_commercial_vw as a
                    LEFT JOIN user_behavior.session as s
                    ON a.session_id = s.session_id
                    LEFT JOIN public.zip_geography as z
                    ON a.provider_location_zip_code = z.zip_code
                    left JOIN PROVIDER_ANALYTICS.practice_parent_entity_mapping_vw as e
                    ON a.monolith_provider_id = e.MONOLITH_PROVIDER_ID
                    LEFT JOIN provider.location l
                    ON a.location_id = l.location_id
                    WHERE a.is_created_appointment = 'TRUE'
                    AND a.appointment_created_timestamp_local between '{first_date.strftime('%Y-%m-%d')}' and '{last_date.strftime('%Y-%m-%d')}'
                    AND procedure_id NOT IN ('pc_3BvV0dlIbkC_Hj_HQ8V4nB', 'pc_m459Tgq50kGv-faPIAb_3h','pc_wloLfj5vbUmMiwvX709pXx','pc_m0Wv6TPsc0iu-5Ju3vj7wh')
                    --parent IDs
                    AND e.SALESFORCE_ULTIMATE_PARENT_ACCOUNT_ID like '%{unique_id_str}%'
                    --child IDs
                    --and e.MONOLITH_PROVIDER_ID in ('1187')
                    order by Booking_Time;"""# Example SQL query template for parent_id
        df = pd.read_sql(sql_query_parent_template, conn)
        date_columns = ['BOOKING_TIME', 'APPOINTMENT_TIME','LATEST_APPOINTMENT_CHANGE_TIME','APPOINTMENT_OUTCOME_TIMESTAMP_UTC','APPOINTMENT_OUTCOME_TIMESTAMP_EASTERN','SESSION_START_TIMESTAMP']
                # Assuming the date format needs to be changed to 'YYYY-MM-DD'
        df[date_columns] = df[date_columns].applymap(lambda x: x.strftime('%Y-%m-%d %H:%M:%S')if not pd.isnull(x) else np.nan)
                
        directory = r'C:\Users\Vinayak.Laxmeshwar\Documents\LPG Clients Booking Downloads'
        filename = os.path.join(directory, f"{name_str} Booking Download - {Last_month}.csv")
                ##for row in resultList:
        df.to_csv(filename, index=False,header=True)
    else:
        sql_query_child_template = f"""SELECT
                     distinct a.appointment_id,
                     e.parent_entity_id,
                     e.parent_entity_name,
                      a.provider_npi as Provider_NPI
                       ,monolith_professional_id
                       , a.provider_first_name as Provider_First_Name
                       , a.provider_last_name as Provider_Last_Name
                       , a.booking_specialty_name as Specialty
                       , a.procedure_name as Procedure_Name
                       , a.insurance_carrier_name as Insurance_Carrier
                       , a.insurance_plan_name as Insurance_Plan
                       , a.insurance_plan_type as Insurance_Type
                       , CASE WHEN a.is_new_to_provider_from_pe_vw = TRUE then 'New' else 'Existing' END as New_Or_Existing_Patient
                       , a.appointment_created_timestamp_local as Booking_Time
                       , CASE WHEN a.is_appointment_created_time_after_hours_local = TRUE then 'After Hours' else 'Business Hours' END as After_Hours_vs_Business_Hours_Booking
                       , a.platform as Device_Type
                       , a.referrer_type_category as Booking_Source
                       , CASE
                                   WHEN a.is_non_chargeable_cancellation_from_pe_vw = TRUE then 'Non_Chargable_Patient_Cancellation'
                                   WHEN a.cancellation_reason like 'Patient_%' then 'Cancelled_by_Patient'
                                   WHEN a.cancellation_reason like 'Provider_%' then 'Cancelled_by_Provider'
                                   WHEN a.appointment_outcome = 'RealizedAppointment' then 'Confirmed'
                                   WHEN a.appointment_outcome = 'BookingFailed' then 'Rescheduling Error'
                                   WHEN a.appointment_outcome is null then 'Upcoming Appointment'
                                       else a.appointment_outcome end as Booking_Outcome
                       , CASE
                                   WHEN a.cancellation_reason like 'Patient_%' then 'Patient'
                                   WHEN a.cancellation_reason like 'Provider_%' then 'Provider'
                                   else null end as Cancellation_Initiator
                       , a.cancellation_reason as Cancellation_Reason
                       , (a.hours_between_creation_time_and_initial_appointment_time / 24.00) as Booking_Lead_Time_in_Days
                       , a.latest_appointment_time_local as Appointment_Time
                       , a.latest_event_timestamp_utc as Latest_Appointment_Change_Time
                                   ,a.APPOINTMENT_OUTCOME_TIMESTAMP_UTC
                                   ,convert_timezone('America/New_York', a.APPOINTMENT_OUTCOME_TIMESTAMP_UTC) as APPOINTMENT_OUTCOME_TIMESTAMP_EASTERN
                       , datediff(minute,booking_time,appointment_time)/60 as hours_between_booking_time_and_appointment_time 
                       , datediff(minute,a.appointment_created_timestamp_utc, a.appointment_outcome_timestamp_utc)/60 as hours_between_booking_time_and_appointment_outcome_time
                       , a.realized_cost as Booking_Cost -- is this the correct field?
                       , CASE WHEN a.is_premium_booking = TRUE then 'Premium Booking' else null END as Premium_Booking
                       , CASE WHEN a.is_spo_booking = TRUE then 'SPO Booking' else null END as SPO_Booking
                       , CASE WHEN a.is_virtual_location = TRUE then 'Video Visit' else 'In Person Visit' end as Visit_Type
                       , a.practice_name as Practice_Name
                       , a.MONOLITH_PROVIDER_LOCATION_ID
                       ,l.Name as Location_Name
                       , a.provider_location_address as Street_Address
                       , l.address2 as Suite_Number
                       , a.provider_location_city as City
                       , z.county as County
                       , a.provider_location_state as State
                       , a.provider_location_zip_code as Zip_Code
                       , a.strategic_name as Client_Name
                       , a.session_id  as Session_ID
                       , CASE WHEN a.referrer_type_category = 'Marketplace' then null else s.white_label_directory_id end as White_Label_ID
                       , CASE WHEN a.referrer_type_category = 'Marketplace' then null else s.landing_page_url end as Landing_Page_URL
                       , CASE WHEN a.referrer_type_category = 'Marketplace' then null else s.landing_page_url_network_location end as Landing_Page_URL_Network_Location
                       , CASE WHEN a.referrer_type_category = 'Marketplace' then null else s.landing_page_url_path_component_1 end as Landing_Page_URL_Component_1
                       , CASE WHEN a.referrer_type_category = 'Marketplace' then null else s.landing_page_url_path_component_2 end as Landing_Page_URL_Component_2
                       , CASE WHEN a.referrer_type_category = 'Marketplace' then null else s.referrer_url end as Referrer_URL
                       , CASE WHEN a.referrer_type_category = 'Marketplace' then null else s.platform end as Platform_Type
                       , CASE WHEN a.referrer_type_category = 'Marketplace' then null else s.platform_detail end as Platform_Category
                       , CASE WHEN a.referrer_type_category = 'Marketplace' then null else s.session_start_timestamp_utc end as Session_Start_Timestamp
                       , CASE WHEN a.referrer_type_category = 'Marketplace' then null else s.tracking_id end as Tracking_ID
                       , CASE WHEN a.referrer_type_category = 'Marketplace' then null else s.utm_campaign end as UTM_Campaign
                       , CASE WHEN a.referrer_type_category = 'Marketplace' then null else s.utm_medium end as UTM_Medium
                       , CASE WHEN a.referrer_type_category = 'Marketplace' then null else s.utm_source end as UTM_Source
                       --, a.strategic_id
                       --, a.monolith_strategic_id
                       --, a.parent_strategic_id
                       --, a.monolith_parent_strategic_id
                    FROM APPOINTMENT.appointment_summary_commercial_vw as a
                    LEFT JOIN user_behavior.session as s
                    ON a.session_id = s.session_id
                    LEFT JOIN public.zip_geography as z
                    ON a.provider_location_zip_code = z.zip_code
                    left JOIN PROVIDER_ANALYTICS.practice_parent_entity_mapping_vw as e
                    ON a.monolith_provider_id = e.MONOLITH_PROVIDER_ID
                    LEFT JOIN provider.location l
                    ON a.location_id = l.location_id
                    WHERE a.is_created_appointment = 'TRUE'
                    AND a.appointment_created_timestamp_local between '{first_date.strftime('%Y-%m-%d')}' and '{last_date.strftime('%Y-%m-%d')}'
                    AND procedure_id NOT IN ('pc_3BvV0dlIbkC_Hj_HQ8V4nB', 'pc_m459Tgq50kGv-faPIAb_3h','pc_wloLfj5vbUmMiwvX709pXx','pc_m0Wv6TPsc0iu-5Ju3vj7wh')
                    --child IDs
                    and e.MONOLITH_PROVIDER_ID in ({child_unique_id_str})
                    order by Booking_Time;"""
        df = pd.read_sql(sql_query_child_template, conn)
                # List of date column name
        date_columns = ['BOOKING_TIME', 'APPOINTMENT_TIME','LATEST_APPOINTMENT_CHANGE_TIME','APPOINTMENT_OUTCOME_TIMESTAMP_UTC','APPOINTMENT_OUTCOME_TIMESTAMP_EASTERN','SESSION_START_TIMESTAMP']
                # Assuming the date format needs to be changed to 'YYYY-MM-DD'
        df[date_columns] = df[date_columns].applymap(lambda x: x.strftime('%Y-%m-%d %H:%M:%S')if not pd.isnull(x) else np.nan)
        directory =r'C:\Users\Vinayak.Laxmeshwar\Documents\LPG Clients Booking Downloads'
        filename = os.path.join(directory, f"{name_str} Booking Download - {Last_month}.csv")
                ##for row in resultList:
        df.to_csv(filename, index=False,header=True)  

conn.close()


# In[16]:


import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import os.path

email = 'vinayak.laxmeshwar@zocdoc.com'
password = 'rubzzqumlkcrzagk'
to = "vinayak.laxmeshwar@zocdoc.com"
cc = "akshay.kumar@zocdoc.com,tanvi.malik@zocdoc.com,prithwidip.das@zocdoc.com"
subject = 'LPG Bookings Download- ' + Last_month + '.'
message = """Hi All,

The LPG Client Booking Downloads has ran successfully.

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




