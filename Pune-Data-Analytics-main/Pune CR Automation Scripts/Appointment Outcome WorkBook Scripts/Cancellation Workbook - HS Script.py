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
import openpyxl
#import win32com.client as win32
import xlwings as xw
import win32com.client as win32

# Gets the version
conn = snowflake.connector.connect(
    user="prithwidip.das@zocdoc.com",
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
data = pd.read_excel(r'C:\Users\prithwidip.das\Downloads\Health System.xlsx')
##filtered_data = data[data['parent'].isnull()]  # Filter rows where Column1 is blank
client_name = data[str('Report Name')]
unique_ids_parent = data['PARENT_ENTITY_ID']
data['Child_ids'] = data['Child_ids'].fillna('')
unique_ids_child = data['Child_ids'].astype(str)# Assuming the file is in the same directory

directory = r'C:\Users\prithwidip.das\Downloads\Health System Clients'
template_file_path = r'C:\Users\prithwidip.das\Downloads\Appointment Outcome Analysis Template.xlsx'


# In[4]:


import datetime
today = datetime.date.today()
first = today.replace(day=1)
last_month = first - datetime.timedelta(days=1)
Last_month = last_month.strftime("%B %Y")


# In[5]:


def refresh_data_using_win32com():
    # Open the Excel file using win32com.client
    excel = win32.Dispatch('Excel.Application')
    wb = excel.Workbooks.Open(filename)
    excel.DisplayAlerts = True
    excel.Visible = True
    
    # Iterate through all sheets and refresh them
    wb.RefreshAll()  # Assuming you have a QueryTable
    excel.CalculateUntilAsyncQueriesDone()
    # Save the workbook (optional)
    wb.Save()

    # Close the workbook
    wb.Close()
    # Quit Excel application
    excel.Quit()
    


# In[6]:


def excel_convert(template_file_path,filename,df):       
        wb = xw.Book(template_file_path)
        #sheet = wb.sheets['Raw Bookings Data']  # Replace 'Sheet1' with the sheet name you're working with
        # Step 3: Insert Data
        # Assuming cell A1 contains a placeholder for the name and cell B1 for the age
        sheet = wb.sheets['Raw Bookings Data'].range('A18').options(index=False).value = df
        # Step 4: Save and Refresh
        wb.save(filename)
        # wb.app.calculate()  # Refresh formulas and calculations in the Excel file
        
        # Close the workbook (optional)
        wb.close()
        refresh_data_using_win32com()


# In[7]:


for unique_id, child_unique_id, name in zip(unique_ids_parent, unique_ids_child, client_name):
    unique_id_str = str(unique_id)
    child_unique_id_str = str(child_unique_id)
    name_str = str(name)
    if child_unique_id_str == '':
        sql_query_parent_template = f""" SELECT
                                            e.parent_entity_name
                                            ,a.appointment_id
                                            ,a.provider_npi as Provider_NPI
                                            ,a.MONOLITH_PROFESSIONAL_ID
                                            ,a.provider_first_name as Provider_First_Name
                                            ,a.provider_last_name as Provider_Last_Name
                                            ,concat(a.provider_first_name,' ', a.provider_last_name) as Provider_Full_Name
                                            ,concat(a.provider_first_name,' ', a.provider_last_name,' - ',a.booking_specialty_name) as Provider_Full_Name_Specialty
                                            ,concat(a.provider_first_name,' ', a.provider_last_name,' - ',a.procedure_name) as Provider_Full_Name_VR
                                            ,concat(a.provider_first_name,' ', a.provider_last_name,' - ',a.insurance_carrier_name) as Provider_Full_Name_Insurance
                                            ,a.booking_specialty_name as Specialty
                                            ,a.procedure_name as Procedure_Name
                                            ,concat(a.procedure_name,' - ',a.booking_specialty_name)as Visit_Reason_Specialty
                                            ,a.insurance_carrier_name as Insurance_Carrier
                                            ,a.insurance_plan_name as Insurance_Plan
                                            ,concat(a.insurance_plan_name,' - ',a.insurance_carrier_name)as Insurance_Plan_Carrier
                                            ,a.insurance_plan_type as Insurance_Type
                                            ,CASE WHEN a.is_new_to_provider_from_pe_vw = TRUE then 'New' else 'Existing' END as New_Or_Existing_Patient
                                            ,to_char (a.appointment_created_timestamp_local ,'MM-DD-YYYY HH24:MI:SS')as Booking_Time
                                            ,CASE WHEN a.is_appointment_created_time_after_hours_local = TRUE then 'After Hours' else 'Business Hours' END as After_Hours_vs_Business_Hours_Booking
                                            ,CASE 
                                                WHEN a.latest_appointment_time_local IS NULL THEN 'Upcoming Appointment'
                                                ELSE to_char (date_trunc(week,a.latest_appointment_time_local),'MM-DD-YYYY HH24:MI:SS') END as Appointment_Outcome_Week
                                            ,a.referrer_type_category as Booking_Source
                                            ,CASE
                                                WHEN a.is_non_chargeable_cancellation_from_pe_vw = TRUE then 'Non_Chargable_Patient_Cancellation'
                                                WHEN a.cancellation_reason like 'Provider_ReschedulingPatient' then 'Rescheduled_by_Provider'
                                                WHEN a.cancellation_reason like 'Patient_%' then 'Cancelled_by_Patient'
                                                WHEN a.cancellation_reason like 'Provider_%' then 'Cancelled_by_Provider'
                                                WHEN a.appointment_outcome = 'RealizedAppointment' then 'Confirmed'
                                                WHEN a.appointment_outcome = 'BookingFailed' then 'Rescheduling Error'
                                                WHEN a.appointment_outcome = 'Rescheduled_by_Provider' then 'Rescheduled by Provider'
                                                WHEN a.appointment_outcome is null then 'Upcoming Appointment'
                                                else a.appointment_outcome end as Booking_Outcome
                                            ,CASE
                                                WHEN a.cancellation_reason like 'Patient_%' then 'Patient'
                                                WHEN a.cancellation_reason like 'Provider_%' then 'Provider'
                                                else null end as Cancellation_Initiator
                                             ,a.cancellation_reason as Cancellation_Reason
                                             ,(a.hours_between_creation_time_and_initial_appointment_time / 24.00) as Booking_Lead_Time_in_Days
                                             ,CASE
                                                 WHEN Booking_Lead_Time_in_Days <= 1 THEN '1 Day'
                                                 WHEN Booking_Lead_Time_in_Days <= 2 THEN '2 Days'
                                                 WHEN Booking_Lead_Time_in_Days <= 3 THEN '3 Days'
                                                 WHEN Booking_Lead_Time_in_Days <= 4 THEN '4 Days'
                                                 WHEN Booking_Lead_Time_in_Days <= 5 THEN '5 Days'
                                                 WHEN Booking_Lead_Time_in_Days <= 10 THEN '5-10 Days'
                                                 WHEN Booking_Lead_Time_in_Days <= 20 THEN '10-20 Days'
                                                 ELSE '20+ Days' END AS Lead_Time_Category
                                             ,to_char (a.latest_appointment_time_local,'MM-DD-YYYY HH24:MI:SS')  as Appointment_Time
                                             ,a.realized_cost as Booking_Cost
                                             ,CASE WHEN a.is_premium_eligible_with_current_mapping_from_pe_vw = TRUE then 'Premium Booking' else 'Non Premium Booking' END as Premium_Booking
                                             ,CASE WHEN a.is_spo_booking = TRUE then 'SPO Booking' else null END as SPO_Booking
                                             ,CASE WHEN a.is_virtual_location = TRUE then 'Video Visit' else 'In Person Visit' end as Visit_Type                     
                                             ,a.practice_name as Practice_Name
                                             ,a.monolith_provider_location_id
                                             ,a.provider_location_address as Street_Address
                                             ,a.provider_location_city as City
                                             ,z.county as County
                                             ,a.provider_location_state as State
                                             ,a.provider_location_zip_code as Zip_Code
                                             ,concat(a.provider_location_address, ', ', a.provider_location_city, ', ', a.provider_location_state, ' ', a.provider_location_zip_code) AS Full_Address
                                             ,e.child_entity_name as Client_Name
                                             ,e.child_monolith_entity_id as Child_ID
                                             ,l.name as Location_Name



                                        FROM appointment.appointment_summary_commercial_vw as a


                                        LEFT JOIN public.zip_geography as z
                                         ON a.provider_location_zip_code = z.zip_code

                                         left JOIN PROVIDER_ANALYTICS.practice_parent_entity_mapping_vw as e
                                        ON a.monolith_provider_id = e.MONOLITH_PROVIDER_ID

                                        left join provider.location as l
                                        ON a.location_id = l.location_id

                                        WHERE a.is_created_appointment = 'TRUE'
                                        AND a.procedure_id NOT IN ('pc_3BvV0dlIbkC_Hj_HQ8V4nB', 'pc_m459Tgq50kGv-faPIAb_3h','pc_wloLfj5vbUmMiwvX709pXx','pc_m0Wv6TPsc0iu-5Ju3vj7wh')

                                        --Adjust the time period below--
                                        AND a.LATEST_APPOINTMENT_TIME_LOCAL between '{first_date.strftime('%Y-%m-%d')}' and '{last_date.strftime('%Y-%m-%d')}'

                                     
                                        --AND a.is_new_to_provider_from_pe_vw = TRUE

                       
                                        AND a.is_premium_eligible_with_current_mapping_from_pe_vw= TRUE


                                        --For LPGs, use one of the following fields--
                                        --AND e.parent_entity_id = 'XXX' --parent account
                                          AND e.parent_entity_id = '{unique_id_str}'

                                        order by Appointment_Time;

                                        """
        df = pd.read_sql(sql_query_parent_template, conn)
        df.columns = df.iloc[0,:].values
        df = df.tail(-1)
        filename = os.path.join(directory, f"{name_str} Appointment Outcome Analysis - {Last_month}.xlsx")
        excel_convert(template_file_path,filename,df) 
        ##refresh_data_using_win32com()
    else:
        sql_query_child_template = f"""SELECT
                                        e.parent_entity_name
                                        ,a.appointment_id
                                        ,a.provider_npi as Provider_NPI
                                        ,a.MONOLITH_PROFESSIONAL_ID
                                        ,a.provider_first_name as Provider_First_Name
                                        ,a.provider_last_name as Provider_Last_Name
                                        ,concat(a.provider_first_name,' ', a.provider_last_name) as Provider_Full_Name
                                        ,concat(a.provider_first_name,' ', a.provider_last_name,' - ',a.booking_specialty_name) as Provider_Full_Name_Specialty
                                        ,concat(a.provider_first_name,' ', a.provider_last_name,' - ',a.procedure_name) as Provider_Full_Name_VR
                                        ,concat(a.provider_first_name,' ', a.provider_last_name,' - ',a.insurance_carrier_name) as Provider_Full_Name_Insurance
                                        ,a.booking_specialty_name as Specialty
                                        ,a.procedure_name as Procedure_Name
                                        ,concat(a.procedure_name,' - ',a.booking_specialty_name)as Visit_Reason_Specialty
                                        ,a.insurance_carrier_name as Insurance_Carrier
                                        ,a.insurance_plan_name as Insurance_Plan
                                        ,concat(a.insurance_plan_name,' - ',a.insurance_carrier_name)as Insurance_Plan_Carrier
                                        ,a.insurance_plan_type as Insurance_Type
                                        ,CASE WHEN a.is_new_to_provider_from_pe_vw = TRUE then 'New' else 'Existing' END as New_Or_Existing_Patient
                                        ,to_char (a.appointment_created_timestamp_local ,'MM-DD-YYYY HH24:MI:SS')as Booking_Time
                                        ,CASE WHEN a.is_appointment_created_time_after_hours_local = TRUE then 'After Hours' else 'Business Hours' END as After_Hours_vs_Business_Hours_Booking
                                        ,CASE 
                                            WHEN a.latest_appointment_time_local IS NULL THEN 'Upcoming Appointment'
                                            ELSE to_char (date_trunc(week,a.latest_appointment_time_local),'MM-DD-YYYY HH24:MI:SS') END as Appointment_Outcome_Week
                                        ,a.referrer_type_category as Booking_Source
                                        ,CASE
                                            WHEN a.is_non_chargeable_cancellation_from_pe_vw = TRUE then 'Non_Chargable_Patient_Cancellation'
                                            WHEN a.cancellation_reason like 'Provider_ReschedulingPatient' then 'Rescheduled_by_Provider'
                                            WHEN a.cancellation_reason like 'Patient_%' then 'Cancelled_by_Patient'
                                            WHEN a.cancellation_reason like 'Provider_%' then 'Cancelled_by_Provider'
                                            WHEN a.appointment_outcome = 'RealizedAppointment' then 'Confirmed'
                                            WHEN a.appointment_outcome = 'BookingFailed' then 'Rescheduling Error'
                                            WHEN a.appointment_outcome = 'Rescheduled_by_Provider' then 'Rescheduled by Provider'
                                            WHEN a.appointment_outcome is null then 'Upcoming Appointment'
                                            else a.appointment_outcome end as Booking_Outcome
                                        ,CASE
                                            WHEN a.cancellation_reason like 'Patient_%' then 'Patient'
                                            WHEN a.cancellation_reason like 'Provider_%' then 'Provider'
                                            else null end as Cancellation_Initiator
                                         ,a.cancellation_reason as Cancellation_Reason
                                         ,(a.hours_between_creation_time_and_initial_appointment_time / 24.00) as Booking_Lead_Time_in_Days
                                         ,CASE
                                             WHEN Booking_Lead_Time_in_Days <= 1 THEN '1 Day'
                                             WHEN Booking_Lead_Time_in_Days <= 2 THEN '2 Days'
                                             WHEN Booking_Lead_Time_in_Days <= 3 THEN '3 Days'
                                             WHEN Booking_Lead_Time_in_Days <= 4 THEN '4 Days'
                                             WHEN Booking_Lead_Time_in_Days <= 5 THEN '5 Days'
                                             WHEN Booking_Lead_Time_in_Days <= 10 THEN '5-10 Days'
                                             WHEN Booking_Lead_Time_in_Days <= 20 THEN '10-20 Days'
                                             ELSE '20+ Days' END AS Lead_Time_Category
                                         ,to_char (a.latest_appointment_time_local,'MM-DD-YYYY HH24:MI:SS')  as Appointment_Time
                                         ,a.realized_cost as Booking_Cost
                                         ,CASE WHEN a.is_premium_eligible_with_current_mapping_from_pe_vw = TRUE then 'Premium Booking' else 'Non Premium Booking' END as Premium_Booking
                                         ,CASE WHEN a.is_spo_booking = TRUE then 'SPO Booking' else null END as SPO_Booking
                                         ,CASE WHEN a.is_virtual_location = TRUE then 'Video Visit' else 'In Person Visit' end as Visit_Type                     
                                         ,a.practice_name as Practice_Name
                                         ,a.monolith_provider_location_id
                                         ,a.provider_location_address as Street_Address
                                         ,a.provider_location_city as City
                                         ,z.county as County
                                         ,a.provider_location_state as State
                                         ,a.provider_location_zip_code as Zip_Code
                                         ,concat(a.provider_location_address, ', ', a.provider_location_city, ', ', a.provider_location_state, ' ', a.provider_location_zip_code) AS Full_Address
                                         ,e.child_entity_name as Client_Name
                                         ,e.child_monolith_entity_id as Child_ID
                                         ,l.name as Location_Name



                                    FROM appointment.appointment_summary_commercial_vw as a


                                    LEFT JOIN public.zip_geography as z
                                     ON a.provider_location_zip_code = z.zip_code

                                     left JOIN PROVIDER_ANALYTICS.practice_parent_entity_mapping_vw as e
                                    ON a.monolith_provider_id = e.MONOLITH_PROVIDER_ID

                                    left join provider.location as l
                                    ON a.location_id = l.location_id

                                    WHERE a.is_created_appointment = 'TRUE'
                                    AND a.procedure_id NOT IN ('pc_3BvV0dlIbkC_Hj_HQ8V4nB', 'pc_m459Tgq50kGv-faPIAb_3h','pc_wloLfj5vbUmMiwvX709pXx','pc_m0Wv6TPsc0iu-5Ju3vj7wh')

                                    --Adjust the time period below--
                                    AND a.LATEST_APPOINTMENT_TIME_LOCAL between '{first_date.strftime('%Y-%m-%d')}' and '{last_date.strftime('%Y-%m-%d')}'

                           
                                    --AND a.is_new_to_provider_from_pe_vw = TRUE

                                    AND a.is_premium_eligible_with_current_mapping_from_pe_vw= TRUE                

                                    --For Health system, use one of the following fields--
                                    --AND e.parent_entity_id = 'XXX' --parent account
                                    AND a.monolith_strategic_id_with_current_mapping IN ({child_unique_id_str})

                                    order by Appointment_Time;

                                    """
        df = pd.read_sql(sql_query_child_template, conn)
        df.columns = df.iloc[0,:].values
        df = df.tail(-1)
        # Step 2: Open the Excel Template   
        filename = os.path.join(directory, f"{name_str} Appointment Outcome Analysis - {Last_month}.xlsx")
        excel_convert(template_file_path,filename,df)  
        ##refresh_data_using_win32com()
conn.close()


# In[ ]:




