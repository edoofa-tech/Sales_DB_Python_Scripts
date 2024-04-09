import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import mysql.connector
from datetime import datetime

# Set up logging
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def authenticate_google_sheets():
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/spreadsheets',
             'https://www.googleapis.com/auth/drive']
    creds_dict = json.loads(os.getenv('GCP_CREDENTIALS'))
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)

def fetch_form_id_from_sheet(client, spreadsheet_id, sheet_name='Duplicacy Check'):
    worksheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
    # Assuming the form_id is in cell A2
    form_id = worksheet.acell('A2').value
    return form_id

def check_duplicate_in_database(connection, form_id, table_name='leads'):
    cursor = connection.cursor()
    query = f"SELECT COUNT(*) FROM {table_name} WHERE form_id = %s"
    cursor.execute(query, (form_id,))
    result = cursor.fetchone()
    return result[0] > 0  # Returns True if duplicate, False otherwise

def update_sheet_with_check_result(client, spreadsheet_id, form_id, is_duplicate, sheet_name='Duplicacy Check'):
    worksheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
    result_text = f"{form_id} is {'Duplicate' if is_duplicate else 'Not Duplicate'}"
    # Assuming you want to write the result in cell B2
    worksheet.update_acell('B2', result_text)

def fetch_duplicate_details(connection, form_id, table_name='leads'):
    cursor = connection.cursor()
    query = f"""
    SELECT sales_status, 
           chat_start_date, 
           CONCAT(first_name_of_student, ' ', last_name_of_student) AS full_name,
           counselor, 
           timestamp_when_lead_assigned_to_a_user
    FROM {table_name}
    WHERE form_id = %s
    """
    cursor.execute(query, (form_id,))
    result = cursor.fetchone()
    if result:
        # Format datetime fields
        formatted_result = list(result)
        if result[1]:  # Assuming chat_start_date is the second field
            formatted_result[1] = result[1].strftime('%Y-%m-%d %H:%M:%S')
        if result[4]:  # Assuming timestamp_when_lead_assigned_to_a_user is the fifth field
            formatted_result[4] = result[4].strftime('%Y-%m-%d %H:%M:%S')
        return formatted_result
    return None

def write_lead_details_to_sheet(client, spreadsheet_id, lead_details, sheet_name='Duplicacy Check'):
    worksheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
    # Starting from cell A5, write each detail in the row
    if lead_details:
        worksheet.update('A5', [lead_details])  # Writing details in row 5
    else:
        worksheet.update_acell('A5', 'No details found for duplicate lead.')


# Database configuration
DB_HOST = '103.168.19.58'
DB_USER = 'techsupport'
DB_PASSWORD = 'EdoofaTech@123'
DB_NAME = 'edoofadb'

# Authenticate and connect
creds_file_path = 'C:\\Users\\aditya\\OneDrive\\Documents\\HeidiSQL-Edoofa-DB\\credentials.json'  # Windows path format
client = authenticate_google_sheets()
db_connection = mysql.connector.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASSWORD, database=DB_NAME)

# Fetch form_id from the sheet
SPREADSHEET_ID = '1DrQ19wXhxt4GR-qQdaZyzkjFtsNLOmdlglA49yCEFtc'
# After fetching form_id from the sheet
form_id = fetch_form_id_from_sheet(client, SPREADSHEET_ID)

# Check for duplicate in the database and fetch details if duplicate
is_duplicate = check_duplicate_in_database(db_connection, form_id)

# Update the Google Sheet with the check result, now including form_id in the message
update_sheet_with_check_result(client, SPREADSHEET_ID, form_id, is_duplicate)

if is_duplicate:
    # Fetch additional lead details if it's a duplicate
    lead_details = fetch_duplicate_details(db_connection, form_id)
    # Write the details to the Google Sheet
    logging.info(f"Lead Details: {lead_details}")
    write_lead_details_to_sheet(client, SPREADSHEET_ID, lead_details)
else:
    # Optionally clear previous details from the sheet or leave a message
    write_lead_details_to_sheet(client, SPREADSHEET_ID, None)

db_connection.close()
logging.info("Process completed.")
