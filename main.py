from __future__ import print_function

from typing import List, Any

import google
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from tqdm import tqdm
import pandas as pd
import os.path
import re
from time import sleep

people_that_have_filter = []

def get_df(spreadsheet_id):
    # If modifying these scopes, delete the file token.json.
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    try:
        service = build('sheets', 'v4', credentials=creds)

        # Call the Sheets API
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=spreadsheet_id,
                                    range='תגובות לטופס 1').execute()
        values = result.get('values', [])

        if not values:
            print('No data found.')
            return

        # Convert to a dataframe
        df = pd.DataFrame.from_records(values[1:], columns=values[1]+['delete1', 'delete2'])

    except HttpError as err:
        print(err)

    return df


def init_spreadsheet(spreadsheet_id, gsheet_id):
    # If modifying these scopes, delete the file token.json.
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    try:
        service = build('sheets', 'v4', credentials=creds)

        my_range = {
            'sheetId': gsheet_id,
            'startRowIndex': 1,
            'startColumnIndex': 0,
        }
    except HttpError as error:
        print(f"An error occurred: {error}")

    return my_range, service

def apply_filter(kosher):
  """Applies the appropriate filter based on the value of the `kosher` variable.

  Args:
    kosher: A string representing the value of the `kosher` variable.

  Returns:
    A dictionary representing the filter to apply, or `None` if no filter should be applied.
  """

  if kosher != 'לא' and kosher != '':
    return {
        '12': {
      'condition': {
        'type': 'TEXT_NOT_CONTAINS',
        'values': {
          'userEnteredValue': 'לא'
        }
      }
    }
    }
  elif kosher == 'לא':
    return {
        '12': {
      'condition': {
        'type': 'TEXT_EQ',
        'values': {
          'userEnteredValue': 'לא'
        }
      }
    }
    }
  else:
    return None


def create_filter_view_request(my_range, full_name, index, number_of_guests, kosher, pets, accessible, mamad):
    addfilterviewrequest = {
        'addFilterView': {
            'filter': {
                'title': index + '_' + full_name,
                'range': my_range,
                'sortSpecs': [
                    {
                    'dimensionIndex': 4, # I choose 4 because the number of guests column is in the 4th column.
                    'sortOrder': 'ASCENDING'
                    },
                    {
                        'dimensionIndex': 12,
                        'sortOrder': 'ASCENDING',
                    }
                ],
                'criteria': {
                    '4': {
                        'condition': {
                            'type': 'NUMBER_GREATER_THAN_EQ',
                            'values': {
                                'userEnteredValue': number_of_guests
                            }
                        }
                    },
                    '7': {
                        'condition': {
                            'type': 'TEXT_NOT_CONTAINS',
                            'values': {
                                'userEnteredValue': 'לא'  # אם כתוב שם "חסר מקסימום אורחים" אז לא להביא אותם
                            }
                        }
                    },
                    '9': {
                        'condition': {
                            'type': 'TEXT_NOT_CONTAINS',
                            'values': {
                                'userEnteredValue': 'לא'
                            }
                        }
                    } if (mamad != 'לא' and mamad != '') else None, # todo: כל מה שלא מכיל "לא",
                    # The logic of column 16:
                    # if pets is not "לא" then filter all the rows that not contains "לא" in the pets column
                    '16': {
                            'condition': {
                                'type': 'TEXT_NOT_CONTAINS',
                                'values': {
                                    'userEnteredValue': 'לא'
                                }
                            }
                        } if (pets != 'לא' and pets != '') else None,
                    # '16': {
                    #         'condition': {
                    #             'type': 'TEXT_CONTAINS',
                    #             'values': {
                    #                 'userEnteredValue': accessible
                    #             }
                    #         }
                    #     } if accessible else None,
                }
            }
        }
    }
    # The logic of column 12:
    # if kosher is not "לא" then filter all the rows that not contains "לא" in the kosher column
    # if kosher is "לא" then filter all the rows that contains "לא" in the kosher column
    # if kosher is blank then do nothing
    kosher_filter = apply_filter(kosher)
    if kosher_filter:
        addfilterviewrequest['addFilterView']['filter']['criteria'].update(kosher_filter)

    body = {'requests': [addfilterviewrequest]}
    return body


def update_spreadsheet(spreadsheet_id, body, service):
    my_error = False
    try:
        addfilterviewresponse = service.spreadsheets() \
            .batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
        print(str(addfilterviewresponse))
    except HttpError as error:
        print(f"An error occurred: {error}")
        if 'שם אחר' in error.error_details:
            people_that_have_filter.append(body['requests'][0]['addFilterView']['filter']['title'])
        else:
            my_error = True
    return my_error


def get_df_from_google_sheet(df_type: str = '', give_spreadsheet_id: str = '', need_spreadsheet_id: str = ''):
    if not df_type:
        return None
    if df_type == 'need_df':
        spreadsheet_id = need_spreadsheet_id
    elif df_type == 'give_df':
        spreadsheet_id = give_spreadsheet_id

    df = get_df(spreadsheet_id)

    return df


def pre_clean_data(df):
    # Remove " from each column string
    df.columns = df.columns.str.replace('"', '')

    df = df.replace("זוג", 2)

    return df


def clean_need_df(need_house_df):
    # read the excel file
    need_house_df = pre_clean_data(need_house_df)

    # Rename the columns to English
    need_house_df = need_house_df.rename(columns={
    "חותמת זמן": "timestamp",
    "כתובת אימייל": "email address",
    "שם מלא": "full name",
    "טלפון  (אנא ציינו רק ספרות, ללא מקף ורווח)": "phone number",
    "מאיזה יישוב אתם מגיעים ?  ": "origin city",
    "מה מספר אורחים שצריכים מקום? ": "number of guests",
    "הערות/בקשות": "notes/requests",
    "האם יש בעח שבאים איתכם ?": "pets",
    "האם זקוקים לעזרה בהסעות?": "transportation assistance",
    "האם שומרי כשרות?": "kosher",
    "האם זקוקים לבית מונגש ?": "accessible",
    "פירוט על בעח": "pets details",
    "פירוט לגבי בית מונגש": "accessible details",
    "פירוט לגבי כשרות": "kosher details",
    "הערות": "notes",
    "בטיפול של מי?": "who is in charge?",
    "אצל מי מתארחים": "who is hosting",
    "שונות": "other",
    "מצב הבקשה": "request status",
    "Unnamed: 19": "unknown",
    'האם חובה ממד  ?(שימו לב-  בית עם מקלט במקום ממד מזרז משמעותית זמני טיפול) ': 'mamad',
})
    need_house_df = post_clean_data(need_house_df)

    # Filter only rows that have nan in the "request status" column or have "בטיפול" in the "request status" column
    need_house_df = need_house_df[need_house_df['request status'].isna() | need_house_df['request status'].str.contains('בטיפול')]

    # Reset index
    # need_house_df = need_house_df.reset_index()
    return need_house_df


def post_clean_data(df):
    # Filter rows that have nan in the "number of guests" column
    df = df[df['number of guests'].notna()]

    # Convert the "number of guests" column to string
    df['number of guests'] = df['number of guests'].astype(str)

    # Convert the "can have pets" column to string
    df['pets'] = df['pets'].astype(str)

    # Convert the "kosher" column to string
    df['kosher'] = df['kosher'].astype(str)

    return df


def create_filters(need_house_df, my_range, service, spreadsheet_id) -> list:
    rows_with_errors = []
    tqdm_need_houses = tqdm(need_house_df.iterrows())
    n = 1
    # iterate through the need houses
    for i, row in tqdm_need_houses:
        # if row['full name'] != 'יעל גולן':
        #     continue
        # if n < 166:
        #     n += 1
        #     continue
        body = create_filter_view_request(my_range,
                                          row['full name'],
                                          str(i+2),
                                          row['number of guests'],
                                          row['kosher'],
                                          row['pets'],
                                          row['accessible'],
                                          row['mamad'])
        error = update_spreadsheet(spreadsheet_id, body, service)
        if error:
            rows_with_errors.append([row['full name'], str(i+2)])
        sleep(1)
        # n += 1
    return rows_with_errors


def main():
    give_spreadsheet_id = '1fIrzqDHykh9CoigUJqmZR0WtORr2z8tyapZC2X93srg'
    give_gsheet_id = 1511246512
    need_spreadsheet_id = '1nTaltqeLeyTDP8kwC9U1sEQjHuZPVEib59n0ZhCOrXY'

    need_house_df = get_df_from_google_sheet('need_df', give_spreadsheet_id=give_spreadsheet_id, need_spreadsheet_id=need_spreadsheet_id)
    need_house_df = clean_need_df(need_house_df)

    my_range, service = init_spreadsheet(give_spreadsheet_id, give_gsheet_id)
    rows_with_errors = create_filters(need_house_df, my_range, service, give_spreadsheet_id)
    print(rows_with_errors)

    # write to text file the people that have filter:
    with open('people_that_have_filter.txt', 'w') as f:
        for item in people_that_have_filter:
            f.write("%s\n" % item)

    # write to text file the people that have errors:
    with open('people_that_have_errors.txt', 'w') as f:
        for item in rows_with_errors:
            f.write("%s\n" % item)

    print(f'Done! (with {len(rows_with_errors)} errors)')

if __name__ == '__main__':
    main()

# TODO: run the script automatic twice a day



