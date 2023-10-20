from __future__ import print_function
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from typing import List, Any
from tqdm import tqdm
from time import sleep
import pandas as pd
import os.path
import google

people_that_have_filter = []

date_column = "0"
number_of_guests_column = "4"
kosher_column = "11"
pets_column = "15"
mamad_column = "8"
request_status = "20"


def get_df(spreadsheet_id) -> pd.DataFrame:
    """
    Get the Google sheet and convert it to a dataframe
    """
    # If modifying these scopes, delete the file token.json.
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    try:
        service = build("sheets", "v4", credentials=creds)

        # Call the Sheets API
        sheet = service.spreadsheets()
        result = (
            sheet.values()
            .get(spreadsheetId=spreadsheet_id, range="גיליון צריכים אירוח")
            .execute()
        )
        values = result.get("values", [])

        if not values:
            raise ValueError("No data found in the google sheet.")


        # Convert to a dataframe
        df = pd.DataFrame.from_records(values[1:], columns=values[1])

    except HttpError as err:
        print(err)

    return df


def init_spreadsheet(spreadsheet_id, gsheet_id):
    """
    This function init the spreadsheet and return the my_range and service
    :param spreadsheet_id:
    :param gsheet_id:
    :return:
    """
    # If modifying these scopes, delete the file token.json.
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    try:
        service = build("sheets", "v4", credentials=creds)

        my_range = {
            "sheetId": gsheet_id,
            "startRowIndex": 1,
            "startColumnIndex": 0,
        }
    except HttpError as error:
        print(f"An error occurred: {error}")

    return my_range, service


def apply_filter(kosher):
    """Applies the appropriate filter based on the value of the `kosher` variable.
        The logic of column 11 ('kosher' column):
        if kosher is not "לא" then filter all the rows that not contains "לא" in the kosher column
        if kosher is "לא" then filter all the rows that contains "לא" in the kosher column
        if kosher is blank then do nothing
    Args:
      kosher: A string representing the value of the `kosher` variable.

    Returns:
      A dictionary representing the filter to apply, or `None` if no filter should be applied.
    """

    if kosher != "לא" and kosher != "":
        return {
            "11": {
                "condition": {
                    "type": "TEXT_NOT_CONTAINS",
                    "values": {"userEnteredValue": "לא"},
                }
            }
        }
    elif kosher == "לא":
        return {
            "12": {
                "condition": {"type": "TEXT_EQ", "values": {"userEnteredValue": "לא"}}
            }
        }
    else:
        return None


def create_filter_view_request(
    my_range, full_name, index, number_of_guests, kosher, pets, mamad
):


    addfilterviewrequest = {
        "addFilterView": {
            "filter": {
                "title": index + "_" + full_name,
                "range": my_range,
                "sortSpecs": [
                    {
                        "dimensionIndex": number_of_guests_column,
                        "sortOrder": "ASCENDING",
                    },
                    {
                        "dimensionIndex": kosher_column,
                        "sortOrder": "ASCENDING",
                    },
                ],
                "criteria": {
                    number_of_guests_column: {
                        "condition": {
                            "type": "NUMBER_GREATER_THAN_EQ",
                            "values": {"userEnteredValue": number_of_guests},
                        }
                    },
                    "22": {
                        "hiddenValues": ["שובץ", "לא רלוונטי", "בטיפול"]
                    },  # איפוס, בטיפול, או ריק
                    "16": {
                        "condition": {
                            "type": "TEXT_NOT_CONTAINS",
                            "values": {"userEnteredValue": "לא"},
                        }
                    }
                    if (mamad != "לא" and mamad != "")
                    else None,  # todo: כל מה שלא מכיל "לא",
                    # The logic of column 16:
                    # if pets is not "לא" then filter all the rows that not contains "לא" in the pets column
                    "15": {
                        "condition": {
                            "type": "TEXT_NOT_CONTAINS",
                            "values": {"userEnteredValue": "לא"},
                        }
                    }
                    if (pets != "לא" and pets != "")
                    else None,
                    # '16': {
                    #         'condition': {
                    #             'type': 'TEXT_CONTAINS',
                    #             'values': {
                    #                 'userEnteredValue': accessible
                    #             }
                    #         }
                    #     } if accessible else None,
                },
            }
        }
    }

    kosher_filter = apply_filter(kosher)
    if kosher_filter:
        addfilterviewrequest["addFilterView"]["filter"]["criteria"].update(
            kosher_filter
        )

    body = {"requests": [addfilterviewrequest]}
    return body


def update_spreadsheet(spreadsheet_id, body, service) -> bool:
    """
    This function update the spreadsheet with the filter`s body and return True if there is an error
    :param spreadsheet_id:
    :param body:
    :param service:
    :return:
    """
    my_error = False
    try:
        addfilterviewresponse = (
            service.spreadsheets()
            .batchUpdate(spreadsheetId=spreadsheet_id, body=body)
            .execute()
        )
        print(str(addfilterviewresponse))
    except HttpError as error:
        print(f"An error occurred: {error}")
        if "שם אחר" in error.error_details:
            people_that_have_filter.append(
                body["requests"][0]["addFilterView"]["filter"]["title"]
            )
        else:
            my_error = True
    return my_error


def get_df_from_google_sheet(
    df_type: str = "", give_spreadsheet_id: str = "", need_spreadsheet_id: str = ""
) -> pd.DataFrame:
    if not df_type:
        return None
    if df_type == "need_df":
        spreadsheet_id = need_spreadsheet_id
    elif df_type == "give_df":
        spreadsheet_id = give_spreadsheet_id

    df = get_df(spreadsheet_id)

    return df


def pre_clean_data(df) -> pd.DataFrame:
    """
    This function clean the data before the main cleaning
    :param df:
    :return:
    """
    # Remove " from each column string
    df.columns = df.columns.str.replace('"', "")

    df = df.replace("זוג", 2)

    return df


def clean_need_df(need_house_df) -> pd.DataFrame:
    """
    This function clean the need_house_df
    :param need_house_df:
    :return:
    """
    # read the excel file
    need_house_df = pre_clean_data(need_house_df)

    # Rename the columns to English
    need_house_df = need_house_df.rename(
        columns={
            # "חותמת זמן": "timestamp",
            # "כתובת אימייל": "email address",
            "שם מלא": "full name",
            # "טלפון  (אנא ציינו רק ספרות, ללא מקף ורווח)": "phone number",
            # "מאיזה יישוב אתם מגיעים ?  ": "origin city",
            "מה מספר אורחים שצריכים מקום?": "number of guests",
            # "הערות/בקשות": "notes/requests",
            "האם יש בעח שבאים איתכם ?": "pets",
            # "האם זקוקים לעזרה בהסעות?": "transportation assistance",
            "האם שומרי כשרות?": "kosher",
            # "האם זקוקים לבית מונגש ?": "accessible",
            # "פירוט על בעח": "pets details",
            # "פירוט לגבי בית מונגש": "accessible details",
            # "פירוט לגבי כשרות": "kosher details",
            # "הערות": "notes",
            "בטיפול של מי?": "treatment",
            # "אצל מי מתארחים": "who is hosting",
            # "שונות": "other",
            "מצב הבקשה": "request status",
            # "Unnamed: 19": "unknown",
            "האם חובה ממד  ?(שימו לב-  בית עם מקלט במקום ממד מזרז משמעותית זמני טיפול) ": "mamad",
        }
    )
    need_house_df = post_clean_data(need_house_df)

    # Filter only rows that have nan in the "request status" column or have "בטיפול" in the "request status" column
    need_house_df = need_house_df[
        need_house_df["request status"].isna()
        | need_house_df["request status"].str.contains("בטיפול")
        | need_house_df["request status"].str.contains("איפוס")
        | need_house_df["request status"].str.contains("ממתינים לבית ריק")
        | need_house_df["request status"].str.contains("איפוס (להחזיר שורה ללבנה)")
    ]

    # If pets column appear twice so drop only one of them
    try:
        if need_house_df["pets"].shape[1] == 2:
            pets_column = need_house_df["pets"].iloc[:, 0]
            need_house_df = need_house_df.drop(columns=["pets"])
            need_house_df["pets"] = pets_column
    except IndexError:
        pass
    # Reset index
    # need_house_df = need_house_df.reset_index()
    return need_house_df


def post_clean_data(df) -> pd.DataFrame:
    """
    This function clean the data after the main cleaning
    :param df:
    :return:
    """
    # Filter rows that have nan in the "number of guests" column
    df = df[df["number of guests"].notna()]

    # Convert the "number of guests" column to string
    df["number of guests"] = df["number of guests"].astype(str)

    # Convert the "can have pets" column to string
    df["pets"] = df["pets"].astype(str)

    # Convert the "kosher" column to string
    df["kosher"] = df["kosher"].astype(str)

    return df


def give_filters(need_house_df, my_range, service, spreadsheet_id) -> list:
    """
    This function add filter views to the give houses spreadsheet
    :param need_house_df:
    :param my_range:
    :param service:
    :param spreadsheet_id:
    :return:
    """
    rows_with_errors = []
    tqdm_need_houses = tqdm(need_house_df.iterrows())
    n = 1
    # iterate through the need houses
    for i, row in tqdm_need_houses:
        # if row['full name'] != 'אהרון אילנית':
        #     continue
        # if n < 166:
        #     n += 1
        #     continue
        body = create_filter_view_request(
            my_range,
            row["full name"],
            str(i + 2),
            row["number of guests"],
            row["kosher"],
            row["pets"],
            # row['accessible'],
            row["mamad"],
        )
        error = update_spreadsheet(spreadsheet_id, body, service)
        if error:
            rows_with_errors.append([row["full name"], str(i + 2)])
        sleep(1)
        # n += 1
    return rows_with_errors


def create_txt_files(people_that_have_filter, rows_with_errors) -> None:
    """
    This function create two txt files:
    1. people_that_have_filter.txt - contains the people that have filter
    2. people_that_have_errors.txt - contains the people that have errors

    :param people_that_have_filter:
    :param rows_with_errors:
    :return:
    """
    # write to text file the people that have filter:
    with open("people_that_have_filter.txt", "w") as f:
        for item in people_that_have_filter:
            f.write("%s\n" % item)

    # write to text file the people that have errors:
    with open("people_that_have_errors.txt", "w") as f:
        for item in rows_with_errors:
            f.write("%s\n" % item)


def print_info_about_errors(rows_with_errors) -> None:
    """
    This function print info about the errors
    :param rows_with_errors:
    :return:
    """
    if len(rows_with_errors) == 0:
        print(f"No errors occurred")
    else:
        print(f"rows with errors: \n {rows_with_errors}")
        print(f" {len(rows_with_errors)} errors occurred")


def create_give_house_filters(need_house_df, give_spreadsheet_id, give_gsheet_id) -> None:
    """
    This function add the give houses filters to the spreadsheet
    :param need_house_df:
    :param give_spreadsheet_id:
    :param give_gsheet_id:
    :return:
    """
    my_range, service = init_spreadsheet(give_spreadsheet_id, give_gsheet_id)
    rows_with_errors = give_filters(
        need_house_df, my_range, service, give_spreadsheet_id
    )
    print_info_about_errors(rows_with_errors)
    create_txt_files(people_that_have_filter, rows_with_errors)


def create_filter_view_request_treatment(my_range, treatment_name):
    """
    This function create the filter view request for the man who treat the need house
    :param my_range:
    :param treatment_name:
    :return:
    """
    addfilterviewrequest = {
        "addFilterView": {
            "filter": {
                "title": treatment_name,
                "range": my_range,
                "sortSpecs": [
                    {
                        "dimensionIndex": 0,  # I choose 0 because its the date column.
                        "sortOrder": "ASCENDING",
                    },
                ],
                "criteria": {
                    "17": {
                        "condition": {
                            "type": "TEXT_EQ",
                            "values": {"userEnteredValue": treatment_name},
                        }
                    },
                },
            }
        }
    }
    body = {"requests": [addfilterviewrequest]}
    return body


def treatment_filters(need_house_df, my_range, service, need_spreadsheet_id) -> list:
    """
    This function add filter views to the treatment column in the need houses spreadsheet
    :param need_house_df:
    :param my_range:
    :param service:
    :param need_spreadsheet_id:
    :return:
    """
    rows_with_errors = []
    # Remove rows that need_house_df['treatment'] has '' in them
    need_house_df = need_house_df.loc[need_house_df["treatment"] != ""]
    treatment_people = need_house_df["treatment"].unique()
    need_house_df = need_house_df.loc[need_house_df["treatment"].isin(treatment_people)]
    # choose only one row for each treatment
    need_house_df = need_house_df.drop_duplicates(subset=["treatment"])
    tqdm_need_houses = tqdm(need_house_df.iterrows())
    n = 1
    # iterate through the need houses
    for i, row in tqdm_need_houses:
        body = create_filter_view_request_treatment(my_range, row["treatment"])
        error = update_spreadsheet(need_spreadsheet_id, body, service)
        if error:
            rows_with_errors.append([row["full name"], str(i + 2)])
        sleep(1)
        # n += 1
    return rows_with_errors


def create_treatment_filters(need_house_df, need_spreadsheet_id, need_gsheet_id) -> None:
    """
    This function add the treatment filters to the spreadsheet
    :param need_house_df:
    :param need_spreadsheet_id:
    :param need_gsheet_id:
    :return:
    """
    my_range, service = init_spreadsheet(need_spreadsheet_id, need_gsheet_id)
    rows_with_errors = treatment_filters(
        need_house_df, my_range, service, need_spreadsheet_id
    )


def create_filter_view_request_type(my_range, request_type):
    """
    This function create the filter view request for the request type in the need houses spreadsheet
    :param my_range:
    :param request_type:
    :return:
    """
    if request_type == None:
        request_type = ""
    addfilterviewrequest = {
        "addFilterView": {
            "filter": {
                "title": "-- " + request_type,
                "range": my_range,
                "sortSpecs": [
                    {
                        "dimensionIndex": 0,  # I choose 0 because its the date column.
                        "sortOrder": "ASCENDING",
                    },
                ],
                "criteria": {
                    "20": {
                        "condition": {
                            "type": "TEXT_EQ",
                            "values": {"userEnteredValue": request_type},
                        }
                    },
                },
            }
        }
    }
    body = {"requests": [addfilterviewrequest]}
    return body


def request_type_filters(need_house_df, my_range, service, need_spreadsheet_id) -> list:
    """
    This function add filter views to the request type column in the need houses spreadsheet
    :param need_house_df:
    :param my_range:
    :param service:
    :param need_spreadsheet_id:
    :return:
    """
    rows_with_errors = []
    request_type = need_house_df["request status"].unique()
    need_house_df = need_house_df.loc[
        need_house_df["request status"].isin(request_type)
    ]
    # choose only one row for each treatment
    need_house_df = need_house_df.drop_duplicates(subset=["request status"])
    tqdm_need_houses = tqdm(need_house_df.iterrows())
    n = 1
    # iterate through the need houses
    for i, row in tqdm_need_houses:
        body = create_filter_view_request_type(my_range, row["request status"])
        error = update_spreadsheet(need_spreadsheet_id, body, service)
        if error:
            rows_with_errors.append([row["full name"], str(i + 2)])
        sleep(1)
        # n += 1
    return rows_with_errors


def create_request_type_filters(need_house_df, need_spreadsheet_id, need_gsheet_id) -> None:
    """
    This function add the request type filters to the spreadsheet
    :param need_house_df:
    :param need_spreadsheet_id:
    :param need_gsheet_id:
    :return:
    """
    my_range, service = init_spreadsheet(need_spreadsheet_id, need_gsheet_id)
    rows_with_errors = request_type_filters(
        need_house_df, my_range, service, need_spreadsheet_id
    )


def main():
    give_spreadsheet_id = ""
    give_gsheet_id = 0
    need_spreadsheet_id = ""
    need_gsheet_id = 0

    need_house_df = get_df_from_google_sheet(
        "need_df",
        give_spreadsheet_id=give_spreadsheet_id,
        need_spreadsheet_id=need_spreadsheet_id,
    )
    need_house_df = clean_need_df(need_house_df)

    create_give_house_filters(need_house_df, give_spreadsheet_id, give_gsheet_id)
    create_treatment_filters(need_house_df, need_spreadsheet_id, need_gsheet_id)
    create_request_type_filters(need_house_df, need_spreadsheet_id, need_gsheet_id)

    print(f"Done!")


if __name__ == "__main__":
    main()

