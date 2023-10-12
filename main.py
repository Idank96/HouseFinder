from pathlib import Path
import pandas as pd
from tqdm import tqdm
import pickle
import os

def get_need_houses(need_house_path):
    # read the excel file
    need_house_df = pd.read_excel(need_house_path, skiprows=1)
    #Remove " from each column string
    need_house_df.columns = need_house_df.columns.str.replace('"', '')
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
})

    # Filter only rows that have nan in the "request status" column
    need_house_df = need_house_df[need_house_df['request status'].isna()]

    # Filter rows that have nan in the "number of guests" column
    need_house_df = need_house_df[need_house_df['number of guests'].notna()]

    # Convert the "number of guests" column to string
    need_house_df['number of guests'] = need_house_df['number of guests'].astype(str)

    # Convert the "can have pets" column to string
    need_house_df['pets'] = need_house_df['pets'].astype(str)

    # Convert the "kosher" column to string
    need_house_df['kosher'] = need_house_df['kosher'].astype(str)

    # Reset index
    need_house_df = need_house_df.reset_index()
    return need_house_df


def get_give_houses(give_house_path):
    give_house_df = pd.read_excel(give_house_path, skiprows=1)
    # Remove " from each column string
    give_house_df.columns = give_house_df.columns.str.replace('"', '')
    # Rename the columns to English
    give_house_df = give_house_df.rename(columns={
    "חותמת זמן": "timestamp",
    "כתובת אימייל": "email address",
    "שם מלא": "full name",
    "טלפון  ": "phone number",
    "מה מספר אורחים שאתם יכולים לארח? ": "number of guests",
    "הערות": "notes",
    "יישוב ": "city",
    "האם שומרי כשרות": "kosher",
    "האם הבית מונגש": "accessible",
    "האם יש לכם בעח בבית? ": "pets",
    "הערות לגבי כשרות": "kosher details",
    " האם יכולים לארח בעח?": "can have pets",
    "האם יש ממד בבית": "balcony",
    "האם מדובר במרחב נפרד (יחד/בית/דירה) שיעמוד לרשות האורחים?": "separate space",
    "מיון": "sorting",
    "בטיפול מי": "who is in charge?",
    "Unnamed: 16": "unknown 16",
    "Unnamed: 17": "unknown 17",
    "Unnamed: 18": "unknown 18",
    "Unnamed: 19": "unknown 19",
    "Unnamed: 20": "unknown 20",
})

    # Filter rows that have nan in the "number of guests" column
    give_house_df = give_house_df[give_house_df['number of guests'].notna()]

    # Convert the "number of guests" column to string
    give_house_df['number of guests'] = give_house_df['number of guests'].astype(str)

    # Convert the "can have pets" column to string
    give_house_df['can have pets'] = give_house_df['can have pets'].astype(str)

    # Convert the "kosher" column to string
    give_house_df['kosher'] = give_house_df['kosher'].astype(str)
    give_house_df = give_house_df.reset_index()

    # Reset index
    give_house_df = give_house_df.reset_index()

    return give_house_df


def iterate_through_houses(need_houses_df, give_houses_df):
    print('start matching...')
    matches = {}
    tqdm_need_houses = tqdm(need_houses_df.iterrows())
    # iterate through the need houses
    for i, row in tqdm_need_houses:
        # iterate through the give houses
        for i2, row2 in give_houses_df.iterrows():
            if row['number of guests'] in row2['number of guests']:
                # if ((row['pets'] == 'כן' and 'לא' not in row2['can have pets']) or
                #         (row['pets'] == 'לא' and 'לא' in row2['can have pets'])):
                if ((row['kosher'] == 'כן' and 'כן' in row2['kosher']) or
                        (row['kosher'] == 'לא' and 'לא' in row2['kosher'])):
                    matches[i] = [] if i not in matches else matches[i]
                    matches[i].append(i2)

    return matches


def create_table_for_each_match(matches, need_houses_df, give_houses_df):
    # For each match, create a table with the need house and the give house
    for need_house_index, give_houses_indexes in matches.items():
        # Create new dataframe
        df = pd.DataFrame(columns=need_houses_df.columns)
        # Add the need house to the dataframe
        df = pd.concat([df, need_houses_df.loc[[need_house_index]]])
        # Create the second row empty
        df.loc[len(df)] = pd.Series(dtype='float64')
        # Add the give houses to the dataframe
        df = pd.concat([df, give_houses_df.iloc[give_houses_indexes]])
        # Return to origin columns in Hebrew
        df = df.rename(columns={
        "timestamp": "חותמת זמן",
        "email address": "כתובת אימייל",
        "full name": "שם מלא",
        "phone number": "טלפון  ",
        "number of guests": "מה מספר אורחים שאתם יכולים לארח? ",
        "notes": "הערות",
        "city": "יישוב ",
        "kosher": "האם שומרי כשרות",
        "accessible": "האם הבית מונגש",
        "pets": "האם יש לכם בעח בבית? ",
        "kosher details": "הערות לגבי כשרות",
        "can have pets": " האם יכולים לארח בעח?",
        "balcony": "האם יש ממד בבית",
        "separate space": "האם מדובר במרחב נפרד (יחד/בית/דירה) שיעמוד לרשות האורחים?",
        "sorting": "מיון",
        "who is in charge?": "בטיפול מי",
        "unknown 16": "unknown 16",
        "unknown 17": "unknown 17",
        "unknown 18": "unknown 18",
        "unknown 19": "unknown 19",
        "unknown 20": "unknown 20",
        })

        need_full_name = need_houses_df.iloc[need_house_index]['full name']
        path_to_save = os.path.join("matches", f"{need_house_index}_{need_full_name}.xlsx")
        # Write the df to a new excel file
        df.to_excel(path_to_save, index=False)


def save_matches_to_pickle(matches):
    with open('matches.pickle', 'wb') as file:
        pickle.dump(matches, file, protocol=pickle.HIGHEST_PROTOCOL)


def main():
    need_house_path = Path(r'need_house.xlsx')
    give_house_path = Path(r'give_house.xlsx')

    need_houses_df = get_need_houses(need_house_path)
    give_houses_df = get_give_houses(give_house_path)

    # Load the matches from a pickle file
    with open('matches.pickle', 'rb') as handle:
        matches = pickle.load(handle)

    # matches = iterate_through_houses(need_houses_df, give_houses_df)
    # save_matches_to_pickle(matches)

    create_table_for_each_match(matches, need_houses_df, give_houses_df)

if __name__ == '__main__':
    main()
    print('Done')