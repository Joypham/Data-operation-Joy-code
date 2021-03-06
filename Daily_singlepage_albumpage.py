from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pandas as pd
import mysql.connector
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import numpy as np

mydb = mysql.connector.connect(
  host="localhost",
  user="sysadm",
  password="ek7F7ck3",
  database="v4",
  port= 3308
)
mycursor = mydb.cursor()

def service():
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'Data operation-8a21cdf07e0f.json',SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)
    return service

def gspread_values(SPREADSHEET_ID,SHEET_NAME):
    # Call the Sheets API
    sheet = service().spreadsheets()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
                                range=SHEET_NAME).execute()
    values = result.get('values')
    return values

def get_df():
    data = gspread_values(SPREADSHEET_ID,SHEET_NAME)
    column = data[0]
    check_fistrow = data[1]
    x = len(column) - len(check_fistrow)
    k = [None] * x
    check_fistrow.extend(k)                             # if only have column name but all data of column null =>> error
    row = data[2:]
    row.insert(0, check_fistrow)
    df = pd.DataFrame(row, columns=column)
    return df

def get_album_info(tuple_albumuuid):
    get_album_info_result = [['uuid','title','artist','wiki_url','wiki_content']]
    mycursor.execute(f"SELECT uuid, title, artist, info ->> '$.wiki_url',info ->> '$.wiki.brief' from albums WHERE uuid in {tuple_albumuuid}")
    result = mycursor.fetchall()
    for i in result:
        if i == []:
            continue
        else:
            result = list(i)
            get_album_info_result.append(result)
    return (get_album_info_result)

def get_track_info(tuple_trackid):
    get_track_info_result = [['trackid','title','artist','wiki_url','wiki_content']]
    mycursor.execute(f"SELECT id, title, artist, info ->> '$.wiki_url',info ->> '$.wiki.brief' from tracks WHERE id in {tuple_trackid}")
    result = mycursor.fetchall()
    for i in result:
        if i == []:
            continue
        else:
            result = list(i)
            get_track_info_result.append(result)
    return (get_track_info_result)

def get_track_lyrics(tuple_trackid):
    get_track_info_result = [['trackid','title','artist','lyrics']]
    mycursor.execute(f"SELECT id, title, artist,Lyrics from tracks WHERE id in {tuple_trackid}")
    result = mycursor.fetchall()
    for i in result:
        if i == []:
            continue
        else:
            result = list(i)
            get_track_info_result.append(result)
    return (get_track_info_result)

def update_value(list_result, range_to_update):
    body = {
        'values': list_result                                   # list_result is array 2 dimensional (2D)
    }
    result = service().spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID, range=range_to_update,
        valueInputOption='RAW', body=body).execute()

def creat_new_spreadsheet(new_spreadsheet_title):
    new_spread_title = {'properties': {'title': f"{new_spreadsheet_title}"}}
    spreadsheet = service().spreadsheets().create(body=new_spread_title,
                                                fields='spreadsheetId').execute()
    print('Spreadsheet ID: {0}'.format(spreadsheet.get('spreadsheetId')))
    return print('Spreadsheet ID: {0}'.format(spreadsheet.get('spreadsheetId')))

def add_new_worksheet(SPREADSHEET_ID,new_worksheet_title):
    scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("Data operation-8a21cdf07e0f.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SPREADSHEET_ID)      # open by ggsheet_id
    # sheet = client.open_by_key(ggsheet_id)        # open by ggsheet_id
    # sheet = client.open("Joy draf")               # open by ggsheet_title
    # sheet.update_cell(i[2], column_index, i[1])   # Update one cell (row, colum)
    sheet.add_worksheet(title=new_worksheet_title, rows="1000", cols="20")

def final_process_get_track_info():
    print("Get df from spreadsheet Single page")
    df = get_df()
    print("tuple_trackid to get wiki info")
    tuple_trackid = tuple(df[(df.TrackId != '') & (df.TrackId != 'TrackId') & (df.TrackId.notnull())]['TrackId'].tolist())
    print(tuple_trackid)

    # add new worksheet
    new_worksheet_title = 'Jay xau tinh'
    print(f"Adding new worksheet title:  {new_worksheet_title}")
    add_new_worksheet(SPREADSHEET_ID, new_worksheet_title)

    # Update data in new worksheet
    print("Updating new worksheet")
    range_to_update = f"{new_worksheet_title}!A1:E"
    list_result = get_track_info(tuple_trackid)
    update_value(list_result, range_to_update)

def final_process_get_track_lyrics():
    print("Get df from spreadsheet Singlepage")
    df = get_df()
    print("tuple_trackid to get lyrics")
    tuple_trackid = tuple(df[(df.TrackId != '') & (df.TrackId != 'TrackId') & (df.TrackId.notnull())]['TrackId'].tolist())
    print(tuple_trackid)

    # add new worksheet
    new_worksheet_title = 'Jay rat xau tinh'
    print(f"Adding new worksheet title:  {new_worksheet_title}")
    add_new_worksheet(SPREADSHEET_ID, new_worksheet_title)

    # Update data in new worksheet
    print("Updating new worksheet")
    range_to_update = f"{new_worksheet_title}!A1:E"
    list_result = get_track_lyrics(tuple_trackid)
    update_value(list_result, range_to_update)

def final_process_get_album_info():
    print("Get df from spreadsheet Albumpage")
    df = get_df()
    print("tuple_albumuuid to get wiki info")
    tuple_albumuuid = tuple(df[(df.albumuuid != '')& (df.albumuuid != 'Album_UUID')& (df.albumuuid.notnull())]['albumuuid'].tolist())
    print(tuple_albumuuid)

    # add new worksheet
    new_worksheet_title = 'Jay xau tinh'
    print(f"Adding new worksheet title:  {new_worksheet_title}")
    add_new_worksheet(SPREADSHEET_ID, new_worksheet_title)

    # Update data in new worksheet
    print("Updating new worksheet")
    range_to_update = f"{new_worksheet_title}!A1:E"
    list_result = get_album_info(tuple_albumuuid)
    update_value(list_result, range_to_update)

def crawl_artistimage_albumpage():
    df = get_df()
    joy_xinh = df[(df.Artist_UUID != '') & (df.Artist_UUID != 'Artist_UUID') & (df.image_url.notnull()) & (df.A12 == 'missing') & (df.image_url != '')][['Artist_UUID','A12','image_url']]
    row_index = joy_xinh.index
    with open("query.txt","w") as f:
        for i in row_index:
            x = df['Artist_UUID'].iloc[i]
            y = df['image_url'].iloc[i]
            joy_xinh = f"insert into crawlingtasks(Id, ActionId,objectid ,TaskDetail, Priority) values (uuid4(), 'OA9CPKSUT6PBGI1ZHPLQUPQCGVYQ71S9','{x}',JSON_SET(IFNULL(crawlingtasks.TaskDetail, JSON_OBJECT()), '$.url','{y}','$.object_type',\"artist\",'$.when_exists',\"replace\",'$.PIC',\"Joy_xinh\"),99);"
            print(joy_xinh)
            f.write(joy_xinh + "\n")
    # time.sleep(300)

def get_artistimage_cant_upload_albumpage(tuple_artistuuid):
    get_artistimage_cant_upload_albumpage_result = [['Artistname','ArtistUUID','Old_imageurl','Crawlingtask_status','artists.Imageurl']]
    mycursor.execute(f" SELECT artists.name, artists.UUID,crawlingtasks.TaskDetail ->> '$.url',crawlingtasks.`Status`,artists.square_image_url from crawlingtasks\
                        Join artists on artists.UUID = crawlingtasks.ObjectId\
                        WHERE ActionId = 'OA9CPKSUT6PBGI1ZHPLQUPQCGVYQ71S9'\
                        AND DATE(crawlingtasks.CreatedAt) = CURRENT_DATE\
                        AND status = 'incomplete'\
                        AND ObjectId in {tuple_artistuuid}")
    result = mycursor.fetchall()
    for i in result:
        if i == []:
            continue
        else:
            result = list(i)
            get_artistimage_cant_upload_albumpage_result.append(result)
    return (get_artistimage_cant_upload_albumpage_result)

def final_process_get_artistimage_cant_upload_albumpage_result():

    print("Get df from spreadsheet Albumpage")
    df = get_df()
    print("tuple_artistuuid to get artistimage cant upload albumpage result")
    tuple_artistuuid = tuple(df[(df.Artist_UUID != '') & (df.Artist_UUID != 'Artist_UUID') & (df.image_url.notnull()) & (df.A12 == 'missing') & (df.image_url != '')]['Artist_UUID'].tolist())
    print(tuple_artistuuid)

    # add new worksheet
    new_worksheet_title = 'ArtistImage cant upload'
    print(f"Adding new worksheet title:  {new_worksheet_title}")
    add_new_worksheet(SPREADSHEET_ID, new_worksheet_title)

    # Update data in new worksheet
    print("Updating new worksheet")
    range_to_update = f"{new_worksheet_title}!A1:E"
    list_result = get_artistimage_cant_upload_albumpage(tuple_artistuuid)
    if len(list_result) == 1:
        list_result.append(['Upload thành công 100% nhé các em ^ - ^'])
    else:
        list_result = list_result
    update_value(list_result, range_to_update)

def crawl_artistimage_singlepage():
    df = get_df()
    joy_xinh = df[(df.Artist_UUID != '') & (df.Artist_UUID != 'Artist_UUID') & (df.image_url.notnull()) & (
                df.s12 == 'missing') & (df.image_url != '')][['Artist_UUID', 's12', 'image_url']]
    row_index = joy_xinh.index
    with open("query.txt", "w") as f:
        for i in row_index:
            x = df['Artist_UUID'].iloc[i]
            y = df['image_url'].iloc[i]
            joy_xinh = f"insert into crawlingtasks(Id, ActionId,objectid ,TaskDetail, Priority) values (uuid4(), 'OA9CPKSUT6PBGI1ZHPLQUPQCGVYQ71S9','{x}',JSON_SET(IFNULL(crawlingtasks.TaskDetail, JSON_OBJECT()), '$.url','{y}','$.object_type',\"artist\",'$.when_exists',\"replace\",'$.PIC',\"Joy_xinh\"),99);"
            print(joy_xinh)
            f.write(joy_xinh + "\n")
    # time.sleep(300)

def get_artistimage_cant_upload_singlepage(tuple_artistuuid):
    get_artistimage_cant_upload_singlepage_result = [['Artistname', 'ArtistUUID', 'Old_imageurl', 'Crawlingtask_status', 'artists.Imageurl']]
    mycursor.execute(f" SELECT artists.name, artists.UUID,crawlingtasks.TaskDetail ->> '$.url',crawlingtasks.`Status`,artists.square_image_url from crawlingtasks\
                        Join artists on artists.UUID = crawlingtasks.ObjectId\
                        WHERE ActionId = 'OA9CPKSUT6PBGI1ZHPLQUPQCGVYQ71S9'\
                        AND DATE(crawlingtasks.CreatedAt) = CURRENT_DATE\
                        AND status = 'incomplete'\
                        AND ObjectId in {tuple_artistuuid}")
    result = mycursor.fetchall()
    for i in result:
        if i == []:
            continue
        else:
            result = list(i)
            get_artistimage_cant_upload_singlepage_result.append(result)
    return (get_artistimage_cant_upload_singlepage_result)

def final_process_get_artistimage_cant_upload_singlepage_result():

    print("Get df from spreadsheet Singlepage")
    df = get_df()
    print("tuple_artistuuid to get artistimage cant upload singlepage result")
    tuple_artistuuid = tuple(df[(df.Artist_UUID != '') & (df.Artist_UUID != 'Artist_UUID') & (df.image_url.notnull()) & (df.s12 == 'missing') & (df.image_url != '')]['Artist_UUID'].tolist())
    print(tuple_artistuuid)

    # add new worksheet
    new_worksheet_title = 'ArtistImage cant upload'
    print(f"Adding new worksheet title:  {new_worksheet_title}")
    add_new_worksheet(SPREADSHEET_ID, new_worksheet_title)

    # Update data in new worksheet
    print("Updating new worksheet")
    range_to_update = f"{new_worksheet_title}!A1:E"
    list_result = get_artistimage_cant_upload_singlepage(tuple_artistuuid)
    if len(list_result) == 1:
        list_result.append(['Upload thành công 100% nhé các em ^ - ^'])
    else:
        list_result = list_result
    update_value(list_result, range_to_update)

def final_update_wiki_singlepage():
    print("Get df from spreadsheet Singlepage")
    df = get_df()
    joy_xinh = df[((df.memo == 'added')|(df.memo == 'not ok')) & (df.content_to_add.notnull()) & (df.url_to_add.notnull())][['id', 'url_to_add', 'content_to_add', 'memo']]
    row_index = joy_xinh.index
    print("Writing query to update tracks.wiki")
    with open("query.txt", "w") as f:
        for i in row_index:
            id = df['id'].iloc[i]
            memo = df['memo'].iloc[i]
            url = df['url_to_add'].iloc[i]
            content = df['content_to_add'].iloc[i].replace('\'', '\\\'').replace("\"", "\\\"")
            joy_xinh = f"Update tracks set info =  Json_replace(Json_remove(info,'$.wiki'),'$.wiki_url','not ok') where id = '{id}';"
            query = ""
            if memo == "added":
                query = f"UPDATE tracks SET info = Json_set(if(info is null,JSON_OBJECT(),info), '$.wiki', JSON_OBJECT('brief', '{content}'), '$.wiki_url','{url}') WHERE id = '{id}';"
            else:
                query = query
            f.write(joy_xinh + "\n" + query +  "\n")
    print("Completed writing query and saved in query.ext")
    #     Update file Spreadsheet:

    # Add column pandas complicated Conditions                          # Tạo column chỉ với 1 điều kiện có thể dùng where : df['Joy xinh'] = np.where(df['memo'] != 'not ok', False, 'remove wiki'):
    print("Updating spreadsheet wiki_sheet")
    conditions = [                                                      # create a list of condition => if true =>> update value tương ứng
        (df['memo'] == 'not ok'),
        (df['memo'] == 'added') & (df['content_to_add'].notnull()) & (df.url_to_add.notnull()),
        True
    ]
    values = ['remove wiki', 'wiki added',None]                         # create a list of the values tương ứng với conditions ơ trên
    df['joy xinh'] = np.select(conditions, values)                      # create a new column and use np.select to assign values to it using our lists as arguments
    column_title = ['Joy note']
    list_result = np.array(df['joy xinh']).reshape(-1,1).tolist()       # Chuyển về list từ 1 chiều về 2 chiều sử dung Numpy
    list_result.insert(0, column_title)
    range_to_update = f"{SHEET_NAME}!H1"
    # time.sleep(300)
    update_value(list_result, range_to_update)
    print("Completed update data in spreadsheet")

def final_update_wiki_albumpage():
    print("Get df from spreadsheet albumpage")
    df = get_df()
    joy_xinh = df[((df.memo == 'added')|(df.memo == 'not ok')) & (df.content_to_add.notnull()) & (df.url_to_add.notnull())][['uuid', 'url_to_add', 'content_to_add', 'memo']]
    row_index = joy_xinh.index
    print("Writing query to update albums.wiki")
    with open("query.txt", "w") as f:
        for i in row_index:
            uuid = df['uuid'].iloc[i]
            memo = df['memo'].iloc[i]
            url = df['url_to_add'].iloc[i]
            content = df['content_to_add'].iloc[i].replace('\'', '\\\'').replace("\"", "\\\"")
            joy_xinh = f"Update albums set info =  Json_replace(Json_remove(info,'$.wiki'),'$.wiki_url','not ok') where uuid = '{uuid}';"
            query = ""
            if memo == "added":
                query = f"UPDATE albums SET info = Json_set(if(info is null,JSON_OBJECT(),info), '$.wiki', JSON_OBJECT('brief', '{content}'), '$.wiki_url','{url}') WHERE uuid = '{uuid}';"
            else:
                query = query
            f.write(joy_xinh + "\n" + query +  "\n")
    print("Completed writing query and saved in query.ext")
    #     Update file Spreadsheet:

    # Add column pandas complicated Conditions                          # Tạo column chỉ với 1 điều kiện có thể dùng where : df['Joy xinh'] = np.where(df['memo'] != 'not ok', False, 'remove wiki'):
    print("Updating spreadsheet wiki_sheet")
    conditions = [                                                      # create a list of condition => if true =>> update value tương ứng
        (df['memo'] == 'not ok'),
        (df['memo'] == 'added') & (df['content_to_add'].notnull()) & (df.url_to_add.notnull()),
        True
    ]

    values = ['remove wiki', 'wiki added',None]                         # create a list of the values tương ứng với conditions ơ trên
    df['joy xinh'] = np.select(conditions, values)                      # create a new column and use np.select to assign values to it using our lists as arguments
    column_title = ['Joy note']
    list_result = np.array(df['joy xinh']).reshape(-1,1).tolist()       # Chuyển về list từ 1 chiều về 2 chiều sử dung Numpy
    list_result.insert(0, column_title)
    range_to_update = f"{SHEET_NAME}!J1"
    # time.sleep(300)
    update_value(list_result, range_to_update)
    print("Completed update data in spreadsheet")

if __name__ == "__main__":
    start_time = time.time()

# INPUT HERE
    SPREADSHEET_ID = '1uhp3BKGJwR1eRSVx-j1iF3BBu03zei87tW2w9EHBkSk'     #  albumpage
    SHEET_NAME = '03.08.2020'



# CHOOSE A FUNCTION TO RUN

    print("Select tool to run...")

# GET INFO
#     final_process_get_track_info()
#     final_process_get_track_lyrics()
    final_process_get_album_info()
# Crawl artist_image
#     crawl_artistimage_albumpage()
#     crawl_artistimage_singlepage()
# Export image can't upload and re_fixed
#     final_process_get_artistimage_cant_upload_albumpage_result()
#     final_process_get_artistimage_cant_upload_singlepage_result()
# Update wiki
#      final_update_wiki_singlepage()
#      final_update_wiki_albumpage()

print("--- %s seconds ---" % (time.time() - start_time))



