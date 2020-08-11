import mysql.connector
import time
import numpy as np
import pandas as pd
import json
import datetime


pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 2000)

mydb = mysql.connector.connect(
  host="localhost",
  user="sysadm",
  password="ek7F7ck3",
  database="v4",
  port= 3308
)
mycursor = mydb.cursor()


def get_list_collectionuuid_youtuburl():
    # Only miragte if not exist: collectionid and youtube_url (Please note that: primary key collection_datasource base on: collectionuuid and datasourceuuid

    mycursor.execute(f" SELECT\
                        collections.UUID as collectionUUID,\
                        'https://www.youtube.com/watch?v=' ||\
                        if(LEFT(right(filename,LENGTH(filename)-8),3)='___',\
                        SUBSTRING_INDEX(right(filename,LENGTH(filename)-8),'___',2),\
                        SUBSTRING_INDEX(right(filename,LENGTH(filename)-8),'___',1)\
                        ) as youtube_url,\
                        videos.id as videoId,\
                        collection_video.rank as `rank`,\
                        collection_video.ext as `ext`\
                        FROM collections\
                        join users on users.id = collections.UserId and users.Valid = 1\
                        JOIN collection_video on collections.id = collection_video.CollectionId and collection_video.CreatedAt > '2019-06-28'\
                        join videos on videos.id = collection_video.VideoId and videos.Valid = 1\
                        LEFT JOIN collection_datasource on collection_datasource.CollectionId = collections.UUID AND collection_datasource.SourceURI =\
						'https://www.youtube.com/watch?v=' ||\
                        if(LEFT(right(videos.filename,LENGTH(videos.filename)-8),3)='___',\
                        SUBSTRING_INDEX(right(videos.filename,LENGTH(videos.filename)-8),'___',2),\
                        SUBSTRING_INDEX(right(videos.filename,LENGTH(videos.filename)-8),'___',1))\
                        where users.id = '616177616402580'\
                        and collections.valid = 1\
                        and collection_datasource.DatasourceId is NULL order by collections.UUID,collection_video.rank asc\
                        ")
    result = mycursor.fetchall()
    list_collectionuuid_youtuburl = []
    for i in result:
        if i == []:
            continue
        else:
            list_collectionuuid_youtuburl.append(list(i))
    column_name = ['collectionUUID','youtube_url','videoId','rank','ext']
    df = pd.DataFrame(np.array(list_collectionuuid_youtuburl),
                       columns=column_name)
    return df

def get_best_datasourceId_from_youtubeurl():
    df = get_list_collectionuuid_youtuburl()
    tuple_youtube_url = tuple(set(df['youtube_url'].tolist()))                  # Select disctint youtube_url, then put into tuple (Increase performance)
    mycursor.execute(f" SELECT SourceURI, datasource_id\
                            from ( SELECT t1.*, row_number () over (PARTITION BY t1.SourceURI  ORDER BY t1.datasource_count desc,t1.datasource_formatID_priority desc) as row_num\
                            from ( SELECT DISTINCT\
                            datasources.SourceURI,\
                            datasources.id as datasource_id,\
                            datasources.TrackId,\
                            if(datasources.FormatID = '74BA994CF2B54C40946EA62C3979DDA3', 10,if( datasources.FormatID = '1A67A5F1E0D84FB9B48234AE65086375', 9,if( datasources.FormatID = 'C78F687CB3BE4D90B30F49435317C3AC',8,if(datasources.FormatID <> '',7,6)))) as datasource_formatID_priority,\
                            tracks.title,\
                            tracks.artist,\
                            CAST( trackcountlog.DataSourceCount ->> '$.all' AS UNSIGNED ) AS datasource_count\
                            FROM datasources\
                            join tracks on tracks.id = datasources.TrackId and tracks.Valid = 1\
                            Join itunes_album_tracks_release on itunes_album_tracks_release.TrackName = tracks.Title and itunes_album_tracks_release.TrackArtist = tracks.Artist and itunes_album_tracks_release.Valid = 1\
                            LEFT JOIN trackcountlog on trackcountlog.TrackID = datasources.TrackId\
                            where datasources.SourceURI in {tuple_youtube_url}\
                            and datasources.Valid =1\
                            and datasources.TrackId <> ''\
                            ORDER BY SourceURI) as t1 ) as t2\
                            where row_num = 1")
    result = mycursor.fetchall()
    list_best_datasourceId_from_youtube_url = []
    for i in result:
        if i == []:
            continue
        else:
            list_best_datasourceId_from_youtube_url.append(list(i))
    column_name = ['youtube_url', 'datasourceId']
    df = pd.DataFrame(np.array(list_best_datasourceId_from_youtube_url),
                      columns=column_name)
    return (df)

def youtubeurl_not_existed_best_datasource():
    df1 = get_list_collectionuuid_youtuburl()
    df2 = get_best_datasourceId_from_youtubeurl()
    total_youtubeurl = list(set(df1['youtube_url']))
    youtubeurl_existed_best_datasource = list(set(df2['youtube_url']))
    youtubeurl_not_existed_best_datasource = np.setdiff1d(total_youtubeurl, youtubeurl_existed_best_datasource)
    df = pd.DataFrame(np.array(youtubeurl_not_existed_best_datasource),
                      columns=['youtube_url'])
    return df

def migrate_videos_to_datasource():
    df1 = get_list_collectionuuid_youtuburl()[['youtube_url', 'videoId']]
    df2 = youtubeurl_not_existed_best_datasource()
    df = pd.merge(df1, df2, on=['youtube_url'], how='inner').drop_duplicates(subset=['videoId'],keep='first') # remove duplicate videoId, giu nguyen row_inex
    df['youtubeId'] = df['youtube_url'].str[-11:]
    row_index = df.index
    with open("query.txt", "w") as f:
        for i in row_index:
            youtube_url = df['youtube_url'].loc[i]
            youtubeId = df['youtubeId'].loc[i]
            videoId = df['videoId'].loc[i]
            mycursor.execute(
                f"SELECT EXISTS (SELECT id from datasources WHERE OldVideoId = '{videoId}') as `true/false`;")
            result = mycursor.fetchall()
            if result[0][0] == 1:                       # Check videos đã được migrate xuống datasource rồi thì không migrate lại nữa
                query = f"-- videoId = {videoId} have been migrate to datasources already"
            else:
                query = f"INSERT into datasources (`id`,`TrackId`,`Valid`,`UpdatedAt`,`CreatedAt`,`FormatID`,`SourceName`,`SourceURI`,`DataVerified`,`DataVerifiedAt`,`IsVideo`,`ArtistName`,`CDN`,`FileName`,`DurationMs`,`IntegratedLoudness`,`Width`,`Height`,`Info`,`DisplayStatus`,`ext`,`OldVideoId`,`youtubeid`,`IsMigrate`) SELECT uuid4() as `id`, '' as `TrackId`,1 as `Valid`,null as `UpdatedAt`,CreatedAt as `CreatedAt`,'' as `FormatID`,'youtube' as `SourceName`,\
                            '{youtube_url}' as `SourceURI`,0 as `DataVerified`,null as `DataVerifiedAt`,1 as `IsVideo`,videos.Artist as `ArtistName`,'berserker0.vibbidi-vid.com' as`CDN`,videos.Filename || '.mp4'  as `FileName`, CAST(videos.Duration * 1000 as UNSIGNED) as `DurationMs`,videos.IntegratedLoudness AS `IntegratedLoudness`,videos.Width as `Width`,videos.Height as `Height`,JSON_OBJECT('vibbidi_title',videos.Title,'vibbidi_artist', videos.Artist) as `Info`,0 as `DisplayStatus`,JSON_INSERT(`Ext`,'$.Title',videos.Title) as `ext`,{videoId} as `OldVideoId`,'{youtubeId}' as `youtubeid`,200805 as `IsMigrate`\
                            from videos WHERE id = '{videoId}';"
            f.write(query + "\n")

def match_collection_datasource_from_videos_migrated():                                                   # Chú ý, phải có ít nhất 2 videosId chưa được match collectiondatasource thì mới chạy được
    df1 = get_list_collectionuuid_youtuburl()
    df2 = youtubeurl_not_existed_best_datasource()
    df = df1.merge(df2, on=['youtube_url'], how='inner').astype({"videoId": int})
    tuple_videoId = tuple(df['videoId'].tolist())
    mycursor.execute(f"SELECT OldVideoId,id from datasources WHERE OldVideoId in {tuple_videoId} ;")      # Error if not have any record result
    result = mycursor.fetchall()
    list_videoId_datasourceId = []
    for i in result:
        if i == []:
            continue
        else:
            list_videoId_datasourceId.append(list(i))
    column_name = ['videoId', 'datasourceId']
    df_videoId_datasourceId = pd.DataFrame(np.array(list_videoId_datasourceId),
                      columns=column_name).astype({"videoId": int})
    df_collection_datasource = df.merge(df_videoId_datasourceId, on=['videoId'], how='left').drop_duplicates(subset=['collectionUUID','youtube_url'],keep='first').astype({"rank": int}).sort_values(['collectionUUID', 'rank'], ascending=[True, True]).reset_index(drop=True)
    return df_collection_datasource


def match_collection_best_datasourceid():
    df1 = get_list_collectionuuid_youtuburl()
    df2 = get_best_datasourceId_from_youtubeurl()
    return df1.merge(df2, on=['youtube_url'], how='inner').drop_duplicates(subset=['collectionUUID', 'youtube_url'], keep= 'first').astype({"rank": int}).sort_values(['collectionUUID', 'rank'], ascending=[True, True]).reset_index(drop=True)

def match_collection_final():
    df_collection_datasource1 = match_collection_datasource_from_videos_migrated()
    df_collection_datasource2 = match_collection_best_datasourceid()
    df_collection_datasource_final = pd.concat([df_collection_datasource1, df_collection_datasource2]).drop_duplicates(subset=['collectionUUID', 'youtube_url'], keep= 'first').astype({"rank": int}).sort_values(['collectionUUID', 'rank'], ascending=[True, True]).reset_index(drop=True)
    print(df_collection_datasource_final)
    row_index = df_collection_datasource_final.index
    with open("query.txt", "w") as f:
        for i in row_index:
            collectionUUID = df_collection_datasource_final['collectionUUID'].loc[i]
            youtube_url = df_collection_datasource_final['youtube_url'].loc[i]
            rank = df_collection_datasource_final['rank'].loc[i]
            ext = df_collection_datasource_final['ext'].loc[i]
            ext_reformat = json.dumps(ext,ensure_ascii=False)           # Note that ensure_ascii= False => keep text not change: Eg "Joy xinh qua" =>> "hsajfghou"
            datasourceId = df_collection_datasource_final['datasourceId'].loc[i]
            query = f"INSERT into collection_datasource(`CollectionId`, `DatasourceId`, `Priority`, `Ext`, `SourceURI`) values ('{collectionUUID}','{datasourceId}',{rank},cast ({ext_reformat} as json),'{youtube_url}');"
            # print(query)
            f.write(query + "\n")

if __name__ == "__main__":
    start_time = time.time()
    # time.sleep(20)
    print(get_list_collectionuuid_youtuburl())
    # print(get_best_datasourceId_from_youtubeurl())
    # print(youtubeurl_not_existed_best_datasource())
    # print(migrate_videos_to_datasource())
    # match_collection_datasource_from_videos_migrated()
    # match_collection_best_datasourceid()
    match_collection_final()
print("--- %s seconds ---" % (time.time() - start_time))
