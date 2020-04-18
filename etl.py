import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from datetime import datetime

def process_song_file(cur, filepath):
    # open song file
    if filepath is None:
        print("Invalid or empty filepath")
        return
    
    df_all =  pd.read_json(filepath,lines=True)
    df = df_all[['song_id','title','artist_id','year','duration']]
    # insert song record
    song_data = df.values[0].tolist()
    #print(song_data)
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data =  df_all[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df[df.page.str.contains('NextSong')]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts']) 
    
    # insert time data records
    time_data = t.dt.to_pydatetime()
    column_labels = ('year','month','day','hour','weekday','week','millis')
    time_df = pd.DataFrame(columns=[col for col in column_labels])
    time_df['year'] = t.dt.year
    time_df['month'] = t.dt.month
    time_df['day']= t.dt.day
    time_df['hour']= t.dt.hour
    time_df['weekday']= t.dt.weekday
    time_df['week']= t.dt.week
    time_df['millis']= t.dt.time
    
        
    for i, row in time_df.iterrows():
        try:
            cur.execute(time_table_insert, list(row))
        except psycopg2.Error as e: 
            print("Error: Could not insert into time table of the Postgres database")
            print(e)

    # load user table
    user_df =  df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        try:
            cur.execute(user_table_insert, row)
        except psycopg2.Error as e: 
            print("Error: Could not insert the %s th row into users table of the Postgres database",i)
            print(e)
    # insert songplay records
    for index, row in df.iterrows():
        try:
             # get songid and artistid from song and artist tables
            cur.execute(song_select, (row.song, row.artist, row.length))
            results = cur.fetchone()
        except psycopg2.Error as e: 
            print("Error: Could not get %s th song and artist data from the Postgres database",index)
            print(e)
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = ((datetime.fromtimestamp(row.ts/1000)).time(),\
                         row.userId,row.level,songid,artistid,row.sessionId,row.location,row.userAgent)
    
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()