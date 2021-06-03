import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import datetime

def process_song_file(cur, filepath):
    """
    Description: This function is responsible for process JSON files in a directory,
    and then insert into tables in the database.

    Arguments:
        cur: the cursor object.
        filepath: log data or song data file path.

    Returns:
        None
    """  
    
    df = pd.read_json(filepath, lines=True).replace({pd.np.nan: None}) 

    song_data = list(df[['song_id','title','artist_id','year','duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    artist_data = list(df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Description: This function is responsible for process JSON files in a directory,
    filter rows and convert data type, and then insert into tables in the database.

    Arguments:
        cur: the cursor object.
        filepath: log data or song data file path.

    Returns:
        None
    """    
    
    df = pd.read_json(filepath, lines=True).replace({pd.np.nan: None})
    df = df[df['page']=="NextSong"]
    t = pd.to_datetime(df['ts'], unit='ms')
    
    time_data = (t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ("start_time", "hour", "day", "week", "month", "year", "weekday")
    time_df = pd.DataFrame(dict(zip(column_labels,time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    user_df = df[['userId','firstName','lastName','gender','level']].rename(
        columns={'userId': 'user_id', 
                 'firstName':'first_name', 
                 'lastName':'last_name'}).drop_duplicates()

    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    for index, row in df.iterrows():        
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        songplay_data = (datetime.datetime.fromtimestamp(row.ts/1000.0), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Description: This function is responsible for listing the files in a directory,
    and then executing the ingest process for each file according to the function
    that performs the transformation to save it to the database.

    Arguments:
        cur: the cursor object.
        conn: connection to the database.
        filepath: log data or song data file path.
        func: function that transforms the data and inserts it into the database.

    Returns:
        None
    """

    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Description: This function is responsible for establishing connection to the database,
    processing files and finally close connection.

    Arguments:
        None

    Returns:
        None
    """ 

    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()