import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """Processes song data file and inserts song and artist records.

    Given a filepath to a song data .json file this
    function will insert a song and artist record into the
    song and artist table respectively.

    Parameters
    ----------
    cur : cursor
        cursor to the sparkify database connection
    filepath : str
        file path to song data .json file
    """

    # open song file
    df = pd.read_json(filepath, typ='series')

    # insert song record
    song_data = df[[
        'song_id',
        'title',
        'artist_id',
        'year',
        'duration']
    ].values.tolist()
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = df[[
        'artist_id',
        'artist_name',
        'artist_location',
        'artist_latitude',
        'artist_longitude']
    ].values.tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """Processes log data file and inserts timedata, user and songplay records.

    Given a filepath to a log data .json file (expecting a json on each line)
    this function will filter by 'NextSong' action and convert timestamps into
    datetimes and insert a time data record into the time data table. It will
    also insert user records into the users table and last insert songplay
    records into the songplays table if no song and artist records are matched
    from the songs and artists table (using song name, artist name and song
    length to match) then None will be used for song and artist id.

    Parameters
    ----------
    cur : cursor
        cursor to the sparkify database connection
    filepath : str
        file path to log data .json file
    """

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    df['ts'] = t

    # insert time data records
    time_data = (
        t,
        t.dt.hour,
        t.dt.day,
        t.dt.weekofyear,
        t.dt.month,
        t.dt.year,
        t.dt.weekday
    )
    column_labels = (
        'start_time',
        'hour',
        'day',
        'week',
        'month',
        'year',
        'weekday'
    )
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[[
        'userId',
        'firstName',
        'lastName',
        'gender',
        'level']
    ]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
            row.ts,
            row.userId,
            row.level,
            songid,
            artistid,
            row.sessionId,
            row.location,
            row.userAgent
        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """Processes over all .json files inside a directory plus all subdirs.

    Given a file path to a directory this function iterates over all .json
    files found inside the directory including inside all subdirectories.
    All found .json files will be processed using the supplied function.
    Prints process progress to terminal.

    Parameters
    ----------
    cur : cursor
        cursor to the sparkify database connection
    conn : connection
        connection to the sparkify database
    filepath : str
        file path to directory
    func : function
        process function to use to process each file with
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
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
    """ Processes all .json files inside data/song_data and data/log_data."""

    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student \
        password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
