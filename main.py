import json

import psycopg2

# Load in the artist and song data from the json files and set the keys to lowercase.
with open("data/artist.json") as json_file:
    artist_list_lowercase_list = [
        {k.lower(): v for k, v in artist.items()} for artist in json.load(json_file)
    ]

with open("data/song.json") as json_file:
    song_list_lower_case = [
        {k.lower(): v for k, v in song.items()} for song in json.load(json_file)
    ]

try:
    connection = psycopg2.connect(
        user="admin", password="s3cret", host="localhost", port="5432", database="music-db"
    )
    cursor = connection.cursor()
    print("Connection established with PostgreSQL")

    # SQL query to create a new song table
    create_song_table_query = """CREATE TABLE song
          (ID        SERIAL PRIMARY KEY,
           NAME      TEXT,
           YEAR      INT,
           ARTIST    TEXT,
           SHORTNAME TEXT,
           BPM       INT,
           DURATION  INT,
           GENRE     TEXT,
           SPOTIFYID TEXT,
           ALBUM     TEXT
           ); """
    cursor.execute(create_song_table_query)

    cursor.executemany(
        """
        INSERT INTO 
        song(name, year, artist, shortname, bpm, duration, genre, spotifyid, album)
        VALUES (%(name)s, %(year)s, %(artist)s, %(shortname)s, %(bpm)s, %(duration)s, %(genre)s, %(spotifyid)s, %(album)s)
        """,
        song_list_lower_case,
    )

    print("Song table created successfully in PostgreSQL")

    create_artist_table_query = """CREATE TABLE artist
          (ID   SERIAL PRIMARY KEY,
           NAME TEXT
           ); """
    cursor.execute(create_artist_table_query)

    cursor.executemany(
        """
        INSERT INTO 
        artist(name) 
        VALUES (%(name)s)
        """,
        artist_list_lowercase_list,
    )

    print("Artist table created successfully in PostgreSQL ")

    connection.commit()

except (Exception) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
