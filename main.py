import sqlalchemy


class postgres:

    def __init__(self, bd):
        self.sql = sqlalchemy.create_engine(bd).connect()

    def create_tables(self, tables_dict):
        for table_name in tables_dict:
            for desc in tables_dict[table_name]:
                self.sql.execute(f"""CREATE TABLE IF NOT EXISTS {table_name} (
                            {desc}
                            );
                """)
        return True

    def insert(self, main_data, default=None):
        for table_name in main_data:
            for data in main_data[table_name]:
                if default:
                    self.sql.execute(f"""INSERT INTO {table_name}
                    VALUES (DEFAULT, {data});
                        """)
                else:
                    self.sql.execute(f"""INSERT INTO {table_name}
                    VALUES ({data});
                        """)
        return True

    def select(self, select_list):
        for select in select_list:
            answer = self.sql.execute(f"""SELECT {select};""").fetchall()
            print(answer, end='\n\n')
        return True


tables = {
                'genres': ["""
                        id SERIAL PRIMARY KEY,
                        name varchar(40) NOT NULL UNIQUE
                            """],
                'artists': ["""
                        id SERIAL PRIMARY KEY, 
                        name varchar(40) NOT NULL,
                        rating numeric NOT NULL CHECK(rating > 0)
                            """],
                'albums': ["""
                        id SERIAL PRIMARY KEY, 
                        name varchar(40) NOT NULL,
                        year varchar(4) NOT NULL
                            """],
                'tracks': ["""
                        id SERIAL PRIMARY KEY, 
                        name varchar(40) NOT NULL,
                        duration numeric NOT NULL CHECK(duration > 0),
                        album_id integer REFERENCES albums(id) NOT NULL
                            """],
                'collections': ["""
                        id SERIAL PRIMARY KEY, 
                        name varchar(40) NOT NULL,
                        year varchar(4) NOT NULL
                            """],
                'GenresArtists': ["""
                        gener_id integer REFERENCES genres(id),
                        artist_id integer REFERENCES artists(id),
                        CONSTRAINT genresartists_pk PRIMARY KEY (gener_id, artist_id)
                            """],
                'ArtistsAlbums': ["""
                        artist_id integer REFERENCES artists(id),
                        album_id integer REFERENCES albums(id),
                        CONSTRAINT artistsalbums_pk PRIMARY KEY (album_id, artist_id)
                            """],
                'CollectionsTracks': ["""
                        collection_id integer REFERENCES collections(id),
                        track_id integer REFERENCES tracks(id),
                        CONSTRAINT collectionstracks_pk PRIMARY KEY (collection_id, track_id)
                            """]
                 }

main_info = {
    'genres': ["'rap'", "'rock'", "'pop'", "'electro'", "'jazz'"],
    'artists': [
        "'Eminem', 4.5",
        "'AC/DC', 5.8",
        "'Lady Gaga', 4.6",
        "'Boris Brejcha', 5.2",
        "'Frank Sinatra', 6.0",
        "'2pac', 4.2",
        "'Nirvana', 6.0",
        "'Maroon 5', 4.4",
        "'Dj Snake', 5.2",
        "'Miles Davis', 3.2"
    ],
    'albums': [
        "'The Eminem Show', 2002",
        "'Live at River Plate', 2012",
        "'Chromatica', 2020",
        "'Space Driver', 2020",
        "'Live At The Meadowands', 2009",
        "'All Eyez On Me', 1996",
        "'Nevermind', 1991",
        "'V', 2015",
        "'Carte Blanche', 2019",
        "'Everything''s Beautiful', 2016",
        "'Mixed Album', 2021"
    ],
    'tracks': [
        "'Soldier', 3.46, 1",
        "'Drips', 4.46, 1",
        "'The Jack', 10.12, 2",
        "'Thunderstruck', 5.32, 2",
        "'Alice', 2.58, 3",
        "'911', 2.52, 3",
        "'Blue Lake', 8.10, 4",
        "'Future', 7.12, 4",
        "'Monologue', 3.08, 5",
        "'Holla at Me', 4.55, 6",
        "'Breed', 3.04, 7",
        "'Stay Away', 3.31, 7",
        "'Animals', 3.51, 8",
        "'Sugar', 3.55, 8",
        "'Try My', 3.18, 9",
        "'Paris', 3.45, 9",
        "'Song for Selim', 2.40, 10",
        "'Violets', 3.24, 10",
        "'Tone Deaf', 4.50, 1"
    ],
    'collections': [
        "'Super Hits', 2008",
        "'Mega Songs', 2012",
        "'Top 1000 Songs Ever', 2020",
        "'Lost Collection p1', 2007",
        "'Lost Collection p2', 2008",
        "'All Best Tracks', 1996",
        "'Sing With Me', 1991",
        "'Karaoke songs', 2015",
    ]
}

connections = {
    'GenresArtists': [
        "1, 1",
        "2, 2",
        "3, 3",
        "4, 4",
        "5, 5",
        "1, 6",
        "2, 7",
        "3, 8",
        "4, 9",
        "5, 10",
    ],
    'ArtistsAlbums': [
        "1, 1",
        "2, 2",
        "3, 3",
        "4, 4",
        "5, 5",
        "6, 6",
        "7, 7",
        "8, 8",
        "9, 9",
        "10, 10",
        "1, 11",
        "2, 11",
        "3, 11"
    ],
    'CollectionsTracks': [
        "1, 1",
        "2, 2",
        "3, 3",
        "4, 4",
        "5, 5",
        "6, 6",
        "7, 7",
        "8, 8",
        "1, 9",
        "2, 10",
        "3, 11",
        "4, 12",
        "5, 13",
        "6, 14",
        "7, 15",
        "8, 16",
        "1, 17",
        "2, 18",
    ]
}

select_list = [
    """name, year FROM albums
    WHERE year = '2020'""",

    """name, duration FROM tracks
    ORDER BY duration DESC
    LIMIT 1""",

    """name FROM tracks
    WHERE duration >= 3.50""",

    """name FROM collections
    WHERE year = '2018' or year = '2019' or year = '2020'""",

    """name FROM tracks
    WHERE duration BETWEEN 3.20 and 7.00""",

    """id, name, rating FROM artists
    WHERE name NOT ILIKE '%% %%'""",

    """name FROM tracks
    WHERE name ILIKE '%%my%%'"""
]

select_list_2 = [
    """name, COUNT(gs.artist_id) FROM genres AS g
    JOIN genresartists AS gs ON g.id = gs.gener_id
    GROUP BY name""",

    """a.name, COUNT(t.name) AS c FROM albums AS a
    JOIN tracks AS t ON a.id = t.album_id
    WHERE year = '2020' or year = '2019'
    GROUP BY a.name
    ORDER BY c DESC""",

    """a.name, AVG(t.duration) AS d FROM albums AS a
    JOIN tracks as T ON a.id = t.album_id
    GROUP BY a.name
    ORDER BY d DESC""",

    """ar.name FROM artists AS ar
    JOIN artistsalbums AS aa ON ar.id = aa.artist_id
    JOIN albums AS al ON al.id = aa.album_id
    WHERE year != '2020'
    GROUP BY ar.name""",

    """c.name FROM collections AS c
    JOIN collectionstracks AS ct ON c.id = ct.collection_id
    JOIN TRACKS AS T ON ct.track_id = t.id
    JOIN albums AS al ON t.album_id = al.id
    JOIN artistsalbums AS aa ON al.id = aa.album_id
    JOIN artists AS ar ON aa.artist_id = ar.id
    WHERE ar.name = 'Boris Brejcha'""",

    """al.name FROM albums AS al
    JOIN artistsalbums AS aa ON al.id = aa.album_id
    JOIN artists AS ar ON aa.artist_id = ar.id
    JOIN genresartists AS gs ON ar.id = gs.artist_id
    GROUP BY al.name
    HAVING COUNT(gs.gener_id) > 1""",

    """t.name FROM tracks AS t
    LEFT JOIN collectionstracks AS ct ON t.id = ct.track_id
    WHERE ct.track_id IS NULL""",

    """ar.name FROM artists AS ar
    JOIN artistsalbums AS aa ON ar.id = aa.artist_id
    JOIN albums AS al ON aa.album_id = al.id
    JOIN tracks AS t ON al.id = t.album_id
    WHERE t.duration <= (
        SELECT MIN(duration) FROM tracks)""",

    """al.name, COUNT(t.id) AS c FROM albums AS al
    JOIN tracks AS t ON al.id = t.album_id
    GROUP BY al.name
    HAVING COUNT(t.id) = (
        SELECT MIN(c) FROM (
            SELECT al.name, COUNT(t.id) AS c FROM albums AS al
            JOIN tracks AS t ON al.id = t.album_id
            GROUP BY al.name) AS minimum
        )"""
]

with open('/Users/rup/Work/BTC/Rules.txt') as file:
    connect = file.readline().strip('\n')

netology = postgres(connect)
netology.create_tables(tables)
netology.insert(main_info, default=True)
netology.insert(connections)
netology.select(select_list)
netology.select(select_list_2)
