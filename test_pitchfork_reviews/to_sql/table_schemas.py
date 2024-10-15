reviews_table_schema = """
CREATE TABLE IF NOT EXISTS reviews (
    reviewid INT,
    title CHAR(230),
    artist CHAR(95),
    url VARCHAR(265),
    score DECIMAL(4,2),
    best_new_music INT,
    author CHAR(70),
    author_type CHAR(40),
    pub_date DATE,
    pub_weekday INT,
    pub_day INT,
    pub_month INT,
    pub_year INT
)
"""

# CREATE TABLE reviews (
#     reviewid INTEGER,
#     title TEXT,
#     artist TEXT,
#     url TEXT,
#     score REAL,
#     best_new_music INTEGER,
#     author TEXT,
#     author_type TEXT,
#     pub_date TEXT,
#     pub_weekday INTEGER,
#     pub_day INTEGER,
#     pub_month INTEGER,
#     pub_year INTEGER)

artists_table_schema = """
CREATE TABLE IF NOT EXISTS artists (
    reviewid INT,
    artist CHAR(70)
)
"""

genres_table_schema = """
CREATE TABLE genres (
    reviewid INT,
    genre CHAR(20)
)
"""

labels_table_schema = """
CREATE TABLE labels (
    reviewid INT,
    label CHAR(40)
)
"""

years_table_schema = """
CREATE TABLE years (
    reviewid INT,
    year INT
)
"""

content_table_schema = """
CREATE TABLE content (
    reviewid INT,
    content TEXT
)
"""