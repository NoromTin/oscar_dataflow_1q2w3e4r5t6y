oscar_src_schema = '''CREATE SCHEMA IF NOT EXISTS {schema}'''

oscar_movies = '''
CREATE TABLE IF NOT EXISTS {schema}.oscar_movies (
	id              int4 NOT NULL generated always  as identity,
	"name"          varchar NOT NULL,
	released_year   smallint NOT NULL,
    oscar           smallint NOT NULL,
	CONSTRAINT oscar_movies_id_pk PRIMARY KEY (id)
);
CREATE UNIQUE INDEX ix_oscar_movies__name ON {schema}.oscar_movies ("name") 
'''

oscar_directors = '''
CREATE TABLE IF NOT EXISTS {schema}.oscar_directors (
	id              int4 NOT NULL generated always  as identity,
	"name"          varchar NOT NULL,
	CONSTRAINT oscar_directors__id__pk PRIMARY KEY (id)
);
CREATE UNIQUE INDEX oscar_directors__name ON {schema}.oscar_directors ("name") 
'''

oscar_stars = '''
CREATE TABLE IF NOT EXISTS {schema}.oscar_stars (
	id              int4 NOT NULL generated always  as identity,
	"name"          varchar NOT NULL,
	CONSTRAINT oscar_stars__id__pk PRIMARY KEY (id)
);
CREATE UNIQUE INDEX oscar_stars__name ON {schema}.oscar_stars ("name") 
'''

oscar_movie_directors = '''
CREATE TABLE IF NOT EXISTS {schema}.oscar_movie_directors (
	movie_id            int4 NOT NULL,
	director_id         int4 NOT NULL,
	CONSTRAINT oscar_movie_directors__movie_id__director_id__pk PRIMARY KEY (movie_id, director_id)
);
'''

oscar_movie_stars = '''
CREATE TABLE IF NOT EXISTS {schema}.oscar_movie_stars (
	movie_id       int4 NOT NULL,
	star_id        int4 NOT NULL,
	CONSTRAINT oscar_movie_stars__movie_id__director_id__pk PRIMARY KEY (movie_id, star_id)
);
'''