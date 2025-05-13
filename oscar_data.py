from datetime import datetime, timedelta
import requests

from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

from psycopg2.extras import execute_values
from airflow.providers.postgres.hooks.postgres import PostgresHook

# dbt
from dbtsl.env import PLATFORM
PLATFORM.anonymous = True
from dbt.cli.main import dbtRunner

from queries.create_table import oscar_src_schema, oscar_movies, oscar_directors, oscar_stars, oscar_movie_directors, oscar_movie_stars


args = {
    'owner': 'sochi',
    'start_date': datetime.datetime(2008, 1, 1),
    'email': ['airflow_dba@sochi.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 10,
    'retry_delay': timedelta(minutes=10),
    'provide_context': False,
}

dag = DAG(
    dag_id='oscar_data_flow',
    default_args=args,
    schedule_interval='30 9 * * 1-5/2',
    catchup=False,
    max_active_runs=1,
    concurrency = 1
)


Variable.set('oscar_flow_state', '')

def get_data():

    # get json data
    
    url = 'https://github.com/sharmadhiraj/free-json-datasets/blob/master/datasets/oscar-best-picture-award-winners.json'
    url = url.replace('github', 'raw.githubusercontent')
    url = url.replace('blob', 'refs/heads')
    r = requests.get(url)
    if r.status_code != 200:
        # raise Exception ("can't get json file")
        Variable.set('oscar_flow_state', "oscar data flow error: can't get json file")
        return
    json_data = r.json()
    
    # pg hook
    
    try:
        pg_hook = PostgresHook(postgres_conn_id='my_postgres')
        pg_conn = pg_hook.get_conn()
        pg_cursor = pg_conn.cursor()
    except:
        Variable.set('oscar_flow_state', "oscar data flow error: can't establish postgres connection")
        return
        

    # create tables if not exist
    
    schemas = ['src','ods']
    for schema in schemas:
    
        pg_cursor.execute(oscar_src_schema.format(schema=schema))
        pg_cursor.execute(oscar_movies.format(schema=schema))
        pg_cursor.execute(oscar_directors.format(schema=schema))
        pg_cursor.execute(oscar_stars.format(schema=schema))
        pg_cursor.execute(oscar_movie_directors.format(schema=schema))
        pg_cursor.execute(oscar_movie_stars.format(schema=schema))
    
    
    # process data
    
    movies      = []
    directors   = []
    stars       = []
    movie_directors = []
    movie_stars     = []
    
    
    for record in json_data:
        try:
            name            = record['name']
            oscar           = record['oscar']
            released_year   = record['released_year']
            directors       = record['directors']
            stars           = record['stars']
        except:
            Variable.set('oscar_flow_state', f'oscar data flow error: wrong json, record: {record}')
            return
        
        movies.append([name, released_year, oscar])
        movie_directors += [(name, director) for director in directors]
        movie_stars     += [(name, star) for star in stars]

        directors       += list(set([r[1] for r in movie_directors]))
        stars           += list(set([r[1] for r in movie_stars]]))

    # insert data
    execute_values(pg_cursor, f'''INSERT INTO src.oscar_directors("name")
                                    SELECT * FROM
                                    VALUES %s d("name") 
                                    WHERE NOT EXISTS (SELECT * FROM src.oscar_directors d2 where d2."name" = d."name")''', directors)
    
    execute_values(pg_cursor, f'''INSERT INTO src.oscar_stars("name") 
                                    VALUES %s s("name") 
                                    WHERE NOT EXISTS (SELECT * FROM src.oscar_stars s2 where s2."name" = s."name")''', stars)
    
    execute_values(pg_cursor, f'''INSERT INTO src.oscar_movies("name", released_year, oscar) 
                                    SELECT * FROM
                                    VALUES %s m("name", released_year, oscar) 
                                    WHERE NOT EXISTS (SELECT * FROM src.oscar_movies m2 where m2."name" = m."name")''', movies)

    execute_values(pg_cursor, f'''INSERT INTO src.movie_directors (movie_id, director_id) 
                                    SELECT m.id, d.id FROM 
                                    VALUES %s md(movie_name, director_name)
                                    JOIN src.oscar_movies       m on m."name" = md.movie_name
                                    JOIN src.oscar_directors    d on d."name" = md.director_name
                                    WHERE NOT EXISTS (SELECT * FROM src.movie_directors md2 where md2.movie_id = m.id)''', movie_directors)
                                    
    execute_values(pg_cursor, f'''INSERT INTO src.movie_stars (movie_id, star_id) 
                                    SELECT m.id, s.id FROM 
                                    VALUES %s ms(movie_name, star_name)
                                    JOIN src.oscar_movies   m on m."name" = ms.movie_name
                                    JOIN src.oscar_stars    s on s."name" = ms.star_name
                                    WHERE NOT EXISTS (SELECT * FROM src.movie_stars ms2 where ms2.movie_id = m.id)''', movie_stars)

def from_src_to_osd():
    if Variable.get('oscar_flow_state') != '':
        return

    dbt_cli = dbtRunner()

    dbt_query = '''

    {% set table_names          = ["oscar_directors", "oscar_stars", "oscar_movies", "movie_directors", "movie_stars"] %}
    {% set table_key_columns    = ["id", "id", "id", "movie_id", "movie_id"] %}
    {% set tables_keys          = zip(table_names, table_key_columns) | list %}
    
    {% for (table_name, key_column_name) in tables_keys %}
    
        INSERT INTO ods.{{table_name}}
        SELECT * FROM src.{{table_name}} src
        WHERE NOT EXISTS (SELECT * FROM ods.{{table_name}} dst2 WHERE dst2.{{key_column_name}} = src.{{key_column_name}}

    {% endfor %}
    '''
    
    result = dbt_cli.invoke(['compile','--inline', dbt_query])
    
    if not result.success:
        Variable.set('oscar_flow_state', f"oscar data flow error: dbt macros execution error: {result.exception}")
        return
    
def send_status_to_telegram():

    if tg_message:=Variable.get('oscar_flow_state') == '':
        tg_message = 'oscar data flow good' 

    conn_info       = BaseHook.get_connection('telegram_allert')
    user_id         = conn_info.login # dba tg user id
    bot_token       = conn_info.password

    send_url = f'https://api.telegram.org/bot{bot_token}/sendMessage'

    resp = requests.post(send_url, json={'chat_id': chat_id, 'text': tg_message})
    if resp.status_code != 200:
        Variable.set('oscar_flow_state', f"oscar data flow error: все не работает, даже в телегу аллерт, даже вспомнил русский  -_+")


get_data_task                = PythonOperator(task_id='get_data', default_args=args, dag=dag, python_callable=get_data)    
from_src_to_osd_task         = PythonOperator(task_id='from_src_to_osd', default_args=args, dag=dag, python_callable=from_src_to_osd)    
send_status_to_telegram_task = PythonOperator(task_id='send_status_to_telegram', default_args=args, dag=dag, python_callable=send_status_to_telegram)    
    
get_data_task >> from_src_to_osd_task
from_src_to_osd_task >> send_status_to_telegram_task