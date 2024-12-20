from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

# Define the DAG
with DAG(
    dag_id = 'nasa_apod_postgres',
    start_date = days_ago(1),
    schedule_interval = '@daily',
    catchup = False
) as dag:
    ## Step 1 : Create the table if it doesn't exist
    @task
    def create_table():
        ## Initialize the Postgreshook
        postgres_hook = PostgresHook(postgres_conn_id = "my_postgres_connection")

        ## SQL query to create table (apod_data)
        create_table_query = """
        CREATE TABLE IF NOT EXISTS apod_data(
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
        );
        """
        ## Execute the table cration query
        postgres_hook.run(create_table_query)

    ## Step 2 : Extract the nasa API Data(Apod) 
    extract_apod=SimpleHttpOperator(
        task_id='extract_apod',
        http_conn_id='nasa_api', ## Connection Id Defined in Airflow for NASA API
        endpoint="planetary/apod",
        method="GET",
        ## Using the API Key from connection
        data={"api_key":"{{conn.nasa_api.extra_dejson.api_key}}"},
        response_filter=lambda response:response.json(),
    )

    ## Step 2 : Transform the data(Pick the information that i need to save)
    @task
    def transform_apod_data(response):
        apod_data={
            'title':response.get('title',''),
            'explanation': response.get('explanation',''),
            'url': response.get('url',''),
            'date': response.get('date',''),
            'media_type': response.get('media_type','')
        }
        return apod_data
    
    ## Step 4 : Load the Data into Postgres SQL
    @task
    def load_data_to_postgres(apod_data):
        ## Initialize the Postgreshook
        postgres_hook = PostgresHook(postgres_conn_id = "my_postgres_connection")

        # Defining insert query
        insert_query = """
        INSERT INTO apod_data (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s)
        """

        # Execute the SQL Query
        postgres_hook.run(insert_query, parameters=(
            apod_data['title'],
            apod_data['explanation'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type'],
        ))

    ## Step 6 : Define the task Dependencies
    create_table() >> extract_apod
    api_response = extract_apod.output
    transformed_data = transform_apod_data(api_response)
    load_data_to_postgres(transformed_data)