import airflow
import datetime
import urllib.request as request
import pandas as pd
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

default_args_dict = {
    'start_date': datetime.datetime(2021, 10, 1, 0, 0, 0),
    'concurrency': 1,
    'schedule_interval': "0 17 * * 5",
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

rasmus_dag = DAG(
    dag_id='second_dag',
    default_args=default_args_dict,
    catchup=False,
    template_searchpath=['/opt/airflow/dags/']
)


# Downloading a file from an API/endpoint?

def _get_spreadsheet(epoch, url, output_folder):
    request.urlretrieve(url=url, filename=f"{output_folder}/{epoch}.xlsx")


task_one = PythonOperator(
    task_id='get_spreadsheet',
    dag=rasmus_dag,
    python_callable=_get_spreadsheet,
    op_kwargs={
        "output_folder": "/opt/airflow/dags",
        "epoch": "{{ execution_date.int_timestamp }}",
        "url": "http://www.lutemusic.org/spreadsheet.xlsx"
    },
    trigger_rule='all_success',
    depends_on_past=False,
)

# oh noes :( it's xlsx... let's make it a csv.

task_two = BashOperator(
    task_id='transmute_to_csv',
    dag=rasmus_dag,
    bash_command="xlsx2csv /opt/airflow/dags/{{ execution_date.int_timestamp }}.xlsx > /opt/airflow/dags/{{ execution_date.int_timestamp }}_correct.csv",
    trigger_rule='all_success',
    depends_on_past=False,
)


# Now we have to filter out

def _time_filter(previous_epoch: int, epoch: int, next_epoch: int, output_folder: str):
    df = pd.read_csv(f'{output_folder}/{str(epoch)}_correct.csv')
    df_filtered = df[(df['Modified'] >= int(previous_epoch)) & (df['Modified']) <= int(next_epoch)]
    df_filtered.to_csv(path_or_buf=f'{output_folder}/{str(previous_epoch)}_filtered.csv')


task_three = PythonOperator(
    task_id='time_filter',
    dag=rasmus_dag,
    python_callable=_time_filter,
    op_kwargs={
        "output_folder": "/opt/airflow/dags",
        "epoch": "{{ execution_date.int_timestamp }}",
        "previous_epoch": "{{ prev_execution_date.int_timestamp }}",
        "next_epoch": "{{ next_execution_date.int_timestamp }}",
    },
    trigger_rule='all_success',
    depends_on_past=False,
)


def _emptiness_check(previous_epoch: int, output_folder: str):
    df = pd.read_csv(f'{output_folder}/{str(previous_epoch)}_filtered.csv')
    length = len(df.index)
    if length == 0:
        return 'end'
    else:
        return 'split'


task_four = BranchPythonOperator(
    task_id='emptiness_check',
    dag=rasmus_dag,
    python_callable=_emptiness_check,
    op_kwargs={
        'previous_epoch': '{{ prev_execution_date.int_timestamp }}',
        "output_folder": "/opt/airflow/dags"
    },
    trigger_rule='all_success',
)


def _split(previous_epoch: int, output_folder: str):
    df = pd.read_csv(f'{output_folder}/{str(previous_epoch)}_filtered.csv')

    df = df.replace(',|"|\'|`', '', regex=True)

    df_character = df[['Piece', 'Type', 'Key', 'Difficulty', 'Date', 'Ensemble']]
    df_composer = df['Composer'].drop_duplicates()

    df_character.to_csv(path_or_buf=f'{output_folder}/{str(previous_epoch)}_character.csv')
    df_composer.to_csv(path_or_buf=f'{output_folder}/{str(previous_epoch)}_composer.csv')


task_five = PythonOperator(
    task_id='split',
    dag=rasmus_dag,
    python_callable=_split,
    op_kwargs={
        'output_folder': '/opt/airflow/dags',
        'previous_epoch': '{{ prev_execution_date.int_timestamp }}',
    },
    trigger_rule='all_success',
)

# 1. name*
# 2. attributes*
# 3. race*
# 4. languages*
# 5. class*
# 6. profficiency_choices*
# 7. level*
# 8. spells*
def _create_character_query(previous_epoch: int, output_folder: str):
    df = pd.read_csv(f'{output_folder}/{str(previous_epoch)}_character.csv')
    with open("/opt/airflow/dags/character_inserts.sql", "w") as f:
        df_iterable = df.iterrows()
        f.write(
            "CREATE TABLE IF NOT EXISTS character (\n"
            "name VARCHAR(255),\n"
            "attributes VARCHAR(1024),\n"
            "race VARCHAR(255),\n"
            "languages VARCHAR(255),\n"
            "class VARCHAR(255),\n"
            "profficiency_choices VARCHAR(1024),\n"
            "level VARCHAR(255),\n"
            "spells VARCHAR(255),\n"
        )
        for index, row in df_iterable:
            piece = row['Piece']
            type = row['Type']
            key = row['Key']
            difficulty = row['Difficulty']
            date = row['Date']
            ensemble = row['Ensemble']

            f.write(
                "INSERT INTO character VALUES ("
                f"'{piece}', '{type}', '{key}', '{difficulty}', '{date}', '{ensemble}'"
                ");\n"
            )

        f.close()


task_six_a = PythonOperator(
    task_id='create_character_query',
    dag=rasmus_dag,
    python_callable=_create_character_query,
    op_kwargs={
        'previous_epoch': '{{ prev_execution_date.int_timestamp }}',
        'output_folder': '/opt/airflow/dags',
    },
    trigger_rule='all_success',
)


def _create_composer_query(previous_epoch: int, output_folder: str):
    df = pd.read_csv(f'{output_folder}/{str(previous_epoch)}_composer.csv')
    with open("/opt/airflow/dags/composer_inserts.sql", "w") as f:
        df_iterable = df.iterrows()
        f.write(
            "CREATE TABLE IF NOT EXISTS composer (\n"
            "name VARCHAR(255)\n"
            ");\n"
        )
        for index, row in df_iterable:
            composer = row['Composer']

            f.write(
                "INSERT INTO composer VALUES ("
                f"'{composer}'"
                ");\n"
            )


task_six_b = PythonOperator(
    task_id='create_composer_query',
    dag=rasmus_dag,
    python_callable=_create_composer_query,
    op_kwargs={
        'previous_epoch': '{{ prev_execution_date.int_timestamp }}',
        'output_folder': '/opt/airflow/dags',
    },
    trigger_rule='all_success',
)

task_seven_a = PostgresOperator(
    task_id='insert_character_query',
    dag=rasmus_dag,
    postgres_conn_id='postgres_default',
    sql='character_inserts.sql',
    trigger_rule='all_success',
    autocommit=True
)

task_seven_b = PostgresOperator(
    task_id='insert_composer_query',
    dag=rasmus_dag,
    postgres_conn_id='postgres_default',
    sql='composer_inserts.sql',
    trigger_rule='all_success',
    autocommit=True,
)

join_tasks = DummyOperator(
    task_id='coalesce_transformations',
    dag=rasmus_dag,
    trigger_rule='none_failed'
)

end = DummyOperator(
    task_id='end',
    dag=rasmus_dag,
    trigger_rule='none_failed'
)

task_one >> task_two >> task_three >> task_four
task_four >> [task_five, end]
task_five >> [task_six_a, task_six_b]
task_six_a >> task_seven_a
task_six_b >> task_seven_b
[task_seven_a, task_seven_b] >> join_tasks
join_tasks >> end
