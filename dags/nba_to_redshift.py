from airflow import DAG
from NbaPlugin.hooks.nba_hook import NbaHook
from NbaPlugin.operators.nba_to_s3_operator import NbaToS3Operator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

default_args = {
                'owner': 'airflow',
                'start_date': datetime(2017, 10, 19)
}

dag = DAG('le_bull',
          default_args=default_args,
          schedule_interval='@once'
          )


def aaron_gordon():
    g = NbaHook(endpoint='player', method='PlayerCareer',
                id='203932', stats='regular_season_totals')

    return g.call()


players = {
    'kristaps_porzingis': 204001,
    'frankie_smokes': 1628373,
    'aaron_gordon': 203932,
    'greek_freak': 203507,
    'john_wall': 202322
}

with dag:
    kick_off_dag = DummyOperator(task_id='kick_off_dag')
    for player in players:

        #hook_pull = PythonOperator(task_id='hook_pull', python_callable=aaron_gordon)
        to_s3 = NbaToS3Operator(
            task_id='{0}_to_s3'.format(player),
            player_name=player,
            endpoint='player',
            method='PlayerCareer',
            id=players[player],
            stats='regular_season_totals',
            s3_conn_id='astronomer-s3',
            s3_bucket='astronomer-workflows-dev',
            s3_key='nba-test.json',
            )

        kick_off_dag >> to_s3
