from airflow import DAG
from NbaPlugin.hooks.nba_hook import NbaHook
from NbaPlugin.operators.nba_to_s3_operator import NbaToS3Operator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks import NbaHook


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
                id=203932, stats='regular_season_totals')

    return g.call()


with dag:
    kick_off_dag = DummyOperator(task_id='kick_off_dag')
    github_pull = PythonOperator(task_id='pull_issues', python_callable=aaron_gordon)
    to_s3 = NbaToS3Operator(
        task_id='this_wont_work',
        endpoint='player',
        method='PlayerCareer',
        id=203932,
        stats='regular_season_totals',
        s3_conn_id='astronomer-s3',
        s3_bucket='astronomer-workflows-dev',
        s3_key='nba-test.json',
        )

    kick_off_dag >> github_pull >> to_s3
