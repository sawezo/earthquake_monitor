# # import sys
# # # from datetime import datetime

# # from airflow import DAG
# # # from airflow.operators.dummy_operator import DummyOperator
# # # from airflow.operators.python_operator import PythonOperator
# # # from airflow.operators.check_operator import CheckOperator, IntervalCheckOperator, ValueCheckOperator

# # sys.path.append("../src/")
# # from training import train
# # from testing import test

# # # run vars
# # DB_CONNECTION_ID = 'dev_postgres'
# # RUN_EVERY_N_MINUTES = 2


# # # define dag
# # with DAG(dag_id='model',
# #          description='earthquake prediction model pipeline',
# #          schedule_interval=f'*/{RUN_EVERY_N_MINUTES} * * * *', # CRON expression
# #         #  start_date=datetime(2020, 1, 6)
# #         ) as dag:


# #     # define tasks
# #     # check_interaction_data = CheckOperator(
# #     #     task_id='check_interaction_data',
# #     #     sql='SELECT COUNT(1) FROM interaction WHERE interaction_date = CURRENT_DATE',
# #     #     DB_CONNECTION_ID=DB_CONNECTION_ID
# #     # )

# #     # check_interaction_intervals = IntervalCheckOperator(
# #     #     task_id='check_interaction_intervals',
# #     #     table='interaction',
# #     #     metrics_thresholds={'COUNT(*)': 1.5,
# #     #                         'MAX(amount)': 1.3,
# #     #                         'MIN(amount)': 1.4,
# #     #                         'SUM(amount)': 1.3},
# #     #     date_filter_column='interaction_date',
# #     #     days_back=5,
# #     #     DB_CONNECTION_ID=DB_CONNECTION_ID
# #     # )

# #     # check_unique_products_value = ValueCheckOperator(
# #     #     task_id='check_unique_products_value',
# #     #     sql="SELECT COUNT(DISTINCT(product_id)) FROM interaction WHERE interaction_date=CURRENT_DATE - 1",
# #     #     pass_value=150,
# #     #     tolerance=0.3,
# #     #     DB_CONNECTION_ID=DB_CONNECTION_ID
# #     # )

# #     train_model = PythonOperator(task_id='train_model', python_callable=train)

# #     test_model = PythonOperator(task_id='test_model', python_callable=test)


# #     # grouping tasks and setting orders
# #     checks = [check_interaction_data,
# #               check_interaction_intervals,
# #               check_interaction_amount_value,
# #               check_unique_products_value,
# #               check_replaced_amount_value]

# #     enter_point >> checks >> train_model >> test_model


from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, date

dag = DAG(
	dag_id = 'train_model',
	start_date = datetime(2021, 1, 1),
	schedule_interval = '*/1 * * * *')

def print_hello():
	return "hello!"

def print_goodbye():
	return "goodbye!"

print_hello = PythonOperator(
	task_id = 'print_hello',
	#python_callable param points to the function you want to run 
	python_callable = print_hello,
	#dag param points to the DAG that this task is a part of
	dag = dag)

print_goodbye = PythonOperator(
	task_id = 'print_goodbye',
	python_callable = print_goodbye,
	dag = dag)

#Assign the order of the tasks in our DAG
print_hello >> print_goodbye