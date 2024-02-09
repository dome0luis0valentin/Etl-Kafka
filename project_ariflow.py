from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Configura los argumentos del DAG
default_args = {
    'owner': 'valen',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define el DAG
dag = DAG(
    'etl_kafka_dag',
    default_args=default_args,
    description='ETL process with Kafka in Airflow',
    schedule_interval=timedelta(days=1),  # Define la frecuencia de ejecución
)

# Tarea para iniciar el entorno
start_environment_task = BashOperator(
    task_id='start_environment',
    bash_command='source /home/valen/etl-kafka/myenv/bin/activate',
    dag=dag,
)

# Tarea para iniciar Zookeeper y Kafka
start_zookeeper_kafka_task = BashOperator(
    task_id='start_zookeeper_kafka',
    bash_command='$ZK_HOME/bin/zkServer.sh start > /home/valen/etl-kafka/kafka.log 2>&1',
    dag=dag,
)

# Tarea para iniciar Kafka
start_kafka_task = BashOperator(
    task_id='start_kafka',
    bash_command='cd $KAFKA_HOME && ./bin/kafka-server-start.sh config/server.properties',
    dag=dag,
)

# Tarea para producir datos a Kafka
get_token_task = PythonOperator(
    task_id='get_token_task',
    python_callable=lambda: exec(open("/home/valen/etl-kafka/Etl-Kafka/obtener_token.py").read()),
    dag=dag,
)

# Tarea para producir más datos a Kafka
produce_data_enterprice = PythonOperator(
    task_id='produce_data_enterprice',
    python_callable=lambda: exec(open("/home/valen/etl-kafka/Etl-Kafka/producer.py").read()),
    dag=dag,
)

# Tarea para producir datos fijos a Kafka
produce_fixed_data_task = PythonOperator(
    task_id='produce_fixed_data',
    python_callable=lambda: exec(open("/home/valen/etl-kafka/Etl-Kafka/datos_fijos_producer.py").read()),
    dag=dag,
)

# Tarea para consumir datos de Kafka
consume_enterprise_data_task = PythonOperator(
    task_id='consume_data_enterprice',
    python_callable=lambda: exec(open("/home/valen/etl-kafka/Etl-Kafka/stream_empresas.py").read()),
    dag=dag,
)

# Tarea para consumir más datos de Kafka
consume_fixed_data_task = PythonOperator(
    task_id='consume_fixed_data',
    python_callable=lambda: exec(open("/home/valen/etl-kafka/Etl-Kafka/consumer_fijos.py").read()),
    dag=dag,
)

# Tarea final
finish_task = BashOperator(
    task_id='finish',
    bash_command='echo "Finished"',
    dag=dag,
)

# Define la secuencia de ejecución de las tareas
start_environment_task >> start_zookeeper_kafka_task >> start_kafka_task
start_kafka_task >> get_token_task
get_token_task >> produce_data_enterprice
get_token_task >> produce_fixed_data_task
produce_fixed_data_task >> consume_fixed_data_task
produce_data_enterprice >> consume_enterprise_data_task

#Define tarea final despues de consume_enterprise_data_task y consume_fixed_data_task
[consume_enterprise_data_task, consume_fixed_data_task] >> finish_task 