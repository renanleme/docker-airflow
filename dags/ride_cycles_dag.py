import logging
from datetime import datetime, timedelta
from sys import path

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from airflow import DAG


EXECUTION_CONFIG = Variable.get("RIDES_SPARK_EXECUTION_CONFIG", deserialize_json=True)
GCP_PROJECT_ID = EXECUTION_CONFIG.get("project_id")
GCP_KEY_FILE_PATH = EXECUTION_CONFIG.get("gcp_key_path") + GCP_PROJECT_ID + ".json"


default_args = {
    "owner": "dott",
    "trigger_rule": "all_success",
    "depends_on_past": True,
    "start_date": datetime(2020, 7, 29, 00),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=600),
}

dag = DAG(
    dag_id="rides_cycle_dag",
    description="ETL that retrieves Dott Rides, Deployments and Pickups, build one gold read table with the last cycles of deployment -> pickups",
    max_active_runs=1,
    catchup=True,
    schedule_interval="@daily",
    default_args=default_args,
)


def create_dummy_task(task_name):
    task = DummyOperator(task_id=task_name, dag=dag)
    return task


def create_spark_task(task_id, execution_config_variable, cluster_config_variable):
    def config_to_str():
        result = ""
        for k, v in CLUSTER_CONFIG.items():
            result = result + "--" + k + " " + v + " \\\n"
        return result[:-2]

    def job_parameters():
        result = " "
        if (EXECUTION_CONFIG.get("parameters")):
            for k,v in EXECUTION_CONFIG.get("parameters").items():
                result = result + v
        return result

    def job_properties():
        result = "--properties  ^#^"
        for k, v in JOB_PROPERTIES.items():
            result = result + k + "=" + v + "#"
        return result[:-1] + " \\"

    def create_labels():
        result = "--labels "
        for k, v in CLUSTER_LABELS.items():
            result = result + k + "=" + v + ","
        return result[:-1]

    def add_ret_code(cmd):
        return cmd + "\n" + "result+=$?" + "\n"

    def create_cluster():
        CREATE_CMD = "gcloud dataproc clusters create {cluster_name} --project {gcp_project} --bucket {gcp_bucket} \\\n"
        CREATE_CMD = CREATE_CMD + config_to_str() + create_labels()
        return add_ret_code(CREATE_CMD)

    def submit_job():
        SUBMIT_CMD = """gcloud dataproc jobs submit spark --project {gcp_project} \\
           --cluster {cluster_name} \\
           --jar {main_jar_path} \\
           --region {cluster_region} \\
           """
        SUBMIT_CMD = SUBMIT_CMD + job_properties()
        SUBMIT_CMD = SUBMIT_CMD + "\n-- {execution_date}" + job_parameters()
        return add_ret_code(SUBMIT_CMD)

    def delete_cluster():
        DELETE_CMD = "gcloud dataproc clusters delete  {cluster_name} --project {gcp_project} --region {cluster_region} --quiet"
        return add_ret_code(DELETE_CMD)

    def auth_gcp():
        auth = "gcloud auth activate-service-account  --key-file={gcp_key_file}"
        return add_ret_code(auth)

    SPARK_JOB = EXECUTION_CONFIG.get("spark_task_id")
    SPARK_APP_VERSION = EXECUTION_CONFIG.get("spark_app_version")
    SPARK_JAR_FOLDER = GCP_PROJECT_ID + EXECUTION_CONFIG.get("spark_jar_folder")
    SPARK_MAIN_JAR = "gs://" + SPARK_JAR_FOLDER + "/" + SPARK_JOB.lower() + "_" + SPARK_APP_VERSION.lower() + ".jar"
    EXECUTION_DATE = "{{execution_date.strftime('%Y%m%d%H%M')}}"
    CLUSTER_NAME = SPARK_JOB + "-" + EXECUTION_DATE

    cluster_configs = Variable.get(cluster_config_variable, deserialize_json=True)
    CLUSTER_CONFIG = cluster_configs.get("cluster-config")
    CLUSTER_REGION = CLUSTER_CONFIG.get("region")
    JOB_PROPERTIES = cluster_configs.get("job-config")
    CLUSTER_LABELS = cluster_configs.get("cluster-labels")

    SPARK_SHELL_CMD = auth_gcp() + create_cluster() + auth_gcp() + submit_job() + auth_gcp() + delete_cluster() + "exit $result"

    spark_task = BashOperator(
        task_id=task_id,
        bash_command=SPARK_SHELL_CMD.format(
            cluster_name=CLUSTER_NAME
            , gcp_project=GCP_PROJECT_ID
            , main_jar_path=SPARK_MAIN_JAR
            , cluster_region=CLUSTER_REGION
            , spark_job=SPARK_JOB
            , gcp_bucket=GCP_PROJECT_ID
            , execution_date=EXECUTION_DATE
            , gcp_key_file=GCP_KEY_FILE_PATH
        ),
        dag=dag,
    )

    return spark_task

def load_rides_to_bq(task_id):
    tables = ['rides', 'deployments', 'pickups']
    dataset = "silver_read"
    shell_cmd = """
        gcloud auth activate-service-account  --key-file={bq_key_file_location}        
        """.format(bq_key_file_location=GCP_KEY_FILE_PATH)
    load_cmd = """ 
        bq load --service_account_credential_file={bq_key_file_location} \
        --project_id={bq_project_id} \
        --replace={replace_option} \
        --source_format={format_option} \
        --use_avro_logical_types={avro_logical_types_option} \
        {bq_dataset}.{bq_table} \
        "gs://{gcs_project_id}/data/ldw/last_execution/{gcs_path}/*.avro"
        """
    for table in tables:
        shell_cmd += load_cmd.format(
            bq_key_file_location=GCP_KEY_FILE_PATH,
            bq_project_id=GCP_PROJECT_ID,
            replace_option="true",
            format_option="AVRO",
            avro_logical_types_option="true",
            bq_dataset=dataset,
            bq_table=table,
            gcs_project_id=GCP_PROJECT_ID,
            gcs_path="tables/" + table
        )

    task = BashOperator(
        task_id=task_id,
        bash_command=shell_cmd,
        dag=dag
    )
    return task

start_flow = create_dummy_task("start_flow")
end_flow = create_dummy_task("end_flow")

spark_rides = create_spark_task("start_cluster_and_run_rides_cycle", "RIDES_SPARK_EXECUTION_CONFIG", "RIDES_SPARK_CLUSTER_CONFIG")
load_rides_to_bq = load_rides_to_bq(task_id="load_to_bq_lpm")

start_flow >> spark_rides >> load_rides_to_bq >> end_flow