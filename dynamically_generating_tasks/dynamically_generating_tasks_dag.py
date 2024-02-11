"""
### Sample DAG for dynamically generating tasks
Example of performing a set of 'Download data -> Run analysis notebook' for any number of times
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

DAG_NAME = "dag_for_dynamically_generating_tasks"
APP_PATH = "/home/xxx/apps/analyze_data/current"
ANALYZE_NOTEBOOK_NAME = "analyze_template.ipynb"
DATA_DL_SCRIPT_DIR_PATH = f"{APP_PATH}/script"
DATA_DL_BASE_DIR_PATH = "/data/xxx/analyze/data"
OUTPUT_BASE_DIR_PATH = "/data/xxx/analyze/output"


@dataclass
class AnalyzeTarget:
    id: str
    label: str


@dataclass
class AnalyzeTask:
    dataset_type: str
    analyze_targets: List[AnalyzeTarget]


ANALYZE_TARGETS_A = [
    AnalyzeTarget("1", "xxx"),
    AnalyzeTarget("2", "yyy"),
    AnalyzeTarget("3", "zzz"),
]
ANALYZE_TARGETS_B = [AnalyzeTarget("4", "www")]

ANALYZE_TASKS = [
    AnalyzeTask("A", ANALYZE_TARGETS_A),
    AnalyzeTask("B", ANALYZE_TARGETS_B),
]


with DAG(
    DAG_NAME,
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description=f"{DAG_NAME}",
    schedule="0 9 * * 6",  # every saturday from 9 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["tips"],
) as dag:
    for i, analyze_task in enumerate(ANALYZE_TASKS):
        # generate a download task
        dataset_type = analyze_task.dataset_type
        task_id = f"download_data_{dataset_type}"
        command = (
            f"cd {DATA_DL_SCRIPT_DIR_PATH} &&",
            f" bash download.sh",
            f" {DATA_DL_BASE_DIR_PATH}/{dataset_type}",
        )
        download_task = BashOperator(task_id=task_id, bash_command=command)

        # generate an analyze task
        analyze_tasks = []
        analyze_targets = analyze_task.analyze_targets
        for target in analyze_targets:
            task_id = f"analyze_{target.id}"
            command = (
                f"cd {APP_PATH} &&",
                f" papermill -k 'ex' ./{ANALYZE_NOTEBOOK_NAME}",
                f" {OUTPUT_BASE_DIR_PATH}/{target.id}",
            )
            analyze_task = BashOperator(task_id=task_id, bash_command=command)
            analyze_tasks.append(analyze_task)

        if i == 0:
            task = download_task
        else:
            # From the second loop onwards,
            # it needs to be connected from the previous analyze_task.
            task >> download_task
            task = download_task

        for analyze_task in analyze_tasks:
            task >> analyze_task
            task = analyze_task
