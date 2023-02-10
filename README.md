# astronomer-dag-authoring-course-notes
notes from astronomer dag authoring course

# The Basics

## Defining your DAG: the right way

- scheduler will only pick up the DAG if the words "airflow" or "dag" are in the file, somewhere.
- .airflowignore - files you don't want to parse in DAG folder
- from airflow import DAG

## Variables

#### Instantiate a DAG, there are two ways.

- context manager - is using the with statement
- alternative is redundant: dag = DAG(...), then DummyOperator(dag=dag)

#### Parameters to use within your DAG:

- dag_id is the unique identifier for this DAG. if not unique, then the airflow UI will pick one at random to show in the UI.
- description - explain what is the goal of the DAG
- start_date - this is not (technically) required, but essentially needs to be there to help the tasks know what the start date is. it's going to be a
datatime object. this will be applied to all of the tasks. this is best practice
- though each task can have it's own start date - this is a weird edge case.
- schedule_interval - can use time delta object, CRON language. By default, this value is one day.
- difference between timedelta and cron interval
- dagrun_timeout - if your DAG is taking longer than x minutes to complete, then it fails. there is no default value.
- airflow will trigger the next DAG run, even if the current one is still running.
- tags - use tags to filter your DAGs in the UI
- catchup - best practice is to set this to false. for example, if the DAG start date is 1 year ago, then it may automatically rerun the entirety of the DAG runs.

```
from airflow import DAG
from datetime import datetime, timedelta

with DAG (dag_id ="my_dag",description ="DAG in charge of processing customer data",
    start_date = datetime(2021,1,1), schedule_interval = "@daily", dagrun_timeout =timdelta(minutes=10), tags=["data_science","customers"],
    catchup = False) as dag:
    None
```

## DAG Scheduling 101

- execution date of the DAG run is the start date - interval
- DAG is effectively trigger after the startdate plus the schedule interval
- if start date is 01/01/2021 and the schedule_interval is every 10 minutes
- at 10am nothing happens, it will wait to 10:10AM then the first dag run is effectively triggered. but the execution date of the dagrun is 10am.
- the 10:20 dag run is effectively triggered at 10:20, but the execution date is 10:10.

## Cron vs Timedelta

- CRON definition is stateless - i want to trigger my DAG as specified in the expression.
- Timedelta defintion is stateful or relative - i want to trigger my DAG relative to the previous execution date.
- Timedelta is 24 hours from start date, but the CRON job waits until midnight. this is based on the execution date.

## Task Idempotence and determinism

- Deterministic - when you execute your task with the same input, you will always get the same output.
- Idempotent - when you execute multiple times, your task will always produce the same side effect.

```
from airflow import DAG
from datetime import datetime, timedelta

with DAG (dag_id ="my_dag",description ="DAG in charge of processing customer data",
    start_date = datetime(2021,1,1), schedule_interval = "@daily", 
    dagrun_timeout =timdelta(minutes=10), tags=["data_science","customers"],
    catchup = False) as dag:

    ### Example 1

    # this task is not idempotent - if you try to execute that task twice, you will get an error.
    PostgresOperator(task_id ="create_table", sql = "CREATE TABLE my_table ...")

    # to make the above idempotent, you could use this:
    PostgresOperator(task_id ="create_table", sql = "CREATE TABLE if not exists my_table ...")

    ### Example 2

    # this task is not idempotent - if you execute this more than once, you will have an error.
    BashOperator(task_id = "creating_folders", bash_command = "mkdir my_folder")
```

## Backfilling

- How can you run a DAG for the previous year, for example?
- if you made a mistake and had to pause your DAG for 5 days, then you can rerun your dags for those five days, and this is done automatically.
- even if the catchup parameter is false, you can still do a backfill

```
with DAG (dag_id ="my_dag",description ="DAG in charge of processing customer data",
    start_date = datetime(2021,1,1), schedule_interval = "@daily", 
    dagrun_timeout =timdelta(minutes=10), tags=["data_science","customers"],
    catchup = False) as dag:

# bash
# backfill your data pipeline even if catchup is set to False
airflow dags backfill - s 2022-01-01 -e 2021-01-1

# max active runs - it means you wont have more than one dag run running at a time
with DAG (dag_id ="my_dag",description ="DAG in charge of processing customer data",
    start_date = datetime(2021,1,1), schedule_interval = "@daily", 
    dagrun_timeout =timdelta(minutes=10), tags=["data_science","customers"],
    catchup = False, max_active_runs = 1) as dag:
```

# Master Your Variables

## Variables

- variable is an object with a key-value pair
- use that variable in your DAG. if the value changes, you change it in one place, not multiple.
- prefix the variable with the dag where it is used.
- in the airflow UI, you can hide senstive values by using one of the keywords it looks for in the name of the key.

```
#### HELPFUL
# to test a DAG locally in bash:
airflow tasks test my_dag extract 2021-01-01

from airport.models import Variables
from airflow.operators.python_operator import PythonOperator

def _extract():
    partner = Variable.get("my_dag_partner")
    print (partner)

with DAG (dag_id ="my_dag",description ="DAG in charge of processing customer data",
    start_date = datetime(2021,1,1), schedule_interval = "@daily", 
    dagrun_timeout =timdelta(minutes=10), tags=["data_science","customers"],
    catchup = False, max_active_runs = 1) as dag:

    extract = PythonOperator(
        task_id ="extract",
        python_callable=_extract
    )
```

## Properly Fetch your Variables

- each time you run Variable.get, it creates a connection to the airflow metadatabase.
- each dag checks variables every 30 seconds, so this can be costly.

- define json value in the key:value pair of the variable menu in airflow ui
- this is for values that are related to one another
- to fetch this, use the below:

```
partner_settings = Variable.get("my_dag_partner_secret", deserialize_json = True)
name = partner_settings['name']
api_key = partner_settings['api_secret']
```

- In Action

```
from airport.models import Variables
from airflow.operators.python_operator import PythonOperator

# adding partner_name as argument
def _extract(partner_name):
    print(partner_name)

with DAG (dag_id ="my_dag",description ="DAG in charge of processing customer data",
    start_date = datetime(2021,1,1), schedule_interval = "@daily", 
    dagrun_timeout =timdelta(minutes=10), tags=["data_science","customers"],
    catchup = False, max_active_runs = 1) as dag:

    # add ops_args
    # uses template_engine
    # only fetches this once
    extract = PythonOperator(
        task_id ="extract",
        python_callable=_extract,
        ops_args = ["{{var.json.my_dag_partner.name}}"]
    )
```

## The Power of Environment Variables

- environmental variables in your docker file with an AIRFLOW_ prefix will be accessible from airflow
- hiding them from users of airflow
- avoid making connection to the metadatabase

```
ENV_AIRFLOW_VAR_MY_DAG_PARTNER = '{"name":"partner_a","api_secret":"123"}
```

# The Power of the Task Flow API

## Add data at runtime with templating

## Sharing Data with XCOMs & Limitations

- xcoms is a mechanism to share data between your tasks.
- there are some limitations with xcoms

```
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta

# access context of the task and xcom mechanism with ti
def _extract(ti):
    partner_name = "Netflix"
    partner_path = '/path'
    # first way to push
    ti.xcom_push(key="partner_name", value = partner_name)
    # second way to push
    return partner_name
    # third way - handle multiple values
    return {"partner_name": partner_name, "partner_path": partner_path}

def _process(ti):
    print ("process")
    # firs tway to pull
    partner_name = ti.xcom_pull(key="partner_name", task_ids = "extract")
    # second way
    partner_name = ti.xcom_pull(task_ids = "extract")
    # third_way
    partner_settings = ti.xcom_pull(task_ids = "extract")
    print (partner_settings['partner_name'])

with DAG (dag_id ="my_dag",description ="DAG in charge of processing customer data",
    start_date = datetime(2021,1,1), schedule_interval = "@daily", 
    dagrun_timeout =timdelta(minutes=10), tags=["data_science","customers"],
    catchup = False, max_active_runs = 1) as dag:

    extract = PythonOperator(
        task_id ="extract",
        python_callable=_extract
    )
```

- one limitation of using xcoms is the size of what your sharing
- if you are trying to share a data frame for example, and that data frame is too big, then that will be a problem

- xcoms are limited in size - depends on metadatabase

- sqllite - 2GB
- postgres - 1GB
- mysql - 64KB

- use data for smaller size of things
- memory overflow error

## The "New Way of Creating Dags"

- create date pipelines in a easier and faster way
- made up of
- decorators - goal is to help you in order to create daga in an easier and faster way
    - @task.python on top of your python function - automatically it will create that operator for you
    - @task.virtualenv - in order to execute it within virtualenv
    - @task.group - group multiple tasks together into a group
 - xcom args
    - automically create dependnecies between tasks that have an implciity dependency with use of xcoms
    - no need to use xcom push and xcom pull

- to use the taskflow api, use the following import:

```
from airflow.decorator import task,dag

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta

@task.python
def extract(ti):
    partner_name = "Netflix"
    partner_path = '/path'
    return {"partner_name": partner_name, "partner_path": partner_path}

@task.python
def process(ti):
    partner_settings = ti.xcom_pull(task_ids = "extract")
    print (partner_settings['partner_name'])

# you can use the dag decorator to change this block to the one below it:
with DAG (dag_id ="my_dag",description ="DAG in charge of processing customer data",
    start_date = datetime(2021,1,1), schedule_interval = "@daily", 
    dagrun_timeout =timdelta(minutes=10), tags=["data_science","customers"],
    catchup = False, max_active_runs = 1) as dag:

@dag (dag_id ="my_dag",description ="DAG in charge of processing customer data",
    start_date = datetime(2021,1,1), schedule_interval = "@daily", 
    dagrun_timeout =timdelta(minutes=10), tags=["data_science","customers"],
    catchup = False, max_active_runs = 1)

def my_dag():
    extract() >> process()

my_dag()

    # the decorator in line 244, means you can remove this code block. it's auto created for you.
    #extract = PythonOperator(
    #    task_id ="extract",
    #    python_callable=_extract
    #)

# change this from extract to extract() and process to process()
    extract() >> process()
```

## XComs with the Taskflow API

- How to have two XComs, not one dictionary, for two places witout using xcom_push twice.
- you can pass arguments to decorators

```
from airflow.decorator import task,dag

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta
```

- you can use the argument multiple_outputs = True
- you are saying that this xcom shouldn't be one dictionary, but instead generate two xcoms
- do_xcom_push=False will ensure that it doesn't return the dictionary value, just the seperate xcoms

```
@task.python (task_id = "extract_partners",do_xcom_push=False, multiple_outputs = True)
def extract():
    partner_name = "Netflix"
    partner_path = '/path'
    return {"partner_name": partner_name, "partner_path": partner_path}

# to access the mutiple xcoms, you need to add another argumeent called partner_path
@task.python
def process(partner_name,partner_path):
    print (partner_name)
    print (partner_path)

@dag (dag_id ="my_dag",description ="DAG in charge of processing customer data",
    start_date = datetime(2021,1,1), schedule_interval = "@daily",
    dagrun_timeout =timdelta(minutes=10), tags=["data_science","customers"],
    catchup = False, max_active_runs = 1)

def my_dag():

    partner_settings = extract()
    process(partner_settings['partner_name'],partner_settings['partner_path']])

my_dag()
```

- alternatively, you an use do this instead:
- you'll also need to import Dict type
- this is equivalent to using multiple_outputs

```
from typing import Dict

@task.python (task_id = "extract_partners")
def extract() -> Dict[str,str]:
    partner_name = "Netflix"
    partner_path = '/path'
    return {"partner_name": partner_name, "partner_path": partner_path}

@task.python
def process(parnter_name):
    partner_settings = ti.xcom_pull(task_ids = "extract")
    print (partner_settings['partner_name'])

@dag (dag_id ="my_dag",description ="DAG in charge of processing customer data",
    start_date = datetime(2021,1,1), schedule_interval = "@daily",
    dagrun_timeout =timdelta(minutes=10), tags=["data_science","customers"],
    catchup = False, max_active_runs = 1)

def my_dag():
    extract() >> process()

my_dag()
```

# Grouping your tasks

- there are two ways to group your tasks - subdags and taskgroups
- taskgroups are objectively better

## SubDags: The Hard Way of Grouping your Tasks

- this propagates into graph view
- subdag - is nothing more than a dag within another dag
- in order to create a subdag you need two components - a subdag operator, and a factory function in charge of creating that subdag and retuurning the other other subdag

- you create the subdag factory in a folder - subdags/subdag_factory.py and populate it
- to use this, use the import below:

```
from airflow.operators.subdag import SubDagOperator
from airflow.decorator import task,dag
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
```

-need this import to import the subdag_factory

```
from subdag.subdag_factory import subdag_factory
```

```
@task.python (task_id = "extract_partners",do_xcom_push=False, multiple_outputs = True)
def extract():
    partner_name = "Netflix"
    partner_path = '/path'
    return {"partner_name": partner_name, "partner_path": partner_path}

# keep the same start date for all of your tasks between the parent dag and the subdag
default_args = {
    "start_date": datetime (2021,1,1)
}

@dag (dag_id ="my_dag",description ="DAG in charge of processing customer data",
    default_args = default_args, schedule_interval = "@daily",
    dagrun_timeout =timdelta(minutes=10), tags=["data_science","customers"],
    catchup = False, max_active_runs = 1)

def my_dag():

    partner_settings = extract()

    process_tasks = SubDagOperator(
        task_id = "process_tasks",
        # use this to call the factory function
        subdag = subdag_factory("my_dag","process_tasks", default_args, partner_settings)
    )

    # to create this subdag structure we copy and paste the three below, and paste them into the /subdag/subdag_factory.py file we created
    #process_a(partner_settings['partner_name'],partner_settings['partner_path']])
    #process_b(partner_settings['partner_name'],partner_settings['partner_path']])
    #process_c(partner_settings['partner_name'],partner_settings['partner_path']])

my_dag()
```

## TaskGroups: The Best Way of Grouping

- the big difference between task groups and subdags is that you just need to group your tasks together visually, and don't have to put a dag within a dag

```
from airflow.utils.task_group import TaskGroup

# correspond to the xcom args
partner_settings = extract()

# here you can add an argument: add_sufflix_on_collision = True
# that will take care of cases where you write the same id to a different group
with TaskGroup(group_id = 'process_tasks') as process_tasks:
    process_a(partner_settings['partner_name'],partner_settings['partner_path'])
    process_b(partner_settings['partner_name'],partner_settings['partner_path'])
    process_c(partner_settings['partner_name'],partner_settings['partner_path'])

# graph view, the blue taks corresponds to the task view
# only grouped together visually
# will make DAGs easier to read

# you can also import a task group by saving it to it's own file
# you can also put a task group within another task group

# there is also a task group decorator
# this can replace the with context manager

@task_group
```

## Advanced Concepts

##The (not so) dynamic tasks

- if some tasks are similar, you can think about generating these dynamically, then just put the differences in a dictionary, for example.

- 1: create a loop in charge of creating those tasks dynamically
- 2: generated dynamically based on a dictionary

```
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task, dag
from airflow.operators.subdag import SubDagOperator
from airflow.utils.task_group import TaskGroup

from datetime import datetime, timedelta
from typing import Dict
from groups.process_tasks import process_tasks

# this is our dictionary for creating tasks dynamically
partners = {
    "partner_snowflake";
    {
        "name": "snowflake",
        "path": "/partners/snowflake"
    },
    "partner_netflix";
    {
        "name": "netflix",
        "path": "/partners/netflix"
    },
    "partner_astronomer";
    {
        "name": "astronomer",
        "path": "/partners/astronomer"
    },
}


default_args = {
    "start_date": datetime (2021,1,1)
}

@dag(description= "DAG in charge of processing customer_data",
default_args = default_args, schedule_interval = "@daily", dagrun_timeout = timedelta(minutes=10),
tags["data_science","customers"], catchup = False, max_active_runs =1)

def my_dag():

    start = DummyOperator (task_id = "start")

    # this is the for loop used to create your tasks
    for partner,details in partners.items():

        @task.python (task_id = f"extract_{partner}", do_xcom_push = Falsse, multiple_outputs = True)
        def extract (partner_name, partner_path):
            return {"partner_name": partner_name, "partner_path": partner_path}

        extracted_values =extract(details['name'], details['partner'])

        start >> extracted_values
        process_tasks(extracted_values)

dag = my_dag()
```

## Making your choices with Branching

- with branching, you are able to choose one task or another according to a condition.
- if your condition is true, your going to return one task id or multiple task IDs corresponding to the tasks that you want to execute next.

- Four Types of Branching Operators Avalagble
- Branch Python Operator
- Branch SQL Operator
- Branch Datetime Operator
- Branch Day of Week Operator

- What if you only want to extract the snowflake data if current day is mon, netflix on wed, and astronomer on frid?
- How can you do thaat if this DAG is scheduled for daily?
- Here we use the branch python operator

- one issue is that after using branching, if you schedule a task after it, it will fail because only one of the three parent tasks succeeded

- we use trigger rules to help with this - next section/video

```
from airflow.operators.python import BranchPythonOperator

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task, dag
from airflow.operators.subdag import SubDagOperator
from airflow.utils.task_group import TaskGroup

from datetime import datetime, timedelta
from typing import Dict
from groups.process_tasks import process_tasks

# this is our dictionary for creating tasks dynamically
partners = {
    "partner_snowflake";
    {
        "name": "snowflake",
        "path": "/partners/snowflake"
    },
    "partner_netflix";
    {
        "name": "netflix",
        "path": "/partners/netflix"
    },
    "partner_astronomer";
    {
        "name": "astronomer",
        "path": "/partners/astronomer"
    },
}


default_args = {
    "start_date": datetime (2021,1,1)
}

# returns task id of what you want to run next
# you can always return multiple task ids here
# but will mess up if not one of those three days - we use the stop dummy operator
def _choosing_partner_based_on_day(execution_date):
    day = execution_date.day_of_week
    if (day ==1):
        return 'extract_partner_snowflake'
    if (day ==3):
        return 'extract_partner_netflix'
    if (day ==5):
        return 'extract_partner_astronomer'
    return 'stop'

@dag(description= "DAG in charge of processing customer_data",
default_args = default_args, schedule_interval = "@daily", dagrun_timeout = timedelta(minutes=10),
tags["data_science","customers"], catchup = False, max_active_runs =1)

def my_dag():

    start = DummyOperator (task_id = "start")

    # here we create a new task with the Python Branch Operator

    choosing_partner_based_on_day = BranchPythonOperator (
        task_id = 'choosing_partner_based_on_day',
        # expects python function in charge of returning the task IDs of the next task
        # to execute based on your condition
        python_callable = _choosing_partner_based_on_day
    )

    stop = DummyOperator(task_id ="stop")

    # now storing is executed, with the trigger rules
    storing = DummyOperator (task_id ="storing", trigger_rule = "non_failed_or_skipped")

    choosing_partner_based_on_day >>stop

    # this is the for loop used to create your tasks
    for partner,details in partners.items():

        @task.python (task_id = f"extract_{partner}", do_xcom_push = Falsse, multiple_outputs = True)
        def extract (partner_name, partner_path):
            return {"partner_name": partner_name, "partner_path": partner_path}

        extracted_values =extract(details['name'], details['partner'])

        # add branching task here
        start >> choosing_partner_based_on_day >> extracted_values
        process_tasks(extracted_values) >> storing

dag = my_dag()
```

## Change task execution with trigger rules

- trigger rule defines the behavior of your task, why your task is going to be trigger,
- by default, the trigger that is applied to all of your tasks is all_success

- by default, a task gets triggered only if all of its parents have succeeded
- another option is all_failed - this triggers a task if all of the parents have failed.
- this could be helpful for a slack notification, for example
- another option is all_done - as long as the tasks have finished, your tasks get triggered as well

- what if you wat to trigger your task as soon as one of the parents has failed?
- this is one_failed

- if you want to trigger your task as soon as one of the parents has successed
- use one_success

- if you want to trigger your task if one of the parents have been skipped OR have succeeeded
- none_failed

- none_skipped
- if you wantt to execute your task only if all of its parents haven't been skipped

- none_failed_or_skipped
- i want to trigger my task if at least one of the parents have succeeded and all of them have completed.

- trigger_rule = dummy
- your task gets triggered regardless

## Dependencies and Helpers

- there are two ways to define dependencies in airflow
- old way and new way

```
with DAG('dependency', schedule_interval = '@daily', default_args = default_args, catchup = False) as dag:
    t1 = DummyOperator(task_id="t1")
    t2 = DummyOperator(task_id="t2")
    t3 = DummyOperator(task_id="t3")
    t4 = DummyOperator(task_id="t4")
    t5 = DummyOperator(task_id="t5")
    t6 = DummyOperator(task_id="t6")

# old way of doing things
# new way of doing things with the # bit shift operator
t2.set_upstream(t1)  t1<<t2
t1.set_downstream(t2) t1>>t2
```

- what if you want to create cross dependencies?
- use function called cross_downstream

```
from airflow.models.baseoperator import cross_downstream

# cross_donwstream returns nothing
cross_downstream([t1,t2,t3],[t4,t5,t6])
[t4,t5,t6]>> t7

# more complex dependencies

from airflow.models.baseoperator import chain 
# second list needs to be same size as first
chain(t1,[t2,t3],[t4,t5],t6)

# mix cross_downstream and chain

cross_downstream([t2,t3],[t4,t5])
chain(t1,t2,t5,t6)
```

## Get the control of your tasks

- how does concurrency work in airflow?

- Options 1 - At the Airflow Instance Level

- first way to define concurrency is in an airflow configuration file

- First Setting: Parallelism
- 32 by default, the number of tasks that can be executed at the same time for entire airflow instance

- Second Setting: Dag Concurrency
- 16 by default, the number of tasks that can be executed at the same time for any given dag

- max_active_runs_per_dag
- 16 by default, number of dag runs that can run at the same time, for a given dag

- Option 2 - At the DAG Level

- what if you want to define this at the DAG level?
- you can set concurrency at the DAG level

- if you set concurrency to 2, then you can execute at most two tasks across
- all of the dagruns of a given dag.

- this is two tasks across all of the dagruns of that task, not just a single dag run.
- max number of dag runs at a given time

```
@dag(description= "DAG in charge of processing customer_data",
default_args = default_args, schedule_interval = "@daily", 
dagrun_timeout = timedelta(minutes=10),tags["data_science","customers"], 
catchup = False,
concurrency=2,
max_active_runs =1)
```

- Option 3 - At the Dag Level

- task_concurrency = 1 is at the task level
- this is across all dag runs
- pool = 'default_pool'

## Dealing with resource consuming tasks with Pools

- How do you configure resources so they are used based on where its needed
- Example: i want my ML model training to only run one at a time because they are doing a lot

vs

- Example: i have ML model testing right after that - and it's cool if those two tasks happen at the same time


- Pool - you have a pool with a number of worker slots
- Each time a task is running, then that task takes a worker slot until the task is completed.
- when it's done, then that worker slot is released!
- by default, all of your tasks run in the same pool called default_pool

- Admin --> Pool ---> 128 slots
- this means you can execute, at most, 128 slots at the same time in your entire airflow instance.
- you can modify this number

- Running Slots vs. Queued Slots
- if the pool is filled, the next task to run will get queued

- Why pools? with pools, yoca specify the number of tasks to run at the same time for a specific set of tasks

- maybe you only to want to run one task at a time for tasks that are particularly resource consuming
- whereas, you want to run as many tasks in parallel for other tasks in your DAG

- you can create a new pool with the airflow UI

- define name of that pool here
- this now runs in a dedicated pool with a dedicated worker slot

```
@task.python (task_id = f"extract_{partner}", do_xcom_push = False, pool = 'name of pool you created', multiple_outputs = True)
```

- pool_slots = 1
- defines the number of worker slots that a given task takes as soon as its triggered

- NOTE
- if you set a pool at the SubDAG operator, this pool wont be used by the tasks within your subdag
- in fact, only the subdag operator will respect that pool
- you need to define the pool for each tasks of your subdag

## Execute critical tasks first, the others after

- use the concept of task priority
- any operator in airflow has an argument called PRIORITY_WEIGHT
- set to 1 by default, which means the scheduler will pick one task at random to kick off next if they are all on the same line
- now, you can modify this by changing this argument

- the higher the number, the higher priority the task is

```
partners = {
    "partner_snowflake";
    {
        "name": "snowflake",
        "path": "/partners/snowflake",
        "priority": 2
    },
    "partner_netflix";
    {
        "name": "netflix",
        "path": "/partners/netflix",
        "priority": 3
    },
    "partner_astronomer";
    {
        "name": "astronomer",
        "path": "/partners/astronomer",
        "priority": 1
    },
}
```

- TASK PRIORITIES ARE EVAULATED AT THE POOL-LEVEL
- If you have different tasks with different pools, then the task priorities won't be evaulated
- If you trigger your dag manually, your task priorities won't be taken into account.

- WEIGHT_RULE - computes how the weight priorites of your tasks are executed
- DEFAULT - DOWNSTEAM - this means the priority of a task is that task's priority + everything downstream of it. each task has a priority weight that is the sum of itself and the downstream tasks of it
- UPSTREAM - the opposite multiple dag runs
- ABSOLUTE - based on the priority weights yuu define. you have full control of your priorities

## What if a task needs the output of its previous execution

- DEPENDS_ON_PAST argument - helpful, but confusing

- prevent from running a task in a dagrun if the same task didn't succeed in the previous dag run.
- depends on the previous execution of the same task in the previous task run

- what if you have depends on past on all tasks
- will dag run 2 get triggered only if the previous dagrun one has succeeded? ANSWER IS NO

- doesnt prevent running the next dagrun if the previous dagrun didn't succeed
- if your task is not triggered because the previous task instance didn't suceed, then your task wont have a status -
- it will not be triggered.

## Demystifying wait for downstream

- wait for downstream allows you to say, run that task, only if the same task in the previous dagrun has succeeded as well as its *direct* downstream tasks.
- you are not waiting for all tasks downstram of the task where wait for downstream is applied, you are only waiting for the direct downstream task
- as soon as you set wait for downstream on a task, that task has depends on past = true automatically, which makes sense.
- why use it? avoid race conditions, good for ensuring that multiple tasks aren't modifying the same resources at the same time.

## All you need to know about sensors

- sensor - an operator waiting for something to happen, or a condition to be true, before moving to the next task.
- wait for file to land at a specific location, for example.

- operator waiting for a condition to be true, before moving to the next task.

- example 1 datetime sensor - wait for a specific datetime, before moving forwar

```
from airflow.sensors_date_time import DateTimeSensor
```

- ARGUMENT ONE: POKE_INTERVAL

- poke_interval - set to 60 seconds, by default.
- poke interval - frequency at which your sensor will check if your condition is true or not.
- in this case, you are verifying every 60 seconds if your datetime is 10am or not.
- then multiple it to get a larger interval, 60*60 = 3600 seconds or 1 hour

- ARGUMENT TWO: MODE

- mode - set to poke, by default.
- mode is good because a sensor is just waiting for something to happen, so it's kind of wasting your resources and time.

- when mode is set to reschedule, every hour, the sensor will check if the sensor is true or not
- if it's not, it will free up that worker slot, then take one back again in one hour to check the condition atain
- reschedule uses a worker slot, then frees it up during idle time

- ARGUMENT THREE: TIMEOUT
- what happens if your condition is never true?
- sensor will wait until it times out after 7 days, by default.
- you might end up with a deadlock, and you can't run any tasks, because your worker slots are taken by sensors
- should timeout as oone

- ARGUMENT FOUR: EXECUTION_TIMEOUT
- no default value
- difference between timeout and execution timeout?
- execution timeout is available to all operators
- timeout is reserved for this sensor
- soft fail means that according to the timeout, your sensor wont fail, but it'll be skipped. execution timeout wont take this into account.

- if you use timeout with soft fail, you can say that you if your sensor times out, then it will just be skipped, not fail.

- ARGUMENT FIVE: EXPONENTIAL_BACK_OFF
- increase waiting time betweene each interval of time
- good for APIs

```
delay= DateTimeSensor(
    task_id = 'delay',
    target_time ="{{execution_date.add(hours=9)}}",
    poke_interval = 60 * 60,
    mode = 'reschedule',
    timeout = 60*60*10,
    soft_fail = True,
    exponential_backoff = True
)
```

## Don't get stuck with your Tasks by using Timeouts

- you can define a timeout within your dag
- you need to use the dagrun_timeout argument in the DAG
- if the DAG run is not finished after X minutes, then it fails
- if one of your tasks get stuck, that's why always run a dagrun timeout
- this only works for scheduled dag runs, not manual runs

- you can also ouse execution_timeout = timedelta(minutes=10)
- if that tasks takes more than 10 minutes, then it times out
- this argument has no value by default.

- always define timeout at task level and dag level.

## How to react in case of failure? or retry?

- two options
- leverage trigger rules
- callbacks

- callbacks - function called if an event happens
- if your dag fails, then you can use callback to do something
- can be used to send an email or slack notification

```
on_success_callback = _success_callback

# context contains information about your DAG
def _success_callback(context):
    print (context)

on_failure_callback = _failure_callback

def _failure_callback(context):
    print (context)
```

- you can also define callbacks at the task level

- 1: on success callback
- 2: on failure callback
- 3: on retry callback

```
def _extract_callback_success(context):
    print ('SUCCESS CALLBACK')

from airflow.exceptions import AirflowTaskTimeout, AirflowSensorTimeout

def _extract_callback_failure(context):
    if (context['exception']):
        # victim of timeout?
        # these exceptions need to be imported
        if (isinstance(context['exception']), AirflowTaskTimeout)
        if (isinstance(context['exception']), AirflowSensorTimeout)
    print ('failured CALLBACK')

def _extract_callback_retry(context):
    if (context['ti'.try_number()>2]):
        print ('RETRY CALLBACK')

@task.python (task_id ='blah', on_success_callback = _extract_callback_success, on_failure_callback = _extract_callback_failure, on_retry_callback = _extract_callback_retry)
```

## The different (and smart) ways of retrying your tasks

- retries - by default, equal to 0, defines number of retries before your task fails
- you can apply this at the airflow instance level - default_retry_level
- you can specify this in default_args, but can be overridden by any value set below

- you can also use retry_delay - time between each retry
- i want to wait 5 minutes between each retrt, 5 minutes by default

- you can use retry_exponential_backoff = when true, instead of every 5 minutes, then it will wait longer at each retry
- good for APIs

- you can also use max_retry_delay
- this puts a cap on the max retry
- good for exponential backoff

```
@task.python (task_id ='blah', retries =0, retry_delay = timedelta(minutes=5), max_retry_delay = timedlta(minuetes=15),on_success_callback = _extract_callback_success, on_failure_callback = _extract_callback_failure, on_retry_callback = _extract_callback_retry)
```

## Get notified with SLAs

- what if you want to be warned if a task or dag is taking a lot longer than expected.

- SLA vs. timeout

- SLA - if your task is taking longer to complete, then i want to receive a notification, but not necessarily timeout or kill anything
- relative to the dag execution date, not the task startime where the sla is defined
- as soon as an sla is missed, a callback will hit

- you can define that callback in the dag,
- this function will be called by any task in the dag that misses it's sla

- slas are not triggered if the dag is triggered manually
- you need to configure SMTP server if you want emails for missing SLAs

```
def _sla_miss_callback(dag,task_list, blocking_task_list, slas, blicking_tis):
    print(task_list)
    print (blocking_tis)
    print (slas)

sla_miss_callback = _sla_miss_callback

@task.python (task_id ='blah', sla = timedelta(minutes = 5), retries =0, retry_delay = timedelta(minutes=5), max_retry_delay = timedlta(minuetes=15),on_success_callback = _extract_callback_success, on_failure_callback = _extract_callback_failure, on_retry_callback = _extract_callback_retry)
```

## DAG Versioning

- no real mechanism to deal with multiple versions of your dag

- what if you trigger your task, then add a new task
- new task added will have no status for a previous DAG run

- what if you remove a task
- it removes it completely from the previous dag runs, especially the logs
- careful when you add a task, or remove a task from your DAG.

- recommends using version naming to your dagh name

```
with DAG('process_dag_1_0_1')
```

# Dynamic  DAGs: The two ways

- Dynamic DAGs - What an important topic! 🤩

- The concept of "dynamic" in this context is fairly simple. Whenever you manage multiple DAGs using identical tasks where only the inputs change, it might make more sense to generate these DAGs dynamically

- Why waste time manually writing the same DAGs over and over again with minimal variation between them? 😀

- There are two ways of generating DAGs dynamically. Let's begin by looking at the first one: 

- The Single-File Method
- I don't really like this one and if you use it, well, read what comes next, it's gonna be very useful for you.

- The Single-File Method  is based on a single Python file in charge of generating all the DAGs based on some inputs.

- For example, a list of customers, a list of tables, APIs and so on.

- Here is an example of implementing that method:

``

from airflow import DAG
from airflow.decorators import task
from datetime import datetime

partners = {
    'snowflake': {
        'schedule': '@daily',
        'path': '/data/snowflake'
    },
    'netflix': {
        'schedule': '@weekly',
        'path': '/data/netflix'
    }, 
}

def generate_dag(dag_id, schedule_interval, details, default_args):

    with DAG(dag_id, schedule_interval=schedule_interval, default_args=default_args) as dag:
        @task.python
        def process(path):
            print(f'Processing: {path}')

        process(details['path'])

    return dag

for partner, details in partners.items():
    dag_id = f'dag_{partner}'
    default_args = {
        'start_date': datetime(2021, 1, 1)
    }
    globals()[dag_id] = generate_dag(dag_id, details['schedule'], details, default_args)

```

```
All good! But, there are some drawbacks with this method.

DAGs are generated every time the Scheduler parses the DAG folder (If you have a lot of DAGs to generate, you may experience performance issues
There is no way to see the code of the generated DAG on the UI
If you change the number of generated DAGs, you will end up with DAGs visible on the UI that no longer exist 
So, yes, it's easy to implement, but hard to maintain. That's why, I usually don't recommend this method in production for large Airflow deployments.

That being said, let's see the second method, which I prefer a lot more! 🤩

The Multi-File Method
This time, instead of having a single Python file in charge of generating your DAGs, you are going to use a script that will create a file for each DAG that is generated. At the end you will get one Python File per generated DAG.

I won't provide the full code here,  but let me give you the different steps:

Create a template file corresponding to your DAG structure with the different tasks. In it, for the inputs, you put some placeholders that the script will use to replace with the actual values.
Create a python script in charge of generating the DAGs by creating a file and replacing the placeholders in your template file with the actual values.
Put the script, somewhere else than in the folder DAGs.
Trigger this script either manually or with your CI/CD pipeline.
The pros of this method are numerous!

It is Scalable as DAGs are not generated each time the folder dags/ is parsed by the Scheduler
Full visibility of the DAG code (one DAG -> one file)
Full control over the way DAGs are generated (script, less prone to errors or "zombie" DAGs)
Obviously, it takes more work to set up but if you have a lot of DAGs to generate, that's truly the way to go!
```

# DAG Dependencies

## Wait for multiple DAGs with the ExternalTaskSensor

- external task sensor
- will wait for a task in another task to complete before moving forward
- will wait for the task storing on the my_dag dag.

```
from airflow.sensors.external_task import ExternalTaskSensor

waiting_for_task = ExternalTaskSensor (
    task_iud ='waiting_for_task',
    - dag of the task you are waiting for
    external_dag_id = 'my_dag',
    - task id in the other dag
    external_task_id = 'storing',
    - this function will return the execution dates on which the external task sensor will check on the tasks in the other dag.
    execution_date_fn ='',
    - list of statuses - by default, this is empty.
    failed_states =['failed','skipped'],
    - if the task that you are waiting on succeeds, then it works.
    allowed_states = ['success']
)
```

## DAG dependencies with the TriggerDagRunOperator

- TriggerDagRunOperator
- easier way of defining dag dependencies

```
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

- this is not a sensor
- mode is not available for this operator
cleaning_xcoms = TriggerDagRunOperator(
    task_id ='trigger_cleaning_xcoms',
    # dag you want to trigger from your current DAG
    trigger_dag_id = 'clean_dag',
    # string or datetime date
    # will be used as the execution date of the DAG that you are triggering
    # good for backfilling
    execution_date = '{{ds}}',
    # defines if you want the triggered dag to be completed before moving to the next task
    wait_for_completion = True,
    # 60 by default
    # frequency at which you will check if your triggered dag is completed or not
    poke_interval = 60,
    # reset_dag_run
    # false by default
    # if you want to rerun
    reset_dag_run = True
    # by default, this is empty.
    failed_states =['failed']
)
```
