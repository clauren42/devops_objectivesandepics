import os
import azureml.core
from azureml.core import Workspace, Experiment, Datastore
from azureml.core.compute import AmlCompute
from azureml.core.compute import ComputeTarget
from azureml.widgets import RunDetails

from azureml.data.data_reference import DataReference
from azureml.pipeline.core import Pipeline, PipelineData
from azureml.pipeline.steps import PythonScriptStep, EstimatorStep
from azureml.train.estimator import Estimator

# Check core SDK version number
print("SDK version:", azureml.core.VERSION)

from azureml.core import Workspace
ws = Workspace.from_config()
print(ws.get_details())

from azureml.pipeline.core import PublishedPipeline, Schedule
old_pipes = PublishedPipeline.list(ws)

for old_pipe in old_pipes:
    old_schedules = Schedule.list(ws, pipeline_id=old_pipe.id)
    for schedule in old_schedules:
        schedule.disable(wait_for_provisioning=True)

    old_pipe.disable()


ds = ws.get_default_datastore()

params = {
    '--data_path': ws.get_default_datastore().path('data'), 
    '--analyze': '',
    '--load_open': '',
    '--load_closed': '',
}
project_folder = '.'
def_blob_store = ws.get_default_datastore()
print("Blobstore's name: {}".format(def_blob_store.name))

blob_output_data = DataReference(
    datastore=def_blob_store,
    data_reference_name="data",
    path_on_datastore="data")
print("DataReference object created")

est = Estimator(source_directory='.', 
                compute_target=ws.compute_targets['cpu'], 
                entry_script='azureml-issues.py',
                pip_packages=['azure-devops','pandas'])

data_processing = EstimatorStep(
    estimator=est,
    estimator_entry_script_arguments=[ '--data_path', blob_output_data,
                                       '--analyze', 
                                       '--load_open', 
                                       '--load_closed'],
    inputs=[blob_output_data],
    compute_target=ws.compute_targets['cpu'],
    allow_reuse=False)
 

pipeline = Pipeline(workspace=ws, steps=[data_processing])
print ("Pipeline is built")

pipeline.validate()
print("Simple validation complete") 

pipeline_run = Experiment(ws, 'issues_pipeline').submit(pipeline)
print("Pipeline is submitted for execution")

published_pipeline = pipeline.publish(name="Issues_Stats", description="Pull data from DevOps and aggregate for PowerBI")
print(published_pipeline.id)

from azureml.pipeline.core.schedule import ScheduleRecurrence, Schedule

recurrence = ScheduleRecurrence(frequency="Hour", interval=1, start_time="2020-01-13T12:55:00") 

schedule = Schedule.create(workspace=ws, name="Bug_Stats",
                           pipeline_id=published_pipeline.id, 
                           experiment_name='Schedule_Run',
                           recurrence=recurrence,
                           wait_for_provisioning=True,
                           description="Bug Stats Run")

# You may want to make sure that the schedule is provisioned properly
# before making any further changes to the schedule

print("Created schedule with id: {}".format(schedule.id))


schedules = Schedule.list(ws, pipeline_id=published_pipeline.id)

# We will iterate through the list of schedules and 
# use the last ID in the list for further operations: 
print("Found these schedules for the pipeline id {}:".format(published_pipeline.id))
for fetched_schedule in schedules: 
    print("Updated schedule:", fetched_schedule.id, 
          "\nNew name:", fetched_schedule.name,
          "\nNew frequency:", fetched_schedule.recurrence.frequency,
          "\nNew interval:", fetched_schedule.recurrence.interval,
          "\nNew start_time:", fetched_schedule.recurrence.start_time,
          "\nNew time_zone:", fetched_schedule.recurrence.time_zone,
          "\nNew hours:", fetched_schedule.recurrence.hours,
          "\nNew minutes:", fetched_schedule.recurrence.minutes,
          "\nNew week_days:", fetched_schedule.recurrence.week_days,
          "\nNew status:", fetched_schedule.status)

