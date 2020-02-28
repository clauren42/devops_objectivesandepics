import csv
import argparse, os
from utils import *
import argparse, os
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument("--data_path", help="path to which the bug files are written and from which they are read", default='./outputs/')
parser.add_argument('--load_open', dest='load_open', action='store_true')
parser.add_argument('--no-load_open', dest='load_open', action='store_false')
parser.set_defaults(load_open=False)
parser.add_argument('--load_closed', dest='load_closed', action='store_true')
parser.add_argument('--no-load_closed', dest='load_closed', action='store_false')
parser.set_defaults(load_closed=False)
parser.add_argument('--analyze', dest='analyze', action='store_true')
parser.add_argument('--no-analyze', dest='analyze', action='store_false')
parser.set_defaults(analyze=False)

args = parser.parse_args()
project = get_connstrings()
project = project[0]

use_cache = False

output_path = args.data_path
os.makedirs(output_path, exist_ok=True)

conn = get_devopsconnection(project)

if use_cache:
    Objective = pd.read_csv(os.path.join(output_path, 'Objectives.csv'))
    ObjectivesChildren = pd.read_csv(os.path.join(output_path, 'ObjectivesChildren.csv'))

    Epic = pd.read_csv(os.path.join(output_path, 'TrackingEpics.csv'))
    EpicChildren = pd.read_csv(os.path.join(output_path, 'EpicChildren.csv'))

    Project = pd.read_csv(os.path.join(output_path, 'Projects.csv'))
    ProjectChildren = pd.read_csv(os.path.join(output_path, 'ProjectChildren.csv'))
else:
    Objective, ObjectivesChildren = get_WorkItems('Objective', conn, project)
    write_csv([{'name':'Objectives.csv', 'data':Objective}, {'name':'ObjectivesChildren.csv', 'data':ObjectivesChildren}], output_path)

    Epic, EpicChildren = get_WorkItems('Epic', conn, project)
    write_csv([{'name':'TrackingEpics.csv', 'data':Epic}, {'name':'EpicChildren.csv', 'data':EpicChildren}], output_path)

    Project, ProjectChildren = get_WorkItems('Project', conn, project)
    write_csv([{'name':'Projects.csv', 'data':Project}, {'name':'ProjectChildren.csv', 'data':ProjectChildren}], output_path)


df_ObjectivesandEpics = ObjectivesChildren.merge(Epic, right_on='EpicID', left_on='ChildID', how='right', suffixes=('', ''))
ObjectivesandEpics = df_ObjectivesandEpics[['ObjectiveID', 'EpicID', 'ObjectiveTitle', 'EpicTitle', 'EpicArea', 'EpicSubArea','EpicStatus', 'State','AssignedTo','Semester', 'IterationPath', 'KeyCustomer', 'target_month', 'TargetDate', 'scheduled']]
x = ObjectivesandEpics.merge(Objective, right_on='ObjectiveID', left_on='ObjectiveID', how='left', suffixes=('', '_y'))

rename_cols = {'KeyCustomer_y': 'ObjectiveKeyCustomer','KeyCustomer': 'EpicKeyCustomer','scheduled_y': 'ObjectiveScheduled','scheduled': 'Scheduled','Semester_y': 'ObjectiveSemester','State_y': 'ObjectiveState','AssignedTo_y': 'ObjectiveAssignedTo' }
x.rename(columns=rename_cols, inplace=True)

write_csv([{'name':'ObjectivesandEpics.csv', 'data':x}], output_path)