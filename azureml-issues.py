import csv
import time
from datetime import datetime, date, timedelta
import argparse, os
from devops_connection import get_query_papercuts, get_connstrings, get_devopsconnection, get_query_ids, get_work_items
from utils import html2text
import argparse, os
import pandas as pd


try: 
    from azureml.core import Workspace
    ws = Workspace.from_config()
except Exception as e:
    print(f"error {e} getting get AzureML Workspace")
    print("trying run")
    try:
        from azureml.core import Run
        ws = Run.get_context().experiment.workspace
    except Exception as e:
        print(f"error {e} getting get AzureML Workspace")
        ws = None

IMPORT_FIELDS = ['id', 'rev', 'AreaPath', 'TeamProject', 'IterationPath', 'WorkItemType', 'State', 'Reason', 'AssignedTo', 'CreatedDate', 'CreatedBy', 'ChangedDate', 'ChangedBy', 'CommentCount', 'Title', 'StateChangeDate', 'Priority', 'Tags', ]

parser = argparse.ArgumentParser()
parser.add_argument("--data_path", help="path to which the bug files are written and from which they are read", required=True)
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

elements = pd.read_csv('elements.csv')

def area(path):
    split = path.split('\\')
    sub_area = split[-1]
    if len(split)>1:
        main_area = split[1]
    else:
        main_area = split[0]
    return (main_area, sub_area)

def format_name(user):
    if user is None:
        return None
    else:
        return user['displayName'] + " <" + user['uniqueName'] + ">"

def year_for_element(element):
    if element == 'Vibranium':
        return 2019
        
    number = elements.query(f"Element=='{element}'").AtomicNumber
    if len(number) != 1:
        return None
    else:
        return ((int(number)-1)//2)+2008

def target_month(iteration_path):
    segments = iteration_path.split('\\')
    if len(segments) < 3:
        return None
    
    year = year_for_element(segments[1])
    if year is None:
        return None

    try:
        month = int(segments[2][:2])
    except:
        return None

    return date(year, month, 1)

def load_open(output_path):
    print("running load_open")
    try:  
        os.mkdir(output_path)
    except OSError:  
        print ("Creation of the directory %s failed" % output_path)

    project = get_connstrings()
    
    if ws is None:
        project['token'] = os.getenv(project['tokenName'])
    else: 
        project['token'] = ws.get_default_keyvault().get_secret(project['tokenName'])

    conn = get_devopsconnection(project)

    start_time = time.time()

    print ("Getting Papercuts")
    bugids = get_query_ids(conn, get_query_papercuts())
    work_items = get_work_items(conn, bugids)
    print("Papercuts: ", len(work_items))
    print("Papercuts import time: ", time.time() - start_time)  


    from datetime import datetime
    today = datetime.utcnow().date().isoformat()
    work_items_array = []
    count = 0
    for wi in work_items:
        if (count % 100) == 0:
            print(count, wi.id)
        count+=1
        wi_dict = wi.as_dict()
        fields = wi_dict['fields']
        del wi_dict['fields']
        wi_dict.update(fields)
        wi_dict = {key.split('.')[-1]: wi_dict[key] for key in wi_dict if key.split('.')[-1] in IMPORT_FIELDS}
        wi_dict['import_date'] = today
        wi_dict['weburl'] = f"{project['url']}/{project['id']}/_workitems/edit/{wi.id}"
        wi_dict['CreatedBy'] = format_name(wi_dict['CreatedBy'])
        wi_dict['ChangedBy'] = format_name(wi_dict['ChangedBy'])
        if 'AssignedTo' in wi_dict:
            wi_dict['AssignedTo'] = format_name(wi_dict['AssignedTo'])
        main_area, sub_area = area(wi_dict['AreaPath'])
        wi_dict['main_area'] = main_area
        wi_dict['sub_area'] = sub_area
        tags = [x.strip().lower() for x in wi_dict['Tags'].split(';')]
        wi_dict['papercut'] = 1 if ('papercut' in tags or 'papercuts' in tags) else 0
        wi_dict['ws20ga'] = 1 if ('ws20ga' in tags) else 0
        wi_dict['customer'] = 1 if ('customer' in tags) else 0
        wi_dict['target_month'] = target_month(wi_dict['IterationPath'])
        wi_dict['triaged'] = 0 if (wi_dict['target_month'] is None) else 1
        work_items_array.append(wi_dict)


    import pandas as pd
    df = pd.DataFrame(work_items_array)

    df.to_csv(output_path + '/issues-' + today + '.csv', index=False)

    df.to_csv(output_path + '/issues-latest.csv', index=False)

def analyze_bugs(data_path):
    print("running analyze_bugs")
    import pandas as pd
    import glob

    allFiles = glob.glob(data_path + "/issues-[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9].csv")

    list_ = []

    for file_ in allFiles:
        df = pd.read_csv(file_,index_col=None, header=0)
        list_.append(df)

    df = pd.concat(list_, axis = 0, ignore_index = True, sort=False)


    from datetime import date, timedelta 
    print(pd.to_datetime(df.head()['CreatedDate']))
    df['CreatedDate'] = pd.to_datetime(df['CreatedDate']) 
    if df['CreatedDate'].dt.tz is None:
        df['CreatedDate'] = df['CreatedDate'].dt.tz_localize('UTC')

    df['import_date'] = pd.to_datetime(df['import_date']).dt.tz_localize('UTC')
    df['Age'] = (df['import_date'] - df['CreatedDate']).dt.days
    df.loc[df['Age'] > 30, 'over30days'] = 1 
    df.loc[df['Age'] <= 30, 'over30days'] = 0
    df.loc[df['Age'] > 60, 'over60days'] = 1 
    df.loc[df['Age'] <= 60, 'over60days'] = 0
    df['over30days'] = df['over30days'].astype(int)
    df['over60days'] = df['over60days'].astype(int)

    df['TagsArray'] = df.Tags.str.split(';')
    df['TagsArray'] = df.TagsArray.apply(lambda x: [y.strip().lower() for y in x])
    df['papercut'] = df.TagsArray.apply(lambda x: 1 if 'papercut' in x or 'papercuts' in x else 0)
    df['ws20ga'] = df.TagsArray.apply(lambda x: 1 if 'ws20ga' in x else 0)
    df['customer'] = df.TagsArray.apply(lambda x: 1 if 'customer' in x else 0)

    #gems = pd.read_csv(data_path + '/gems.csv')
    #df = pd.merge(df, gems, how='left', on=['AreaPath'])    
    #grouped = df.fillna('nan')[['import_date', 'GEM', 'EM', 'main_area', 'sub_area', 'State', 'Priority', 'over30days', 'over60days', 'Age']].groupby(['import_date', 'GEM', 'EM', 'main_area', 'sub_area', 'State', 'Priority', 'over30days', 'over60days'])

    grouped = df.fillna('nan')[['import_date', 'target_month', 'main_area', 'sub_area', 'State', 'Priority', 'over30days', 'over60days', 'papercut', 'ws20ga', 'customer', 'Age']].groupby(['import_date', 'target_month', 'main_area', 'sub_area', 'State', 'Priority', 'over30days', 'over60days', 'papercut', 'ws20ga', 'customer'])
    aggregated = grouped.size().reset_index(name='counts')

    age = grouped.sum().reset_index()['Age']
    aggregated['SumAge'] = age

    aggregated['hasMaxImportDate'] = (aggregated['import_date'] == max(aggregated['import_date']))

    aggregated.to_csv(data_path + '/age-summary.csv', index=False)

    print(aggregated.dtypes)



if args.load_open:
    load_open(output_path=args.data_path)

if args.load_closed:
    pass
    #load_closed(output_path=args.data_path)

if args.analyze:
    analyze_bugs(data_path=args.data_path)