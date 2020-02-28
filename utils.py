# %load utils.py
import os
import re, html
import pandas as pd
from datetime import datetime, date, timedelta
from devops_connection import * #get_queryTrackingEpics, get_query_objectives, get_connstrings, get_devopsconnection, get_query_ids, get_work_items
import time

def html2text(htm):
    # from https://stackoverflow.com/a/54296631/871738
    ret = ""
    try:
        ret = html.unescape(htm)
        ret = ret.translate({
            8209: ord('-'),
            8220: ord('"'),
            8221: ord('"'),
            160: ord(' '),
        })
        ret = re.sub(r"\s", " ", ret, flags = re.MULTILINE)
        ret = re.sub("<li>", "• ", ret, flags = re.IGNORECASE)
        ret = re.sub("<br>|<br />|</p>|</div>|</h\d>|</li>", "\n", ret, flags = re.IGNORECASE)
        ret = re.sub('<.*?>', ' ', ret, flags=re.DOTALL)
        ret = re.sub(r"  +", " ", ret)
    except:
        pass
    return ret


# +
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
        return user['displayName'] #+ " <" + user['uniqueName'] + ">"

def year_for_element(element):
    if element == 'Vibranium':
        return 2019
        
    number = elements.query(f"Element=='{element}'").AtomicNumber
    if len(number) != 1:
        return None
    else:
        return ((int(number)-1)//2)+2008

def monday_of_calenderweek(year, week):
    first = date(year, 1, 1)
    base = 1 if first.isocalendar()[1] == 1 else 8
    return first + timedelta(days=base - first.isocalendar()[2] + 7 * (week - 1))

def semester(iteration_path):
    semester=None
    
    segments = iteration_path.split('\\')
    if len(segments) >1:
        semester=segments[1]
    
    return semester

def write_csv(dataframes, output_path):
  
    for df in dataframes:
        file_name = os.path.join(output_path, df['name'])
        print('Writing ' + file_name)
        df['data'].to_csv(file_name, index=False)

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

def format_targetdate(targetdate):
    if len(targetdate)>9:
        targetdate = targetdate[0:10]
    return targetdate

def target_date(iteration_path):
    targetdate = None
    segments = iteration_path.split('\\')
    if len(segments) < 4:
        return None
    
    year = year_for_element(segments[1])
    if year is None:
        return None
    
    try:
        month = int(segments[2][:2])
    except:
        return None

    if '_' in segments[3]: 
        try:
            day = int(segments[3].rpartition('_')[-1][-2:])
            targetdate = date(year, month, day)
        except: 
            return None
    elif 'Week' in str(segments[3]):
        try:
            targetdate = monday_of_calenderweek(year, int(segments[3][-2:]))
        except:
            return None
    elif 'Wk Sprint' in str(segments[3]):
        try:
            day = int(str(segments[3]).replace(')','')[-2:])
            targetdate = date(year, month, day)
        except:
            return None

    return targetdate

def get_tags(wi_dict):
    tags=''
    try:
        tags = [x.strip().lower() for x in wi_dict['Tags'].split(';')]
    except:
        tags = ''
    return tags

def load_open(output_path):
    print("running load_open")
    try:  
        os.mkdir(output_path)
    except OSError:  
        print ("Creation of the directory %s failed" % output_path)


# -

elements = pd.read_csv('elements.csv')


def get_WorkItemDataFrame(workitemtype, project, work_items):
    from datetime import datetime
    today = datetime.utcnow().date().isoformat()
    work_items_array = []
    parentchildmap=[]
    idname = str(workitemtype + 'ID')
    print(idname)
    titlename = workitemtype + 'Title'
    areaname = workitemtype + 'Area'
    subareaname = workitemtype + 'SubArea'
    
    if workitemtype == 'Objective':
        IMPORT_FIELDS = ['id', 'rev', 'AreaPath', 'TeamProject', 'IterationPath', 'WorkItemType', 'State', 'Reason', 'AssignedTo', 'CreatedDate', 'CreatedBy', 'ChangedDate', 'ChangedBy', 'CommentCount', 'Title', 'StateChangeDate', 'Priority', 'Tags','TargetDate']
    elif (workitemtype =='Epic' or workitemtype =='Project'):
        IMPORT_FIELDS = ['id', 'Status', 'EpicStatus', 'rev', 'AreaPath', 'TeamProject', 'IterationPath', 'WorkItemType', 'State', 'Reason', 'AssignedTo', 'CreatedDate', 'CreatedBy', 'ChangedDate', 'ChangedBy', 'CommentCount', 'Title', 'StateChangeDate', 'Priority', 'Tags','TargetDate']

    count = 0
    for wi in work_items:
        children=[]
        parent_id=''

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
        if 'TargetDate' in wi_dict:
            wi_dict['TargetDate'] = format_targetdate(wi_dict['TargetDate'])
        if 'AssignedTo' in wi_dict:
            wi_dict['AssignedTo'] = format_name(wi_dict['AssignedTo'])
        main_area, sub_area = area(wi_dict['AreaPath'])
        wi_dict[idname] = str(wi.id)
        wi_dict[titlename] = wi_dict['Title']
        wi_dict[areaname] = main_area
        wi_dict[subareaname] = sub_area
        tags = get_tags(wi_dict)
        wi_dict['KeyCustomer'] = "Office" if ('office' in tags) else ""
        wi_dict['papercut'] = 1 if ('papercut' in tags or 'papercuts' in tags) else 0
        wi_dict['ws20ga'] = 1 if ('ws20ga' in tags) else 0
        wi_dict['customer'] = 1 if ('customer' in tags) else 0
        wi_dict['Semester'] = semester(wi_dict['IterationPath'])
        wi_dict['target_month'] = target_month(wi_dict['IterationPath'])    
        wi_dict['scheduled'] = 1 if (target_month(wi_dict['IterationPath'])) else 0
        wi_dict['target_date'] = target_date(wi_dict['IterationPath'])
        wi_dict['triaged'] = 0 if (wi_dict['target_month'] is None) else 1

        if wi.relations:
            for relationship in wi.relations:
                if relationship.rel =='System.LinkTypes.Hierarchy-Forward':
                    childid=relationship.url.rpartition('/')[-1]
                    children.append(childid)
                    parentchildmap.append({idname:str(wi.id), titlename: wi_dict['Title'], "ChildID":str(childid)})
                elif relationship.rel =='System.LinkTypes.Hierarchy-Reverse':
                    parent_id = relationship.url.rpartition('/')[-1]
        wi_dict['ParentID'] = parent_id
        wi_dict['Children'] = children

        work_items_array.append(wi_dict)

    df = pd.DataFrame(work_items_array)
    parentchildmapDF = pd.DataFrame(parentchildmap)
    return df, parentchildmapDF


def get_WorkItems(workitemtype, conn, project):
    start_time = time.time()
    
    if workitemtype == 'Objective':
        query = get_query_objectives()
    elif workitemtype =='Epic':
        query = get_queryTrackingEpics()
    elif workitemtype =='Project':
        query = get_query210()

    print(f"Getting {workitemtype}s ...")
    ids = get_query_ids(conn, query)
    work_items = get_work_items(conn, ids)
    print(f"{workitemtype}: ", len(work_items))
    print(f"{workitemtype} import time: ", time.time() - start_time) 

    df, dfchildren = get_WorkItemDataFrame(workitemtype, project, work_items)
    return df, dfchildren
