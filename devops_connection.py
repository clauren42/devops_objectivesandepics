import json
from azure.devops.connection import Connection
from msrest.authentication import BasicAuthentication
from azure.devops.v5_1.work_item_tracking.models import Wiql

def get_connstrings():
    with open('security.json') as f:
        devops_projects = json.load(f)
        return devops_projects

def get_query_papercuts():
    query = """
    SELECT
        [System.Id],
        [System.CreatedDate],
        [System.Title],
        [System.CreatedBy],
        [System.State],
        [Microsoft.VSTS.Common.Priority],
        [System.AreaPath],
        [System.AssignedTo],
        [System.Tags],
        [System.IterationPath]
    FROM workitems
    WHERE
        [System.TeamProject] = 'Vienna'
        AND (
            [System.WorkItemType] <> ''
            AND [System.Tags] CONTAINS 'Papercuts'
            OR [System.Tags] CONTAINS 'papercut'
            OR [System.Tags] CONTAINS 'WS20GA'
        )
    ORDER BY [System.AreaPath]
    """
    return query

def get_queryTrackingEpics():
    query = """
    SELECT
        [System.Id]
    FROM workitems
    WHERE
        (
            [System.TeamProject] = 'Vienna'
            AND [System.WorkItemType] = 'Epic'
            AND [System.State] <> 'Removed'
            AND [System.Tags] CONTAINS 'Tracking'
        )
    ORDER BY [System.Id]
    """

    return query

def get_query210():
    query = """
    SELECT 
            [System.Id]
    FROM workitems
    WHERE
            [System.TeamProject] = 'Vienna'
            AND [System.State] <> 'Removed'
            AND [System.Tags] CONTAINS '2x10'
    ORDER BY [System.Id] 
    """

    return query

def get_devopsconnection(project):
    
    token = project["token"]
    url = project["url"]
    
    credentials = BasicAuthentication("PAT", token)
    connection = Connection(base_url=url, creds=credentials)

    conn = connection.get_client('azure.devops.v5_1.work_item_tracking.work_item_tracking_client.WorkItemTrackingClient')

    return conn

def get_query_ids(conn, query):
    print ("Executing query:", query)
    query_wiql = Wiql(query=query)
    results = conn.query_by_wiql(query_wiql).work_items
    ids=list(wir.id for wir in results)
    return ids

def get_work_items(conn, ids):
    id_start = 0
    batch_size = 200
    work_items=[]
    while id_start < len(ids) :
        work_items.extend(conn.get_work_items(ids[id_start:id_start+batch_size], expand="Relations"))
        id_start += batch_size
    return work_items

def get_last_comment(conn, id):
    try:
        comment = conn.get_comments(project="Vienna", work_item_id = id , top=1, order="desc").comments[0].text
    except:
        comment = ""
    return comment

def get_root_area_path(area_path):
    try:
        return area_path.split("\\")[1]
    except:
        return ""
    

def get_work_items_with_specific_fields(conn, ids, fields):
    id_start = 0
    batch_size = 200
    work_items=[]
    while id_start < len(ids) :
        work_items.extend(conn.get_work_items(ids[id_start:id_start+batch_size], fields=fields))
        id_start += batch_size
        print (id_start)
    return work_items


def get_field(wi,field):
    try:
        return wi.fields[field]
    except:
        return None

def get_target_date(wi):
    return get_field(wi, 'Microsoft.VSTS.Scheduling.TargetDate')

def get_parent_id(work_item):
    try:
        parent = next((relation for relation in work_item.relations if relation.rel == 'System.LinkTypes.Hierarchy-Reverse'))
        parent_id = int(parent.url.rsplit('/', 1)[-1])
    except:
        parent_id = 0
    return parent_id

def get_child_ids(work_item):
    child_ids = []
    try:
        for relation in work_item.relations :
            try:
                if relation.rel == 'System.LinkTypes.Hierarchy-Forward':
                    child_ids.append(int(relation.url.rsplit('/', 1)[-1]))
            except:
                print("get_child_ids: Invalid relation found")
    except:
        print ("get_child_ids: No relations found in work item")
    return child_ids


