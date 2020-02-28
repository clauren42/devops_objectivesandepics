#pip install azure-kusto-data

import os
from datetime import timedelta
import pandas
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.kusto.data.request import KustoClient, KustoConnectionStringBuilder, ClientRequestProperties

use_cache = False

if use_cache:
    print('using cache')

else:
    cluster = "https://icmcluster.kusto.windows.net"
    output_path = './outputs'
    os.makedirs(output_path, exist_ok=True)

    # It is highly recommended to create one instance and use it for all of your queries.
    kcsb = KustoConnectionStringBuilder.with_aad_device_authentication(cluster)

    # The authentication method will be taken from the chosen KustoConnectionStringBuilder.
    client = KustoClient(kcsb)
    db = "IcmDataWarehouse"
    query = open("kustoicmquery.txt", "r").read() 
    file_name = os.path.join(output_path, 'ICM.csv')

    response = client.execute(db, query)

    # we also support dataframes:
    df_incidents = dataframe_from_result_table(response.primary_results[0])

    df_incidents.to_csv(file_name)


