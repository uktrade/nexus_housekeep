import requests
import pickle
import logging
import os, sys
from collections import defaultdict, OrderedDict
from datetime import datetime, timedelta
#import pdb; pdb.set_trace()

params_list = ['lite-builds-raw','lite-builds']                                 #List of Assets.
files_to_keep = 10                                                              #No of files to keep that are older then 3 months.
nexus_url = 'http://nexus.mgmt.licensing.service.trade.gov.uk.test'
nexus_home_dir = '/Users/jay/Documents/Work/DIT/Work/WebOps/nexus_clean'
logging.basicConfig(filename=nexus_home_dir + '/NexusHousekeep.log', level=logging.INFO)
logging.info('Date of clean: %s', datetime.now())
                                                                                #Create list of files to delete that are older the 3 months
                                                                                #and cull the list leaving only (files_to_keep) number of group of files.
def make_cull_list(full_list, team_name_field, app_name_field, file_date_field):
    not_date_stamped = 0
    #print ('Total files: ' + str(len(full_list)))

    sorted_list = []
    sorted_list = (sorted(full_list))                                           #Order the list date.
    test_date = datetime.now() - timedelta(days=90)

    olderthan3months_list = []                                                  #List of files that are older then 3 Months.
    for current_name in sorted_list:
        get_date = current_name[file_date_field]
        try:    
            file_date = datetime.strptime(get_date, '%Y%m%d.%H%M%S')
            if (file_date < test_date):
                olderthan3months_list.append(current_name)
        except:
            not_date_stamped += 1
    #print ('No of files older then 3 Months: ' + str(len(olderthan3months_list)))

    grouped_items = defaultdict(list)                                           #Remove number of (files_to_keep) and create new list.
    for current_items in olderthan3months_list:
        grouped_items[current_items[team_name_field]].append(current_items)
    for group_name, files in grouped_items.items():
        grouped = OrderedDict()
        for item in files:
            file_base = item[app_name_field] + item[file_date_field]
            if file_base not in grouped:
                grouped[file_base] = []
            grouped[file_base].append(item)
        grouped_items[group_name] = grouped
    grouped_delete_list = []
    for group_name, files in grouped_items.items():
        old_files = files.items()[:-files_to_keep]
        grouped_delete_list.extend(old_files)     
    delete_list = []
    for files in grouped_delete_list:
        for _file in files[1]:
            delete_list.append(os.path.join(*_file))

    for current_item in delete_list:                                            #Use the Nexus API to mark files for deletion.
        logging.info('Deleting -> %s', current_item)
        #resp = requests.delete(nexus_url + '/repository/' + current_item , auth=('admin','admin123'))
    logging.info('Total files marked for deletion for %s asset: %d', full_list[0][0], len(delete_list))
    print 'Total files marked for deletion for', full_list[0][0], 'asset: ', len(delete_list)   

for asset in params_list:                                                       #Start purge on each Nexus asset.
    params = {'repositoryId': asset}
    ctoken = None
    full_list = []

                                                                                #Retrieve a list of all assets from API, Nexus will return
    while True:                                                                 #a set number of items per page therefore a page token is required.
        if ctoken:
            params['continuationToken'] = ctoken
        response = requests.get(nexus_url + '/service/siesta/rest/v1/assets', params = params, auth=('admin','admin123'))
        data = response.json()

        for item in data['items']:                                              #Extract filename and location from list.
            name = item['coordinates']
            full_list.append([asset] + name.split('/'))
        ctoken = data.get('continuationToken', None)
        if not ctoken:
            break
    
    if asset == 'lite-builds':                                                  #Each Asset has different naming conventions so format differs.
        make_cull_list(full_list, 4, 5, 6)                                      #Pass postion of team name, app name and date field.
    else:
        make_cull_list(full_list, 1, 2, 3)                                      #Pass postion of team name, app name and date field.

