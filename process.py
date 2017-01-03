#!/usr/bin/python3
# -*- coding: utf-8 -*-
""" Processes the raw data according to business rules """

import scraperwiki
import utils


def _get_project_listable_items(project, item_key, item_code_key):
    list_items = []
    if len(project[item_key][item_key]) > 0:
        for item_code in project[item_key][item_key]:
            list_items.append(item_code.get(item_code_key))
    return ','.join(map(str, list_items))


def _process_projects(config):

    def update_progress():
        utils.progress(progress_value, progress_max, prefix='Processing Projects:', bar_length=50)

    projects_list = config['collect_result']['json_data']
    progress_max = len(projects_list)  # Used to track the total number of rows for the progress bar
    progress_value = 0  # Used to track the number of rows processed so far
    update_progress()

    scraperwiki.sql.execute('DELETE FROM _tmp_projects')

    for project in projects_list:
        scraperwiki.sql.execute(
            'INSERT INTO _tmp_projects values (' + ','.join('?' * 46) + ')', (
                project.get('agencyName'),
                project.get('applicationCode'),
                project.get('applicationID'),
                project.get('beneficiariesAdults'),
                project.get('beneficiariesBoys'),
                project.get('beneficiariesChildren'),
                project.get('beneficiariesFemale'),
                project.get('beneficiariesGirls'),
                project.get('beneficiariesMale'),
                project.get('beneficiariesMen'),
                project.get('beneficiariesTotal'),
                project.get('beneficiariesWomen'),
                project.get('cerfGenderMarkerID'),
                project.get('cerfGenderMarkerName'),
                project.get('continentName'),
                project.get('countryCode'),
                project.get('countryID'),
                project.get('countryName'),
                project.get('dateUSGSignature'),
                project.get('disbursementDate'),
                project.get('emergencyCategoryName'),
                project.get('emergencyGroupName'),
                project.get('emergencyTypeID'),
                project.get('emergencyTypeName'),
                project.get('implementingAgencyID'),
                project.get('implementingAgencyName'),
                project.get('letterSentToAgencyDate'),
                project.get('projectCode'),
                project.get('projectID'),
                project.get('projectStartDate'),
                project.get('projectStatus'),
                project.get('projectTitle'),
                project.get('projectTypeID'),
                project.get('projectTypeName'),
                project.get('regionName'),
                project.get('subRegionName'),
                project.get('tableName'),
                project.get('totalAmountApproved'),
                project.get('windowFullName'),
                project.get('windowID'),
                project.get('year'),
                _get_project_listable_items(project, 'projectsectors', 'sectorName'),
                _get_project_listable_items(project, 'projectsectors', 'clusterName'),
                _get_project_listable_items(project, 'projectsectors', 'iascSectorname'),
                _get_project_listable_items(project, 'projectcapcode', 'capCode'),
                _get_project_listable_items(project, 'projectgrouping', 'groupingName')
            )
        )
        progress_value += 1
        update_progress()

    scraperwiki.sql.execute('DELETE FROM projects')
    scraperwiki.sql.execute('INSERT INTO projects SELECT * FROM _tmp_projects')

    return config


def process(config):
    config['process_result'] = {
        'success': False,
        'messages': []
    }

    _process_projects(config)

    config['process_result']['success'] = True
    return config


