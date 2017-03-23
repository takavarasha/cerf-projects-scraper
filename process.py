#!/usr/bin/python3
# -*- coding: utf-8 -*-
""" Processes the raw data according to business rules """

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

    with utils.db_create_connection('./scraperwiki.sqlite') as db:
        cursor = db.cursor()
        cursor.execute('begin')
        cursor.execute('DELETE FROM projects')
        try:
            for project in projects_list:
                dateUSGSignature = project.get('dateUSGSignature')
                if dateUSGSignature:
                    dateUSGSignature = dateUSGSignature[:10]
                projectsectors = _get_project_listable_items(project, 'hdxprojectsectors', 'sectorName')
                projectclusters = _get_project_listable_items(project, 'hdxprojectsectors', 'clusterName')
                projectcapcodes = _get_project_listable_items(project, 'hdxprojectcapcode', 'capCode')
                projectgroupings = _get_project_listable_items(project, 'hdxprojectgrouping', 'groupingName')
                cursor.execute(
                    'INSERT INTO projects values (' + ','.join('?' * 18) + ')', (
                        project.get('agencyName'),
                        project.get('continentName'),
                        project.get('countryCode'),
                        project.get('countryName'),
                        dateUSGSignature,
                        project.get('emergencyTypeName'),
                        project.get('projectCode'),
                        project.get('projectID'),
                        project.get('projectTitle'),
                        project.get('regionName'),
                        project.get('tableName'),
                        project.get('totalAmountApproved'),
                        project.get('windowFullName'),
                        project.get('year'),
                        projectsectors,
                        projectclusters,
                        projectgroupings,
                        projectcapcodes
                    )
                )
                progress_value += 1
                update_progress()
            cursor.execute('commit')
        except Exception as err:
            cursor.execute('rollback')
            raise err

    return config


def process(config):
    config['process_result'] = {
        'success': False,
        'messages': []
    }

    _process_projects(config)

    config['process_result']['success'] = True
    return config


