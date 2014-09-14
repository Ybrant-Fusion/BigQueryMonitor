#!/usr/bin/env python

from bqmonitor.api import SetGoogleAPIFromServiceAccount, GetGoogleBigQueryClient
from bqmonitor.notifier import SendMail
from bqmonitor import logger
from sys import exit
from prettytable import PrettyTable
import argparse
import datetime
import json
import os 


# Define the parser for the command line.
def commandLineParser():

    main = argparse.ArgumentParser(prog='bqmonitor', description='Jobs monitoring machine that monitors your Google BigQuery load jobs and sends you an email alert when one or more detected as failed.')
    main.add_argument('-v', '--version', action='version', version="1.0", help='show version and exit')
    group = main.add_argument_group('required arguments')
    group.add_argument('--duration', action='store', dest='duration',
                       type=int, default=5, help='time-frame to monitor')
    group.add_argument('--unit', action='store', dest='unit',
                       type=str, default='M', help='unit of time (\'H\' for hours or \'M\' for minutes)')
    return main.parse_args()


# Validate the given arguments.
def validated(duration, unit):

    if (str(unit).upper() in ['H', 'M', 'D']) and \
      ((str(unit).upper() == 'D' and (0 < duration <= 365)) or \
       (str(unit).upper() == 'H' and (0 < duration <= 23)) or \
       (str(unit).upper() == 'M' and (0 < duration <= 59))):
        return True
    else:
        logger.error('Invalid --duration/--unit value.')
        return False


def isFailedJob(failed_jobs, job, duration, unit):

    try:
        if 'load' in job['configuration']:
            job_creation_time = (long(job['statistics']['creationTime']) / 1000.0)
            job_creation_time = datetime.datetime.utcfromtimestamp(job_creation_time)
            delta = datetime.datetime.utcnow() - job_creation_time

            # Check whether the job executed in the given time-frame.
            if (str(unit).upper() == 'D' and delta < datetime.timedelta(days=duration, hours=0, minutes=0)) or \
               (str(unit).upper() == 'H' and delta < datetime.timedelta(days=0, hours=duration, minutes=0)) or \
               (str(unit).upper() == 'M' and delta < datetime.timedelta(days=0, hours=0, minutes=duration)):

                # Check the final error result of the job.
                if 'errorResult' in job['status']:
                    job_id         = job['jobReference']['jobId']
                    dataset        = job['configuration']['load']['destinationTable']['datasetId']
                    table          = job['configuration']['load']['destinationTable']['tableId']
                    source_uri     = job['configuration']['load']['sourceUris'][0]
                    failure_reason = job['status']['errorResult']['message']
                    failed_jobs.add_row([len(failed_jobs._rows) + 1, job_id, job_creation_time,
                                        dataset, table, source_uri, failure_reason])

                    # Logs the failed job.
                    logger.info('Job \'' + job_id + '\' has completed and was unsuccessful.'
                                + ' Reason: ' + failure_reason)
    except KeyError:
        pass

    return


# Main function.
def main():

    # Constants.
    configpath = os.path.dirname(os.path.abspath(__file__)) + '/config'
    configfile = configpath + '/bqmonitor.conf'

    # Enable the command line interface.
    args = commandLineParser()

    # Arguments validation.
    if not validated(args.duration, args.unit):
        logger.debug('Exit.')
        exit()
    else:

        # Load the config file.
        with open(configfile, 'rb') as f:
            conf = json.load(f)

        # Define the final table columns and settings.
        failed_jobs = PrettyTable(['No.', 'Job Id', 'Creation Time', 'Dataset', 'Table', 'SourceURI', 'Failure Reason'])
        failed_jobs.sortby = 'No.'
        failed_jobs.align  = 'l'
        failed_jobs.format = True

        # Store the service account information.
        api = SetGoogleAPIFromServiceAccount(conf['api']['projectNumber'],
                                             conf['api']['serviceEmail'],
                                             configpath + '/' + conf['api']['keyFilename'])

        # Authenticate with the Google BigQuery API.
        bq = GetGoogleBigQueryClient(api)

        # Initial page token.
        page_token = None

        # Initial number of jobs.
        numOfJobs  = 0

        logger.info('Searching failed jobs in project id ' + str(conf['api']['projectNumber']) + '...')

        # Get a list of jobs as long as the counter below the limit.
        limit = (100 * args.duration * conf['api']['jobsPerMinute'] if str(args.unit).upper() == 'D' else 
                (10  * args.duration * conf['api']['jobsPerMinute'] if str(args.unit).upper() == 'H' else 
                (1   * args.duration * conf['api']['jobsPerMinute'] if str(args.unit).upper() == 'M' else 
                                       conf['api']['jobsPerMinute']))) # Otherwise, use the default limit

        while numOfJobs < limit:
            response = bq.getListOfJobs(page_token)
            if response['jobs'] is not None:

                for job in response['jobs']:

                    # Check whether the current job is failed or not.
                    isFailedJob(failed_jobs, job, args.duration, args.unit)

                    # Increase the counter.
                    numOfJobs += 1

            try:
                # Pagination; set the 'nextPageToken' used by the 'getListOfJobs' function.
                if response['nextPageToken']:
                    page_token = response['nextPageToken']
            except KeyError:
                break

        # Send an alert email if one or more failed jobs detected as failed.
        if len(failed_jobs._rows) > 0:
            notification = SendMail(**dict(conf['notifier']))
            notification.send(failed_jobs.get_html_string())
            logger.info('One or more jobs failed. A notification mail has been sent.')
        else:
            logger.info('All jobs completed successfully.')
            logger.debug('Exit.')


# Go to Main function.
if __name__ == '__main__':
    main()
