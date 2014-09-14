#!/usr/bin/env python

from oauth2client.client import SignedJwtAssertionCredentials
from apiclient.discovery import build
from bqmonitor import logger
import httplib2
import json


# Store API information for a Google service account.
class SetGoogleAPIFromServiceAccount(object):

    def __init__(self, project_number, service_email, key_path):

        self.project_number = project_number
        self.service_email = service_email
        self.key = self.readKey(key_path)

    # Read the API private key.
    def readKey(self, key_path):

        try:
            with open(key_path, 'rb') as f:
                key = f.read()
            logger.info('Google BigQuery API private key has been loaded. Using key: \'' + key_path + '\'.')
            return key
        except IOError:
            logger.error('No such file or directory: \'' + key_path + '\'.')


# A basic Google BigQuery interface.
class GetGoogleBigQueryClient(object):

    def __init__(self, api):

        if isinstance(api, SetGoogleAPIFromServiceAccount):
            self.api = api
            self.scope = 'https://www.googleapis.com/auth/bigquery.readonly'
            self.service = None
            self.authenticateService()
        else:
            logger.error('The API object given was not valid and is of type %s.' % type(api))

    def authenticateService(self):

        logger.info('Authenticating with the Google BigQuery API.')
        if self.service is None:
            credentials = SignedJwtAssertionCredentials(self.api.service_email, self.api.key, self.scope)
            http_object = credentials.authorize(httplib2.Http())
            self.service = build('bigquery', 'v2', http=http_object)
            logger.info('Authenticated successfully.')
        else:
            return False

    # Get a list of jobs related to our project.
    def getListOfJobs(self, page_token=None):

        jobs = self.service.jobs()
        response = jobs.list(projectId=self.api.project_number,
                             allUsers=True,
                             # maxResults=500,
                             projection='full',
                             stateFilter='done',
                             pageToken=page_token).execute()
        logger.debug(str(json.dumps(response, sort_keys=True, indent=4)))
        return response
