'api calls for issue resources'
from functools import partial
import json
import logging
import os

from google.appengine.api import search
from google.appengine.ext import ndb

# pylint: disable=F0401
from pulldb.base import create_app, Route, OauthHandler
from pulldb.models.base import model_to_dict
from pulldb.models import comicvine
from pulldb.models import issues
from pulldb.models.issues import Issue
from pulldb.models import users
from pulldb.models.volumes import Volume

# pylint: disable=W0232,E1101,R0903,C0103

class DropIndex(OauthHandler):
    def get(self, doc_id):
        user = users.user_key(app_user=self.user).get()
        if not user.trusted:
            logging.warn('Untrusted access attempt: %r', self.user)
            self.abort(401)
        index = search.Index(name='issues')
        try:
            index.delete(doc_id)
        except search.Error as error:
            response = {
                'status': 500,
                'message': 'Error dropping document %s' % doc_id,
            }
            logging.error(response['message'])
            logging.exception(error)
        else:
            response = {
                'status': 200,
                'message': 'Document %s dropped' % doc_id,
            }
        self.response.write(json.dumps(response))

class GetIssue(OauthHandler):
    @ndb.tasklet
    def issue_dict(self, issue):
        if issue.volume:
            volume = yield issue.volume.get_async()
        else: # TODO(rgh): Remove when legacy issues all gone
            volume = yield issue.key.parent().get_async()
        raise ndb.Return({
            'volume': model_to_dict(volume),
            'issue': model_to_dict(issue),
        })

    def get(self, identifier):
        query = Issue.query(Issue.identifier == int(identifier))
        results = query.map(self.issue_dict)
        self.response.write(json.dumps({
            'status': 200,
            'results': results
        }))

class RefreshIssue(OauthHandler):
    @ndb.tasklet
    def refresh_issue(self, issue):
        cv_issue = self.cv.fetch_issue(issue.identifier)
        issue_key = issues.issue_key(cv_issue)
        issue = yield issue_key.get_async()
        raise ndb.Return({
            'issue': model_to_dict(issue),
        })

    def get(self, issue):
        self.cv = comicvine.load()
        query = Issue.query(Issue.identifier == int(issue))
        updated_issues = query.map(self.refresh_issue)
        if updated_issues:
            status = {
                'status': 200,
                'count': len(updated_issues),
                'message': '%d issues updated' % len(updated_issues),
                'results': updated_issues,
            }
        else:
            status = {
                'status': 404,
                'message': 'Issue %r not found' % (issue),
            }
        logging.debug(status['message'])
        self.response.write(json.dumps(status))

class Reindex(OauthHandler):
    def get(self, identifier):
        user = users.user_key(app_user=self.user).get()
        if not user.trusted:
            logging.warn('Untrusted access attempt: %r', self.user)
            self.abort(401)
        query = issues.Issue.query(issues.Issue.identifier == int(identifier))
        issue = query.get()
        if issue:
            issue.index_document()
            response = {
                'status': 200,
                'message': 'Issue %s reindexed' % identifier,
            }
        else:
            response = {
                'status': 404,
                'message': 'Issue %s not found' % identifier,
            }
        self.response.write(json.dumps(response))

class SearchIssues(OauthHandler):
    def get(self):
        index = search.Index(name='issues')
        results = []
        issues = index.search(self.request.get('q'))
        logging.debug('results: %r', issues)
        for issue in issues:
            result = {
                'id': issue.doc_id,
                'rank': issue.rank,
            }
            for field in issue.fields:
                result[field.name] = unicode(field.value)
            results.append(result)
        self.response.write(json.dumps({
            'status': 200,
            'count': issues.number_found,
            'results': results,
        }))

app = create_app([
    Route('/api/issues/<identifier>/reindex', Reindex),
    Route('/api/issues/get/<identifier>', GetIssue),
    Route('/api/issues/refresh/<issue>', RefreshIssue),
    Route('/api/issues/index/<doc_id>/drop', DropIndex),
    Route('/api/issues/search', SearchIssues),
])
