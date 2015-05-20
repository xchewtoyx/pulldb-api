'api calls for issue resources'
import json
import logging

from google.appengine.api import search
from google.appengine.datastore.datastore_query import Cursor
from google.appengine.ext import ndb

# pylint: disable=F0401
from pulldb.base import create_app, Route, OauthHandler
from pulldb.models.base import model_to_dict
from pulldb.models import comicvine
from pulldb.models import issues
from pulldb.models import pulls
from pulldb.models import users
from pulldb.models import volumes

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
    def issue_context(self, issue):
        volume_dict = {}
        if self.request.get('context'):
            volume = yield issue.volume.get_async()
            volume_dict = model_to_dict(volume)
        raise ndb.Return({
            'volume': volume_dict,
            'issue': model_to_dict(issue),
        })

    def get(self, identifier):
        query = issues.Issue.query(issues.Issue.identifier == int(identifier))
        results = query.map(self.issue_context)
        self.response.write(json.dumps({
            'status': 200,
            'results': results
        }))


class IssueStats(OauthHandler):
    def get(self):
        include_total = False
        if self.request.get('all'):
            include_total = True
            total_count = issues.Issue.query().count_async()
        queued_count = issues.Issue.query(
            issues.Issue.complete == False).count_async()
        toindex_count = issues.Issue.query(
            issues.Issue.indexed == False).count_async()
        result = {
            'status': 200,
            'counts': {
                'queued': queued_count.get_result(),
                'toindex': toindex_count.get_result(),
            },
        }
        if include_total:
            result['counts']['total'] = total_count.get_result()
        self.response.write(json.dumps(result))


class ListIssues(OauthHandler):
    order_keys = {
        ('pubdate', 'asc'): issues.Issue.pubdate,
        ('pubdate', 'desc'): -issues.Issue.pubdate,
    }

    @ndb.tasklet
    def issue_context(self, issue):
        volume_dict = {}
        pull_dict = {}
        if self.request.get('context'):
            pull_key = ndb.Key('Pull', issue.key.id(), parent=self.user_key)
            pull, volume = yield (
                pull_key.get_async(),
                issue.volume.get_async()
            )
            pull_dict = model_to_dict(pull)
            volume_dict = model_to_dict(volume)
        raise ndb.Return({
            'pull': pull_dict,
            'volume': volume_dict,
            'issue': model_to_dict(issue),
        })

    @ndb.tasklet
    def fetch_page(self, query):
        limit = self.request.get('limit', 10)
        cursor = Cursor(urlsafe=self.request.get('position'))
        issue_matches, next_cursor, more = yield query.fetch_page_async(
            limit, start_cursor=cursor)
        context_futures = [self.issue_context(issue) for issue in issue_matches]
        results = yield context_futures
        raise ndb.Return(
            results,
            next_cursor,
            more,
        )

    def get(self):
        self.user_key = users.user_key(self.user)
        query = issues.Issue.query()
        if self.request.get('queued'):
            query = query.filter(issues.Issue.complete == False)
        count_future = query.count_async()
        sort_key = (
            self.request.get('sort_key'),
            self.request.get('sort_order')
        )
        sort = self.order_keys.get(sort_key)
        if sort:
            query = query.order(sort)
        results, next_cursor, more = self.fetch_page(query).get_result()
        if next_cursor:
            position = next_cursor.urlsafe()
        else:
            position = ''
        self.response.write(json.dumps({
            'status': 200,
            'message': '%d issues found' % count_future.get_result(),
            'more_results': more,
            'next_page': position,
            'results': list(results),
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
        query = issues.Issue.query(issues.Issue.identifier == int(issue))
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
    Route('/api/issues/index/<doc_id>/drop', DropIndex),
    Route('/api/issues/list', ListIssues),
    Route('/api/issues/refresh/<issue>', RefreshIssue),
    Route('/api/issues/search', SearchIssues),
    Route('/api/issues/stats', IssueStats),
])
