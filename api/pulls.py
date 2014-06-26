'API endpoints for pull management'
from collections import defaultdict
from functools import partial
import json
import logging

from google.appengine.ext import ndb

# pylint: disable=F0401

from pulldb.base import create_app, OauthHandler, Route
from pulldb.models.base import model_to_dict
from pulldb.models import issues
from pulldb.models import pulls
from pulldb.models import subscriptions
from pulldb.models import users
from pulldb.models import volumes

# pylint: disable=W0232,E1101,R0903,R0201,C0103

@ndb.tasklet
def pull_context(pull, context=False):
    issue_dict = {}
    volume_dict = {}
    if context:
        issue, volume = yield (
            pull.issue.get_async(),
            pull.volume.get_async(),
        )
        issue_dict = model_to_dict(issue)
        volume_dict = model_to_dict(volume)
    raise ndb.Return({
        'pull': model_to_dict(pull),
        'issue': issue_dict,
        'volume': volume_dict,
    })

class AddPulls(OauthHandler):
    def post(self):
        user_key = users.user_key(self.user)
        request = json.loads(self.request.body)
        issue_ids = request['issues']
        results = defaultdict(list)
        query = issues.Issue.query(
            issues.Issue.identifier.IN(
                [int(identifier) for identifier in issue_ids]
            )
        )
        records = query.fetch()
        issue_dict = {record.key.id(): record for record in records}
        candidates = []
        for issue_id in issue_ids:
            issue = issue_dict.get(issue_id)
            if issue:
                try:
                    pull_key = pulls.pull_key(
                        issue, user=user_key, create=False)
                    candidates.append((issue.key, pull_key))
                except pulls.NoSuchIssue as error:
                    logging.info(
                        'Unable to add pull, issue %s/%r not found',
                        issue_id, issue
                    )
                    results['failed'].append(issue_id)
            else:
                logging.info(
                    'Unable to add pull, issue %s/%r not found',
                    issue_id, issue)
                results['failed'].append(issue_id)
        # prefetch for efficiency
        ndb.get_multi(pull for issue, pull in candidates)
        new_pulls = []
        for issue_key, pull_key in candidates:
            if pull_key.get():
                logging.info(
                    'Unable to add pull, issue %s already pulled',
                    issue_key.id()
                )
                # Already exists
                results['skipped'].append(pull_key.id())
            else:
                new_pulls.append(pulls.Pull(
                    key=pull_key,
                    issue=issue_key,
                    read=False,
                ))
                results['added'].append(pull_key.id())
        ndb.put_multi(new_pulls)
        response = {
            'status': 200,
            'results': results
        }
        self.response.write(json.dumps(response))

class FetchPulls(OauthHandler):
    def post(self):
        user_key = users.user_key(self.user)
        request = json.loads(self.request.body)
        pull_keys = []
        for pull_id in request.get('ids', []):
            pull_keys.append(
                pulls.pull_key(pull_id, user=user_key, create=False))
        pulls = ndb.get_multi(pull_keys)
        if pulls:
            status = 200
            message = 'Found %d pulls' % identifier
        else:
            status = 404
            message = 'No pulls found (%r)' % identifier
        self.response.write(json.dumps({
            'status': status,
            'message': message,
            'results': pulls,
        }))

class GetPull(OauthHandler):
    def get(self, identifier):
        self.user_key = users.user_key(self.user)
        query = pulls.Pull.query(
            pulls.Pull.identifier == int(identifier)
        )
        context_callback = partial(
            pull_context, context=self.request.get('context'))
        results = query.map(context_callback)
        if results:
            status = 200
            message = 'Found pull for %r' % identifier
        else:
            status = 404
            message = 'Pull not found (%r)' % identifier
        self.response.write(json.dumps({
            'status': status,
            'message': message,
            'results': results,
        }))

class ListPulls(OauthHandler):
    def get(self):
        user_key = users.user_key(self.user)
        query = pulls.Pull.query(ancestor=user_key)
        context_callback = partial(
            pull_context, context=self.request.get('context'))
        results = query.map(context_callback)
        self.response.write(json.dumps({
            'status': 200,
            'message': '%d pulls found' % len(results),
            'results': list(results),
        }))

class NewIssues(OauthHandler):
    def get(self):
        user_key = users.user_key(self.user)
        query = pulls.Pull.query(
            pulls.Pull.pulled == False,
            ancestor=user_key
        ).order(pulls.Pull.pubdate)
        context_callback = partial(
            pull_context, context=self.request.get('context'))
        new_pulls = query.map(context_callback)
        result = {
            'status': 200,
            'results': new_pulls,
        }
        self.response.write(json.dumps(result))

class RefreshPull(OauthHandler):
    @ndb.tasklet
    def refresh_pull(self, pull):
        if pull.issue and not pull.volume:
            pull.volume = volumes.volume_key(pull.subscription.id())
            if pull.volume:
                logging.info('Adding missing volume attribute to pull %r',
                             pull.key)
                yield pull.put_async()
                raise ndb.Return({
                    'pull': model_to_dict(pull)
                })

    def get(self, identifier):
        self.user_key = users.user_key(self.user)
        query = pulls.Pull.query(
            pulls.Pull.identifier == int(identifier)
        )
        result = query.map(self.refresh_pull)
        self.response.write({
            'status': 200,
            'message': 'pull refreshed',
        })

class UnreadIssues(OauthHandler):
    def get(self):
        if self.request.get('weighted'):
            sortkey = pulls.Pull.weight
        else:
            sortkey = pulls.Pull.pubdate
        user_key = users.user_key(self.user)
        query = pulls.Pull.query(
            pulls.Pull.pulled == True,
            pulls.Pull.read == False,
            ancestor=user_key
        ).order(sortkey)
        context_callback = partial(
            pull_context, context=self.request.get('context'))
        unread_pulls = query.map(context_callback)
        result = {
            'status': 200,
            'message': 'Found %d unread pulls' % len(unread_pulls),
            'results': unread_pulls,
        }
        self.response.write(json.dumps(result))

class UpdatePulls(OauthHandler):
    def post(self):
        user_key = users.user_key(self.user)
        request = json.loads(self.request.body)
        issue_ids = (
            request.get('pull', []) +
            request.get('read', []) +
            request.get('unread', [])
        )
        results = defaultdict(list)
        query = issues.Issue.query(issues.Issue.identifier.IN(
            [int(identifier) for identifier in issue_ids]))
        records = query.fetch()
        issue_dict = {record.key.id(): record for record in records}
        candidates = []
        for issue_id in issue_ids:
            issue = issue_dict.get(issue_id)
            if issue:
                pull_key = pulls.pull_key(issue_id, user=user_key)
                candidates.append(pull_key)
            else:
                # no such issue
                results['failed'].append(issue_id)
        # prefetch for efficiency
        ndb.get_multi(candidates)
        updated_pulls = []
        for pull_key in candidates:
            pull = pull_key.get()
            if pull:
                if pull.issue.id() in request.get('pull', []):
                    if pull.pulled:
                        results['skipped'].append(pull_key.id())
                    else:
                        results['updated'].append(pull_key.id())
                        pull.pulled = True
                        updated_pulls.append(pull)
                if pull.issue.id() in request.get('read', []):
                    if pull.read:
                        results['skipped'].append(pull_key.id())
                    else:
                        results['updated'].append(pull_key.id())
                        pull.pulled = True
                        pull.read = True
                        updated_pulls.append(pull)
                if pull.issue.id() in request.get('unread', []):
                    if pull.read:
                        results['updated'].append(pull_key.id())
                        pull.read = False
                        updated_pulls.append(pull)
                    else:
                        results['skipped'].append(pull_key.id())
            else:
                # No such pull
                results['failed'].append(pull_key.id())
        ndb.put_multi(updated_pulls)
        response = {
            'status': 200,
            'results': results
        }
        self.response.write(json.dumps(response))

app = create_app([
    Route('/api/pulls/add', AddPulls),
    Route('/api/pulls/fetch', FetchPulls),
    Route('/api/pulls/<identifier>/get', GetPull),
    Route('/api/pulls/<identifier>/refresh', RefreshPull),
    Route('/api/pulls/list/all', ListPulls),
    Route('/api/pulls/list/new', NewIssues),
    Route('/api/pulls/list/unread', UnreadIssues),
    Route('/api/pulls/update', UpdatePulls),
])
