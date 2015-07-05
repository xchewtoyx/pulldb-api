'API endpoints for pull management'
from collections import defaultdict
from functools import partial
import json
import logging

from google.appengine.datastore.datastore_query import Cursor
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
        pull_list = ndb.get_multi(pull_keys)
        if pull_list:
            status = 200
            message = 'Found %d pulls' % identifier
        else:
            status = 404
            message = 'No pulls found (%r)' % identifier
        self.response.write(json.dumps({
            'status': status,
            'message': message,
            'results': pull_list,
        }))

class GetPull(OauthHandler):
    def get(self, identifier):
        self.user_key = users.user_key(self.user)
        query = pulls.Pull.query(
            pulls.Pull.identifier == int(identifier),
            ancestor=self.user_key,
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
    @ndb.tasklet
    def fetch_page(self, query):
        limit = self.request.get('limit', 100)
        cursor = Cursor(urlsafe=self.request.get('position'))
        pulls, next_cursor, more = yield query.fetch_page_async(
            limit, start_cursor=cursor)
        context_callback = partial(
            pull_context, context=self.request.get('context'))
        context_futures = map(context_callback, pulls)
        results = yield context_futures
        raise ndb.Return(
            results,
            next_cursor,
            more,
        )

    def query(self, pull_type):
        sortkey = pulls.Pull.pubdate
        if self.request.get('weighted'):
            sortkey = pulls.Pull.weight
        if self.request.get('reverse'):
            sortkey = -sortkey
        query_method = getattr(self, 'query_' + pull_type)
        return query_method().order(sortkey)

    def query_all(self):
        return pulls.Pull.query(
            ancestor=self.user_key
        )

    def query_ignored(self):
        return pulls.Pull.query(
            pulls.Pull.ignored == True,
            ancestor=self.user_key
        )

    def query_new(self):
        if self.request.get('all'):
            query = pulls.Pull.query(
                pulls.Pull.pulled == False,
                ancestor=self.user_key
            )
        else:
            query = pulls.Pull.query(
                pulls.Pull.pulled == False,
                pulls.Pull.ignored == False,
                ancestor=self.user_key
            )
        return query

    def query_unread(self):
        return pulls.Pull.query(
            pulls.Pull.pulled == True,
            pulls.Pull.ignored == False,
            pulls.Pull.read == False,
            ancestor=self.user_key
        )

    def get(self, pull_type=None):
        self.user_key = users.user_key(self.user)
        query = self.query(pull_type)
        count_future = query.count_async()
        results, next_cursor, more = self.fetch_page(query).get_result()
        if next_cursor:
            position = next_cursor.urlsafe()
        else:
            position = ''
        self.response.write(json.dumps({
            'status': 200,
            'message': '%d pulls found' % count_future.get_result(),
            'more_results': more,
            'next_page': position,
            'results': list(results),
        }))


class PullStats(OauthHandler):
    def get(self):
        user_key = users.user_key(self.user)
        total_count = pulls.Pull.query(
            ancestor=user_key).count_async()
        ignored_count = pulls.Pull.query(
            pulls.Pull.ignored == True,
            ancestor=user_key).count_async()
        new_count = pulls.Pull.query(
            pulls.Pull.pulled == False,
            pulls.Pull.ignored == False,
            ancestor=user_key).count_async()
        unread_count = pulls.Pull.query(
            pulls.Pull.pulled == True,
            pulls.Pull.read == False,
            ancestor=user_key).count_async()
        read_count = pulls.Pull.query(
            pulls.Pull.pulled == True,
            pulls.Pull.read == True,
            ancestor=user_key).count_async()
        result = {
            'status': 200,
            'counts': {
                'ignored': ignored_count.get_result(),
                'new': new_count.get_result(),
                'unread': unread_count.get_result(),
                'read': read_count.get_result(),
                'total': total_count.get_result(),
            },
        }
        self.response.write(json.dumps(result))


class RefreshPull(OauthHandler):
    @ndb.tasklet
    def refresh_pull(self, pull):
        if pull.issue and not pull.volume:
            logging.info('Adding missing volume attribute to pull %r',
                         pull.key)
            pull.volume = volumes.volume_key(pull.subscription.id())
            pull_changed = True
        if pull.ignored and pull.pulled:
            pull.pulled = False
            pull_changed = True
        if pull_changed:
            yield pull.put_async()
            raise ndb.Return({
                'pull': model_to_dict(pull)
            })

    def single(self, identifier):
        query = pulls.Pull.query(
            pulls.Pull.identifier == int(identifier),
            ancestor=user_key
        )

    def shard(self, identifier):
        query = pulls.Pull.query(
            pulls.Pull.shard == int(identifier),
            ancestor=user_key
        )

    def get(self, identifier, pull_type=None):
        self.user_key = users.user_key(self.user)
        if pull_type == shard:
            query = self.query_shard(identifier)
        else:
            query = self.query_single(identifier)
        result = query.map(self.refresh_pull)
        response = {
            'status': 200,
            'count': len(result),
            'message': 'pull refreshed',
        }
        self.response.write(json.dumps(response))

class RemovePulls(OauthHandler):
    @ndb.toplevel
    def post(self):
        user_key = users.user_key(self.user, create=False)
        request = json.loads(self.request.body)
        issue_ids = request['issues']
        results = defaultdict(list)
        pull_keys = [ ndb.Key(
            pulls.Pull, issue_id, parent=user_key) for issue_id in issue_ids]
        records = ndb.get_multi(pull_keys)
        issue_dict = {record.key.id(): record for record in records}
        candidates = []
        for issue_id, pull in zip(issue_ids, records):
            if pull:
                results['removed'].append(issue_id)
                candidates.append(pull.key)
            else:
                results['skipped'].append(issue_id)
        ndb.delete_multi(candidates)
        response = {
            'status': 200,
            'message': 'Removed %d pulls' % len(results['removed']),
            'results': results
        }
        self.response.write(json.dumps(response))

class UpdatePulls(OauthHandler):
    operations = ['pull', 'unpull', 'read', 'unread', 'ignore', 'unignore']

    def __init__(self, *args, **kwargs):
        self.user_key = None
        self.results = defaultdict(list)
        super(UpdatePulls, self).__init__(*args, **kwargs)

    @ndb.tasklet
    def check_pulls(self, pull_ids):
        pull_keys = [
            pulls.pull_key(pull_id, user=self.user_key) for pull_id in pull_ids]
        pull_list = yield ndb.get_multi_async(pull_keys)
        raise ndb.Return(pull_list)

    @ndb.tasklet
    def pull(self, pull_ids):
        pull_list = yield self.check_pulls(pull_ids)
        updated_pulls = []
        for pull in pull_list:
            if pull:
                if pull.pulled:
                    self.results['skipped'].append(pull.key.id())
                else:
                    self.results['updated'].append(pull.key.id())
                    logging.info('pulling %r', pull.issue.id())
                    pull.pulled = True
                    updated_pulls.append(pull)
            else:
                self.results['failed'].append(pull.key.id())
        updated_keys = yield ndb.put_multi_async(updated_pulls)
        raise ndb.Return(updated_keys)

    @ndb.tasklet
    def unpull(self, pull_ids):
        pull_list = yield self.check_pulls(pull_ids)
        updated_pulls = []
        for pull in pull_list:
            if pull:
                if pull.pulled:
                    self.results['updated'].append(pull.key.id())
                    logging.info('unpulling %r', pull.issue.id())
                    pull.pulled = False
                    updated_pulls.append(pull)
                else:
                    self.results['skipped'].append(pull.key.id())
            else:
                self.results['failed'].append(pull.key.id())
        updated_keys = yield ndb.put_multi_async(updated_pulls)
        raise ndb.Return(updated_keys)

    @ndb.tasklet
    def read(self, pull_ids):
        pull_list = yield self.check_pulls(pull_ids)
        updated_pulls = []
        for pull in pull_list:
            if pull:
                if pull.read:
                    self.results['skipped'].append(pull.key.id())
                else:
                    self.results['updated'].append(pull.key.id())
                    logging.info('marking %r read', pull.key.id())
                    pull.pulled = True
                    pull.read = True
                    updated_pulls.append(pull)
            else:
                self.results['failed'].append(pull.key.id())
        updated_keys = yield ndb.put_multi_async(updated_pulls)
        raise ndb.Return(updated_keys)

    @ndb.tasklet
    def unread(self, pull_ids):
        pull_list = yield self.check_pulls(pull_ids)
        updated_pulls = []
        for pull in pull_list:
            if pull:
                if pull.read:
                    self.results['updated'].append(pull.key.id())
                    logging.info('marking %r unread', pull.issue.id())
                    pull.read = False
                    updated_pulls.append(pull)
                else:
                    self.results['skipped'].append(pull.key.id())
            else:
                self.results['failed'].append(pull.key.id())
        updated_keys = yield ndb.put_multi_async(updated_pulls)
        raise ndb.Return(updated_keys)

    @ndb.tasklet
    def ignore(self, pull_ids):
        pull_list = yield self.check_pulls(pull_ids)
        updated_pulls = []
        for pull in pull_list:
            if pull:
                if pull.ignored:
                    self.results['skipped'].append(pull.key.id())
                else:
                    self.results['updated'].append(pull.key.id())
                    logging.info('ignoring %r', pull.issue.id())
                    pull.ignored = True
                    pull.pulled = False
                    updated_pulls.append(pull)
            else:
                self.results['failed'].append(pull.key.id())
        updated_keys = yield ndb.put_multi_async(updated_pulls)
        raise ndb.Return(updated_keys)

    @ndb.tasklet
    def unignore(self, pull_ids):
        pull_list = yield self.check_pulls(pull_ids)
        updated_pulls = []
        for pull in pull_list:
            if pull:
                if pull.ignored:
                    self.results['updated'].append(pull.key.id())
                    logging.info('unignoring %r', pull.issue.id())
                    pull.ignored = False
                    updated_pulls.append(pull)
                else:
                    self.results['skipped'].append(pull.key.id())
            else:
                self.results['failed'].append(pull.key.id())
        updated_keys = yield ndb.put_multi_async(updated_pulls)
        raise ndb.Return(updated_keys)

    def post(self):
        self.user_key = users.user_key(self.user)
        request = json.loads(self.request.body)
        logging.debug('Decoded post data: %r' % request)
        updated_pulls = []
        for operation in self.operations:
            method = getattr(self, operation)
            if method and request.get(operation):
                updated_pulls.append(method(request[operation]))
        ndb.Future.wait_all(updated_pulls)
        response = {
            'status': 200,
            'results': self.results
        }
        self.response.write(json.dumps(response))

app = create_app([
    Route('/api/pulls/add', AddPulls),
    Route('/api/pulls/fetch', FetchPulls),
    Route('/api/pulls/<identifier>/get', GetPull),
    Route('/api/pulls/<identifier>/refresh<shard:(shard)?>', RefreshPull),
    Route('/api/pulls/list/<pull_type:(all|new|unread|ignored)>', ListPulls),
    Route('/api/pulls/remove', RemovePulls),
    Route('/api/pulls/stats', PullStats),
    Route('/api/pulls/update', UpdatePulls),
])
