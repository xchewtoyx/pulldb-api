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
                pull_key = pulls.pull_key(issue)
                candidates.append((issue.key, pull_key))
            else:
                logging.info(
                    'Unable to add pull, issue %s not found', issue_id)
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
                results['failed'].append(pull_key.id())
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

class GetPull(OauthHandler):
    @ndb.tasklet
    def pull_context(self, pull):
        if pull.subscription:
            volume_key = volumes.volume_key(pull.subscription.id())
            issue, volume = yield pull.issue.get_async(), volume_key.get_async()
        else: # TODO(rgh): remove when legacy pulls removed
            issue = yield pull.issue.get_async()
            volume = yield issue.key.parent().get_async()
        raise ndb.Return({
            'pull': model_to_dict(pull),
            'issue': model_to_dict(issue),
            'volume': model_to_dict(volume),
        })

    def get(self, identifier):
        self.user_key = users.user_key(self.user)
        query = issues.Issue.query(
            issues.Issue.identifier == int(identifier)
        )
        result = query.map(self.pull_context)
        if result:
            self.response.write({
                'status': 200,
                'pull': result['pull'],
                'issue': result['issue'],
            })

class ListPulls(OauthHandler):
    @ndb.tasklet
    def pull_context(self, pull):
        if pull.subscription:
            volume_key = volumes.volume_key(pull.subscription.id())
            issue, volume = yield pull.issue.get_async(), volume_key.get_async()
        else: # TODO(rgh): remove when legacy pulls removed
            issue = yield pull.issue.get_async()
            volume = yield issue.key.parent().get_async()
        raise ndb.Return({
            'pull': model_to_dict(pull),
            'issue': model_to_dict(issue),
            'volume': model_to_dict(volume),
        })

    def get(self):
        user_key = users.user_key(self.user)
        query = pulls.Pull.query(ancestor=user_key)
        results = query.map(self.pull_context)
        self.response.write(json.dumps({
            'status': 200,
            'results': results,
        }))

class NewIssues(OauthHandler):
    @ndb.tasklet
    def check_pulled(self, issue):
        pull_key = pulls.pull_key(issue.key.id())
        pull = yield pull_key.get_async()
        if not pull:
            raise ndb.Return(issue)

    @ndb.tasklet
    def find_new_issues(self, subscription):
        query = issues.Issue.query(
            issues.Issue.volume == subscription.volume
        ).filter(
            issues.Issue.pubdate > subscription.start_date
        ).order(issues.Issue.pubdate)
        # volume is pre-fetched here for cacheing but not used
        dummy_volume, results = yield (
            subscription.volume.get_async(),
            query.map_async(self.check_pulled))
        if results:
            raise ndb.Return(
                subscription,
                [issue for issue in results if issue]
            )

    def get(self):
        user_key = users.user_key(self.user)
        query = subscriptions.Subscription.query(ancestor=user_key)
        subs = [sub for sub in query.map(self.find_new_issues) if sub]
        new_issues = []
        for subscription, unread in subs:
            volume = subscription.volume.get()
            volume_dict = model_to_dict(volume)
            for issue in unread:
                new_issues.append({
                    'volume': volume_dict,
                    'issue': model_to_dict(issue),
                })
        result = {
            'status': 200,
            'results': new_issues,
        }
        self.response.write(json.dumps(result))

class UnreadIssues(OauthHandler):
    @ndb.tasklet
    def fetch_issue_data(self, pull):
        if pull.volume:
            volume_key = pull.volume
        else:
            # TODO(rgh): Remove when data migrated
            volume_key = ndb.Key(volumes.Volume, pull.key.parent().id())
        issue_key = pull.issue
        volume, issue = yield volume_key.get_async(), issue_key.get_async()
        raise ndb.Return({
            'pull': model_to_dict(pull),
            'issue': model_to_dict(issue),
            'volume': model_to_dict(volume),
        })

    def get(self):
        user_key = users.user_key(self.user)
        query = pulls.Pull.query(ancestor=user_key).filter(
            pulls.Pull.read == False)
        unread_pulls = query.map(self.fetch_issue_data)
        result = {
            'status': 200,
            'results': unread_pulls,
        }
        self.response.write(json.dumps(result))

class UpdatePulls(OauthHandler):
    def post(self):
        user_key = users.user_key(self.user)
        request = json.loads(self.request.body)
        issue_ids = request.get('read', []) + request.get('unread', [])
        results = defaultdict(list)
        query = issues.Issue.query(issues.Issue.identifier.IN(
            [int(identifier) for identifier in issue_ids]))
        records = query.fetch()
        issue_dict = {record.key.id(): record for record in records}
        candidates = []
        for issue_id in issue_ids:
            issue = issue_dict.get(issue_id)
            if issue:
                pull_key = pulls.pull_key(issue_id)
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
                if pull.issue.id() in request.get('read', []):
                    if pull.read:
                        results['skipped'].append(pull_key.id())
                    else:
                        results['updated'].append(pull_key.id())
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
    Route('/api/pulls/get/<identifier>', GetPull),
    Route('/api/pulls/list', ListPulls),
    Route('/api/pulls/new', NewIssues),
    Route('/api/pulls/unread', UnreadIssues),
    Route('/api/pulls/update', UpdatePulls),
])
