'API endpoints for stream management'
from collections import defaultdict
from functools import partial
import json
import logging

from google.appengine.ext import ndb

# pylint: disable=F0401

from pulldb.base import create_app, OauthHandler, Route
from pulldb.models.base import model_to_dict
from pulldb.models import issues
from pulldb.models import publishers
from pulldb.models import pulls
from pulldb.models import streams
from pulldb.models import users
from pulldb.models import volumes

# pylint: disable=W0232,E1101,R0903,R0201,C0103

@ndb.tasklet
def stream_context(stream, context=False):
    issue_list = []
    volume_list = []
    publisher__list = []
    if context:
        issues, volumes, publishers = yield (
            ndb.get_multi_async(stream.issues or []),
            ndb.get_multi_async(stream.volumes or []),
            ndb.get_multi_async(stream.publishers or []),
        )
        issue_list = [model_to_dict(issue) for issue in issues]
        volume_list = [model_to_dict(volume) for volume in volumes]
        publishers_list = [
            model_to_dict(publisher) for publisher in publishers]
    else:
        issue_list = [key.id() for key in stream.issues]
        volume_list = [key.id() for key in stream.issues]
        publisher_list = [key.id() for key in stream.issues]
    raise ndb.Return({
        'stream': model_to_dict(stream),
        'issues': issue_list,
        'volumes': volume_list,
        'publishers': publisher_list,
    })

class AddStreams(OauthHandler):
    def post(self):
        user_key = users.user_key(self.user)
        request = json.loads(self.request.body)
        new_stream_list = request['streams']
        results = defaultdict(list)
        query = streams.Stream.query(ancestor=user_key)
        user_streams = query.fetch()
        stream_names = [stream.name for stream in user_streams]
        candidates = []
        for stream_id in new_stream_list:
            if stream_id not in user_streams:
                new_stream = streams.stream_key(
                    stream_id, user_key=user_key, create=True, batch=True
                )
                results['successful'].append(stream_id)
                candidates.append(new_stream)
            else:
                results['skipped'].append(stream_id)
        ndb.put_multi(candidates)
        response = {
            'status': 200,
            'results': results
        }
        self.response.write(json.dumps(response))

class GetStream(OauthHandler):
    def get(self, identifier):
        user_key = users.user_key(self.user)
        query = streams.Stream.query(
            streams.Stream.name == identifier,
            ancestor=user_key,
        )
        context_callback = partial(
            stream_context, context=self.request.get('context'))
        results = query.map(context_callback)
        if results:
            status = 200
            message = 'Stream %s found' % identifier,
        else:
            status = 404
            message = 'Stream %s not found' % identifier
        self.response.write(json.dumps({
            'status': status,
            'message': message,
            'results': results,
        }))

class ListStreams(OauthHandler):
    def get(self):
        user_key = users.user_key(self.user)
        query = streams.Stream.query(ancestor=user_key)
        context_callback = partial(
            stream_context, context=self.request.get('context'))
        results = query.map(context_callback)
        self.response.write(json.dumps({
            'status': 200,
            'results': results,
        }))

class RefreshStream(OauthHandler):
    def get(self, identifier):
        results = []
        user_key = users.user_key(self.user)
        stream_key = streams.stream_key(
            identifier, user_key=user_key, create=False)
        stream = stream.get()
        if stream:
            query = pull.Pulls.query(
                pulls.Pull.ignored == False,
                pulls.Pull.pulled == True,
                pulls.Pull.read == False,
                pulls.Pull.stream == stream_key,
            )
            pulls = query.fetch(keys_only=True)
            stream.length = len(pulls)
            stream.put()
            status = 200
            message = 'Stream %s updated' % identifier,
            results.append(model_to_dict(stream))
        else:
            status = 404
            message = 'Stream %s not found' % identifier
        self.response.write({
            'status': status,
            'message': message,
            'results': results,
        })

class UpdateStreams(OauthHandler):
    def update_publishers(self, stream, updates):
        for publisher_id in updates.get('add', []):
            update_string = 'stream/%s/publisher/%s/add' % (
                stream.name, publisher_id)
            publisher_key = publishers.publisher_key(
                publisher_id, create=False)
            if publisher_key in stream.publishers:
                self.results['skipped'].append(update_string)
            else:
                stream.publishers.append(publisher_key)
                self.updated.append(stream)
                self.results['successful'].append(update_string)
        for publisher_id in updates.get('delete', []):
            update_string = 'stream/%s/publisher/%s/del' % (
                stream.name, publisher_id)
            publisher_key = publishers.publisher_key(
                publisher_id, create=False)
            if publisher_key not in stream.publishers:
                self.results['skipped'].append(update_string)
            else:
                stream.publishers.remove(publisher_key)
                self.updated.append(stream)
                self.results['successful'].append(update_string)

    def update_volumes(self, stream, updates):
        for volume_id in updates.get('add', []):
            update_string = 'stream/%s/volume/%s/add' % (
                stream.name, volume_id)
            volume_key = volumes.volume_key(
                volume_id, create=False)
            if volume_key in stream.volumes:
                self.results['skipped'].append(update_string)
            else:
                stream.volumes.append(volume_key)
                self.updated.append(stream)
                self.results['successful'].append(update_string)
        for volume_id in updates.get('delete', []):
            update_string = 'stream/%s/volume/%s/del' % (
                stream.name, volume_id)
            volume_key = volumes.volume_key(
                volume_id, create=False)
            if volume_key not in stream.volumes:
                self.results['skipped'].append(update_string)
            else:
                stream.volumes.remove(volume_key)
                self.updated.append(stream)
                self.results['successful'].append(update_string)

    def update_issues(self, stream, updates):
        for issue_id in updates.get('add', []):
            update_string = 'stream/%s/issue/%s/add' % (
                stream.name, issue_id)
            issue_key = issues.issue_key(
                issue_id, create=False)
            if issue_key in stream.issues:
                self.results['skipped'].append(update_string)
            else:
                stream.issues.append(issue_key)
                self.updated.append(stream)
                self.results['successful'].append(update_string)
        for issue_id in updates.get('delete', []):
            update_string = 'stream/%s/issue/%s/del' % (
                stream.name, issue_id)
            issue_key = issues.issue_key(
                issue_id, create=False)
            if issue_key not in stream.issues:
                self.results['skipped'].append(update_string)
            else:
                stream.issues.remove(publisher_key)
                self.updated.append(stream)
                self.results['successful'].append(update_string)

    def post(self):
        self.results = defaultdict(list)
        self.updated = []
        user_key = users.user_key(self.user)
        request = json.loads(self.request.body)
        for stream_updates in request:
            stream = streams.stream_key(
                stream_updates['name'],
                user_key = user_key,
                create=False,
            ).get()
            if not stream:
                self.results['failed'].append(stream_updates['name'])
                continue
            if stream_updates.get('publishers'):
                self.update_publishers(stream, stream_updates['publishers'])
            if stream_updates.get('volumes'):
                self.update_volumes(stream, stream_updates['volumes'])
            if stream_updates.get('issues'):
                self.update_issues(stream, stream_updates['issues'])
        if self.updated:
            ndb.put_multi(self.updated)
            status = 200
            message = '%d stream changes' % len(self.updated)
        else:
            status = 203
            message = 'no changes'
        self.response.write(json.dumps({
            'status': status,
            'message': message,
            'results': self.results,
        }))

app = create_app([
    Route('/api/streams/add', AddStreams),
    Route('/api/streams/<identifier>/get', GetStream),
    Route('/api/streams/<identifier>/refresh', RefreshStream),
    Route('/api/streams/list', ListStreams),
    Route('/api/streams/update', UpdateStreams),
])
