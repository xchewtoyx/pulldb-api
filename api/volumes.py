'Api endpoints for working with volumes'
from collections import defaultdict
import json
import logging

from google.appengine.api import search
from google.appengine.ext import ndb

# pylint: disable=F0401
from pulldb.base import create_app, Route, OauthHandler
from pulldb.models.base import model_to_dict
from pulldb.models import comicvine
from pulldb.models import issues
from pulldb.models import users
from pulldb.models import volumes

# pylint: disable=W0232,E1101,R0903,C0103

class AddVolumes(OauthHandler):
    def post(self):
        cv = comicvine.load()
        request = json.loads(self.request.body)
        volume_ids = request['volumes']
        results = defaultdict(list)
        keys = [
            volumes.volume_key(
                volume_id, create=False
            ) for volume_id in volume_ids
        ]
        ndb.get_multi(keys)
        candidates = []
        for key in keys:
            volume = key.get()
            if volume:
                results['existing'].append(key.id())
            else:
                candidates.append(int(key.id()))
        cv_volumes = cv.fetch_volume_batch(candidates)
        for cv_volume in cv_volumes:
            key = volumes.volume_key(cv_volume)
            if key.get():
                results['added'].append(key.id())
            else:
                results['failed'].append(key.id())
        response = {
            'status': 200,
            'results': results
        }
        self.response.write(json.dumps(response))

class DropIndex(OauthHandler):
    def get(self, doc_id):
        user = users.user_key(app_user=self.user).get()
        if not user.trusted:
            logging.warn('Untrusted access attempt: %r', self.user)
            self.abort(401)
        index = search.Index(name='volumes')
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

class GetVolume(OauthHandler):
    @ndb.tasklet
    def volume_context(self, volume):
        publisher_dict = {}
        if self.request.get('context'):
            publisher = yield volume.publisher.get_async()
            publisher_dict = model_to_dict(publisher)
        raise ndb.Return({
            'volume': model_to_dict(volume),
            'publisher': publisher_dict,
        })

    def get(self, identifier):
        query = volumes.Volume.query(
            volumes.Volume.identifier == int(identifier)
        )
        volumes = query.map(self.volume_context)
        if volumes:
            status = 200
            message = '%d matching volumes found' % len(volumes)
        else:
            status = 404
            message = 'no matching volume found'
        self.response.write(json.dumps({
            'status': status,
            'message': message,
            'results': volumes
        }))

class Issues(OauthHandler):
    def get(self, identifier):
        volume = volumes.volume_key(identifier, create=False).get()
        if volume:
            query = issues.Issue.query(
                ancestor=volume.key
            ).order(issues.Issue.pubdate)
            logging.debug('Looking for issues: %r', query)
            results = query.fetch()
            logging.debug('Query returned %d results', len(results))
            response = {
                'status': 200,
                'message': 'Found %d issues' % len(results),
                'volume': model_to_dict(volume),
                'results': [model_to_dict(issue) for issue in results],
            }
        else:
            logging.info('Volume %s not foune', identifier)
            response = {
                'status': 404,
                'message': 'Volume %s not found' % identifier,
                'results': [],
            }
        self.response.write(json.dumps(response))

class Reindex(OauthHandler):
    def get(self, identifier):
        user = users.user_key(app_user=self.user).get()
        if not user.trusted:
            logging.warn('Untrusted access attempt: %r', self.user)
            self.abort(401)
        volume_key = volumes.volume_key(identifier, create=False)
        volume = volume_key.get()
        if volume:
            volumes.index_volume(volume_key, volume)
            response = {
                'status': 200,
                'message': 'Volume %s reindexed' % identifier,
            }
        else:
            response = {
                'status': 404,
                'message': 'Volume %s not found' % identifier,
            }
        self.response.write(json.dumps(response))

class SearchVolumes(OauthHandler):
    def get(self):
        index = search.Index(name='volumes')
        results = []
        try:
            matches = index.search(self.request.get('q'))
            logging.debug('results: found %d matches', matches.number_found)
            for volume in matches.results:
                result = {
                    'id': volume.doc_id,
                    'rank': volume.rank,
                }
                for field in volume.fields:
                    result[field.name] = unicode(field.value)
                results.append(result)
        except search.Error as e:
            logging.exception(e)
        self.response.write(json.dumps({
            'status': 200,
            'count': matches.number_found,
            'results': results,
        }))

app = create_app([
    Route('/api/volumes/add', AddVolumes),
    Route('/api/volumes/<identifier>/get', GetVolume),
    Route('/api/volumes/<identifier>/list', Issues),
    Route('/api/volumes/<identifier>/reindex', Reindex),
    Route('/api/volumes/index/<doc_id>/drop', DropIndex),
    Route('/api/volumes/search', SearchVolumes),
])
