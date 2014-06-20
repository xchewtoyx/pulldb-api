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
from pulldb.models import volumes

# pylint: disable=W0232,E1101,R0903,C0103

class AddVolumes(OauthHandler):
    def post(self):
        cv = comicvine.load()
        request = json.loads(self.request.body)
        volume_ids = request['volumes']
        results = defaultdict(list)
        keys = [
            ndb.Key(
                volumes.Volume, volume_id
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

class GetVolume(OauthHandler):
    def get(self, identifier):
        volume_key = ndb.Key(volumes.Volume, identifier)
        volume = volume_key.get()
        if volume:
            publisher = volume.publisher.get()
            volume_dict = volume.to_dict()
            publisher_dict = publisher.to_dict()
            response = {
                'status': 200,
                'message': 'matching volume found',
                'volume': {
                    key: unicode(value) for key, value in volume_dict.items()
                },
                'publisher': {
                    key: unicode(value) for key, value in publisher_dict.items()
                }
            }
        else:
            response = {
                'status': 404,
                'message': 'no matching volume found',
            }
        self.response.write(json.dumps(response))

class Issues(OauthHandler):
    def get(self, identifier):
        volume = ndb.Key(volumes.Volume, identifier).get()
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
    Route('/api/volumes/search', SearchVolumes),
])
