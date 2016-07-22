'Api endpoints for working with volumes'
# pylint: disable=missing-docstring, no-member
from collections import defaultdict
import json
import logging
import re

from google.appengine.api import search
from google.appengine.ext import ndb
from google.appengine.ext.ndb.query import Cursor

from pulldb.base import create_app, Route, OauthHandler
from pulldb.models.base import model_to_dict
from pulldb.models import comicvine
from pulldb.models import issues
from pulldb.models import users
from pulldb.models import volumes

class AddVolumes(OauthHandler):
    def post(self):
        cv_api = comicvine.load()
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
        cv_volumes = cv_api.fetch_volume_batch(candidates)
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
        volume_list = query.map(self.volume_context)
        if volumes:
            status = 200
            message = '%d matching volumes found' % len(volume_list)
        else:
            status = 404
            message = 'no matching volume found'
        self.response.write(json.dumps({
            'status': status,
            'message': message,
            'results': volume_list,
        }))

class Issues(OauthHandler):
    def get(self, identifier):
        volume = volumes.volume_key(identifier, create=False).get()
        if volume:
            query = issues.Issue.query(
                issues.Issue.volume == volume.key
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
            logging.info('Volume %s not found', identifier)
            response = {
                'status': 404,
                'message': 'Volume %s not found' % identifier,
                'results': [],
            }
        self.response.write(json.dumps(response))


class ListVolumes(OauthHandler):
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

    @ndb.tasklet
    def fetch_page(self, query):
        limit = self.request.get('limit', 10)
        cursor = Cursor(urlsafe=self.request.get('position'))
        volume_matches, next_cursor, more = yield query.fetch_page_async(
            limit, start_cursor=cursor)
        context_futures = [
            self.volume_context(volume) for volume in volume_matches]
        results = yield context_futures
        raise ndb.Return(
            results,
            next_cursor,
            more,
        )

    def get(self, volume_type='all'):
        query = volumes.Volume.query()
        if volume_type == 'queued':
            query = query.filter(volumes.Volume.complete == False)
        page_future = self.fetch_page(query)
        results, next_cursor, more = page_future.get_result()
        response = {
            'status': 200,
            'count': len(results),
            'next': next_cursor.urlsafe,
            'more': more,
            'results': results,
        }
        self.response.write(json.dumps(response))


class QueueVolume(OauthHandler):
    'Manually queue a volume for updates.'
    def get(self, identifier):
        user = users.user_key(app_user=self.user).get()
        if not user.trusted:
            logging.warn('Untrusted access attempt: %r', self.user)
            self.abort(401)
        volume_key = volumes.volume_key(identifier, create=False)
        volume = volume_key.get()
        if volume:
            volume.complete = False
            volume.put()
            response = {
                'status': 200,
                'message': 'Volume %s queued' % identifier,
            }
        else:
            response = {
                'status': 404,
                'message': 'Volume %s not found' % identifier,
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
            volume.index_document()
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

class SearchComicvine(OauthHandler):
    def __init__(self, *args, **kwargs):
        super(SearchComicvine, self).__init__(*args, **kwargs)
        self.cv_api = comicvine.load()

    #TODO(rgh): paged results are broken.  Need to fix.
    def get(self):
        # pylint: disable=too-many-locals
        query = self.request.get('q')
        volume_ids = self.request.get('volume_ids')
        page = int(self.request.get('page', 0))
        limit = int(self.request.get('limit', 20))
        offset = page * limit
        if volume_ids:
            volume_ids = [
                int(identifier) for identifier in re.findall(
                    r'(\d+)', volume_ids)
            ]
            logging.debug('Found volume ids: %r', volumes)
            results = []
            for index in range(0, len(volume_ids), 100):
                volume_page = volume_ids[
                    index:min([index+100, len(volume_ids)])]
                results.extend(self.cv_api.fetch_volume_batch(volume_page))
            results_count = len(results)
            logging.debug('Found volumes: %r', results)
        elif query:
            results_count, results = self.cv_api.search_volume(
                query, page=page, limit=limit)
            logging.debug('Found volumes: %r', results)
        if offset + limit > results_count:
            page_end = results_count
        else:
            page_end = offset + limit
        logging.info('Retrieving results %d-%d / %d', offset, page_end,
                     results_count)
        results_page = results[offset:page_end]

        self.response.write(json.dumps({
            'status': 200,
            'count': results_count,
            'results': results_page,
        }))

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
        except search.Error as err:
            logging.exception(err)
        self.response.write(json.dumps({
            'status': 200,
            'count': matches.number_found,
            'results': results,
        }))


class VolumeStats(OauthHandler):
    def get(self):
        include_total = False
        if self.request.get('all'):
            include_total = True
            total_count = volumes.Volume.query().count_async()
        queued_count = volumes.Volume.query(
            volumes.Volume.complete == False).count_async()
        toindex_count = volumes.Volume.query(
            volumes.Volume.indexed == False).count_async()
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


#pylint: disable=invalid-name
app = create_app([
    Route('/api/volumes/add', AddVolumes),
    Route('/api/volumes/<identifier>/get', GetVolume),
    Route('/api/volumes/<identifier>/list', Issues),
    Route('/api/volumes/<identifier>/queue', QueueVolume),
    Route('/api/volumes/<identifier>/reindex', Reindex),
    Route('/api/volumes/index/<doc_id>/drop', DropIndex),
    Route('/api/volumes/list/<type>', ListVolumes),
    Route('/api/volumes/search/comicvine', SearchComicvine),
    Route('/api/volumes/search', SearchVolumes),
    Route('/api/volumes/stats', VolumeStats),
])
