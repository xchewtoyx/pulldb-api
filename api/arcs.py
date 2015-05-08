'Api endpoints for working with volumes'
# pylint: disable=missing-docstring,no-init
from collections import defaultdict
import json
import logging
import re

from google.appengine.api import search
from google.appengine.ext import ndb

from pulldb.base import create_app, Route, OauthHandler
from pulldb.models.base import model_to_dict
from pulldb.models import arcs
from pulldb.models import comicvine
from pulldb.models import issues
from pulldb.models import publishers
from pulldb.models import users

class AddArcs(OauthHandler):
    @ndb.toplevel
    def post(self):
        api = comicvine.load()
        request = json.loads(self.request.body)
        arc_ids = request['arcs']
        results = defaultdict(list)
        keys = [
            arcs.arc_key(
                arc_id, create=False
            ) for arc_id in arc_ids
        ]
        ndb.get_multi(keys)
        candidates = []
        for arc_id, key in zip(arc_ids, keys):
            arc = key.get()
            if arc:
                results['existing'].append(arc_id)
            else:
                candidates.append(arc_id)
        cv_arcs = api.fetch_story_arc_batch(candidates)
        for cv_arc in cv_arcs:
            key = arcs.arc_key(cv_arc, create=True, batch=True)
            if key.get():
                results['added'].append(cv_arc['id'])
            else:
                results['failed'].append(cv_arc['id'])
        response = {
            'status': 200,
            'results': results
        }
        self.response.write(json.dumps(response))


class ArcStats(OauthHandler):
    def get(self):
        include_total = False
        if self.request.get('all'):
            include_total = True
            total_count = arcs.StoryArc.query().count_async()
        queued_count = arcs.StoryArc.query(
            arcs.StoryArc.complete == False).count_async()
        toindex_count = arcs.StoryArc.query(
            arcs.StoryArc.indexed == False).count_async()
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


class DropIndex(OauthHandler):
    def get(self, doc_id):
        user = users.user_key(app_user=self.user).get()
        if not user.trusted:
            logging.warn('Untrusted access attempt: %r', self.user)
            self.abort(401)
        index = search.Index(name='arcs')
        try:
            index.delete(doc_id)
        except search.Error as error:
            response = {
                'status': 500,
                'message': 'Error dropping document %s' % (doc_id,)
            }
            logging.error(response['message'])
            logging.exception(error)
        else:
            response = {
                'status': 200,
                'message': 'Document %s dropped' % doc_id,
            }
        self.response.write(json.dumps(response))

class GetArc(OauthHandler):
    @ndb.tasklet
    def arc_context(self, arc):
        publisher = publishers.Publisher()
        if self.request.get('context'):
            publisher = yield arc.publisher.get_async()

        raise ndb.Return({
            'arc': model_to_dict(arc),
            'publisher': model_to_dict(publisher),
        })

    def get(self, identifier):
        query = arcs.StoryArc.query(
            arcs.StoryArc.identifier == int(identifier)
        )
        arc_list = query.map(self.arc_context)
        if arc_list:
            status = 200
            message = '%d matching volumes found' % len(arc_list)
        else:
            status = 404
            message = 'no matching volume found'
        self.response.write(json.dumps({
            'status': status,
            'message': message,
            'results': arc_list,
        }))

class ListIssues(OauthHandler):
    def get(self, identifier):
        arc = arcs.arc_key(identifier, create=False).get()
        if arc:
            query = issues.Issue.query(
                issues.Issue.collection == arc.key
            ).order(issues.Issue.pubdate)
            logging.debug('Looking for issues: %r', query)
            results = query.fetch()
            logging.debug('Query returned %d results', len(results))
            response = {
                'status': 200,
                'message': 'Found %d issues' % len(results),
                'arc': model_to_dict(arc),
                'results': [model_to_dict(issue) for issue in results],
            }
        else:
            logging.info('Arc %s not found', identifier)
            response = {
                'status': 404,
                'message': 'Arc %s not found' % identifier,
                'results': [],
            }
        self.response.write(json.dumps(response))

class Reindex(OauthHandler):
    def get(self, identifier):
        user = users.user_key(app_user=self.user).get()
        if not user.trusted:
            logging.warn('Untrusted access attempt: %r', self.user)
            self.abort(401)
        arc_key = arcs.arc_key(identifier, create=False)
        arc = arc_key.get()
        if arc:
            arc.index_document()
            response = {
                'status': 200,
                'message': 'Arc %s reindexed' % identifier,
            }
        else:
            response = {
                'status': 404,
                'message': 'Arc %s not found' % identifier,
            }
        self.response.write(json.dumps(response))


class SearchComicvine(OauthHandler):
    def fetch_ids(self, arc_ids):
        arc_ids = [
            int(identifier) for identifier in re.findall(
                r'(\d+)', arc_ids)
        ]
        logging.debug('Found arc ids: %r', arcs)
        results = []
        for index in range(0, len(arc_ids), 100):
            arc_page = arc_ids[
                index:min([index+100, len(arc_ids)])]
            results.extend(self.comicvine.fetch_story_arc_batch(arc_page))
        return results

    def fetch_query(self, query):
        return self.comicvine.fetch_story_arc_batch((query,),
                                                    filter_attr='name')

    @ndb.toplevel
    def get(self):
        # pylint: disable=attribute-defined-outside-init
        self.comicvine = comicvine.load()
        query = self.request.get('q')
        arc_ids = self.request.get('arc_ids')
        page = int(self.request.get('page', 0))
        limit = int(self.request.get('limit', 20))
        if arc_ids:
            results = self.fetch_ids(arc_ids)
            results_count = len(results)
            logging.debug('Found arcs: %r', results)
        elif query:
            #results_count, results = self.comicvine.search_story_arc(
            #    query, page=page, limit=limit)
            results = self.fetch_query(query)
            results_count = len(results)
            logging.debug('Found volumes: %r', results)
        for result in results:
            try:
                arc_id = arcs.arc_key(result, batch=True)
                logging.info('Found arc: %r', arc_id)
            except TypeError as error:
                logging.warn(
                    'Unable to lookup arc key for result %r (%r)',
                    result, error)

        self.response.write(json.dumps({
            'status': 200,
            'count': results_count,
            'results': results,
        }))

class SearchArcs(OauthHandler):
    def get(self):
        index = search.Index(name='arcs')
        results = []
        try:
            matches = index.search(self.request.get('q'))
            logging.debug('results: found %d matches', matches.number_found)
            for arc in matches.results:
                result = {
                    'id': arc.doc_id,
                    'rank': arc.rank,
                }
                for field in arc.fields:
                    result[field.name] = unicode(field.value)
                results.append(result)
        except search.Error as err:
            logging.exception(err)
        self.response.write(json.dumps({
            'status': 200,
            'count': matches.number_found,
            'results': results,
        }))

app = create_app([ # pylint: disable=invalid-name
    Route('/api/arcs/add', AddArcs),
    Route('/api/arcs/<identifier>/get', GetArc),
    Route('/api/arcs/<identifier>/list', ListIssues),
    Route('/api/arcs/<identifier>/reindex', Reindex),
    Route('/api/arcs/index/<doc_id>/drop', DropIndex),
    Route('/api/arcs/search/comicvine', SearchComicvine),
    Route('/api/arcs/search/local', SearchArcs),
    Route('/api/arcs/stats', ArcStats),
])
