'api calls for issue resources'
from functools import partial
import json
import logging

from google.appengine.api import search

# pylint: disable=F0401
from pulldb.base import create_app, Route, OauthHandler
from pulldb.models.base import model_to_json
from pulldb.models.issues import Issue, refresh_issue_volume
from pulldb.models import comicvine
from pulldb.models.volumes import Volume

# pylint: disable=W0232,E1101,R0903,C0103

class GetIssue(OauthHandler):
    def get(self, identifier):
        query = Issue.query(Issue.identifier == int(identifier))
        results = query.fetch()
        self.response.write(model_to_json(list(results)))

class RefreshVolume(OauthHandler):
    def get(self, volume):
        cv = comicvine.load()
        refresh_callback = partial(refresh_issue_volume, comicvine=cv)
        query = Volume.query(Volume.identifier == int(volume))
        volume_keys = query.map(refresh_callback)
        volume_count = sum([1 for volume in volume_keys if volume])
        issue_count = sum([len(volume) for volume in volume_keys if volume])
        status = 'Updated %d issues in %d/%d volumes' % (
            issue_count, volume_count, len(volume_keys))
        logging.info(status)
        self.response.write(json.dumps({
            'status': 200,
            'message': status,
        }))

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
                result[field.name] = str(field.value)
            results.append(result)
        self.response.write(json.dumps({
            'status': 200,
            'count': issues.number_found,
            'results': results,
        }))

app = create_app([
    Route('/api/issues/get/<identifier>', GetIssue),
    Route('/api/issues/refresh/<volume>', RefreshVolume),
    Route('/api/issues/search', SearchIssues),
])
