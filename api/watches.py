# pylint: disable=missing-docstring
from collections import defaultdict
import json
import logging

from dateutil.parser import parse as parse_date

from google.appengine.datastore.datastore_query import Cursor
from google.appengine.ext import ndb

from pulldb.base import create_app, Route, OauthHandler
from pulldb.models.base import model_to_dict
from pulldb.models import arcs
from pulldb.models import pulls
from pulldb.models import subscriptions
from pulldb.models import users
from pulldb.models import volumes


class AddWatches(OauthHandler):
    def __init__(self, *args, **kwargs):
        super(AddWatches, self).__init__(*args, **kwargs)
        self.results = defaultdict(list)
        self.user_key = None

    @ndb.tasklet
    def create_watch(self, collection_key):
        collection = yield collection_key.get_async()
        if not collection:
            self.results['failed'].append(repr(collection_key))
            raise ndb.Return(None)

        watch_query = subscriptions.WatchList.query(
            subscriptions.WatchList.collection == collection_key,
            subscriptions.WatchList.user == self.user_key,
        )
        watch = yield watch_query.get_async()
        if watch:
            self.results['skipped'].append(repr(collection_key))
            raise ndb.Return(None)

        watch_key = yield subscriptions.watch_key(
            collection_key, user=self.user_key, create=True, batch=True)
        self.results['added'].append(repr(collection_key))
        raise ndb.Return(watch_key)

    @ndb.toplevel
    def post(self):
        self.user_key = users.user_key(self.user, create=False)
        request = json.loads(self.request.body)
        watches = []
        volume_ids = request.get('volumes', [])
        if volume_ids:
            watches.extend(volume_keys(volume_ids))
        arc_ids = request.get('arcs', [])
        if arc_ids:
            watches.extend(arc_keys(arc_ids))
        watch_keys = [self.create_watch(collection) for collection in watches]
        added = sum(1 for key in watch_keys if key.get_result())
        response = {
            'status': 200,
            'message': 'added %d watches' % (added,),
            'results': self.results,
        }
        self.response.write(json.dumps(response))


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

    def query(self, watch, pull_type):
        sortkey = pulls.Pull.pubdate
        if self.request.get('weighted'):
            sortkey = pulls.Pull.weight
        if self.request.get('reverse'):
            sortkey = -sortkey
        base_query = pulls.Pull.query(
            pulls.Pull.collection == watch.collection,
            ancestor=self.user_key,
        )
        query_method = getattr(self, 'query_' + pull_type)
        return query_method(base_query).order(sortkey)

    def query_all(self, base_query):
        return base_query

    def query_ignored(self, base_query):
        return base_query.filter(
            pulls.Pull.ignored == True,
        )

    def query_new(self, base_query):
        if not self.request.get('all'):
            base_query = base_query.filter(
                pulls.Pull.ignored == False,
            )
        base_query = base_query.filter(
            pulls.Pull.pulled == False,
        )
        return base_query

    def query_unread(self, base_query):
        return pulls.Pull.query(
            pulls.Pull.pulled == True,
            pulls.Pull.ignored == False,
            pulls.Pull.read == False,
            ancestor=self.user_key
        )

    def get(self, identifier, pull_type=None):
        self.user_key = users.user_key(self.user)
        watch_key = ndb.Key(urlsafe=identifier)
        watch = watch_key.get()
        query = self.query(watch, pull_type)
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


class ListWatches(OauthHandler):
    @ndb.tasklet
    def watch_context(self, watch):
        collection_dict = {}
        if self.request.get('context'):
            collection = yield watch.collection.get_async()
            collection_dict = model_to_dict(collection)
        raise ndb.Return({
            'watch': model_to_dict(watch),
            'collection': collection_dict,
        })

    def get(self):
        user_key = users.user_key(self.user, create=False)
        query = subscriptions.WatchList.query(
            subscriptions.WatchList.user == user_key)
        results = query.map(self.watch_context)
        response = {
            'status': 200,
            'count': len(results),
            'results': results,
        }
        self.response.write(json.dumps(response))

class RemoveWatches(OauthHandler):
    def __init__(self, *args, **kwargs):
        super(RemoveWatches, self).__init__(*args, **kwargs)
        self.results = defaultdict(list)
        self.user_key = None

    @ndb.tasklet
    def drop_watch(self, collection):
        query = subscriptions.WatchList.query(
            subscriptions.WatchList.collection == collection,
            subscriptions.WatchList.user == self.user_key,
        )
        watch = yield query.get_async()
        if watch:
            logging.info('Removing watch on: %r', collection)
            self.results['removed'].append(repr(collection))
            yield watch.key.delete_async()
            raise ndb.Return(True)
        else:
            logging.info('Skipping remove. %r is not watched by %r',
                         collection, self.user_key)
            self.results['skipped'].append(repr(collection))

    @ndb.toplevel
    def post(self):
        self.user_key = users.user_key(self.user, create=False)
        request = json.loads(self.request.body)
        watches = []
        if request.get('volumes'):
            watches.extend(volume_keys(request['volumes']))
        if request.get('arcs'):
            watches.extend(arc_keys(request['arcs']))

        results = [self.drop_watch(collection) for collection in watches]
        removed = sum(1 for dropped in results if dropped.get_result())

        response = {
            'status': 200,
            'message': 'removed %d subscriptions' % (removed,),
            'results': self.results,
        }
        self.response.write(json.dumps(response))


class UpdateWatches(OauthHandler):
    def __init__(self, *args, **kwargs):
        super(UpdateWatches, self).__init__(self, *args, **kwargs)
        self.results = defaultdict(list)
        self.user_key = None

    @ndb.tasklet
    def update_watch(self, collection, start_date):
        query = subscriptions.WatchList.query(
            subscriptions.WatchList.collection == collection,
            subscriptions.WatchList.user == self.user_key,
        )
        watch = yield query.get_async()
        if watch:
            logging.info('Updating watch on %r for %r',
                         collection, self.user_key)
            new_start = parse_date(start_date).date()
            if watch.start_date == new_start:
                self.results['skipped'].append(repr(collection))
            else:
                logging.info('watch on %r for %r now starts at %r',
                             collection, self.user_key, new_start)
                watch.start_date = new_start
                self.results['updated'].append(repr(collection))
                yield watch.put_async()
                raise ndb.Return(True)
        else:
            self.results['failed'].append(repr(collection))

    def post(self):
        self.user_key = users.user_key(self.user, create=False)
        request = json.loads(self.request.body)
        watches = []
        if request.get('volumes'):
            volume_list = request['volumes'].keys()
            key_list = volume_keys(volume_list)
            for key, volume in zip(key_list, volume_list):
                watches.append((key, request['volumes'][volume]))
        if request.get('arcs'):
            arc_list = request['arcs'].keys()
            key_list = arc_keys(arc_list)
            for key, arc in zip(key_list, arc_list):
                watches.append((key, request['arcs'][arc]))

        results = [
            self.update_watch(
                collection, start_date
            ) for collection, start_date in watches
        ]
        updated = sum(1 for updated in results if updated.get_result())

        response = {
            'status': 200,
            'message': 'updated %d subscriptions' % (updated,),
            'results': self.results,
        }
        self.response.write(json.dumps(response))


def arc_keys(arc_ids):
    logging.info('Checking arcs: %r', arc_ids)
    keys = [arcs.arc_key(
        arc_id, create=False
    ) for arc_id in arc_ids]

    return keys

def volume_keys(volume_ids):
    logging.info('Checking volumes: %r', volume_ids)
    keys = [volumes.volume_key(
        volume_id, create=False
    ) for volume_id in volume_ids]

    return keys


app = create_app([ # pylint: disable=invalid-name
    Route('/api/watches/add', AddWatches),
    Route('/api/watches/list', ListWatches),
    Route('/api/watches/<identifier>/pulls/'
          '<pull_type:(all|new|unread|ignored)>', ListPulls),
    Route('/api/watches/remove', RemoveWatches),
    Route('/api/watches/update', UpdateWatches),
])
