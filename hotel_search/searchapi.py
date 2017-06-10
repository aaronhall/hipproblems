import simplejson as json
from tornado import gen, ioloop, web
from tornado.httpclient import AsyncHTTPClient
from hotel_search.scrapers import SCRAPERS


PROVIDER_BASE_URL = 'http://localhost:9000/scrapers/%s'
PROVIDERS = [getattr(s, 'provider').lower() for s in SCRAPERS]


class SearchApiHandler(web.RequestHandler):
    @gen.coroutine
    def get(self):
        """Get the results from all search providers in order of ecstasy descending.

        Asyncronously request results from each provider into a list of result sets (assume all
        responses are in order of ecstasy descending), then sort those result sets by their first
        items' ecstasy value in descending order.

        The main loop removes the first item of the first result set and appends it to the merged
        result set. The first result set is then moved down into its sorted position. This is
        repeated until all result sets are empty.
        """

        http_client = AsyncHTTPClient()

        # Call all providers in parallel
        # TODO: HTTP error handling
        result_sets = yield [http_client.fetch(PROVIDER_BASE_URL % p) for p in PROVIDERS]

        # JSON-decode and filter empty result sets
        result_sets = filter(lambda r: len(r), [json.loads(r.body)['results'] for r in result_sets])

        # Sort the result sets by their first result's ecstasy value in descending order
        result_sets.sort(key=lambda r: r[0]['ecstasy'], reverse=True)

        results = []
        while result_sets:
            # Append the first item of the first result set to the merged result set
            results.append(result_sets[0].pop(0))

            # Remove the first result set if it's empty
            if not result_sets[0]:
                result_sets.pop(0)

            # Move the first result set down to its sorted position
            r = result_sets
            cur, next = 0, 1
            while next < len(r) and r[cur][0]['ecstasy'] < r[next][0]['ecstasy']:
                r[next], r[cur] = r[cur], r[next]
                cur += 1
                next += 1

        self.write({
            "results": results,
        })


ROUTES = [
    (r"/hotels/search", SearchApiHandler),
]


def run():
    app = web.Application(
        ROUTES,
        debug=True,
    )

    app.listen(8000)
    print "Server (re)started. Listening on port 8000"

    ioloop.IOLoop.current().start()


if __name__ == "__main__":
    run()
