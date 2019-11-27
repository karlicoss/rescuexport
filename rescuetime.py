#!/usr/bin/env python3
from datetime import datetime, timedelta
import logging
import sys

import requests
import backoff # type: ignore

from rescuetime_secrets import KEY

from kython.klogging import setup_logzero
from kython.klogging import LazyLogger


logger = LazyLogger('rescuetime-backup')


class BackoffMe(Exception):
    pass


@backoff.on_exception(backoff.expo, BackoffMe, max_tries=5)
def run():
    today = datetime.today()
    # minute intervals are only allowed with up to a month timespan
    beg = (today - timedelta(days=30)).strftime('%Y-%m-%d')
    end = today.strftime('%Y-%m-%d')
    res = requests.get(
        "https://www.rescuetime.com/anapi/data",
        dict(
            key=KEY,
            format='json',
            perspective='interval',
            interval='minute',
            restrict_begin=beg,
            restrict_end=end,
        )
    )

    if res.status_code == 200:
        sys.stdout.buffer.write(res.content)
    else:
        logger.error(f"Bad status code {res} while requesting {res.request.url}")
        logger.error(res.content.decode('utf8'))
        raise BackoffMe


def main():
    setup_logzero(logger, level=logging.DEBUG)
    run()


if __name__ == '__main__':
    main()
