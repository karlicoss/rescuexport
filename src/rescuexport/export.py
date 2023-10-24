#!/usr/bin/env python3
from datetime import datetime, timedelta
import json

import backoff
import requests

from .exporthelpers.export_helper import setup_parser, Parser
from .exporthelpers.logging_helper import make_logger


logger = make_logger(__name__)


class BackoffMe(Exception):
    pass


@backoff.on_exception(backoff.expo, BackoffMe, max_tries=5)
def run(key: str):
    today = datetime.today()
    # minute intervals are only allowed with up to a month timespan
    beg = (today - timedelta(days=30)).strftime('%Y-%m-%d')
    end = today.strftime('%Y-%m-%d')
    res = requests.get(
        "https://www.rescuetime.com/anapi/data",
        dict(
            key=key,
            format='json',
            perspective='interval',
            interval='minute',
            restrict_begin=beg,
            restrict_end=end,
        ),
    )

    if res.status_code == 200:
        return res.json()
    else:
        logger.error(f"Bad status code {res} while requesting {res.request.url}")
        logger.error(res.content.decode('utf8'))
        raise BackoffMe


def get_json(**params):
    return run(**params)


def main():
    parser = make_parser()
    args = parser.parse_args()

    params = args.params
    dumper = args.dumper

    j = get_json(**params)
    js = json.dumps(j, ensure_ascii=False, indent=1)
    dumper(js)


def make_parser():
    parser = Parser("Tool to export your personal Rescuetime data")
    setup_parser(parser=parser, params=['key'])
    return parser


if __name__ == '__main__':
    main()
