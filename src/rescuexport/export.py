import json
import logging
from datetime import datetime, timedelta

import requests
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from .exporthelpers.export_helper import Json, Parser, setup_parser
from .exporthelpers.logging_helper import make_logger

logger = make_logger(__name__)


class RetryMe(Exception):
    pass


@retry(
    retry=retry_if_exception_type(RetryMe),
    wait=wait_exponential(max=10),
    stop=stop_after_attempt(5),
    before_sleep=before_sleep_log(logger, logging.INFO),
)
def run(key: str) -> Json:
    today = datetime.today()
    # minute intervals are only allowed with up to a month timespan
    beg = (today - timedelta(days=30)).strftime('%Y-%m-%d')
    end = today.strftime('%Y-%m-%d')
    res = requests.get(
        "https://www.rescuetime.com/anapi/data",
        {
            'key': key,
            'format': 'json',
            'perspective': 'interval',
            'interval': 'minute',
            'restrict_begin': beg,
            'restrict_end': end,
        },
    )

    if res.status_code == 200:
        return res.json()
    else:
        logger.error(f"Bad status code {res} while requesting {res.request.url}")
        logger.error(res.content.decode('utf8'))
        raise RetryMe


def get_json(**params) -> Json:
    return run(**params)


def main() -> None:
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
