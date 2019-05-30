#!/usr/bin/env python3.6
from datetime import datetime
import sys
import time

import requests

from rescuetime_secrets import KEY

import logging
from kython.klogging import setup_logzero



def main():
    logger = logging.getLogger('rescuetime-backup')
    setup_logzero(logger, level=logging.DEBUG)
    today = datetime.today().strftime("%Y-%m-%d")

    exc = None
    for att in range(5, 0, -1):
        res = requests.get(
            "https://www.rescuetime.com/anapi/data",
            dict(
                key=KEY,
                format='json',
                perspective='interval',
                interval='minute',
                restrict_begin="2017-01-01",
                restrict_end=today, # TODO is this even necessary??
            )
        )

        if res.status_code == 200:
            sys.stdout.buffer.write(res.content)
            return
        else:
            url = res.request.url
            sys.stderr.buffer.write(res.content)
            exc = RuntimeError(f"Bad status code {res} while requesting {url}")
            logger.exception(exc)
            time.sleep(60)
    else:
        raise exc

if __name__ == '__main__':
    main()
