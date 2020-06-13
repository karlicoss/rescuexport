#!/usr/bin/env python3
import logging
from pathlib import Path
import json
from datetime import datetime, timedelta
from typing import NamedTuple, Dict, List, Set, Optional, Sequence, Any, Union, Iterator, TypeVar
from functools import lru_cache


def get_logger():
    return logging.getLogger("rescuetime-provider")


PathIsh = Union[str, Path]
Json = Dict[str, Any]

_DT_FMT = "%Y-%m-%dT%H:%M:%S"

class Entry(NamedTuple):
    # TODO ugh, appears to be local time...
    dt: datetime
    duration_s: int
    activity: str

    @classmethod
    def from_row(cls, row: Json) -> 'Entry':
        # COL_DT = 0
        # COL_DUR = 1
        # COL_ACTIVITY = 3
        # TODO I think cols are fixed so could speed up lookup? Not really necessary at te moment though
        COL_DT = 'Date'
        COL_DUR = 'Time Spent (seconds)'
        COL_ACTIVITY = 'Activity'

        dt_s     = row[COL_DT]
        dur      = row[COL_DUR]
        activity = row[COL_ACTIVITY]
        # TODO FIXME!!
        # TODO utc??
        dt = datetime.strptime(dt_s, _DT_FMT)
        return cls(dt=dt, duration_s=dur, activity=activity)


T = TypeVar('T')
Res = Union[Exception, T]


class DAL:
    def __init__(self, sources: Sequence[PathIsh]) -> None:
        self.sources = list(sorted(map(Path, sources)))
        self.logger = get_logger()

    def iter_raw(self) -> Iterator[Res[Json]]: # TODO rename it??
        # TODO -latest thing??
        emitted: Set[Any] = set()
        last = None

        for src in self.sources:
            try:
                # TODO parse in multiple processes??
                j = json.loads(src.read_text())
            except Exception as e:
                raise RuntimeError(f'While processing {src}') from e

            headers = j['row_headers']
            rows = j['rows']

            total = len(rows)
            unique = 0

            for row in rows:
                frow = tuple(row) # freeze for hashing
                if frow in emitted:
                    continue
                drow = dict(zip(headers, row))
                if last is not None and drow['Date'] < last['Date']: # pylint: disable=unsubscriptable-object
                    yield RuntimeError(f'Expected\n{drow}\nto be later than\n{last}')
                    # TODO ugh, for couple of days it was pretty bad, lots of duplicated entries..
                    # for now, just ignore it
                else:
                    yield drow
                    emitted.add(frow)
                    unique += 1
                last = drow

            self.logger.debug(f"{src}: filtered out {total - unique:<6} of {total:<6}. Grand total: {len(emitted)}")

    # TODO why did I use lru cache??
    def iter_entries(self) -> Iterator[Res[Entry]]:
        for row in self.iter_raw():
            if isinstance(row, Exception):
                yield row
                continue
            cur = Entry.from_row(row)
            yield cur


def main():
    import logging
    logging.basicConfig(level=logging.DEBUG)
    logger = get_logger()

    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('path', type=Path)
    args = p.parse_args()
    files = list(sorted(args.path.glob('*.json')))
    model = DAL(files)
    count = 0
    for x in model.iter_entries():
        if isinstance(x, Exception):
            logger.error(x)
        else:
            count += 1
            if count % 10000 == 0:
                logger.info('Processed %d entries', count)
        # print(x)

if __name__ == '__main__':
    main()
