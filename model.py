#!/usr/bin/env python3
import logging
from pathlib import Path
import json
from datetime import datetime, timedelta
from typing import NamedTuple, Dict, List, Set, Optional, Sequence, Any, Union, Iterator, TypeVar
from functools import lru_cache


from kython import JSONType, fget, group_by_cmp


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


class Model:
    def __init__(self, sources: Sequence[PathIsh]) -> None:
        self.sources = list(sorted(map(Path, sources)))
        self.logger = get_logger()

    def iter_raw(self) -> Iterator[Res[Json]]: # TODO rename it??
        # TODO -latest thing??
        emitted: Set[Any] = set()

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
                if frow not in emitted:
                    emitted.add(frow)
                    unique += 1
                    yield dict(zip(headers, row))
            self.logger.debug(f"{src}: filtered out {total - unique:<6} of {total:<6}. Grand total: {len(emitted)}")

    # TODO why did I use lru cache??
    def iter_entries(self) -> Iterator[Res[Entry]]:
        last = None
        for row in self.iter_raw():
            if isinstance(row, Exception):
                yield row
                continue
            cur = Entry.from_row(row)
            if last is not None and cur.dt < last.dt:
                yield RuntimeError(f'Expected {cur} to be later than {last}')
            else:
                yield cur
                last = cur
                # TODO shit, for couple of days it was pretty bad, lots of duplicated entries..


def main():
    import logging
    logging.basicConfig(level=logging.DEBUG)

    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('path', type=Path)
    args = p.parse_args()
    files = list(sorted(args.path.glob('*.json')))
    model = Model(files)
    for x in model.iter_entries():
        if isinstance(x, Exception):
            print(x)
        # print(x)

if __name__ == '__main__':
    main()
