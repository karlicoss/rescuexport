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

_PATH = Path("/L/backups/rescuetime")

def try_load(fp: Path):
    logger = get_logger()
    try:
        return json.loads(fp.read_text())
    except Exception as e:
        # TODO FIXME ?? maybe verify that on export stage instead?
        if 'Expecting value' in str(e):
            logger.warning(f"Corrupted: {fp}")
        else:
            raise e
    return None


_DT_FMT = "%Y-%m-%dT%H:%M:%S"

class Entry(NamedTuple):
    # TODO ugh, appears to be local time...
    dt: datetime
    duration_s: int
    activity: str

    @staticmethod
    def from_row(row: List):
        COL_DT = 0
        COL_DUR = 1
        COL_ACTIVITY = 3
        dt_s     = row[COL_DT]
        dur      = row[COL_DUR]
        activity = row[COL_ACTIVITY]
        # TODO utc??
        dt = datetime.strptime(dt_s, _DT_FMT)
        return Entry(dt=dt, duration_s=dur, activity=activity)

PathIsh = Union[str, Path]
Json = Dict[str, Any]


T = TypeVar('T')
Res = Union[Exception, T]


class Model:
    def __init__(self, sources: Sequence[PathIsh]) -> None:
        self.sources = list(sorted(map(Path, sources)))
        # TODO sort it just in case??

    def iter_raw(self) -> Iterator[Res[Json]]: # TODO rename it??
        # TODO -latest thing??
        emitted: Set[Any] = set()

        for src in self.sources:
            # TODO FIXME dedup
            j = json.loads(src.read_text())
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
            print(f"{src}: filtered out {total - unique}/{total}")

    # def iter_entries(self) -> Iterator[Res[Entry]]:
    #     for raw in it


@lru_cache(1)
def get_rescuetime(latest: Optional[int]=None):
    if latest is None:
        latest = 0

    entries: Set[Entry] = set()

    # pylint: disable=invalid-unary-operand-type
    for fp in list(sorted(_PATH.glob('*.json')))[-latest:]:
        j = try_load(fp)
        if j is None:
            continue

        cols = j['row_headers']
        seen = 0
        total = 0
        for row in j['rows']:
            e = Entry.from_row(row)
            total += 1
            if e in entries:
                seen += 1
            else:
                entries.add(e)
        print(f"{fp}: {seen}/{total}")
        # print(len(j))
    res = sorted(entries, key=fget(Entry.dt))
    return res

def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('path', type=Path)
    args = p.parse_args()
    files = list(sorted(args.path.glob('*.json')))[:10]
    model = Model(files)
    for x in model.iter_raw():
        pass
        # print(x)

if __name__ == '__main__':
    main()
