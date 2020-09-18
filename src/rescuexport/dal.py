#!/usr/bin/env python3
import logging
from pathlib import Path
import json
from datetime import datetime, timedelta
from typing import NamedTuple, Dict, List, Set, Optional, Sequence, Any, Union, Iterator, TypeVar


def get_logger():
    return logging.getLogger("rescuetime-provider")


PathIsh = Union[str, Path]
Json = Dict[str, Any]


_DT_FMT = '%Y-%m-%dT%H:%M:%S'

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


# todo dal helper
T = TypeVar('T')
Res = Union[Exception, T]


class DAL:
    def __init__(self, sources: Sequence[PathIsh]) -> None:
        # todo do ininstance check (so cpath works)
        self.sources = list(sorted(map(Path, sources)))
        self.logger = get_logger()

    def iter_raw(self) -> Iterator[Res[Json]]: # TODO rename it??
        emitted: Set[Any] = set()
        last = None

        for src in self.sources:
            # todo parse in multiple processes??
            try:
                j = json.loads(src.read_text())
            except Exception as e:
                ex = RuntimeError(f'While processing {src}')
                ex.__cause__ = e
                yield ex
                continue

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

    def entries(self) -> Iterator[Res[Entry]]:
        for row in self.iter_raw():
            if isinstance(row, Exception):
                yield row
                continue
            cur = Entry.from_row(row)
            yield cur

    # todo depercate
    iter_entries = entries


# todo quick test (dal helper aided: check that DAL can handle fake data)
def fake_data_generator(rows=100, seed=123):
    # todo ok, use faker/mimesis here??
    from random import Random
    r = Random(seed)

    def row_gen():
        base = datetime(year=2000, month=1, day=1)
        cur = base
        emitted = 0
        i = 0
        while emitted < rows:
            i += 1
            sleeping = 1 <= cur.hour <= 8
            if sleeping:
                cur = cur + timedelta(hours=2)
                continue

            # do something during that period
            duration = r.randint(10, 500)

            if r.choice([True, False]):
                emitted += 1
                yield [
                    cur.strftime(_DT_FMT),
                    duration,
                    1,
                    f'Activity {i % 10}',
                    'Category {i % 3}',
                    i % 2,
                ]
            cur += timedelta(seconds=duration)


    return {
        "notes": "data is an array of arrays (rows), column names for rows in row_headers",
        "row_headers": ["Date", "Time Spent (seconds)", "Number of People", "Activity", "Category", "Productivity"],
        "rows": list(row_gen())
    }


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
