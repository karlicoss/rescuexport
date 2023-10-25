#!/usr/bin/env python3
import argparse
from concurrent.futures import Executor
import json  # TODO use orjson for parsing?
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Set, Sequence, Any, Iterator, Optional

from .exporthelpers.dal_helper import PathIsh, Json, Res, datetime_naive, pathify
from .exporthelpers.logging_helper import make_logger
from .utils import DummyFuture

logger = make_logger(__name__)

seconds = int

_DT_FMT = '%Y-%m-%dT%H:%M:%S'


@dataclass
class Entry:
    dt: datetime_naive
    '''
    Ok, it definitely seems local, by the looks of the data.

    https://www.rescuetime.com/apidoc#analytic-api-reference
    "defined by the user’s selected time zone" -- not sure what it means, but another clue I suppose

    Note that the manual export has something like -08:00, but it's the time is same as local -- doesn't make any sense...
    '''

    duration_s: seconds
    activity: str

    @classmethod
    def from_row(cls, row: Json) -> 'Entry':
        # COL_DT = 0
        # COL_DUR = 1
        # COL_ACTIVITY = 3
        # todo I think cols are fixed so could speed up lookup? Not really necessary at te moment though
        COL_DT = 'Date'
        COL_DUR = 'Time Spent (seconds)'
        COL_ACTIVITY = 'Activity'

        # fmt: off
        dt_s     = row[COL_DT]
        dur      = row[COL_DUR]
        activity = row[COL_ACTIVITY]
        # fmt: on
        dt = datetime.fromisoformat(dt_s)  # much much faster than strptime!
        return cls(dt=dt, duration_s=dur, activity=activity)


# TODO crap. marshalling json via thread pool might take longer than actually processing in single process?
# e.g. if we change this to just return read_text(), there is no such overhead
def _json_load_path(p: Path) -> Json:
    # TODO hmm kinda annoying that we have to keep logging here,
    # otherwise we will log at the time the future is submitted
    logger.info(f'processing {p}')
    jj = json.loads(p.read_text())
    # NOTE: this makes it slightly faster during concurrent processing
    # (about 10% although also while processing compressed files)
    # for now let's not do that until we decide some generic way to approach it
    # would be nice if json parser just returned tuples instead in the first place
    # jj['rows'] = tuple(map(tuple, jj['rows']))
    return jj


class DAL:
    def __init__(self, sources: Sequence[PathIsh], *, cpu_pool: Optional[Executor] = None) -> None:
        self.sources = list(map(pathify, sources))
        self.cpu_pool = cpu_pool

    def raw_entries(self) -> Iterator[Res[Json]]:
        cpu_pool = self.cpu_pool

        futures = []
        for src in self.sources:
            if cpu_pool is not None:
                future = cpu_pool.submit(_json_load_path, src)
            else:
                future = DummyFuture(_json_load_path, src)

            futures.append(future)

        # todo rely on more_itertools for emitting unique items?
        emitted: Set[Any] = set()
        last = None
        for src, future in zip(self.sources, futures):
            try:
                j = future.result()
            except Exception as e:
                ex = RuntimeError(f'While processing {src}')
                ex.__cause__ = e
                yield ex
                continue

            # TODO hmm ijson might be quite nice here...
            headers = j['row_headers']
            rows = j['rows']

            total = len(rows)
            unique = 0

            for row in rows:
                # TODO hmm kinda annoying that using orjson.dumps as hash is faster than tuple hashing???
                frow = tuple(row)  # freeze for hashing
                if frow in emitted:
                    continue
                drow = dict(zip(headers, row))
                if last is not None and drow['Date'] < last['Date']:
                    yield RuntimeError(f'Expected\n{drow}\nto be later than\n{last}')
                    # TODO ugh, for couple of days it was pretty bad, lots of duplicated entries..
                    # for now, just ignore it
                else:
                    yield drow
                    emitted.add(frow)
                    unique += 1
                last = drow

            logger.debug(f"{src}: filtered out {total - unique:<6} of {total:<6}. Grand total: {len(emitted)}")

    def entries(self) -> Iterator[Res[Entry]]:
        for row in self.raw_entries():
            if isinstance(row, Exception):
                yield row
                continue
            cur = Entry.from_row(row)
            yield cur


# todo quick test (dal helper aided: check that DAL can handle fake data)
def fake_data_generator(rows=100, seed=123) -> Json:
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
        "rows": list(row_gen()),
    }


def main() -> None:
    # todo adapt for dal_helper?
    p = argparse.ArgumentParser()
    p.add_argument('path', type=Path)
    args = p.parse_args()
    files = list(sorted(args.path.glob('*.json')))
    model = DAL(files)
    count = 0
    for x in model.entries():
        if isinstance(x, Exception):
            logger.error(x)
        else:
            count += 1
            if count % 10000 == 0:
                logger.info('Processed %d entries', count)
        # print(x)


if __name__ == '__main__':
    main()
