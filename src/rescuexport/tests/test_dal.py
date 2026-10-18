import json
from pathlib import Path

from ..dal import DAL, fake_data_generator


def test_entries_continue_after_bad_file(tmp_path: Path) -> None:
    data = fake_data_generator(rows=4)
    first = tmp_path / 'first.json'
    first.write_text(json.dumps({**data, 'rows': data['rows'][:2]}))
    broken = tmp_path / 'broken.json'
    broken.write_text('{')
    last = tmp_path / 'last.json'
    last.write_text(json.dumps(data))

    [a, b, error, c, d] = DAL([first, broken, last]).raw_entries()
    assert isinstance(error, Exception)
    assert [a, b, c, d] == [dict(zip(data['row_headers'], row, strict=True)) for row in data['rows']]
