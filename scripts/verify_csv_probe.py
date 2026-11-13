"""Small public CSV tool acceptance using the five real fixtures; no raw cells printed."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'backend'))
from app.csv_probe import inspect_csv, SourceParseFailure

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--fixtures', type=Path, default=Path('.runtime/connectors/csv'))
parser.add_argument('--output', type=Path)
args = parser.parse_args()
passed = []
for filename, expected in [('customers-utf8.csv', 3), ('quoted-multiline.csv', 2)]:
    content = inspect_csv((args.fixtures / filename).read_bytes(), 'csv:acceptance')
    table = content['tables'][0]
    assert table['approx_rows'] == expected and table['statistics_kind'] == 'complete_file_scan'
    assert all(v == '[已脱敏]' for row in table['samples'] for v in row.values())
    passed.append(filename + ': complete records and masked samples')
for filename in ['invalid-unclosed-quote.csv', 'invalid-row-width.csv', 'invalid-utf8.csv']:
    try:
        inspect_csv((args.fixtures / filename).read_bytes(), 'csv:acceptance')
    except SourceParseFailure as error:
        assert error.source_ref == 'csv:acceptance' and error.line >= 1 and error.column >= 1
        assert error.code == 'SourceParseFailure'
    else:
        raise AssertionError(filename + ': corrupt batch accepted')
    passed.append(filename + ': entire batch rejected with safe location')
report = json.dumps({'passed': passed}, ensure_ascii=False, indent=2)
if args.output:
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(report, encoding='utf-8')
print(report)
