"""Bounded UTF-8 CSV inspection. A profile exists only after the entire file parses."""
import csv
import io
import re

from .connector_probe import MAX_COLUMNS, POLICY_VERSION, SAMPLE_ROWS

MAX_CSV_BYTES = 2 * 1024 * 1024


class SourceParseFailure(Exception):
    code = 'SourceParseFailure'

    def __init__(self, source_ref, line, column, reason):
        self.source_ref, self.line, self.column = source_ref, line, column
        self.reason = reason
        super().__init__(f'CSV 整批解析失败：{reason}（源 {source_ref}，行 {line}，列 {column}）')

    def detail(self):
        return {'code': self.code, 'detail': str(self), 'source_ref': self.source_ref,
                'line': self.line, 'column': self.column}


def inspect_csv(data, source_ref):
    def fail(line, column, reason):
        raise SourceParseFailure(source_ref, line, column, reason)

    if len(data) > MAX_CSV_BYTES:
        fail(1, 1, '文件超过 2 MiB 上限')
    try:
        text = data.decode('utf-8-sig')
    except UnicodeDecodeError as error:
        prefix = data[:error.start].decode('utf-8-sig')
        fail(prefix.count('\n') + 1, len(prefix.rsplit('\n', 1)[-1]) + 1, '文件必须为有效 UTF-8')
    reader = csv.reader(io.StringIO(text, newline=''), strict=True)
    try:
        headers = next(reader, None)
        if (not headers or len(headers) > MAX_COLUMNS or len(set(headers)) != len(headers) or
                any(not re.fullmatch(r'[a-zA-Z_][a-zA-Z0-9_]{0,63}', name) for name in headers)):
            fail(1, 1, '表头须为 1 至 64 个不重复的英文标识符')
        samples, count = [], 0
        nullable = [False] * len(headers)
        while True:
            start = reader.line_num + 1
            row = next(reader, None)
            if row is None:
                break
            if len(row) != len(headers):
                fail(start, min(len(row), len(headers)) + 1, '记录列数与表头不一致')
            count += 1
            nullable = [old or value == '' for old, value in zip(nullable, row)]
            if len(samples) < SAMPLE_ROWS:
                samples.append(dict.fromkeys(headers, '[已脱敏]'))
    except csv.Error:
        fail(max(1, reader.line_num), 1, '引号或字段格式损坏，无法完整读取')
    return {'server_version': 'UTF-8 / RFC 4180', 'version_kind': 'file_format', 'database': 'managed_csv',
            'source_ref': source_ref,
            'tables': [{'name': 'data', 'columns': [{'name': name, 'type': 'text', 'nullable': nullable[index],
                        'key': ''} for index, name in enumerate(headers)],
                        'approx_rows': count, 'approx_bytes': len(data), 'statistics_kind': 'complete_file_scan',
                        'samples': samples}],
            'budget': {'sample_rows_per_table': SAMPLE_ROWS, 'max_tables': 1, 'max_bytes': MAX_CSV_BYTES,
                       'queries_used': 0}, 'policy_version': POLICY_VERSION}
