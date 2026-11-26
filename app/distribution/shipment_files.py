"""Read-only discovery and parsing of configurable delimited shipment feeds."""
import csv
import fnmatch
import hashlib
import io
import re
import stat
from datetime import date, timedelta
from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field, model_validator

from .models import StrictModel
from .transports import MAX_BYTES, remote_path, sftp_download

MAX_PREVIEW_ROWS = 200
Column = Annotated[int, Field(ge=0, le=999)] | Annotated[str, Field(min_length=1, max_length=200)]
Fields = Literal['order_reference', 'tracking_number', 'carrier', 'tracking_url', 'shipment_reference',
                 'shipment_date', 'sku', 'quantity', 'location', 'package_reference']


class SourceFilter(StrictModel):
    column: Column
    operator: Literal['equals', 'not_equals', 'starts_with', 'ends_with', 'contains',
                      'not_contains', 'empty', 'not_empty']
    value: str = Field(default='', max_length=200)


class ShipmentFeed(StrictModel):
    name: str = Field(min_length=1, max_length=120)
    host: str = Field(pattern=r'^[a-zA-Z0-9.-]{1,253}$')
    port: int = Field(default=22, ge=1, le=65535)
    directory: str = Field(min_length=1, max_length=500)
    host_key: str = Field(default='', max_length=2000, pattern=r'^(?:[A-Za-z0-9+/]+={0,2})?$')
    filename_pattern: str = Field(min_length=1, max_length=200)
    lookback_days: int = Field(default=7, ge=1, le=90)
    delimiter: Literal['\t', ',', ';', '|'] = '\t'
    encoding: Literal['utf-8-sig', 'cp1252', 'iso-8859-1'] = 'utf-8-sig'
    header: bool = False
    column_mode: Literal['position', 'name'] = 'position'
    mappings: dict[Fields, Column]
    filters: list[SourceFilter] = Field(default_factory=list, max_length=20)
    match_all: bool = True
    default_carrier: str = Field(default='', max_length=100)
    shopify_installation_id: UUID | None = None
    order_identifier: Literal['shopify_name', 'shopify_id', 'marketplace_reference'] = 'shopify_name'

    @model_validator(mode='after')
    def validate_configuration(self):
        if not self.directory.startswith('/') or '..' in self.directory.split('/') or re.search(r'[\\\x00-\x1f]', self.directory):
            raise ValueError('Use an absolute directory within the SFTP account.')
        if not safe_name(self.filename_pattern) or re.search(r'[{}]', expand(self.filename_pattern, date.today())):
            raise ValueError('Use a filename with supported date and wildcard variables.')
        if self.column_mode == 'name' and not self.header:
            raise ValueError('Named columns require a header row.')
        if not {'order_reference', 'tracking_number'} <= self.mappings.keys():
            raise ValueError('Map the order reference and tracking number.')
        column_type = int if self.column_mode == 'position' else str
        if any(not isinstance(col, column_type) for col in [*self.mappings.values(), *(f.column for f in self.filters)]):
            raise ValueError('Mappings and filters must use the configured column mode.')
        return self


def safe_name(name):
    return name not in ('', '.', '..') and not re.search(r'[/\\\x00-\x1f\x7f]', name)


def expand(pattern, day):
    for token, value in {'MM': day.strftime('%m'), 'DD': day.strftime('%d'),
                         'YYYY': day.strftime('%Y'), 'YY': day.strftime('%y'), '*': '*'}.items():
        pattern = pattern.replace('{{' + token + '}}', value)
    return pattern


def matching_files(client, config, today):
    patterns = {expand(config.filename_pattern, today - timedelta(days=offset))
                for offset in range(config.lookback_days)}
    matches = []
    for index, entry in enumerate(client.listdir_iter(config.directory, read_aheads=1)):
        if index >= 10000:
            raise ValueError('The remote directory exceeds the listing limit.')
        name = entry.filename
        if (stat.S_ISREG(entry.st_mode or 0) and safe_name(name)
                and any(fnmatch.fnmatchcase(name, pattern) for pattern in patterns)):
            matches.append(name)
    return sorted(set(matches))


def read_file(client, config, name):
    if not safe_name(name):
        raise ValueError('Invalid shipment filename.')
    path = remote_path(config.directory, name)
    if not stat.S_ISREG(client.lstat(path).st_mode or 0):
        raise ValueError('The shipment file must be a regular file.')
    return sftp_download(client, config.directory, name)


def matches(rule, value):
    target = rule.value
    return {
        'equals': value == target,
        'not_equals': value != target,
        'starts_with': value.startswith(target),
        'ends_with': value.endswith(target),
        'contains': target in value,
        'not_contains': target not in value,
        'empty': not value,
        'not_empty': bool(value),
    }[rule.operator]


def preview(content, config, *, limit=MAX_PREVIEW_ROWS):
    if len(content) > MAX_BYTES:
        raise ValueError('The shipment file exceeds the size limit.')
    try:
        source = content.decode(config.encoding)
    except UnicodeError:
        raise ValueError('The file could not be decoded using the selected encoding.') from None
    reader = csv.reader(io.StringIO(source, newline=''), delimiter=config.delimiter, strict=True)
    columns = []
    rows = []
    seen = set()
    truncated = False
    first = True
    try:
        for record in reader:
            if not any(value.strip() for value in record):
                continue
            if first and config.header:
                columns = [value.strip() for value in record]
                if config.column_mode == 'name' and (not all(columns) or len(set(columns)) != len(columns)):
                    raise ValueError('Named columns require unique, nonempty headers.')
                first = False
                continue
            first = False
            if len(rows) == limit:
                truncated = True
                break
            if not config.header:
                columns = list(range(max(len(columns), min(len(record), 1000))))
            rows.append(preview_row(reader.line_num, record, columns, config, seen))
    except csv.Error:
        rows.append({'row': reader.line_num, 'status': 'invalid', 'error': 'Malformed delimited record.'})
    return {'file_hash': hashlib.sha256(content).hexdigest(), 'columns': columns, 'rows': rows, 'truncated': truncated}


def preview_row(number, record, columns, config, seen):
    result = {'row': number}
    headers = {name: index for index, name in enumerate(columns)}

    def cell(column):
        index = column if isinstance(column, int) else headers[column]
        return record[index].strip()

    try:
        decisions = [matches(rule, cell(rule.column)) for rule in config.filters]
        if decisions and not (all(decisions) if config.match_all else any(decisions)):
            return result | {'status': 'skipped'}
        fields = {field: cell(column) for field, column in config.mappings.items()}
    except (IndexError, KeyError):
        return result | {'status': 'invalid', 'error': 'A configured column is missing.'}
    if (not fields['order_reference'] or not fields['tracking_number']
            or any(len(value) > 500 or any(ord(char) < 32 for char in value) for value in fields.values())):
        return result | {'status': 'invalid', 'error': 'Missing or invalid shipment field.'}
    fields['carrier'] = fields.get('carrier') or config.default_carrier
    key = tuple(sorted(fields.items()))
    status = 'duplicate_in_file' if key in seen else 'parsed'
    seen.add(key)
    return result | {'status': status, 'fields': fields}
