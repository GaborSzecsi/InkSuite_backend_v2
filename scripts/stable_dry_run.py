"""Audit original workbooks without SQL writes or production credentials.

python -m scripts.stable_dry_run --directory PATH --output report.json
"""
import argparse
import hashlib
import json
from pathlib import Path

from app.stable.parser import ReviewRequired,parse_book,read_workbook,statement_period
from app.stable.service import json_value
from app.stable.worker import report_date


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    files=sorted(args.directory.glob('*.xlsx'))
    companions={}
    for path in files:
        if 'canada' not in path.name.lower() and 'draft' not in path.name.lower():
            try:
                period=statement_period(read_workbook(path.read_bytes()))
                companions.setdefault(period,[]).append(path)
            except ReviewRequired:
                pass
    results=[]
    for path in files:
        try:
            data=path.read_bytes();book=read_workbook(data);companion=None
            if 'canada' in path.name.lower():
                candidates=companions.get(statement_period(book),[])
                if len(candidates)==1: companion=read_workbook(candidates[0].read_bytes())
            report=parse_book(book,hashlib.sha256(data).hexdigest(),companion=companion,inventory_date=report_date(path.name))
            if 'draft' in path.name.lower(): report.issues.append('Draft statement cannot be automatically posted')
            results.append(dict(file=path.name,kind=report.kind,territory=report.territory,period=report.period,
                currency_hint=report.currency_hint,parser_status='needs_review' if report.issues else 'validated',
                totals=report.totals(report.territory),issues=report.issues))
        except (ReviewRequired,ValueError,KeyError) as exc:
            results.append(dict(file=path.name,parser_status='needs_review',issues=[str(exc)]))
    args.output.write_text(json.dumps(json_value(dict(live_database_verified=False,reports=results)),indent=2),encoding='utf-8')
    print(f'{len(results)} workbooks audited; {sum(r["parser_status"]=="validated" for r in results)} pass parsing/reconciliation. Catalog, SQL constraints, currency configuration and transition overlap remain deployment checks.')
    return 0


if __name__=='__main__': raise SystemExit(main())
