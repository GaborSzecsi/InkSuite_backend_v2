"""Read source evidence without executing spreadsheet expressions or external links.

All amounts retain source precision. Posting, not parsing, rounds to cents.
Sheet names are deliberately not part of section identification.
"""
from __future__ import annotations

import hashlib
import re
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from io import BytesIO
from zipfile import ZipFile

import openpyxl

VERSION = "stable-4-signed-returns"
ZERO = Decimal(0)


class ReviewRequired(ValueError):
    pass


def label(value):
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def number(value, *, optional=False):
    if value is None or value == "":
        if optional:
            return ZERO
        raise ReviewRequired("Missing numeric value or formula cache")
    if isinstance(value, bool):
        raise ReviewRequired("Boolean where numeric value was required")
    try:
        result = Decimal(str(value).replace(",", "").replace("$", ""))
    except InvalidOperation as exc:
        raise ReviewRequired(f"Invalid number: {value!r}") from exc
    if not result.is_finite():
        raise ReviewRequired("Non-finite amount")
    return result


def isbn(value):
    if isinstance(value, (int, float, Decimal)):
        value = str(int(value))
    value = re.sub(r"[^0-9X]", "", str(value or "").upper())
    if len(value) == 10:
        if sum((10-i) * (10 if c == "X" else int(c)) for i, c in enumerate(value)) % 11:
            raise ReviewRequired(f"Invalid ISBN-10 {value}")
        value = "978" + value[:9]
        value += str((-sum(int(c)*(1 if i % 2 == 0 else 3) for i, c in enumerate(value))) % 10)
    if len(value) != 13 or not value.startswith(("978", "979")) or sum(int(c)*(1 if i % 2 == 0 else 3) for i, c in enumerate(value)) % 10:
        raise ReviewRequired(f"Invalid ISBN {value}")
    return value


@dataclass
class Sale:
    isbn: str
    title: str
    buyer: str
    territory: str
    sheet: str
    row: int
    activity: str
    units_sold: Decimal = ZERO
    units_returned: Decimal = ZERO
    gross_sales: Decimal = ZERO
    monetary_net: Decimal = ZERO
    commission: Decimal = ZERO
    handling: Decimal = ZERO
    reserve: Decimal = ZERO
    release: Decimal = ZERO
    unit_adjustment: Decimal = ZERO
    discount_percent: Decimal | None = None
    sales_price: Decimal | None = None
    raw: dict = field(default_factory=dict)

    @property
    def receipts(self):
        return self.monetary_net-self.commission-self.handling-self.reserve+self.release


@dataclass
class Report:
    digest: str
    kind: str
    period: str | None = None
    territory: str | None = None
    as_of: str | None = None
    sales: list[Sale] = field(default_factory=list)
    costs: list[dict] = field(default_factory=list)
    inventory: list[dict] = field(default_factory=list)
    issues: list[str] = field(default_factory=list)
    controls: list[dict] = field(default_factory=list)
    currency_hint: str | None = None
    movements: list[dict] = field(default_factory=list)

    def payload(self):
        # SQL staging contains only posting inputs. Full original cells, rates,
        # formulas and account controls remain in the original S3 workbook.
        return dict(sales=[dict(isbn=s.isbn,buyer=s.buyer,sheet=s.sheet,row=s.row,
                     units_sold=s.units_sold,units_returned=s.units_returned,unit_adjustment=s.unit_adjustment,
                     gross_sales=s.gross_sales,publisher_receipts=s.receipts,
                     discount_percent=s.discount_percent,sales_price=s.sales_price)
                     for s in self.sales],
                    costs=[{k:v for k,v in c.items() if k in ('isbn','kind','amount','sheet','row','inventory')} for c in self.costs],
                    movements=self.movements,
                    inventory=[dict(isbn=i['isbn'],available=i['available'],ending=i['ending']) for i in self.inventory])

    def totals(self, territory):
        rows = [s for s in self.sales if s.territory == territory]
        costs = [c for c in self.costs if c["territory"] == territory]
        return {"units_sold": sum((r.units_sold for r in rows), ZERO),
                "units_returned": sum((r.units_returned for r in rows), ZERO),
                "gross_sales": sum((r.gross_sales for r in rows), ZERO),
                "publisher_receipts": sum((r.receipts for r in rows), ZERO)-sum((c["amount"] for c in costs), ZERO)}


SIGNATURES = {
    "transactions": {"isbn", "account", "saletype", "grossqty", "returnqty", "compqty", "grosssales", "netsales"},
    "accounts": {"acctname", "saletype", "grossunits", "returnunits", "distributionfee", "returnsreserve"},
    "amazon": {"title", "unitssold", "adjunits", "grosssales", "adjustments", "distributionfee"},
    "direct": {"source", "retailercustomer", "qty", "grosssales", "distributionfee"},
    "warehouse": {"isbn", "endingbalanceqty", "monthlystoragecharge"},
    "inventory": {"isbnitemcode", "defaultuseablebalanceqty", "endingbalanceqty"},
}


def sections(book, territory=None):
    result = {}
    for sheet, rows in book.items():
        if territory:
            context = sheet+' '+ ' '.join(str(c) for r in rows[:3] for c in r if c)
            canadian = bool(re.search(r'\bCanada\b|\bCanadian\b|\bCan\b',context,re.I))
            if canadian and territory == 'US':
                continue
        for idx, row in enumerate(rows[:60]):
            headers = [label(c) for c in row]
            # '%' and '$' share normalized names; retain suffixes below.
            unique = set(headers)
            for kind, required in SIGNATURES.items():
                if required <= unique:
                    if kind in result:
                        raise ReviewRequired(f"Multiple {kind} sections")
                    names = []
                    for value in row:
                        name = label(value)
                        if "%" in str(value):
                            name += "rate"
                        names.append(name)
                    data = []
                    for n, values in enumerate(rows[idx+1:], idx+2):
                        if any(v is not None for v in values):
                            data.append((n, dict(zip(names, values))))
                    result[kind] = (sheet, data)
                    break
            else:
                continue
            break
    return result


def read_workbook(data):
    if len(data) > 32*1024*1024:
        raise ReviewRequired("Workbook exceeds 32 MB")
    with ZipFile(BytesIO(data)) as archive:
        infos = archive.infolist()
        if len(infos) > 2000 or sum(i.file_size for i in infos) > 128*1024*1024:
            raise ReviewRequired("Workbook expanded size exceeds limit")
        if any("vbaProject" in i.filename or "externalLinks/" in i.filename for i in infos):
            raise ReviewRequired("Macros/external workbook links are not supported")
    workbook = openpyxl.load_workbook(BytesIO(data), read_only=True, data_only=True, keep_links=False)
    try:
        result = {}
        total = 0
        for sheet in workbook:
            if (sheet.max_row or 0) > 100000 or (sheet.max_column or 0) > 150:
                raise ReviewRequired("Worksheet dimensions exceed limits")
            rows = []
            for row in sheet.iter_rows(values_only=True):
                if len(row) > 150:
                    raise ReviewRequired("Worksheet width exceeds limit")
                total += 1
                if total > 200000:
                    raise ReviewRequired("Workbook row count exceeds limit")
                rows.append(list(row))
            result[sheet.title] = rows
        return result
    finally:
        workbook.close()


def statement_period(book):
    periods = set()
    for rows in book.values():
        for row in rows[:12]:
            for value in row:
                match = re.search(r"Fiscal\s*Period\s*:?\s*(20\d{2})[- /]?(0[1-9]|1[0-2])\b", str(value), re.I)
                if match:
                    periods.add(f"{match[1]}-{match[2]}")
    if len(periods) != 1:
        raise ReviewRequired("Missing or conflicting fiscal period in workbook content")
    return periods.pop()


def _data_row(row, key):
    value = str(row.get(key) or "").strip()
    return bool(value) and not any(t in value.lower() for t in ("grand total", "no transactions", "subtotal"))


def report_territory(book):
    summaries = [str(c) for rows in book.values() for r in rows[:2] for c in r if c and 'settlement summary' in str(c).lower()]
    return 'CA' if summaries and all(re.search(r'\bCanada\b|\bCanadian\b',s,re.I) for s in summaries) else 'US'


def parse_book(book, digest="", *, companion=None, inventory_date=None):
    # A Canada-only statement declares this in its summary. Combined statements
    # require separate territory extraction; unsupported layouts stop for review.
    territory = report_territory(book)
    found = sections(book, territory)
    if "inventory" in found:
        report = Report(digest, "inventory", as_of=inventory_date)
        if not inventory_date:
            report.issues.append("Inventory report date must be verified from report or documented filename convention")
        sheet, rows = found["inventory"]
        seen = set()
        for n, row in rows:
            if not _data_row(row, "isbnitemcode"):
                continue
            code = isbn(row["isbnitemcode"])
            available = number(row.get("defaultuseablebalanceqty"))
            ending = number(row.get("endingbalanceqty"))
            if code in seen or available != available.to_integral_value() or ending != ending.to_integral_value() or min(available, ending) < 0:
                raise ReviewRequired(f"Invalid or duplicate inventory ISBN at {sheet}:{n}")
            seen.add(code)
            report.inventory.append(dict(isbn=code, available=int(available), ending=int(ending), sheet=sheet, row=n))
        if not seen:
            report.issues.append("Inventory section contains no ISBN records")
        return report
    if "accounts" not in found:
        raise ReviewRequired("Not a supported client statement or daily inventory report")
    period = statement_period(book)
    headings = " ".join(str(c) for rows in book.values() for row in rows[:3] for c in row if c)
    report = Report(digest, "statement", period, territory)
    if re.search(r'\bCAD\b',headings):
        report.currency_hint = 'CAD'
    elif re.search(r'\bUSD\b',headings):
        report.currency_hint = 'USD'
    controls = {}
    for n, row in found["accounts"][1]:
        if not _data_row(row, "acctname"):
            continue
        key = (label(row.get("acctname")), label(row.get("saletype")))
        previous = controls.get(key)
        controls[key] = dict(row)
        report.controls.append(dict(sheet=found["accounts"][0], row=n, values=row))
        control = controls[key]
        # Reconstruct only documented arithmetic, never evaluate workbook formulas.
        base = number(control.get('grosssales')) + number(control.get('actualreturn'))
        if control.get('netsales') is None:
            control['netsales']=base
        if control.get('distributionfee') is None:
            control['distributionfee']=-number(control['netsales'])*number(control.get('distributionfeerate'))
        if control.get('returnsreserve') is None:
            control['returnsreserve']=-(number(control['netsales'])+number(control['distributionfee']))*number(control.get('returnsreserverate'))
        derived = {'fulfillmentfee':-(number(control.get('grossunits'))+abs(number(control.get('compunits'))))*Decimal('.63'),
                   'returnsfee':-abs(number(control.get('returnunits')))*Decimal('.63')}
        for column,value in derived.items():
            if control.get(column) is None:
                control[column] = value
        settlement=number(control['netsales'])+sum((number(control[k]) for k in ('distributionfee','returnsreserve','fulfillmentfee','returnsfee')),ZERO)
        if control.get('netduetoclient') is not None and abs(number(control['netduetoclient'])-settlement)>Decimal('.02'):
            report.issues.append(f'Account net-due control mismatch for {row.get("acctname")} at {found["accounts"][0]}:{n}')
        if previous:
            old_net,new_net=number(previous['netsales']),number(control['netsales'])
            if old_net and new_net and any(abs(number(previous[k])/old_net-number(control[k])/new_net)>Decimal('.0001') for k in ('distributionfee','returnsreserve')):
                raise ReviewRequired('Same buyer has different monetary deductions; account-number allocation required')
            for column in ('grossunits','returnunits','compunits','grosssales','actualreturn','netsales','distributionfee','returnsreserve','fulfillmentfee','returnsfee'):
                control[column] = number(control[column])+number(previous[column])
    transactions = found.get("transactions")
    if territory == "CA" and not transactions and companion:
        if statement_period(companion) != period:
            raise ReviewRequired("Canadian companion statement period differs")
        transactions = sections(companion).get("transactions")
    if not transactions:
        if controls:
            report.issues.append("Missing ISBN transaction detail; Canada requires the matching US statement")
    grouped = defaultdict(lambda: defaultdict(lambda: ZERO))
    # Some workbooks put physical returns in negative Gross Qty/Sales columns.
    # Classify them only when the account's independent return-unit control agrees.
    negative_units=defaultdict(lambda:ZERO)
    explicit_returns=defaultdict(lambda:ZERO)
    if transactions:
        for _,row in transactions[1]:
            if not _data_row(row,'isbn'):
                continue
            key=(label(row.get('account')),label(row.get('saletype')))
            negative_units[key]+=max(-number(row.get('grossqty')),ZERO)
            explicit_returns[key]+=abs(number(row.get('returnqty')))
    negative_as_return={}
    for key,qty in negative_units.items():
        if not qty or key not in controls:
            continue
        reported=abs(number(controls[key].get('returnunits')))
        negative_as_return[key]=(reported==explicit_returns[key]+qty)
        if reported not in (explicit_returns[key],explicit_returns[key]+qty):
            report.issues.append(f'Ambiguous negative gross-unit classification for {key}')
    if transactions:
        sheet, rows = transactions
        for n, row in rows:
            if not _data_row(row, "isbn"):
                continue
            activity = str(row.get("saletype") or "")
            is_ca = "canad" in activity.lower()
            if is_ca != (territory == "CA"):
                continue
            buyer = str(row.get("account") or "").strip()
            key = (label(buyer), label(activity))
            control = controls.get(key)
            if not control:
                report.issues.append(f"No account control for {buyer}/{activity} at {sheet}:{n}")
                continue
            gross = number(row.get("grosssales"))
            net = number(row.get("netsales"))
            returned_money = number(row.get("returns"))
            if abs(gross+returned_money-net) > Decimal("0.011"):
                report.issues.append(f"Gross/returns/net mismatch at {sheet}:{n}")
            sold = number(row.get("grossqty"))
            returns = abs(number(row.get("returnqty")))
            unit_adjustment=ZERO
            if sold<0:
                if negative_as_return.get(key):
                    returns+=abs(sold)
                    returned_money+=gross
                    gross=ZERO
                else:
                    unit_adjustment=sold
                sold=ZERO
            signed_comp = number(row.get("compqty"))
            # Negative Comp Qty is a physical return of previously transferred stock.
            returns += max(-signed_comp, ZERO)
            comp = max(signed_comp, ZERO)
            transfer = any(t in activity.lower() for t in ("transfer", "gift", "comp"))
            if transfer and (gross or net or sold):
                report.issues.append(f"Transfer has paid-sales values at {sheet}:{n}")
            if transfer:
                self_territory = 'CA' if label(buyer) == 'amazoncomcaulc' else territory
                self_buyer = 'Amazon (paid sales)' if label(buyer) in ('amazoncom','amazoncomcaulc') else buyer
                report.movements.append(dict(isbn=isbn(row['isbn']),territory=self_territory,buyer=self_buyer,outbound=comp,returned=returns))
            account_net=number(control['netsales'])
            account_fee=-number(control['distributionfee'])
            account_reserve=-number(control['returnsreserve'])
            if not account_net and (account_fee or account_reserve):
                report.issues.append(f'Zero account net with nonzero monetary deductions for {buyer}')
            fee = account_fee*net/account_net if account_net else ZERO
            reserve = account_reserve*net/account_net if account_net else ZERO
            handling = (sold+comp)*Decimal("0.63")+returns*Decimal("0.63")
            sale = Sale(isbn(row["isbn"]), str(row.get("title") or ""), buyer, territory, sheet, n, activity,
                        ZERO if transfer else sold, ZERO if transfer else returns, gross, net, fee, handling, reserve,
                        unit_adjustment=ZERO if transfer else unit_adjustment,raw=row)
            report.sales.append(sale)
            for k, v in {"grossunits":sold, "returnunits":returns, "compunits":comp, "grosssales":gross,
                         "netsales":net, "distributionfee":fee, "returnsreserve":reserve,
                         "fulfillmentfee":(sold+comp)*Decimal("0.63"), "returnsfee":returns*Decimal("0.63")}.items():
                grouped[key][k] += v
    for key, row in controls.items():
        actual = grouped[key]
        for column in ("grossunits", "returnunits", "compunits", "grosssales", "netsales", "distributionfee", "returnsreserve", "fulfillmentfee", "returnsfee"):
            source_key = column+"" if column not in ("fulfillmentfee", "returnsfee") else column
            # Unit fee and dollar fee headers normalize identically; dollar value is the last.
            reported = number(row.get(source_key))
            if column in ('distributionfee','returnsreserve','fulfillmentfee','returnsfee'):
                reported = -reported
            elif column in ('returnunits','compunits'):
                reported = abs(reported)
            if abs(reported-actual[column]) > Decimal("0.02"):
                report.issues.append(f"Account control mismatch {key}: {column}, source={reported}, detail={actual[column]}")
    for kind in ("amazon", "direct"):
        if kind not in found:
            if not (territory == 'CA' and kind == 'direct'):
                report.issues.append(f"Missing {kind} section")
            continue
        sheet, rows = found[kind]
        fx_rate = None
        if kind == 'amazon' and territory == 'CA':
            rates=set()
            for _,values in rows:
                for value in values.values():
                    match=re.search(r'\$?1\s*USD\s*=\s*\$?([0-9]+\.[0-9]+)\s*CAD',str(value),re.I)
                    if match:
                        rates.add(number(match[1]))
            if len(rates)>1:
                raise ReviewRequired('Conflicting Canadian Amazon exchange rates')
            if rates:
                fx_rate=rates.pop()
                if fx_rate<=0:
                    raise ReviewRequired('Invalid Canadian Amazon exchange rate')
                report.currency_hint='USD'
                report.controls.append(dict(sheet=sheet,conversion='Amazon CAD amounts divided by reported CAD per USD',cad_per_usd=fx_rate))
        for n, row in rows:
            code_field = "isbn" if "isbn" in row else "isbn10"
            if not _data_row(row, code_field):
                continue
            code = isbn(row[code_field])
            buyer = "Amazon (paid sales)" if kind == "amazon" else str(row.get("retailercustomer") or row.get("source") or "").strip()
            sold = number(row.get("unitssold" if kind == "amazon" else "qty"))
            adjustment = number(row.get("adjunits"), optional=True) if kind == "amazon" else ZERO
            gross = number(row.get("grosssales"))
            net = number(row.get("netsales")) if kind == "amazon" else gross
            # Reported monetary fee/net due are authoritative; inconsistent
            # percentage labels are neither persisted nor substituted for cash.
            fee = -number(row.get('distributionfee'))
            if abs(number(row.get("netduetoclient"))-(net-fee)) > Decimal("0.011"):
                report.issues.append(f"Paid settlement mismatch at {sheet}:{n}")
            # Signed settlement adjustments change net sold units, not physical returns.
            if fx_rate:
                gross,net,fee=gross/fx_rate,net/fx_rate,fee/fx_rate
            report.sales.append(Sale(code, str(row.get("title") or ""), buyer, territory, sheet, n, kind,
                                     sold, ZERO, gross, net, fee, unit_adjustment=adjustment, raw=row))
    if territory == "US":
        if "warehouse" not in found:
            report.issues.append("Missing monthly warehouse section")
        else:
            sheet, rows = found["warehouse"]
            seen = set()
            for n, row in rows:
                if not _data_row(row, "isbn"):
                    continue
                code = isbn(row["isbn"])
                if code in seen:
                    raise ReviewRequired("Duplicate monthly warehouse ISBN")
                seen.add(code)
                qty = number(row.get("endingbalanceqty"))
                amount = number(row.get("monthlystoragecharge"))
                if abs(amount-qty*Decimal("0.007")) > Decimal("0.011"):
                    report.issues.append(f"Storage mismatch at {sheet}:{n}")
                report.costs.append(dict(isbn=code, territory=territory, kind="storage", amount=amount,
                                         inventory=qty, sheet=sheet, row=n))
    codes = {s.isbn for s in report.sales} | {c["isbn"] for c in report.costs}
    for code in sorted(codes):
        report.costs.append(dict(isbn=code, territory=territory, kind="technology", amount=Decimal("0.50")))
    # Statement prices, rather than today's catalog prices, preserve historical
    # discount tiers. Canada may use the companion's explicitly labelled CA price.
    warehouse = found.get('warehouse')
    if not warehouse and companion:
        warehouse = sections(companion,'US').get('warehouse')
    prices = {};canadian_prices={}
    if warehouse:
        for _,row in warehouse[1]:
            if _data_row(row,'isbn'):
                value = row.get('usprice')
                if value is not None:
                    prices[isbn(row['isbn'])] = number(value)
                if row.get('caprice') is not None:
                    canadian_prices[isbn(row['isbn'])]=number(row['caprice'])
    for sale in report.sales:
        if sale.units_sold > 0:
            sale.sales_price = sale.gross_sales/sale.units_sold
        elif sale.units_returned > 0:
            sale.sales_price = abs(sale.monetary_net)/sale.units_returned
        elif sale.unit_adjustment:
            sale.sales_price = abs(sale.monetary_net/sale.unit_adjustment)
        price=sale.sales_price;catalog=prices.get(sale.isbn)
        if territory=='CA' and sale.activity=='amazon' and any(c.get('cad_per_usd') for c in report.controls):
            catalog=canadian_prices.get(sale.isbn)
            price=number(sale.raw['grosssales'])/sale.units_sold if sale.units_sold else None
        if price is not None and catalog:
            sale.discount_percent = (Decimal(1)-price/catalog)*100
            if not ZERO <= sale.discount_percent <= 100:
                sale.discount_percent=None
    # Monthly summary is an independent control, never an additional sale.
    for rows in book.values():
        for row in rows:
            if len(row)==2 and label(row[0])=='domesticgrosssales':
                total = sum((s.monetary_net if s.activity=='amazon' else s.gross_sales for s in report.sales),ZERO)
                expected = number(row[1]) if row[1] is not None else sum((number(c['grosssales']) for c in controls.values()),ZERO)+sum((s.monetary_net if s.activity=='amazon' else s.gross_sales for s in report.sales if s.activity in ('amazon','direct')),ZERO)
                if abs(expected-total)>Decimal('.02'):
                    report.issues.append(f'Monthly monetary-sales control mismatch: source={row[1]}, detail={total}')
    # A release is not safely attributable to ISBN from an account/summary alone.
    for rows in book.values():
        for row in rows:
            if row and "reservereturnsreleased" in label(row[0]) and len(row) == 2 and number(row[1], optional=True):
                report.issues.append("Reserve release requires a reviewed per-ISBN allocation")
    if not codes:
        report.issues.append("No ISBN evidence in statement")
    return report


def parse(data, **kwargs):
    return parse_book(read_workbook(data), hashlib.sha256(data).hexdigest(), **kwargs)
