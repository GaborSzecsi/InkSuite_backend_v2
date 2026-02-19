# marble_app/services/royalty_calculator.py
from pathlib import Path
import os
from typing import List, Dict, Any

from models.royalty import (
    Book,
    SalesData,
    RoyaltyCalculation,
    PaymentSummary,
    RoyaltyStatement,
    RoyaltyStatementRequest,
)
from services.file_ops import load_json, save_json

BOOKS_PATH = Path(r"C:\Users\szecs\Documents\marble_app\book_data\books.json")

class RoyaltyCalculator:
    def __init__(self):
        # Resolve paths relative to the package
        current_dir = os.path.dirname(os.path.abspath(__file__))
        base_dir = os.path.dirname(current_dir)

        self.royalty_file = os.path.join(base_dir, "book_data", "royalty_statements.json")
        self.author_royalty_file = os.path.join(base_dir, "book_data", "author_royalties.json")
        self.illustrator_royalty_file = os.path.join(base_dir, "book_data", "illustrator_royalties.json")
        self.books_file = os.path.join(base_dir, "book_data", "books.json")

        os.makedirs(os.path.dirname(self.books_file), exist_ok=True)

    @staticmethod
    def _dump(model):
        """Pydantic v2/v1 compatible dump."""
        if hasattr(model, "model_dump"):
            return model.model_dump()
        # fallback for v1
        return model.dict()

    def format_currency(self, val: float) -> str:
        if isinstance(val, (int, float)):
            if val == 0:
                return ""
            return f"(${abs(val):,.2f})" if val < 0 else f"${val:,.2f}"
        return str(val) if val else ""

    def calculate_royalties(self, request: RoyaltyStatementRequest, book: Book) -> Dict[str, Any]:
        lifetime_quantity_by_cat: Dict[str, int] = {}
        returns_to_date_by_cat: Dict[str, int] = {}

        book_id = request.uid
        author_name = book.author.strip() if isinstance(book.author, str) else str(book.author).strip()
        illustrator_name = (book.illustrator.name.strip() if getattr(book, "illustrator", None) else "")

        author_history = self._load_person_royalties(self.author_royalty_file).get(author_name, [])
        illustrator_history = self._load_person_royalties(self.illustrator_royalty_file).get(illustrator_name, [])

        author_statements_for_book = [s for s in author_history if s.get("book_id") == book_id]
        illustrator_statements_for_book = [s for s in illustrator_history if s.get("book_id") == book_id]

        current_period_key = f"{request.period_start}|{request.period_end}"
        for stmt in author_statements_for_book:
            stmt_key = f"{stmt.get('period_start')}|{stmt.get('period_end')}"
            if stmt_key == current_period_key:
                continue
            for row in stmt.get("categories", []):
                cat = row.get("Category")
                try:
                    net_units = int(row.get("Net Units") or 0)
                    returns = int(row.get("Returns") or 0)
                    lifetime_quantity_by_cat[cat] = lifetime_quantity_by_cat.get(cat, 0) + net_units
                    returns_to_date_by_cat[cat] = returns_to_date_by_cat.get(cat, 0) + returns
                except Exception:
                    pass

        summary_rows_author: List[Dict[str, Any]] = []
        summary_rows_illustrator: List[Dict[str, Any]] = []
        total_royalty_author = 0.0
        total_royalty_illustrator = 0.0

        for sales_row in request.sales_data:
            cat = sales_row.category
            units = sales_row.units or 0
            returns = sales_row.returns or 0
            discount = sales_row.discount or 0
            price_or_net = sales_row.unit_price_or_net_revenue or 0.0
            net_revenue_mode = sales_row.net_revenue

            net_units = max(units - returns, 0)

            rate_a = request.author_rates.get(cat, 0.0)
            rate_i = request.illustrator_rates.get(cat, 0.0)

            if net_revenue_mode:
                adjusted_value = price_or_net
                if cat.lower() == "e-book":
                    adjusted_value *= (1 - 0.12)  # platform fee carve-out
                value = adjusted_value
                royalty_a = adjusted_value * (rate_a / 100)
                royalty_i = adjusted_value * (rate_i / 100)
                unit_price_display = ""
            else:
                value = net_units * price_or_net
                royalty_a = value * (rate_a / 100)
                royalty_i = value * (rate_i / 100)
                unit_price_display = f"${price_or_net:.2f}"

            total_royalty_author += royalty_a
            total_royalty_illustrator += royalty_i

            summary_rows_author.append({
                "Category": cat,
                "Units": units,
                "Returns": returns,
                "Net Units": net_units,
                "Lifetime Quantity": f"{lifetime_quantity_by_cat.get(cat, 0) + net_units:,}",
                "Returns to Date": f"{returns_to_date_by_cat.get(cat, 0) + returns:,}",
                "Unit Price": unit_price_display,
                "Royalty Rate (%)": f"{rate_a:.1f}%",
                "Discount": discount,
                "Net Revenue": "✅" if net_revenue_mode else "",
                "Value": f"${value:,.2f}",
                "Royalty": f"${royalty_a:,.2f}",
            })

            summary_rows_illustrator.append({
                "Category": cat,
                "Units": units,
                "Returns": returns,
                "Net Units": net_units,
                "Lifetime Quantity": f"{lifetime_quantity_by_cat.get(cat, 0) + net_units:,}",
                "Returns to Date": f"{returns_to_date_by_cat.get(cat, 0) + returns:,}",
                "Unit Price": unit_price_display,
                "Royalty Rate (%)": f"{rate_i:.1f}%",
                "Discount": discount,
                "Net Revenue": "✅" if net_revenue_mode else "",
                "Value": f"${value:,.2f}",
                "Royalty": f"${royalty_i:,.2f}",
            })

        author_advance = -abs(request.author_advance)
        illustrator_advance = -abs(request.illustrator_advance)

        author_last_balance = self._get_last_balance(author_statements_for_book, author_advance)
        illustrator_last_balance = self._get_last_balance(illustrator_statements_for_book, illustrator_advance)

        author_balance = author_last_balance + total_royalty_author
        illustrator_balance = illustrator_last_balance + total_royalty_illustrator

        author_payable = max(0.0, author_balance)
        illustrator_payable = max(0.0, illustrator_balance)

        return {
            "author": {
                "advance": author_advance,
                "royalty_total": total_royalty_author,
                "categories": summary_rows_author,
                "last_balance": author_last_balance,
                "balance": author_balance,
                "payable": author_payable,
            },
            "illustrator": {
                "advance": illustrator_advance,
                "royalty_total": total_royalty_illustrator,
                "categories": summary_rows_illustrator,
                "last_balance": illustrator_last_balance,
                "balance": illustrator_balance,
                "payable": illustrator_payable,
            },
            "lifetime_quantity_by_cat": lifetime_quantity_by_cat,
            "returns_to_date_by_cat": returns_to_date_by_cat,
        }

    def _get_last_balance(self, statements: List[Dict], default_advance: float) -> float:
        if statements:
            latest = sorted(statements, key=lambda x: x["period_end"], reverse=True)[0]
            return float(latest.get("balance", default_advance) or 0.0)
        return float(default_advance or 0.0)

    def _load_person_royalties(self, file_path: str) -> Dict[str, List]:
        return load_json(file_path, default={})

    def _save_person_royalties(self, file_path: str, data: Dict[str, List]) -> None:
        save_json(file_path, data)

    def save_royalty_statement(self, request: RoyaltyStatementRequest, book: Book) -> Dict[str, Any]:
        calculations = self.calculate_royalties(request, book)

        statement_record = {
            "book_id": request.uid,
            "period_start": request.period_start,
            "period_end": request.period_end,
            "sales_data": [self._dump(s) for s in request.sales_data],
            "author": calculations["author"],
            "illustrator": calculations["illustrator"],
            "lifetime_quantity_by_cat": calculations["lifetime_quantity_by_cat"],
            "returns_to_date_by_cat": calculations["returns_to_date_by_cat"],
        }

        royalties = load_json(self.royalty_file, default=[])
        royalties.append(statement_record)
        save_json(self.royalty_file, royalties)

        self._save_person_statement(
            self.author_royalty_file,
            book.author if isinstance(book.author, str) else str(book.author),
            request.period_start,
            request.period_end,
            {
                "book_id": request.uid,
                "advance": calculations["author"]["advance"],
                "royalty_total": calculations["author"]["royalty_total"],
                "balance": calculations["author"]["balance"],
                "categories": calculations["author"]["categories"],
            },
        )

        if getattr(book, "illustrator", None) and book.illustrator.name:
            self._save_person_statement(
                self.illustrator_royalty_file,
                book.illustrator.name,
                request.period_start,
                request.period_end,
                {
                    "book_id": request.uid,
                    "advance": calculations["illustrator"]["advance"],
                    "royalty_total": calculations["illustrator"]["royalty_total"],
                    "balance": calculations["illustrator"]["balance"],
                    "categories": calculations["illustrator"]["categories"],
                },
            )

        return statement_record

    def _save_person_statement(
        self,
        file_path: str,
        person_name: str,
        period_start: str,
        period_end: str,
        statement_data: Dict,
    ) -> None:
        all_data = self._load_person_royalties(file_path)
        if person_name not in all_data:
            all_data[person_name] = []
        record = {"period_start": period_start, "period_end": period_end, **statement_data}
        all_data[person_name].append(record)
        self._save_person_royalties(file_path, all_data)

    # -------- Books passthroughs to JSON --------
    def get_books(self) -> List[Dict]:
        return load_json(self.books_file, default=[])

    def save_book(self, book: Book) -> Dict:
        books = self.get_books()
        key = (str(book.title).strip(), str(book.author).strip())
        book_dict = self._dump(book)

        idx = next((i for i, b in enumerate(books)
                    if (str(b.get("title","")).strip(), str(b.get("author","")).strip()) == key), None)
        if idx is None:
            books.append(book_dict)
        else:
            books[idx] = book_dict
        save_json(self.books_file, books)
        return book_dict

    def save_book_raw(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Save a book record preserving all fields sent by the frontend.
        Merges with an existing record matching (title, author) instead of
        enforcing a strict schema that might drop fields.
        """
        books = self.get_books()
        title = str(payload.get("title", "")).strip()
        author = str(payload.get("author", "")).strip()
        key = (title, author)

        # sanitize NaN and non-serializable values
        def _clean(value):
            import math
            if isinstance(value, float) and math.isnan(value):
                return None
            if isinstance(value, dict):
                return {k: _clean(v) for k, v in value.items()}
            if isinstance(value, list):
                return [_clean(v) for v in value]
            return value

        cleaned = _clean(payload)

        idx = next((i for i, b in enumerate(books)
                    if (str(b.get("title", "")).strip(), str(b.get("author", "")).strip()) == key), None)
        if idx is None:
            books.append(cleaned)
        else:
            merged = {**books[idx], **cleaned}
            books[idx] = merged
        save_json(self.books_file, books)
        return cleaned

    def delete_book(self, title: str, author: str) -> bool:
        books = self.get_books()
        before = len(books)
        books = [b for b in books if not (
            str(b.get("title","")).strip() == str(title).strip()
            and str(b.get("author","")).strip() == str(author).strip()
        )]
        save_json(self.books_file, books)
        return len(books) < before

    def get_person_statements(self, person_name: str, person_type: str) -> List[Dict]:
        file_path = self.author_royalty_file if person_type == "author" else self.illustrator_royalty_file
        data = self._load_person_royalties(file_path)
        return data.get(person_name, [])

    def delete_statement(self, person_name: str, person_type: str, period_start: str, period_end: str) -> bool:
        file_path = self.author_royalty_file if person_type == "author" else self.illustrator_royalty_file
        data = self._load_person_royalties(file_path)
        if person_name in data:
            before = len(data[person_name])
            data[person_name] = [
                s for s in data[person_name]
                if not (s.get("period_start") == period_start and s.get("period_end") == period_end)
            ]
            if len(data[person_name]) < before:
                self._save_person_royalties(file_path, data)
                return True
        return False
