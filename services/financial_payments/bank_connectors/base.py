from abc import ABC, abstractmethod
from decimal import Decimal
from fastapi import HTTPException


class BankConnector(ABC):
    code: str
    simulated: bool
    capabilities: dict[str, bool]

    @abstractmethod
    def connect(self, display_name: str) -> dict: ...

    @abstractmethod
    def list_accounts(self, connection: dict) -> list[dict]: ...

    @abstractmethod
    def balance(self, account: dict, pending: Decimal, settled: Decimal) -> dict: ...

    @abstractmethod
    def verify_destination(self, secure_account: dict) -> dict: ...

    @abstractmethod
    def submit_credit(self, transfer: dict) -> dict: ...

    def submit_batch(self, transfers: list[dict]) -> list[dict]:
        raise HTTPException(409, 'This bank does not support native ACH batch submission.')

    @abstractmethod
    def transfer_status(self, transfer: dict) -> dict: ...

    @abstractmethod
    def activity(self, transfers: list[dict]) -> list[dict]: ...

    @abstractmethod
    def parse_event(self, transfer: dict, status: str, event_id: str) -> dict: ...
