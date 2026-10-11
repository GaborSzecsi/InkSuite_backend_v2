"""Stateless deterministic sandbox adapter. Persistence belongs to the service."""
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4
from .base import BankConnector


class MockBankConnector(BankConnector):
    code = 'mock'
    simulated = True
    capabilities = dict(balance_read=True, ach_credit=True, ach_batch=False,
                        recipient_verification=True, transfer_status=True,
                        transaction_history=True, webhooks=True)

    def connect(self, display_name):
        return dict(external_connection_id=f'mock-{uuid4()}', display_name=display_name,
                    environment='sandbox', status='connected', capabilities=self.capabilities)

    def list_accounts(self, connection):
        return [dict(external_account_id=connection['external_connection_id']+'-usd',
                     display_name='Simulated royalty funding', bank_name='Mock Bank (simulation)',
                     account_last4='0000', currency='USD')]

    def balance(self, account, pending, settled):
        current = Decimal(str(account['mock_opening_balance']))-settled
        return dict(available_balance=current-pending, current_balance=current,
                    balance_as_of=datetime.now(timezone.utc), commitments_reflected=True,
                    provider_reference='mock-balance')

    def verify_destination(self, secure_account):
        return dict(verification_status='verified',
                    provider_destination_ref=f"mock-destination-{secure_account['id']}")

    def submit_credit(self, transfer):
        # Stable identity: a retried submission cannot create a second transfer.
        return dict(provider_transfer_id=f"mock-transfer-{transfer['id']}", status='submitted')

    def transfer_status(self, transfer):
        return dict(status=transfer['status'], provider_transfer_id=transfer['provider_transfer_id'])

    def activity(self, transfers):
        return [self.transfer_status(t) for t in transfers]

    def parse_event(self, transfer, status, event_id):
        return dict(status=status, provider_event_id=event_id, simulated=True)
