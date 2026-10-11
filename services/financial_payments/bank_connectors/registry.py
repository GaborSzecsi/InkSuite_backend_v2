from fastapi import HTTPException
from .mock import MockBankConnector


def connector(code: str):
    if code == 'mock':
        return MockBankConnector()
    # No live provider can execute until its implementation and rollout gates exist.
    raise HTTPException(409, 'Live ACH is disabled. This provider is not configured and certified.')
