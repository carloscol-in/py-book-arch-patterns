import domain.model as model

from datetime import date
from typing import Optional
from domain.model import OrderLine
from adapters.repository import AbstractRepository


class InvalidSku(Exception):
    pass

def is_valid_sku(sku, batches):
    return sku in {b.sku for b in batches}

def allocate(
    orderid: str,
    sku: str,
    qty: int,
    repo: AbstractRepository,
    session
) -> str:
    batches = repo.list()
    if not is_valid_sku(sku, batches):
        raise InvalidSku(f'Invalid sku {sku}')
    batchref = model.allocate(model.OrderLine(orderid, sku, qty), batches)
    session.commit()
    return batchref

def add_batch(
    ref: str, sku: str, qty: int, eta: Optional[date], repo: AbstractRepository, session,
):
    repo.add(model.Batch(ref, sku, qty, eta))
    session.commit()