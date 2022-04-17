import allocation.domain.model as model

from datetime import date
from typing import Optional
from allocation.domain.model import OrderLine
from allocation.service_layer import unit_of_work
from allocation.service_layer.unit_of_work import AbstractUnitOfWork
from . import messagebus


class InvalidSku(Exception):
    pass

def is_valid_sku(sku, batches):
    return sku in {b.sku for b in batches}

def allocate(
    orderid: str,
    sku: str,
    qty: int,
    uow: unit_of_work.AbstractUnitOfWork
) -> str:
    line = OrderLine(orderid, sku, qty)
    
    # context manager
    with uow:
        product = uow.products.get(sku=line.sku)

        if product is None:
            raise InvalidSku(f'Invalid sku {line.sku}')
        
        batchref = product.allocate(line)
        uow.commit()
        return batchref

def add_batch(
    ref: str, sku: str, qty: int, eta: Optional[date], uow: AbstractUnitOfWork,
):
    with uow:
        product = uow.products.get(sku=sku)
        if product is None:
            product = model.Product(sku, batches=[])
            uow.products.add(product)
        product.batches.append(model.Batch(ref, sku, qty, eta))
        uow.commit()