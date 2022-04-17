import email
from allocation.domain import events
import allocation.domain.model as model

from datetime import date
from typing import Optional
from allocation.domain.model import OrderLine
from allocation.service_layer import unit_of_work
from . import messagebus


class InvalidSku(Exception):
    pass

def is_valid_sku(sku, batches):
    return sku in {b.sku for b in batches}

def allocate(
    event: events.AllocationRequired,
    uow: unit_of_work.AbstractUnitOfWork
) -> str:
    line = OrderLine(event.orderid, event.sku, event.qty)
    
    # context manager
    with uow:
        product = uow.products.get(sku=line.sku)

        if product is None:
            raise InvalidSku(f'Invalid sku {line.sku}')
        
        batchref = product.allocate(line)
        uow.commit()
        return batchref

def add_batch(
    event: events.BatchCreated, uow: unit_of_work.AbstractUnitOfWork,
):
    with uow:
        product = uow.products.get(sku=event.sku)
        if product is None:
            product = model.Product(event.sku, batches=[])
            uow.products.add(product)
        product.batches.append(model.Batch(event.ref, event.sku, event.qty, event.eta))
        uow.commit()


def send_out_of_stock_notification(
    event: events.OutOfStock, uow: unit_of_work.AbstractUnitOfWork
):
    email.send(
        'stock@made.com',
        f'Out of stock for {event.sku}'
    )