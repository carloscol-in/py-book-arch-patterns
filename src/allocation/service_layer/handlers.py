from datetime import date
from typing import Optional

from allocation.domain import commands, events
from allocation.adapters import email, redis_eventpublisher
from allocation.domain.model import OrderLine
from allocation.service_layer import unit_of_work
from . import messagebus
import allocation.domain.model as model


class InvalidSku(Exception):
    pass

def is_valid_sku(sku, batches):
    return sku in {b.sku for b in batches}

def allocate(
    command: commands.Allocate,
    uow: unit_of_work.AbstractUnitOfWork
) -> str:
    line = OrderLine(command.orderid, command.sku, command.qty)
    
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

def change_batch_quantity(
    event: events.BatchQuantityChanged, uow: unit_of_work.AbstractUnitOfWork
):
    with uow:
        product = uow.products.get_by_batchref(batchref=event.ref)
        product.change_batch_quantity(ref=event.ref, qty=event.qty)
        uow.commit()

def publish_allocated_event(
    event: events.Allocated, uow: unit_of_work.AbstractUnitOfWork
):
    redis_eventpublisher.publish('line_allocated', event)


def add_allocation_to_read_model(
    event: events.Allocated, uow: unit_of_work.AbstractUnitOfWork
):
    with uow:
        uow.session.execute(
            'INSERT INTO allocations_view (orderid, sku, batchref)'
            ' VALUES (:orderid, :sku, :batchref)',
            dict(orderid=event.orderid, sku=event.sku, batchref=event.batchref)
        )
        uow.commit()

def remove_allocation_from_read_model(
    event: events.Deallocated, uow: unit_of_work.AbstractUnitOfWork
):
    with uow:
        uow.session.execute(
            'DELETE FROM allocations_view'
            ' WHERE orderid = :orderid AND sku = :sku',
            dict(orderid=event.orderid, sku=event.sku)
        )

def add_allocation_to_read_model(event: events.Allocated, _):
    redis_eventpublisher.update_readmodel(event.orderid, event.sku, event.batchref)

def remove_allocation_from_read_model(event: events.Deallocated, _):
    redis_eventpublisher.update_readmodel(event.orderid, event.sku, None)