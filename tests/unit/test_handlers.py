from datetime import date
import pytest

from allocation.domain import events
from allocation.service_layer import messagebus
from allocation.service_layer.unit_of_work import AbstractUnitOfWork

import allocation.adapters.repository as repository
import allocation.service_layer.handlers as services


class FakeRepository(repository.AbstractRepository):
    """Fake repository"""

    def __init__(self, products):
        super().__init__()
        self._products = set(products)

    def _add(self, product):
        self._products.add(product)

    def _get(self, sku):
        return next((p for p in self._products if p.sku == sku), None)

    def _get_by_batchref(self, batchref):
        return next((
            p for p in self._products for b in p.batches
            if b.reference == batchref
        ), None)


class FakeUnitOfWork(AbstractUnitOfWork):
    def __init__(self):
        self.products = FakeRepository([])
        self.committed = False

    def _commit(self):
        self.committed = True

    def rollback(self):
        pass


class TestAddBatch:
    def test_for_new_product(self):
        uow = FakeUnitOfWork()
        messagebus.handle(
            events.BatchCreated('b1', 'CRUNCHY-ARMCHAIR', 100, None), uow
        )
        assert uow.products.get('CRUNCHY-ARMCHAIR') is not None
        assert uow.committed

class TestAllocate:
    def test_returns_allocation(self):
        uow = FakeUnitOfWork()
        messagebus.handle(events.BatchCreated('batch1', 'COMPLICATED-LAMP', 100, None), uow)
        results = messagebus.handle(
            events.AllocationRequired('o1', 'COMPLICATED-LAMP', 10), uow
        )
        assert results.pop(0) == 'batch1'


class TestChangeBatchQuantity:
    def test_changes_available_quantity(self):
        uow = FakeUnitOfWork()
        messagebus.handle(
            events.BatchCreated('batch1', 'ADORABLE-SETTEE', 100, None), uow
        )
        [batch] = uow.products.get(sku='ADORABLE-SETTEE').batches
        assert batch.available_quantity == 100

        messagebus.handle(events.BatchQuantityChanged('batch1', 50), uow)

        assert batch.available_quantity == 50

    def test_reallocates_if_necessary(self):
        uow = FakeUnitOfWork()
        event_history = [
            events.BatchCreated('batch1', 'INDIFERENT-TABLE', 50, None),
            events.BatchCreated('batch2', 'INDIFERENT-TABLE', 50, date.today()),
            events.AllocationRequired('order1', 'INDIFERENT-TABLE', 20),
            events.AllocationRequired('order2', 'INDIFERENT-TABLE', 20),
        ]
        for e in event_history:
            messagebus.handle(e, uow)
        [batch1, batch2] = uow.products.get(sku='INDIFERENT-TABLE').batches
        assert batch1.available_quantity == 10
        assert batch2.available_quantity == 50

        messagebus.handle(
            events.BatchQuantityChanged('batch1', 25), uow
        )

        # order1 and order2 will be deallocated, so we'll have 25 - 20
        assert batch1.available_quantity == 5
        # and 20 will be reallocated to the next batch
        assert batch2.available_quantity == 30


class FakeMessageBus(messagebus.AbstractMessageBus):
    def __init__(self):
        self.events_published = []

    def handle(self, event, uow):
        self.events_published = []
        queue = [event]

        while queue:
            event = queue.pop(0)

            for handler in self.HANDLERS[type(event)]:
                handler(event, uow=uow)
                queue.extend(uow.collect_new_events())
                if queue:
                    self.events_published.extend(queue)


def test_reallocates_if_necessary_isolated():
    uow = FakeUnitOfWork()
    mbus = FakeMessageBus()

    # test setup as before
    event_history = [
        events.BatchCreated('batch1', 'INDIFFERENT-TABLE', 50, None),
        events.BatchCreated('batch2', 'INDIFFERENT-TABLE', 50, date.today()),
        events.AllocationRequired('order1', 'INDIFFERENT-TABLE', 20),
        events.AllocationRequired('order2', 'INDIFFERENT-TABLE', 20),
    ]
    for e in event_history:
        mbus.handle(e, uow)

    [batch1, batch2] = uow.products.get(sku='INDIFFERENT-TABLE').batches
    assert batch1.available_quantity == 10
    assert batch2.available_quantity == 50


    mbus.handle(events.BatchQuantityChanged('batch1', 25), uow)


    # assert on new events emitted rather than downstream side-effects
    [reallocation_event] = mbus.events_published
    assert isinstance(reallocation_event, events.AllocationRequired)
    assert reallocation_event.orderid in {'order1', 'order2'}
    assert reallocation_event.sku == 'INDIFFERENT-TABLE'