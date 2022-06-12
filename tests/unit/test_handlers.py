from collections import defaultdict
import pytest
from datetime import date

from allocation import bootstrap
from allocation.adapters import notifications
from allocation.domain import commands, events
from allocation.service_layer import messagebus, handlers
from allocation.service_layer.unit_of_work import AbstractUnitOfWork

import allocation.adapters.repository as repository


def bootstrap_test_app():
    return bootstrap.bootstrap(
        start_orm=False,
        uow=FakeUnitOfWork(),
        send_mail=lambda *args: None,
        publish=lambda *args: None
    )


class FakeNotifications(notifications.AbstractNotifications):

    def __init__(self):
        self.sent = defaultdict(list)

    def send(self, destination, message):
        self.sent[destination].append(message)


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


class FakeNotifications(notifications.AbstractNotifications):

    def __init__(self):
        self.sent = defaultdict(list)

    def send(self, destination, message):
        self.sent[destination].append(message)


def bootstrap_test_app():
    return bootstrap.bootstrap(
        start_orm=False,
        uow=FakeUnitOfWork(),
        notifications=FakeNotifications(),
        publish=lambda *args: None,
    )


class TestAddBatch:
    def test_for_new_product(self):
        bus = bootstrap_test_app()
        bus.handle(
            events.BatchCreated('b1', 'CRUNCHY-ARMCHAIR', 100, None)
        )
        assert bus.uow.products.get('CRUNCHY-ARMCHAIR') is not None
        assert bus.uow.committed

    def test_for_existing_product(self):
        bus = bootstrap_test_app()
        bus.handle(commands.CreateBatch("b1", "GARISH-RUG", 100, None))
        bus.handle(commands.CreateBatch("b2", "GARISH-RUG", 99, None))
        assert "b2" in [
            b.reference for b in bus.uow.products.get("GARISH-RUG").batches
        ]

class TestAllocate:
    def test_returns_allocation(self):
        bus = bootstrap_test_app()
        uow = FakeUnitOfWork()
        bus.handle(events.BatchCreated('batch1', 'COMPLICATED-LAMP', 100, None), uow)
        results = messagebus.handle(
            events.AllocationRequired('o1', 'COMPLICATED-LAMP', 10), uow
        )
        assert results.pop(0) == 'batch1'

    def test_errors_for_invalid_sku(self):
        bus = bootstrap_test_app()
        bus.handle(commands.CreateBatch("b1", "AREALSKU", 100, None))

        with pytest.raises(handlers.InvalidSku, match="Invalid sku NONEXISTENTSKU"):
            bus.handle(commands.Allocate("o1", "NONEXISTENTSKU", 10))

    def test_commits(self):
        bus = bootstrap_test_app()
        bus.handle(commands.CreateBatch("b1", "OMINOUS-MIRROR", 100, None))
        bus.handle(commands.Allocate("o1", "OMINOUS-MIRROR", 10))
        assert bus.uow.committed

    def test_sends_email_on_out_of_stock_error(self):
        fake_notifs = FakeNotifications()
        bus = bootstrap_test_app()
        bus.handle(commands.CreateBatch('b1', 'POPULAR-CURTAINS', 9, None))
        bus.handle(commands.Allocate('o1', 'POPULAR-CURTAINS', 10))
        assert fake_notifs.sent['stock@made.com'] == [
            f'Out of stock for POPULAR-CURTAINS',
        ]


class TestChangeBatchQuantity:
    def test_changes_available_quantity(self):
        bus = bootstrap_test_app()
        bus.handle(events.BatchCreated('batch1', 'ADORABLE-SETTEE', 100, None))
        [batch] = bus.uow.products.get(sku='ADORABLE-SETTEE').batches
        assert batch.available_quantity == 100

        messagebus.handle(events.BatchQuantityChanged('batch1', 50))

        assert batch.available_quantity == 50

    def test_reallocates_if_necessary(self):
        bus = bootstrap_test_app()
        event_history = [
            events.BatchCreated('batch1', 'INDIFERENT-TABLE', 50, None),
            events.BatchCreated('batch2', 'INDIFERENT-TABLE', 50, date.today()),
            events.AllocationRequired('order1', 'INDIFERENT-TABLE', 20),
            events.AllocationRequired('order2', 'INDIFERENT-TABLE', 20),
        ]
        for e in event_history:
            bus.handle(e)
        [batch1, batch2] = bus.uow.products.get(sku='INDIFERENT-TABLE').batches
        assert batch1.available_quantity == 10
        assert batch2.available_quantity == 50

        bus.handle(
            events.BatchQuantityChanged('batch1', 25)
        )

        # order1 and order2 will be deallocated, so we'll have 25 - 20
        assert batch1.available_quantity == 5
        # and 20 will be reallocated to the next batch
        assert batch2.available_quantity == 30
