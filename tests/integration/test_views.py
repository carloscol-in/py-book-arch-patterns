import pytest
from datetime import date
from sqlalchemy.orm import clear_mappers

from allocation import views
from allocation import bootstrap
from allocation.domain import commands
from allocation.service_layer import messagebus, unit_of_work

today = date.today()


@pytest.fixture
def sqlite_bus(sqlite_session_factory):
    bus = bootstrap.bootstrap(
        start_orm=True,
        uow=unit_of_work.SqlAlchemyUnitOfWork(sqlite_session_factory),
        send_mail=lambda *args: None,
        publish=lambda *args: None
    )
    yield bus
    clear_mappers()

def test_allocations_view(sqlite_bus):
    sqlite_bus.handle(commands.CreateBatch('sku1batch', 'sku1', 50, None), uow)
    sqlite_bus.handle(commands.CreateBatch('sku2batch', 'sku2', 50, today), uow)
    sqlite_bus.handle(commands.Allocate('order1', 'sku1', 20), uow)
    sqlite_bus.handle(commands.Allocate('order1', 'sku2', 20), uow)

    # add a spurious batch and order to make sure we're gettting the right ones

    sqlite_bus.handle(commands.CreateBatch('sku1batch-later', 'sku1', 50, today), uow)
    sqlite_bus.handle(commands.Allocate('otherorder', 'sku1', 30), uow)
    sqlite_bus.handle(commands.Allocate('otherorder', 'sku2', 10), uow)

    assert views.allocations('order1', sqlite_bus.uow) == [
        {'sku': 'sku1', 'batchref': 'sku1batch'},
        {'sku': 'sku2', 'batchref': 'sku2batch'}
    ]
    