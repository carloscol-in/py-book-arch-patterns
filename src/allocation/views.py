from allocation.adapters import redis_eventpublisher
from allocation.domain import model


def allocations(orderid: str):
    batches: model.Batch = redis_eventpublisher.get_readmodel(orderid)
    
    return [
        {'sku': sku, 'batchref': batchref}
        for sku, batchref in batches.items()
    ]
