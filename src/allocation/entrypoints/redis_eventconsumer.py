"""
This is an entrypoint since data from Redis will come and start executing
the flow of control all the way from the message bus into the business
logic."""

import json
import logging
import redis

from allocation import config, bootstrap
from allocation.adapters import orm
from allocation.domain import commands
from allocation.service_layer import messagebus, unit_of_work

logger = logging.getLogger(__name__)

r = redis.Redis(**config.get_redis_host_and_port())

bus = bootstrap.bootstrap()

def main():
    orm.start_mappers()
    pubsub = r.pubsub(ignore_subscribe_messages=True)
    pubsub.subscribe('change_batch_quantity')

    for m in pubsub.listen():
        handle_change_batch_quantity(m)
    

def handle_change_batch_quantity(m):
    logger.debug('handling %s', m)
    data = json.loads(m['data'])
    cmd = commands.ChangeBatchQuantity(ref=data['batchref'], qty=data['qty'])
    bus.handle(message=cmd)


def handle_allocate(m):
    logger.debug('handling %s', m)
    data = json.loads(m['data'])
    cmd = commands.Allocate(
        orderid=data['orderid'],
        sku=data['sku'],
        qty=data['qty']
    )
    bus.handle(message=cmd)

if __name__ == '__main__':
    main()