"""
This is an adapter since this will connect the application to the Redis
service. The flow of data will be from the application to the Redis 
detail."""

import json
import logging
import redis
from dataclasses import asdict

from allocation import config
from allocation.domain import events

logger = logging.getLogger(__name__)

r = redis.Redis(**config.get_redis_host_and_port())

def publish(channel, event: events.Event):
    logger.debug('publishing: channel=%s, event=%s', channel, event)
    r.publish(channel, json.dumps(asdict(event)))