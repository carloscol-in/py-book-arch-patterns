import email
import logging
from typing import Callable, Dict, List, Type, Union
from tenacity import Retrying, RetryError, stop_after_attempt, wait_exponential

from allocation.domain import commands, events
from allocation.service_layer import handlers, unit_of_work

logger = logging.getLogger(__name__)

Message = Union[commands.Command, events.Event]

class AbstractMessageBus:
    # HANDLERS: Dict[Type[events.Event], List[Callable]]
    HANDLERS = {
        events.OutOfStock: [handlers.send_out_of_stock_notification],
        events.BatchCreated: [handlers.add_batch],
        events.AllocationRequired: [handlers.allocate],
        events.BatchQuantityChanged: [handlers.change_batch_quantity],
    }

    def handle(self, event: events.Event, uow: unit_of_work.AbstractUnitOfWork):
        results = []
        queue = [event]
        while queue:
            event = queue.pop(0)
            for handler in self.HANDLERS[type(event)]:
                results.append(handler(event, uow=uow))
                queue.extend(uow.collect_new_events())
        return results
            

class MessageBus(AbstractMessageBus):
    HANDLERS = {
        events.OutOfStock: [handlers.send_out_of_stock_notification],
        events.BatchCreated: [handlers.add_batch],
        events.AllocationRequired: [handlers.allocate],
        events.BatchQuantityChanged: [handlers.change_batch_quantity],
    }

def handle_event(event: events.Event, queue: List[Message], uow: unit_of_work.AbstractUnitOfWork):
    for handler in EVENT_HANDLERS[type(event)]:
        try:
            for attempt in Retrying(
                stop=stop_after_attempt(3),
                wait=wait_exponential()
            ):
                with attempt:
                    logger.debug(f'handling event {event} with handler {handler}')
                    handler(event, uow=uow)
                    queue.extend(uow.collect_new_events())
        except RetryError as retry_failure:
            logger.error(
                'Failed to handle event %s times, giving up!',
                retry_failure.last_attempt.attempt_number
            )
            continue


def handle_command(command: commands.Command, queue: List[Message], uow: unit_of_work.AbstractUnitOfWork):
    logger.debug('handling command %s', command)
    try:
        handler = COMMAND_HANDLERS[type(command)]
        result = handler(command, uow=uow)
        queue.extend(uow.collect_new_events())
        return result
    except Exception:
        logger.exception('Exception handling command %s', command)
        raise

def handle(message: Message, uow: unit_of_work.AbstractUnitOfWork):
    results = []
    queue = [message]
    while queue:
        message = queue.pop(0)
        if isinstance(message, events.Event):
            handle_event(message, queue, uow)
        elif isinstance(message, commands.Command):
            cmd_result = handle_command(message, queue, uow)
            results.append(cmd_result)
        else:
            raise Exception(f'{message} was not an Event of Command')
    return results


EVENT_HANDLERS: Dict[events.Event, List[Callable]] = {
    events.OutOfStock: [handlers.send_out_of_stock_notification],
}

COMMAND_HANDLERS: Dict[commands.Command, List[Callable]] = {
    commands.Allocate: handlers.allocate,
    commands.CreateBatch: handlers.add_batch,
    commands.ChangeBatchQuantity: handlers.change_batch_quantity,
}
