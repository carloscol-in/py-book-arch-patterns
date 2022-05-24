import email
import logging
from typing import Callable, Dict, List, Type, Union
from tenacity import Retrying, RetryError, stop_after_attempt, wait_exponential

from allocation.domain import commands, events
from allocation.service_layer import handlers, unit_of_work

logger = logging.getLogger(__name__)

Message = Union[commands.Command, events.Event]

class MessageBus:
    def __init__(
        self, uow: unit_of_work.AbstractUnitOfWork,
        event_handlers: Dict[Type[events.Event], List[Callable]],
        command_handlers: Dict[Type[commands.Command], List[Callable]]
    ):
        self._uow = uow
        self._event_handlers = event_handlers
        self._command_handlers = command_handlers

    def handle(self, message: Message):
        results = []
        queue = [message]
        while queue:
            message = queue.pop(0)
            if isinstance(message, events.Event):
                self._handle_event(message, queue)
            elif isinstance(message, commands.Command):
                cmd_result = self._handle_command(message, queue)
                results.append(cmd_result)
            else:
                raise Exception(f'{message} was not an Event of Command')
        return results

    def _handle_event(self, event: events.Event, queue: List):
        for handler in self._event_handlers[type(event)]:
            try:
                for attempt in Retrying(
                    stop=stop_after_attempt(3),
                    wait=wait_exponential()
                ):
                    with attempt:
                        logger.debug(
                            f'handling event {event} with handler {handler}')
                        handler(event)
                        queue.extend(self._uow.collect_new_events())
            except RetryError as retry_failure:
                logger.error(
                    'Failed to handle event %s times, giving up!',
                    retry_failure.last_attempt.attempt_number
                )
                continue

    def _handle_command(self, command: commands.Command, queue: List):
        logger.debug('handling command %s', command)
        try:
            handler = self._command_handlers[type(command)]
            result = handler(command)
            queue.extend(self._uow.collect_new_events())
            return result
        except Exception:
            logger.exception('Exception handling command %s', command)
            raise

def handle_event(event: events.Event, queue: List[Message], uow: unit_of_work.AbstractUnitOfWork):
    for handler in handlers.EVENT_HANDLERS[type(event)]:
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
        handler = handlers.COMMAND_HANDLERS[type(command)]
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
