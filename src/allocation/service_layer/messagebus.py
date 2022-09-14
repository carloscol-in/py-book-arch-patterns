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
        self.uow = uow
        self._event_handlers = event_handlers
        self._command_handlers = command_handlers
        self._queue = []

    def handle(self, message: Message):
        results = []
        self._queue = [message]
        while self._queue:
            message = self._queue.pop(0)
            if isinstance(message, events.Event):
                self._handle_event(message, self._queue)
            elif isinstance(message, commands.Command):
                cmd_result = self._handle_command(message, self._queue)
                results.append(cmd_result)
            else:
                raise Exception(f'{message} was not an Event of Command')
        return results

    def _handle_event(self, event: events.Event, queue: List):
        for handler in self._event_handlers[type(event)]:
            try:
                logger.debug(
                    f'handling event {event} with handler {handler}')
                handler(event)
                self._queue.extend(self.uow.collect_new_events())
            except Exception as e:
                logger.error('Exception handling event %s: %s', event, e)
                continue

    def _handle_command(self, command: commands.Command, queue: List):
        logger.debug('handling command %s', command)
        try:
            handler = self._command_handlers[type(command)]
            result = handler(command)
            self._queue.extend(self.uow.collect_new_events())
            return result
        except Exception:
            logger.exception('Exception handling command %s', command)
            raise
