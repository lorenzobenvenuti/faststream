from typing import TYPE_CHECKING, Optional

from aio_pika import Message
from aio_pika.abc import DeliveryMode

from faststream.broker.message import (
    StreamMessage,
    decode_message,
    encode_message,
    gen_cor_id,
)
from faststream.rabbit.message import RabbitMessage
from faststream.utils.context.repository import context

if TYPE_CHECKING:
    from aio_pika import IncomingMessage
    from aio_pika.abc import DateType, HeadersType

    from faststream.rabbit.subscriber.usecase import LogicSubscriber
    from faststream.rabbit.types import AioPikaSendableMessage
    from faststream.types import DecodedMessage


class AioPikaParser:
    """A class for parsing, encoding, and decoding messages using aio-pika."""

    @staticmethod
    async def parse_message(
        message: "IncomingMessage",
    ) -> StreamMessage["IncomingMessage"]:
        """Parses an incoming message and returns a RabbitMessage object."""
        handler: Optional["LogicSubscriber"] = context.get_local("handler_")
        if (
            handler is not None
            and (path_re := handler.queue.path_regex)
            and (match := path_re.match(message.routing_key or ""))
        ):
            path = match.groupdict()
        else:
            path = {}

        return RabbitMessage(
            body=message.body,
            headers=message.headers,
            reply_to=message.reply_to or "",
            content_type=message.content_type,
            message_id=message.message_id or gen_cor_id(),
            correlation_id=message.correlation_id or gen_cor_id(),
            path=path,
            raw_message=message,
        )

    @staticmethod
    async def decode_message(
        msg: StreamMessage["IncomingMessage"],
    ) -> "DecodedMessage":
        """Decode a message."""
        return decode_message(msg)

    @staticmethod
    def encode_message(
        message: "AioPikaSendableMessage",
        *,
        persist: bool,
        reply_to: Optional[str],
        headers: Optional["HeadersType"],
        content_type: Optional[str],
        content_encoding: Optional[str],
        priority: Optional[int],
        correlation_id: Optional[str],
        expiration: Optional["DateType"],
        message_id: Optional[str],
        timestamp: Optional["DateType"],
        message_type: Optional[str],
        user_id: Optional[str],
        app_id: Optional[str],
    ) -> Message:
        """Encodes a message for sending using AioPika."""
        if isinstance(message, Message):
            return message

        else:
            message_body, generated_content_type = encode_message(message)

            delivery_mode = (
                DeliveryMode.PERSISTENT if persist else DeliveryMode.NOT_PERSISTENT
            )

            return Message(
                message_body,
                content_type=content_type or generated_content_type,
                delivery_mode=delivery_mode,
                reply_to=reply_to,
                correlation_id=correlation_id or gen_cor_id(),
                headers=headers,
                content_encoding=content_encoding,
                priority=priority,
                expiration=expiration,
                message_id=message_id,
                timestamp=timestamp,
                type=message_type,
                user_id=user_id,
                app_id=app_id,
            )
