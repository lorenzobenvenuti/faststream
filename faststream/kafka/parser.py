from typing import TYPE_CHECKING, Any, Optional, Tuple

from faststream.broker.message import decode_message, gen_cor_id
from faststream.kafka.message import ConsumerProtocol, FAKE_CONSUMER, KafkaMessage
from faststream.utils.context.repository import context

if TYPE_CHECKING:
    from aiokafka import ConsumerRecord
    from aiokafka.consumer import AIOKafkaConsumer

    from faststream.broker.message import StreamMessage
    from faststream.kafka.subscriber.usecase import LogicSubscriber
    from faststream.types import DecodedMessage


class AioConsumerProtocol(ConsumerProtocol):
    
    def __init__(self, consumer : "AIOKafkaConsumer"):
        self._consumer = consumer
    
    async def commit(self) -> None:
         await self._consumer.commit()

    async def pause(self) -> None:
        self._consumer.pause(*list(self._consumer.assignment()))
    
    async def resume(self) -> None:
        self._consumer.resume(*list(self._consumer.assignment()))


class AioKafkaParser:
    """A class to parse Kafka messages."""

    @staticmethod
    def wrap_consumer(handler: Optional["LogicSubscriber[Any]"]) -> ConsumerProtocol:
        consumer = getattr(handler, "consumer", None)
        if not consumer:
            return FAKE_CONSUMER
        return AioConsumerProtocol(consumer)

    @staticmethod
    async def parse_message(
        message: "ConsumerRecord",
    ) -> "StreamMessage[ConsumerRecord]":
        """Parses a Kafka message."""
        headers = {i: j.decode() for i, j in message.headers}
        handler: Optional["LogicSubscriber[Any]"] = context.get_local("handler_")
        return KafkaMessage(
            body=message.value,
            headers=headers,
            reply_to=headers.get("reply_to", ""),
            content_type=headers.get("content-type"),
            message_id=f"{message.offset}-{message.timestamp}",
            correlation_id=headers.get("correlation_id", gen_cor_id()),
            raw_message=message,
            consumer=AioKafkaParser.wrap_consumer(handler),
            is_manual=getattr(handler, "is_manual", True),
        )

    @staticmethod
    async def parse_message_batch(
        message: Tuple["ConsumerRecord", ...],
    ) -> "StreamMessage[Tuple[ConsumerRecord, ...]]":
        """Parses a batch of messages from a Kafka consumer."""
        first = message[0]
        last = message[-1]
        headers = {i: j.decode() for i, j in first.headers}
        handler: Optional["LogicSubscriber[Any]"] = context.get_local("handler_")
        return KafkaMessage(
            body=[m.value for m in message],
            headers=headers,
            reply_to=headers.get("reply_to", ""),
            content_type=headers.get("content-type"),
            message_id=f"{first.offset}-{last.offset}-{first.timestamp}",
            correlation_id=headers.get("correlation_id", gen_cor_id()),
            raw_message=message,
            consumer=getattr(handler, "consumer", None) or FAKE_CONSUMER,
            is_manual=getattr(handler, "is_manual", True),
        )

    @staticmethod
    async def decode_message(msg: "StreamMessage[ConsumerRecord]") -> "DecodedMessage":
        """Decodes a message."""
        return decode_message(msg)

    @classmethod
    async def decode_message_batch(
        cls,
        msg: "StreamMessage[Tuple[ConsumerRecord, ...]]",
    ) -> "DecodedMessage":
        """Decode a batch of messages."""
        return [decode_message(await cls.parse_message(m)) for m in msg.raw_message]
