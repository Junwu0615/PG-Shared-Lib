# -*- coding: utf-8 -*-
"""
TODO
    Update Date: 2026-05-04
    Description:
    Notice:
        FIXME : 明文傳送應加密 + 安全性須提升 ( 認證 ...etc. )
"""
from shared.configs import struct
from confluent_kafka import (
    Consumer,
    TopicPartition,
    KafkaError
)


class KafkaConsumerManager:
    def __init__(self, logging, log_main_name: str,
                 config: dict,
                 topic: str,
                 topic_key: str):

        self.logging = logging
        self.main_name = log_main_name

        _config = {
            'bootstrap.servers': '127.0.0.1:9092',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        }
        if not isinstance(config, dict):
            raise
        config = {**_config, **config}
        self.consumer = Consumer(config)

        target_partition = self._get_partition_id(
            self.consumer,
            topic,
            topic_key,
        )
        tp = TopicPartition(topic, target_partition)
        self.consumer.assign([tp])


    def _kafka_murmur2(self, data: bytes):
        """Kafka 官方 Java 版 Murmur2 的 Python 實作"""
        length = len(data)
        seed = 0x9747b28c
        # 'm' and 'r' are mixing constants generated offline.
        # They're not so unique, so they don't have to be random.
        m = 0x5bd1e995
        r = 24

        # Initialize the hash to a 'random' value
        h = seed ^ length
        length_4 = length // 4

        for i in range(length_4):
            i_4 = i * 4
            k = struct.unpack('<I', data[i_4:i_4 + 4])[0]
            k = (k * m) & 0xffffffff
            k ^= (k >> r) & 0xffffffff
            k = (k * m) & 0xffffffff
            h = (h * m) & 0xffffffff
            h ^= k

        # Handle the last few bytes of the input array
        extra_bytes = length % 4
        if extra_bytes == 3:
            h ^= (data[(length & ~3) + 2] << 16) & 0xffffffff
        if extra_bytes >= 2:
            h ^= (data[(length & ~3) + 1] << 8) & 0xffffffff
        if extra_bytes >= 1:
            h ^= (data[length & ~3]) & 0xffffffff
            h = (h * m) & 0xffffffff

        h ^= (h >> 13) & 0xffffffff
        h = (h * m) & 0xffffffff
        h ^= (h >> 15) & 0xffffffff
        return h


    def _get_partition_id(self, consumer, topic_name: str, topic_key: str) -> int:
        """根據 Kafka 的分區邏輯，計算出給定 topic_key 對應的 Partition ID"""

        # 取得分區總數
        metadata = consumer.list_topics(topic=topic_name)
        topic_metadata = metadata.topics.get(topic_name)

        if topic_metadata is None or not topic_metadata.partitions:
            return -1

        num_partitions = len(topic_metadata.partitions)

        # 計算 Partition ID
        target_partition = (self._kafka_murmur2(topic_key.encode('utf-8')) & 0x7fffffff) % num_partitions

        self.logging.info(f"[{topic_key}] 對應 Partition 分區 ID 為: [{target_partition}]")
        return target_partition


    def get(self):
        return self.consumer


    def poll(self, timeout: float=1.0):
        """
        Poll for messages from the Kafka topic.
        :param timeout: Time in seconds to wait for a message before returning None.
        :return: The message value if a message is received, otherwise None.
        """
        msg = self.consumer.poll(timeout)
        if msg is None:
            return None

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # 當前消費完畢 => 目前沒新訊息，繼續等待 ...
                self.logging.info(f"[{self.main_name}] topic: {msg.topic()} | partition: {msg.partition()}")
                return None

            else:
                # 其他錯誤: Broker 斷線、認證失敗 ...etc.
                self.logging.error(f"[{self.main_name}] kafka consumer error: {msg.error()}", exc_info=False)
                raise

        return msg


    def commit(self, asynchronous=False):
        self.consumer.commit(asynchronous=asynchronous)


    def close(self):
        self.consumer.close()
        self.logging.notice(f'[{self.main_name}] 已安全關閉連線 ...', stack_level=0)