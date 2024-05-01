import json

from kafka import KafkaConsumer, OffsetAndMetadata, TopicPartition

class KafkaConsumerWrapper:

    def __init__(self, bootstrap_server, topic, partition):
        self.__consumer = KafkaConsumer(bootstrap_servers=bootstrap_server,
                                      value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                                      auto_offset_reset='latest',
                                      enable_auto_commit=False,
                                      group_id=str(partition),
                                      #max_poll_records=1,
                                      consumer_timeout_ms=10000)
        self.__meta = self.__consumer.partitions_for_topic(topic)
        self.__partition = TopicPartition(topic, partition)
        self.__consumer.assign([self.__partition])

    def poll_next(self):
        message = self.__consumer.poll(timeout_ms=5000, max_records=1, update_offsets=False)
        for topic_data, consumer_records in message.items():
            return consumer_records[0]

    def commit(self, message):
        options = {self.__partition: OffsetAndMetadata(message.offset + 1, self.__meta)}
        self.__consumer.commit(options)

    def seek_to_beginning(self):
        return self.__consumer.seek_to_beginning(self.__partition)

    def seek(self, offset):
        return self.__consumer.seek(self.__partition, offset)
