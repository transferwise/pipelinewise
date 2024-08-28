import os
import confluent_kafka

class KafkaConsumerMessageMock:
    def __init__(self, topic, value, timestamp=None, key=None, timestamp_type=0, partition=0, offset=1, headers=None,
                 checksum=None, serialized_key_size=None, serialized_value_size=None, serialized_header_size=None):
        if headers is None:
            headers = []
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._timestamp = timestamp
        self._timestamp_type = timestamp_type
        self._key = key
        self._value = value
        self._headers = headers
        self._checksum = checksum
        self._serialized_key_size = serialized_key_size
        self._serialized_value_size = serialized_value_size
        self._serialized_header_size = serialized_header_size

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def timestamp(self):
        return self._timestamp

    def key(self):
        return self._key

    def value(self):
        return self._value


def _create_fake_kafka_message(fake_message):
    return KafkaConsumerMessageMock(
        topic=fake_message.get('topic'),
        offset=fake_message.get('offset'),
        timestamp=fake_message.get('timestamp'),
        value=fake_message.get('value'),
        partition=fake_message.get('partition'),
        timestamp_type=fake_message.get('timestamp_type'),
        key=fake_message.get('key'),
        headers=fake_message.get('headers'),
        checksum=fake_message.get('checksum'),
        serialized_key_size=fake_message.get('serialized_key_size'),
        serialized_value_size=fake_message.get('serialized_value_size'),
        serialized_header_size=fake_message.get('serialized_header_size'),
    )


class KafkaConsumerMock(object):
    def __init__(self, fake_messages):
        self.fake_messages = fake_messages
        self.fake_messages_pos = 0
        self.committed_offsets = []
        self.assigned_offsets = []

    def poll(self, timeout):
        if self.fake_messages_pos > len(self.fake_messages) - 1:
            return None
        else:
            self.fake_messages_pos += 1
            current_fake_message = self.fake_messages[self.fake_messages_pos - 1]
            return _create_fake_kafka_message(current_fake_message)

    def assign(self, offsets=None):
        if offsets:
            self.assigned_offsets = offsets

    def commit(self, *args, **kwargs):

        if 'offsets' in kwargs:
            self.committed_offsets = kwargs['offsets']

    def offsets_for_times(self, topic_partitions):
        if topic_partitions[0].offset == 1638132327000:
            topic_partitions[0].offset = 1234

        return topic_partitions
