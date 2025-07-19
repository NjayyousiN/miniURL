import time
import threading


class SnowflakeIDGenerator:
    def __init__(self, node_id, epoch=1609459200000):
        """
        Snowflake ID Generator
        This class generates unique 64-bit IDs based on the Snowflake algorithm.

        :param node_id: A unique identifier for the node (usually between 0 and 1023).
        :param epoch: A custom epoch to start counting from (in milliseconds).
        """

        self.node_id = node_id
        self.epoch = epoch
        self.sequence = 0
        self.last_timestamp = -1

        # Constants
        self.node_id_bits = 10
        self.sequence_bits = 12

        self.max_node_id = -1 ^ (-1 << self.node_id_bits)
        self.max_sequence = -1 ^ (-1 << self.sequence_bits)

        self.node_id_shift = self.sequence_bits
        self.timestamp_shift = self.node_id_bits + self.sequence_bits

        self.lock = threading.Lock()

        if self.node_id < 0 or self.node_id > self.max_node_id:
            raise ValueError(f"Node ID must be between 0 and {self.max_node_id}")

    def _current_timestamp(self):
        return int(time.time() * 1000)

    def _wait_for_next_millis(self, last_timestamp):
        timestamp = self._current_timestamp()
        while timestamp <= last_timestamp:
            timestamp = self._current_timestamp()
        return timestamp

    def generate_id(self):
        """
        Generate a new Snowflake ID.

        :return: A unique 64-bit ID.
        """
        with self.lock:
            timestamp = self._current_timestamp()

            if timestamp < self.last_timestamp:
                raise Exception("Clock moved backwards. Refusing to generate ID.")

            if timestamp == self.last_timestamp:
                self.sequence = (self.sequence + 1) & self.max_sequence
                if self.sequence == 0:
                    timestamp = self._wait_for_next_millis(self.last_timestamp)
            else:
                self.sequence = 0

            self.last_timestamp = timestamp

            id_ = (
                ((timestamp - self.epoch) << self.timestamp_shift)
                | (self.node_id << self.node_id_shift)
                | self.sequence
            )

            return id_
