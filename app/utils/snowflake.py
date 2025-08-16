"""
Snowflake ID Generator Module

A Python implementation of Twitter's Snowflake algorithm for generating unique,
distributed, and time-ordered 64-bit identifiers. This generator is designed
for distributed systems where multiple nodes need to generate unique IDs without
coordination or collision.

Algorithm Overview:
    The Snowflake algorithm generates 64-bit IDs with the following structure:

    |1 bit|         41 bits        |  10 bits |  12 bits  |
    |sign |      timestamp         | node_id  | sequence  |
    | 0   | milliseconds since epoch| 0-1023   |  0-4095   |

    - Sign bit: Always 0 (positive number)
    - Timestamp: 41 bits = ~69 years of milliseconds from custom epoch
    - Node ID: 10 bits = 1024 possible nodes (0-1023)
    - Sequence: 12 bits = 4096 IDs per millisecond per node

Key Features:
    - **Unique**: Guaranteed unique across all nodes and time
    - **Distributed**: No coordination required between nodes
    - **Time-ordered**: IDs are roughly sortable by creation time
    - **High-throughput**: Up to 4,096 IDs per millisecond per node
    - **Thread-safe**: Uses locking for concurrent access protection
    - **Clock drift protection**: Handles backwards clock movement

Performance Characteristics:
    - Theoretical max: 4,096,000 IDs per second per node
    - Practical throughput: Limited by system clock resolution
    - Memory efficient: Minimal state (last timestamp, sequence counter)
    - Low latency: In-memory generation with minimal computation

Use Cases:
    - Distributed database primary keys
    - Event ordering in distributed systems
    - URL shortening services (like this application)
    - Message queue identifiers
    - Any system requiring unique, ordered identifiers

Thread Safety:
    - Uses threading.Lock() for atomic ID generation
    - Safe for concurrent access from multiple threads
    - Sequence counter protected against race conditions
    - Timestamp comparison and updates are atomic

Clock Considerations:
    - Requires monotonic system clock for correctness
    - Handles same-millisecond generation with sequence counter
    - Detects and prevents backwards clock movement
    - Waits for next millisecond when sequence exhausted

Based on: Twitter's Snowflake algorithm
"""

import threading
import time


class SnowflakeIDGenerator:
    """A thread-safe Snowflake ID generator for creating unique identifiers.

    This class implements Twitter's Snowflake algorithm to generate 64-bit,
    time-ordered, and unique identifiers across a distributed system.

    Attributes:
        node_id: The unique ID for this generator instance (0-1023).
        epoch: The custom epoch timestamp in milliseconds.
    """

    NODE_ID_BITS = 10
    SEQUENCE_BITS = 12
    MAX_NODE_ID = (1 << NODE_ID_BITS) - 1
    MAX_SEQUENCE = (1 << SEQUENCE_BITS) - 1
    NODE_ID_SHIFT = SEQUENCE_BITS
    TIMESTAMP_SHIFT = NODE_ID_BITS + SEQUENCE_BITS

    def __init__(self, node_id: int, epoch: int = 1609459200000):
        """Initializes a new Snowflake ID generator instance.

        Args:
            node_id: A unique identifier for this node (0-1023).
            epoch: The custom epoch timestamp in milliseconds.

        Raises:
            ValueError: If the node_id is outside the valid range (0-1023).
        """
        if not 0 <= node_id <= self.MAX_NODE_ID:
            raise ValueError(f"Node ID must be between 0 and {self.MAX_NODE_ID}")

        self.node_id = node_id
        self.epoch = epoch
        self.sequence = 0
        self.last_timestamp = -1
        self.lock = threading.Lock()

    def _current_timestamp(self) -> int:
        """Returns the current timestamp in milliseconds."""
        return int(time.time() * 1000)

    def _wait_for_next_millis(self, last_timestamp: int) -> int:
        """Waits until the next millisecond if the sequence is exhausted.

        Args:
            last_timestamp: The timestamp of the last generated ID.

        Returns:
            The next millisecond timestamp.
        """
        timestamp = self._current_timestamp()
        while timestamp <= last_timestamp:
            timestamp = self._current_timestamp()
        return timestamp

    def generate_id(self) -> int:
        """Generates a new unique Snowflake ID.

        Returns:
            A 64-bit unique Snowflake ID.

        Raises:
            Exception: If the system clock moves backward.
        """
        with self.lock:
            timestamp = self._current_timestamp()

            if timestamp < self.last_timestamp:
                raise Exception("Clock moved backward. Refusing to generate ID.")

            if timestamp == self.last_timestamp:
                self.sequence = (self.sequence + 1) & self.MAX_SEQUENCE
                if self.sequence == 0:
                    timestamp = self._wait_for_next_millis(self.last_timestamp)
            else:
                self.sequence = 0

            self.last_timestamp = timestamp

            return (
                ((timestamp - self.epoch) << self.TIMESTAMP_SHIFT)
                | (self.node_id << self.NODE_ID_SHIFT)
                | self.sequence
            )
