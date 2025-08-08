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

import time
import threading


class SnowflakeIDGenerator:
    """
    Thread-safe Snowflake ID generator for distributed unique identifier creation.

    This class implements Twitter's Snowflake algorithm to generate unique 64-bit
    identifiers that are roughly time-ordered and guaranteed unique across multiple
    nodes in a distributed system. Each ID encodes timestamp, node identifier,
    and sequence number for collision-free generation.

    The generator can produce up to 4,096 unique IDs per millisecond per node,
    making it suitable for high-throughput applications. Thread safety is ensured
    through internal locking mechanisms.

    Attributes:
        node_id (int): Unique identifier for this generator instance (0-1023).
        epoch (int): Custom epoch timestamp in milliseconds for ID generation.
        sequence (int): Current sequence number within the current millisecond.
        last_timestamp (int): Timestamp of the last generated ID for clock validation.

    Constants:
        node_id_bits (int): Number of bits allocated for node ID (10 bits = 1024 nodes).
        sequence_bits (int): Number of bits for sequence (12 bits = 4096 per ms).
        max_node_id (int): Maximum valid node ID value (1023).
        max_sequence (int): Maximum sequence value per millisecond (4095).

    Thread Safety:
        All ID generation operations are protected by a threading.Lock to ensure
        atomic access to sequence counters and timestamp validation.
    """

    def __init__(self, node_id, epoch=1609459200000):
        """
        Initialize a new Snowflake ID generator instance.

        Creates a new generator with the specified node ID and epoch. The node ID
        must be unique across all generator instances in your distributed system
        to ensure ID uniqueness. The epoch defines the starting point for timestamp
        calculations and should be consistent across all nodes.

        Args:
            node_id (int): Unique identifier for this node/instance. Must be between
                          0 and 1023 (10-bit value). Each node in your distributed
                          system must have a different node_id.
            epoch (int, optional): Custom epoch timestamp in milliseconds since
                                  Unix epoch. Defaults to 1609459200000 (Jan 1, 2021).
                                  Should be set to a date before your system starts
                                  generating IDs.

        Raises:
            ValueError: If node_id is outside the valid range (0-1023).

        Example:
            >>> # Create generator for node 1 with default epoch
            >>> generator = SnowflakeIDGenerator(node_id=1)
            >>>
            >>> # Create generator with custom epoch (Jan 1, 2020)
            >>> custom_epoch = int(datetime(2020, 1, 1).timestamp() * 1000)
            >>> generator = SnowflakeIDGenerator(node_id=5, epoch=custom_epoch)

        Note:
            The epoch should be set to a timestamp before your system starts
            generating IDs, as it defines the "zero point" for ID timestamps.
            Using a recent epoch maximizes the lifespan of your ID space.
        """
        self.node_id = node_id
        self.epoch = epoch
        self.sequence = 0
        self.last_timestamp = -1

        # Bit allocation constants for Snowflake algorithm
        self.node_id_bits = 10  # 2^10 = 1024 possible nodes
        self.sequence_bits = 12  # 2^12 = 4096 IDs per millisecond

        # Calculate maximum values for validation
        self.max_node_id = -1 ^ (-1 << self.node_id_bits)  # 1023
        self.max_sequence = -1 ^ (-1 << self.sequence_bits)  # 4095

        # Bit shift positions for ID composition
        self.node_id_shift = self.sequence_bits  # 12 bits
        self.timestamp_shift = self.node_id_bits + self.sequence_bits  # 22 bits

        # Thread safety lock for concurrent access
        self.lock = threading.Lock()

        # Validate node_id range
        if self.node_id < 0 or self.node_id > self.max_node_id:
            raise ValueError(f"Node ID must be between 0 and {self.max_node_id}")

    def _current_timestamp(self):
        """
        Get the current timestamp in milliseconds since Unix epoch.

        This internal method provides the current time in milliseconds, which is
        used as the timestamp component of generated Snowflake IDs. The timestamp
        is adjusted relative to the custom epoch defined during initialization.

        Returns:
            int: Current timestamp in milliseconds since Unix epoch.

        Note:
            This method relies on system clock accuracy. Clock drift or backwards
            movement can affect ID generation and is detected by the main generation
            logic.
        """
        return int(time.time() * 1000)

    def _wait_for_next_millis(self, last_timestamp):
        """
        Wait until the next millisecond when sequence counter is exhausted.

        This method is called when the sequence counter reaches its maximum value
        (4095) within a single millisecond. It blocks until the system clock
        advances to the next millisecond, ensuring that ID generation can continue
        without collisions.

        This situation occurs during high-throughput scenarios where more than
        4,096 IDs are requested within a single millisecond on a single node.

        Args:
            last_timestamp (int): The timestamp of the last generated ID in milliseconds.

        Returns:
            int: The next millisecond timestamp that is greater than last_timestamp.

        Side Effects:
            - Blocks execution until system clock advances
            - May cause slight delays during extreme high-throughput periods

        Example:
            >>> # Internal usage when sequence is exhausted
            >>> next_ts = generator._wait_for_next_millis(current_timestamp)
            >>> # Execution pauses until clock advances to next millisecond

        Note:
            This method ensures temporal ordering of IDs and prevents sequence
            overflow. The blocking behavior is typically very brief (< 1ms) but
            can impact latency during sustained high-throughput periods.
        """
        timestamp = self._current_timestamp()
        while timestamp <= last_timestamp:
            timestamp = self._current_timestamp()
        return timestamp

    def generate_id(self):
        """
        Generate a new unique Snowflake ID.

        This method creates a new 64-bit Snowflake ID that is guaranteed to be
        unique across all nodes and time. The ID encodes the current timestamp,
        node identifier, and sequence number into a single integer.

        Generation Process:
            1. Acquire thread lock for atomic operation
            2. Get current timestamp and validate against clock drift
            3. Handle sequence numbering for same-millisecond requests
            4. Compose final ID by bit-shifting and combining components
            5. Update internal state and release lock

        Thread Safety:
            The entire generation process is protected by a threading lock,
            ensuring that concurrent calls from multiple threads produce
            unique IDs without race conditions.

        Clock Drift Protection:
            If the system clock moves backwards, the generator raises an
            exception rather than potentially creating duplicate IDs.

        Sequence Handling:
            - Same millisecond: Increment sequence counter (0-4095)
            - Sequence overflow: Wait for next millisecond and reset to 0
            - New millisecond: Reset sequence to 0

        Returns:
            int: A unique 64-bit Snowflake ID with the following structure:
                 - Bits 63-22: Timestamp (milliseconds since epoch)
                 - Bits 21-12: Node ID (identifies the generator instance)
                 - Bits 11-0: Sequence number (counter within millisecond)

        Raises:
            Exception: If system clock moves backwards, indicating potential
                      clock synchronization issues that could cause ID collisions.

        Side Effects:
            - Updates internal sequence counter and timestamp state
            - Acquires and releases thread lock for atomic operation
            - May block briefly if sequence overflow occurs (waits for next ms)

        Example:
            >>> generator = SnowflakeIDGenerator(node_id=1)
            >>> id1 = generator.generate_id()
            >>> id2 = generator.generate_id()
            >>> print(f"Generated IDs: {id1}, {id2}")
            >>> # IDs are unique and roughly time-ordered
            >>> assert id1 != id2  # Always true
            >>> assert id2 > id1   # Usually true (time-ordered)

        Performance:
            - Single ID generation: ~1-2 microseconds
            - Throughput: Up to 4,096 IDs per millisecond per node
            - Memory usage: Minimal (only stores last timestamp and sequence)
            - Thread contention: Minimal due to fast lock acquisition

        Note:
            This generator is suitable for high-throughput applications but may
            introduce slight latency during extreme burst scenarios when sequence
            overflow occurs. For maximum performance in single-threaded environments,
            consider removing the lock mechanism.
        """
        with self.lock:
            # Get current timestamp in milliseconds
            timestamp = self._current_timestamp()

            # Detect backwards clock movement (potential system clock issues)
            if timestamp < self.last_timestamp:
                raise Exception(
                    f"Clock moved backwards. Refusing to generate ID. "
                    f"Last: {self.last_timestamp}, Current: {timestamp}"
                )

            # Handle same-millisecond ID generation
            if timestamp == self.last_timestamp:
                # Increment sequence for same millisecond
                self.sequence = (self.sequence + 1) & self.max_sequence

                # Sequence overflow - wait for next millisecond
                if self.sequence == 0:
                    timestamp = self._wait_for_next_millis(self.last_timestamp)
            else:
                # New millisecond - reset sequence counter
                self.sequence = 0

            # Update last timestamp for next generation
            self.last_timestamp = timestamp

            # Compose 64-bit Snowflake ID by bit-shifting and combining components
            # Structure: [1 sign][41 timestamp][10 node_id][12 sequence]
            id_ = (
                ((timestamp - self.epoch) << self.timestamp_shift)  # Timestamp bits
                | (self.node_id << self.node_id_shift)  # Node ID bits
                | self.sequence  # Sequence bits
            )

            return id_
