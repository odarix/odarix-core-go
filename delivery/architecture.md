# Transport

## Client architecture

### Essences

```go
// Data - data to be sent.
type Data unsafe.Pointer

// ShardedData - array of structures (*LabelSet, timestamp, value, LSHash)
type ShardedData unsafe.Pointer

// Segment - encoded data segment.
type Segment []byte

// Redundant - information to create Snapshot.
type Redundant unsafe.Pointer

// Snapshot - snapshot of encoder/decoder.
type Snapshot []byte

// SegmentKey - key to store segment in Exchange and Refill.
type SegmentKey struct {
    ShardID uint16
    Segment uint32
}

// AckStatus - numbers of last confirmed segments by shards and destinations.
type AckStatus interface {
    Ack(key SegmentKey, dest string)
    Last(shardID uint16, dest string) uint32
}

+--------------------------------+
|              Frame             |
|+------------------------------+|
||             Header           ||
|+------------------------------+|
||            11 byte           ||
||      1 typeFrame uint8       ||
||      2 shardID uint16        ||
||      4 segmentID uint32      ||
||      4   size uint32         ||
|+------------------------------+|
|+------------------------------+|
||             Body             ||
|+------------------------------+|
||             Title            ||
|+--------------or--------------+|
||       DestinationNames       ||
|+--------------or--------------+|
|| BinaryBody(Snapshot/Segment) ||
|+--------------or--------------+|
||           Statuses           ||
|+--------------or--------------+|
||        RejectStatuses        ||
|+------------------------------+|
+--------------------------------+

// Frame - frame for write file.
type Frame struct {
	header *Header
	body   []byte
}

// Header - header frame.
// typeFrame - type of frame:
//    title type frame
//    destination names type frame
//    snapshot type frame
//    segment type frame
//    destinations states type frame
//    reject statuses type frame
// shardID - number of shard
// segmentID - number of segment
// size - body size
type Header struct {
	typeFrame TypeFrame
	shardID   uint16
	segmentID uint32
	size      uint32
}

// File write structure.
// First frame - Title, contains:
//   blockID - ID current block
//   shardsNumberPower - number of shards as a power of 2
// Second frame - DestinationsNames, contains:
//   names of destinations writer
// N frame - BinaryBody, contains:
//   binary data unsent segment/snapshot for save refill
// M frame - Statuses, contains:
//   state of writers, last confirmed sent segment
//   written only if different from previous states
// L frame - RejectStatuses, contains:
//   rejected segments with destinations name, segmentID, shardID
//   write only if there is something to write

+---------------------------------------------------------------------------------------------------+
|                                                FILE                                               |
|+------------------+-------------------+------------------------------+----------------+----------+|
||    First frame   |    Second frame   |            N frame           |    L frame     | M frame  ||
|+------------------+-------------------+------------------------------+----------------+----------+|
||  Title(17 byte)  | DestinationsNames | BinaryBody(Segment/Snapshot) | RejectStatuses | Statuses ||
|+------------------+-------------------+------------------------------+----------------+----------+|
+---------------------------------------------------------------------------------------------------+

```

```mermaid
---
title: Classes
---
classDiagram
    class DeliveryManagerKeeper {
        Send(ShardedData) SendPromise
        -Rotate(context.Context)
        Shutdown(context.Context)
    }

    DeliveryManagerKeeper --> "1..2" DeliveryManager

    class Sharder {
        Sharding(Data) ShardedData
    }

    class RefillSender {
        Shutdown(context.Context)
    }

    RefillSender --> Refill

    class DeliveryManager {
        Send(ShardedData) SendPromise
        Get(SegmentKey) Segment // synchronously returns the requested segment
        Ack(SegmentKey key, string destinationName)
        Reject(SegmentKey, string destinationName)
        Restore(SegmentKey) (Snapshot, []Segment)
        Shutdown(context.Context)
    }

    DeliveryManager --> "1..*" Encoder
    DeliveryManager --> Exchange
    DeliveryManager --> "1..*" Sender
    Sender ..|> DeliveryManager : Get|Restore|Ack
    DeliveryManager --> Refill

    class Encoder {
        uint16 ShardID
        GetLastEncodedSegment() uint32
        Encode(ShardedData) (Segment, Redundant)
        Snapshot([]Redundant) Snapshot
    }

    class Exchange {
        Get(SegmentKey) Segment
        Put(SegmentKey, Segment, Redundant) SendSegmentPromise
        Ack(SegmentKey key, string destinationName)
        Reject(SegmentKey, string destinationName)
        Rejected() []SegmentKey
        Remove([]SegmentKey)
        Redundant(SegmentKey key) Redundant
        Shutdown(context.Context)
    }

    class Sender {
        string Name
        uint16 ShardID
        Shutdown(context.Context)
    }

    class Refill {
        Get(SegmentKey) Segment
        Ack(SegmentKey key, string destinationName)
        Reject(SegmentKey, string destinationName)
        Restore(SegmentKey) (Snapshot, []Segment)
        WriteSnapshot(uint16 shardID, Snapshot snapshot)
        WriteSegment(SegmentKey, Segment)
        WriteAckStatus(AckStatus)
    }
```

```mermaid
---
title: Encode and put in exchange
---
sequenceDiagram
    participant Producer
    participant Keeper as DeliveryManagerKeeper
    participant Manager as DeliveryManager
    participant Encoder
    participant Exchange

    Producer ->> +Keeper: Send(ShardedData)
    Note over Keeper: get current DeliveryManager with mutex
    Keeper ->> +Manager: Send(ShardedData)
    deactivate Keeper

    Note over Manager: make channel for statuses `res`
    par for each encoder
        Manager ->> +Encoder: Encode(ShardedData)
        Encoder -->> -Manager: Segment, Redundant
        Manager ->> Exchange: Put(Segment, Redundant, res)
    end
    Note over Manager: wait statuses from `res`

    Manager -->> -Keeper: <true|false>
    Keeper -->> Producer: <true|false>
```

```mermaid
---
title: Sender loop
---
sequenceDiagram
    participant Encoder
    participant Refill
    participant Exchange
    participant Manager as DeliveryManager
    participant Sender
    participant Server

    loop
        Note over Sender: create key by shardID<br/>and last sent segment + 1
        Sender ->> Manager: Get(key)
        Manager ->> Exchange: Get(key)
        alt if key.Segment > lastPulled
            Note over Exchange: await Segment
            Exchange -->> Manager: Segment
        else if key in Exchange
            Exchange -->> Manager: Segment
        else otherwise
            Exchange -->> Manager: <nil>
            Note over Refill,Exchange: segment should be in Refill
            Manager ->> Refill: Get(key)
            Refill -->> Manager: Segment
        end
        Manager -->> Sender: Segment
        Sender ->> Server: Segment
    end
```

```mermaid
---
title: On server ack
---
sequenceDiagram
    participant Exchange
    participant Manager as DeliveryManager
    participant Sender
    participant Server

    Server -->> Sender: <ack, segment>
    Note over Sender: create key by shardID<br/>and server segment
    Sender ->> Manager: Ack(key)
    Note over Manager: update AckState in memory
    Manager ->> Exchange: Ack(key)
    Note over Exchange: update segment ack-state if exists

    Note over Manager,Exchange: First Ack in memory,<br/>because in Refill-loop<br/>we remove segment from Exchange<br/>after append it to Refill

    Sender ->> Sender: update lastAck
```

```mermaid
---
title: On send error
---
sequenceDiagram
    participant Exchange
    participant Manager as DeliveryManager
    participant Sender

    loop for i = (lastAck, lastSent]
        Note over Sender: create key by shardID and i
        Sender ->> Manager: Reject(key)
        Manager ->> Exchange: Reject(key)
        Manager ->> Manager: schedule refill
    end
    Note over Exchange: Rejected segments stay in Exchange until Refill-loop collects them and removes
```

```mermaid
---
title: Refill-loop
---
sequenceDiagram
    participant Exchange
    participant Manager as DeliveryManager
    participant Refill

    Manager ->> Exchange: Rejected()
    Exchange -->> Manager: Keys
    Note over Manager: sort Keys by shard id and segment
    Note over Manager: if Keys is empty, skip next loop
    loop for key in Keys
        Manager ->> Exchange: Get(key)
        break if segment not in Exchange
            Exchange -->> Manager: <nil>
            Note over Manager: skip this key
        end
        Exchange -->> Manager: Segment
        Manager ->> Refill: WriteSegment(Segment)
        alt if previous segment or snapshot in Refill
            Refill -->> Manager: <ok>
        else otherwise
            Refill -->> Manager: <unknown shard error>
            Manager ->> Manager: make snapshot
            Manager ->> Refill: WriteSnapshot(Snapshot)
            Refill -->> Manager: <ok>
            Manager ->> Refill: WriteSegment(Segment)
            Refill -->> Manager: <ok>
        end
        Note over Manager: if an error occurred on segment processing<br/>skip all remained segments with same shardID
    end
    Manager ->> Refill: WriteAckState(AckState)
    Note over Refill: if all shards obsolete, remove file
    Refill -->> Manager: <ok>
    Manager ->> Exchange: Remove(Keys)
```

```mermaid
---
title: make snapshot
---
sequenceDiagram
    participant Encoder
    participant Exchange
    participant Manager as DeliveryManager

    Note over Manager: take snapshot by Key
    critical lock input
        Note over Manager: await current encoding operations
        Manager ->> Encoder: GetLastEncodedSegment()
        Encoder -->> Manager: Last
        Note over Manager: if Key.Segment > Last then Last = Key.Segment
        loop for i = [key.Segment, Last]
            Manager ->> Exchange: Redundant(Key.ShardID, i)
            Exchange -->> Manager: Redundant
        end
        Manager ->> Encoder: Snapshot(Redundants)
        Encoder -->> Manager: Snapshot
    end
```

```mermaid
---
title: On re-connect
---
sequenceDiagram
    participant Encoder
    participant Refill
    participant Exchange
    participant Manager as DeliveryManager
    participant Sender
    participant Server

    Note over Sender: create Key for next of last ack segment
    Sender ->> Manager: Restore(Key)
    Manager ->> Refill: Restore(Key)
    alt if shard in Refill
        Refill -->> Manager: Snapshot, Segments
        Manager -->> Sender: Snapshot, Segments
    else otherwise
        Refill -->> Manager: <unknown shard error>
        Manager ->> Manager: make snapshot
        alt if snapshot is successfully made
            Manager -->> Sender: Snapshot, []
        else otherwise
            Note over Manager,Refill: if an error occurred then try again get from refill
            Manager ->> Refill: Restore(Key)
            Refill -->> Manager: Snapshot, Segments
            Manager -->> Sender: Snapshot, Segments
        end
    end
    Sender ->> Server: Snapshot, Segments

    Note over Sender: continue Sender loop
```

```mermaid
---
title: Shutdown
---
sequenceDiagram
    participant Refill
    participant Exchange
    participant Manager as DeliveryManager
    participant Sender

    Manager ->> Exchange: Shutdown
    Note over Exchange: lock, then cancel and remove all unresolved promises
    Exchange -->> Manager: <ok>
    alt if Manager is opened (refill and senders loops)
        Manager ->> Manager: stop refill loop
    else
        Manager ->> Excnahge: Reject all promises
    end
    loop while there are promises in Exchange
        Manager ->> Manager: do refill-loop body
    end
    par for all Senders
        Manager ->> Sender: Shutdown
    end
    Manager ->> Refill: Shutdown
```

1. Pass the destinations list to the Refill constructor.
2.1. If the file does not exist, then we form a new AckStatus based on those passed to the constructor
2.2. If the file exists, then we subtract the destinations list from it
2.2.1 If the list from the file is different from the one passed to the constructor, then we mark refill as not Continuable
2.2.2 AckStatus is always formed using a list from a file
2.2.3 Read last AckFrame
2.2.4 If refill.IsContinuable, then you need to check that all shards are there from the last hit segment, otherwise not Continuable
