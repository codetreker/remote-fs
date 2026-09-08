package httprest

import (
	"fmt"
	"math"
	"time"
)

const (
	minimumMaxBodyBytes  int64 = 1024
	minimumMaxFrameBytes int64 = 1024
	maximumMaxFrameBytes int64 = 1 << 30
	maximumStreams             = 1 << 16
	maximumPageRows            = 1 << 20

	// DefaultMaxBodyBytes is the protocol-wide ceiling used for one non-streaming HTTP
	// body when neither end is given a different one. One gibibyte matches the default
	// ceiling on a file held whole by the mount and keeps an omitted setting bounded.
	DefaultMaxBodyBytes int64 = 1 << 30

	// DefaultMaxInFlightBodyBytes bounds aggregate request-body retention to two
	// gibibytes. Each admitted request reserves its full operation-specific ceiling
	// because an unknown or false Content-Length cannot safely reserve less before it is
	// read.
	DefaultMaxInFlightBodyBytes int64 = 2 << 30

	// DefaultMaxConcurrentBodies is the operation bound paired with the aggregate byte
	// bound. The byte bound is normally tighter for large bodies; this one also bounds a
	// deployment configured with many small bodies.
	DefaultMaxConcurrentBodies = 64

	// DefaultMaxWaitingBodies bounds goroutines waiting for request-body admission.
	// Further requests fail with EAGAIN and can be retried without having consumed a body.
	DefaultMaxWaitingBodies = 64

	// retainedResponseMultiplier reserves room for the storage listing, its wire-shaped
	// entries, the encoded body, and bounded conversion overhead at the same time. Reads
	// use less, but one conservative charge keeps aggregate accounting independent of an
	// operation's representation details.
	retainedResponseMultiplier int64 = 4
	retainedFrameMultiplier    int64 = 3

	DefaultMaxConcurrentResponses         = 64
	DefaultMaxWaitingResponses            = 64
	DefaultMaxInFlightResponseBytes int64 = retainedResponseMultiplier * 2 * DefaultMaxBodyBytes

	// DefaultMaxFrameBytes is the largest encoded replication frame a handler produces
	// and a client accepts when neither side selects another bound.
	DefaultMaxFrameBytes int64 = 8 << 20

	DefaultMaxConcurrentSnapshotFrames         = 16
	DefaultMaxWaitingSnapshotFrames            = 64
	DefaultMaxInFlightSnapshotFrameBytes int64 = retainedFrameMultiplier * DefaultMaxConcurrentSnapshotFrames * DefaultMaxFrameBytes
)

// Limits are the bounds a Handler enforces.
//
// Replication adds long-lived connections and snapshots that retain a resource inside
// the metastore while their rows cross the wire.
type Limits struct {
	// MaxSubscriptions is how many change streams may be attached at once. Saturation is
	// refused immediately with EAGAIN; queuing a stream would merely exchange one
	// retained channel and connection for an unbounded waiter.
	MaxSubscriptions int

	// Snapshots is how many snapshots may be open at once. A request arriving when they
	// all are is refused with EAGAIN rather than queued, because a replica subscribes
	// before it asks for one — so retrying costs it nothing and loses it nothing, and the
	// server holding the request open would be holding exactly what this bounds.
	Snapshots int

	// SnapshotDeadline is how long one snapshot may take to deliver, after which it is
	// abandoned and what it held is released. A snapshot cannot be resumed — a consistent
	// picture that is gone is gone — so a replica that runs into this starts over.
	//
	// It bounds the whole delivery and has nothing to do with the bound a reader keeps on a
	// quiet stream, which Keepalive answers. Reading the two as a pair is a mistake worth
	// naming, because they look like one: this being the larger figure suggests a slow
	// picture is tolerated, when what decides that is whether anything is being said
	// meanwhile. A picture may take all of this as long as it keeps speaking.
	SnapshotDeadline time.Duration

	// SnapshotPage is how many rows travel in one frame. The snapshot is the largest bulk
	// transfer this system has, and sending it whole would occupy the connection for its
	// whole length.
	SnapshotPage int

	// EventPage is how many changes one read of the log may return while a subscription
	// catches up.
	EventPage int

	// Keepalive is how often a stream with nothing to say says so — both kinds, because
	// both can be quiet for reasons that are nobody's fault. A namespace nobody is writing
	// to produces no events, and a store working through a large tree produces no page for
	// as long as it takes.
	//
	// It is what makes a live stream distinguishable from a dead one. Neither of those
	// silences looks any different from a connection a firewall dropped, a machine that
	// vanished, or a partition — the reader sees the same thing in all of them, which is
	// nothing at all. A replica that could not tell them apart would go on answering from a
	// copy it can no longer justify, for as long as the mistake lasted, which is what
	// R-ERR-1 and R-ERR-2 forbid above everything else.
	//
	// It is paired with the bound the reading end keeps: DefaultSilence is three times this
	// figure. The two are configured separately, so raising this above what a client allows
	// severs every one of that client's streams on a timer — which is loud rather than
	// silent, and is the direction to err in. SnapshotDeadline is not part of that pairing
	// and must not be read as though it were; what it bounds is said there.
	Keepalive time.Duration
}

// DefaultLimits returns the replication bounds NewHandler uses.
func DefaultLimits() Limits {
	return Limits{
		MaxSubscriptions: 64,
		Snapshots:        8,
		SnapshotDeadline: 5 * time.Minute,
		SnapshotPage:     1024,
		EventPage:        256,
		Keepalive:        10 * time.Second,
	}
}

func (l Limits) check() error {
	for _, bound := range []struct {
		name  string
		value int64
		max   int64
	}{
		{"Snapshots", int64(l.Snapshots), maximumStreams},
		{"MaxSubscriptions", int64(l.MaxSubscriptions), maximumStreams},
		{"SnapshotDeadline", int64(l.SnapshotDeadline), math.MaxInt64},
		{"SnapshotPage", int64(l.SnapshotPage), maximumPageRows},
		{"EventPage", int64(l.EventPage), maximumPageRows},
		{"Keepalive", int64(l.Keepalive), math.MaxInt64},
	} {
		if bound.value <= 0 {
			return fmt.Errorf("httprest: Limits.%s is %d, and every bound has to leave room for one of whatever it bounds", bound.name, bound.value)
		}
		if bound.value > bound.max {
			return fmt.Errorf("httprest: Limits.%s is %d, above the bounded maximum of %d", bound.name, bound.value, bound.max)
		}
	}
	return nil
}

// HandlerOptions configure request-body, non-streaming response, and replication resources
// retained by a Handler.
type HandlerOptions struct {
	// Files bounds retained sessions, references, and replay records. Zero selects DefaultFileLimits.
	Files FileLimits

	// Lock control admission is independent of bulk bodies and replication. Zero selects
	// the corresponding default; each active call reserves four fixed control bodies.
	MaxConcurrentLockControls int
	MaxWaitingLockControls    int

	// Replication bounds change and snapshot streams. A zero value selects DefaultLimits;
	// otherwise every field must be positive.
	Replication Limits

	// MaxBodyBytes is the largest non-write request or response body the handler accepts.
	// An oversized read is refused with EFBIG, an oversized listing with EIO, and an
	// oversized attribute request with HTTP 413.
	//
	// Zero selects DefaultMaxBodyBytes. There is no value for an unbounded body.
	MaxBodyBytes int64

	// MaxWriteBytes is the largest file-content request accepted by OpWrite. A larger
	// write is refused with EFBIG before storage is called. Zero selects MaxBodyBytes;
	// a non-zero value must not exceed MaxBodyBytes.
	MaxWriteBytes int64

	// MaxConcurrentBodies is how many request-body reads may be active at once, including
	// an unknown-length body being checked for required emptiness. A request waits for a
	// slot before reading, and cancellation ends that wait. Together with
	// MaxInFlightBodyBytes this bounds aggregate request-body retention.
	//
	// Zero selects DefaultMaxConcurrentBodies.
	MaxConcurrentBodies int

	// MaxWaitingBodies is how many requests may wait for body admission. When the bound
	// is full another request fails immediately with EAGAIN. Zero selects
	// DefaultMaxWaitingBodies.
	MaxWaitingBodies int

	// MaxInFlightBodyBytes bounds request bodies retained across concurrent operations.
	// Every admitted body reserves the limit for its operation until that buffer is
	// released, so this must be at least MaxBodyBytes. Zero selects
	// DefaultMaxInFlightBodyBytes.
	MaxInFlightBodyBytes int64

	// MaxConcurrentResponses bounds operations retaining a non-streaming result.
	// MaxInFlightResponseBytes bounds the combined storage result, wire conversion and
	// encoded body retained by those operations. Read and List reserve four times
	// MaxBodyBytes, the conservative simultaneous peak for List; every other
	// non-streaming operation reserves MaxBodyBytes for its success or error JSON. Zero
	// selects the corresponding default.
	MaxConcurrentResponses   int
	MaxInFlightResponseBytes int64
	MaxWaitingResponses      int

	// MaxFrameBytes bounds every encoded replication frame. Change/start frame retention is
	// bounded independently per subscription, so a slow subscriber cannot consume capacity
	// another subscriber or a snapshot needs.
	MaxFrameBytes int64

	// MaxConcurrentSnapshotFrames and MaxInFlightSnapshotFrameBytes bound snapshot pages
	// being produced across snapshots, including the metastore result, wire conversion and
	// encoded frame retained at the same time. MaxWaitingSnapshotFrames bounds producer
	// goroutines waiting for that admission. Fixed control frames are bounded by the
	// subscription/snapshot counts and fault detail is truncated before encoding. Zero
	// selects the corresponding bounded default.
	MaxConcurrentSnapshotFrames   int
	MaxInFlightSnapshotFrameBytes int64
	MaxWaitingSnapshotFrames      int
}

// DefaultHandlerOptions returns the bounds NewHandler uses. MaxWriteBytes remains zero so
// it continues to inherit MaxBodyBytes if the returned protocol limit is changed.
func DefaultHandlerOptions() HandlerOptions {
	return HandlerOptions{
		MaxConcurrentLockControls:     DefaultMaxConcurrentLockControls,
		MaxWaitingLockControls:        DefaultMaxWaitingLockControls,
		Replication:                   DefaultLimits(),
		MaxBodyBytes:                  DefaultMaxBodyBytes,
		MaxConcurrentBodies:           DefaultMaxConcurrentBodies,
		MaxWaitingBodies:              DefaultMaxWaitingBodies,
		MaxInFlightBodyBytes:          DefaultMaxInFlightBodyBytes,
		MaxConcurrentResponses:        DefaultMaxConcurrentResponses,
		MaxInFlightResponseBytes:      DefaultMaxInFlightResponseBytes,
		MaxWaitingResponses:           DefaultMaxWaitingResponses,
		MaxFrameBytes:                 DefaultMaxFrameBytes,
		MaxConcurrentSnapshotFrames:   DefaultMaxConcurrentSnapshotFrames,
		MaxInFlightSnapshotFrameBytes: DefaultMaxInFlightSnapshotFrameBytes,
		MaxWaitingSnapshotFrames:      DefaultMaxWaitingSnapshotFrames,
	}
}

type handlerOptions struct {
	maxConcurrentLockControls     int
	maxWaitingLockControls        int
	replication                   Limits
	maxBodyBytes                  int64
	maxWriteBytes                 int64
	maxConcurrentBodies           int
	maxWaitingBodies              int
	maxInFlightBodyBytes          int64
	maxConcurrentResponses        int
	maxInFlightResponseBytes      int64
	maxWaitingResponses           int
	maxFrameBytes                 int64
	maxConcurrentSnapshotFrames   int
	maxInFlightSnapshotFrameBytes int64
	maxWaitingSnapshotFrames      int
}

func (o HandlerOptions) settle() handlerOptions {
	settled := handlerOptions{
		maxConcurrentLockControls:     o.MaxConcurrentLockControls,
		maxWaitingLockControls:        o.MaxWaitingLockControls,
		replication:                   o.Replication,
		maxBodyBytes:                  o.MaxBodyBytes,
		maxWriteBytes:                 o.MaxWriteBytes,
		maxConcurrentBodies:           o.MaxConcurrentBodies,
		maxWaitingBodies:              o.MaxWaitingBodies,
		maxInFlightBodyBytes:          o.MaxInFlightBodyBytes,
		maxConcurrentResponses:        o.MaxConcurrentResponses,
		maxInFlightResponseBytes:      o.MaxInFlightResponseBytes,
		maxWaitingResponses:           o.MaxWaitingResponses,
		maxFrameBytes:                 o.MaxFrameBytes,
		maxConcurrentSnapshotFrames:   o.MaxConcurrentSnapshotFrames,
		maxInFlightSnapshotFrameBytes: o.MaxInFlightSnapshotFrameBytes,
		maxWaitingSnapshotFrames:      o.MaxWaitingSnapshotFrames,
	}
	if settled.maxConcurrentLockControls == 0 {
		settled.maxConcurrentLockControls = DefaultMaxConcurrentLockControls
	}
	if settled.maxWaitingLockControls == 0 {
		settled.maxWaitingLockControls = DefaultMaxWaitingLockControls
	}
	if settled.replication == (Limits{}) {
		settled.replication = DefaultLimits()
	}
	if settled.maxBodyBytes == 0 {
		settled.maxBodyBytes = DefaultMaxBodyBytes
	}
	if settled.maxWriteBytes == 0 {
		settled.maxWriteBytes = settled.maxBodyBytes
	}
	if settled.maxConcurrentBodies == 0 {
		settled.maxConcurrentBodies = DefaultMaxConcurrentBodies
	}
	if settled.maxWaitingBodies == 0 {
		settled.maxWaitingBodies = DefaultMaxWaitingBodies
	}
	if settled.maxInFlightBodyBytes == 0 {
		settled.maxInFlightBodyBytes = DefaultMaxInFlightBodyBytes
	}
	if settled.maxConcurrentResponses == 0 {
		settled.maxConcurrentResponses = DefaultMaxConcurrentResponses
	}
	if settled.maxInFlightResponseBytes == 0 {
		settled.maxInFlightResponseBytes = DefaultMaxInFlightResponseBytes
	}
	if settled.maxWaitingResponses == 0 {
		settled.maxWaitingResponses = DefaultMaxWaitingResponses
	}
	if settled.maxFrameBytes == 0 {
		settled.maxFrameBytes = DefaultMaxFrameBytes
	}
	if settled.maxConcurrentSnapshotFrames == 0 {
		settled.maxConcurrentSnapshotFrames = DefaultMaxConcurrentSnapshotFrames
	}
	if settled.maxInFlightSnapshotFrameBytes == 0 {
		settled.maxInFlightSnapshotFrameBytes = DefaultMaxInFlightSnapshotFrameBytes
	}
	if settled.maxWaitingSnapshotFrames == 0 {
		settled.maxWaitingSnapshotFrames = DefaultMaxWaitingSnapshotFrames
	}
	return settled
}

// Check reports whether the options resolve to usable bounded settings. Callers may use
// it before opening the storage that will be handed to NewHandlerWithOptions.
func (o HandlerOptions) Check() error {
	if err := o.Files.Check(); err != nil {
		return err
	}
	settled := o.settle()
	if err := checkLockControlLimits(settled.maxConcurrentLockControls, settled.maxWaitingLockControls); err != nil {
		return err
	}
	if err := settled.replication.check(); err != nil {
		return err
	}
	if settled.maxBodyBytes < minimumMaxBodyBytes {
		return fmt.Errorf("httprest: HandlerOptions.MaxBodyBytes is %d; at least %d bytes are required to carry protocol errors within the same bound", settled.maxBodyBytes, minimumMaxBodyBytes)
	}
	if settled.maxBodyBytes == math.MaxInt64 {
		return fmt.Errorf("httprest: HandlerOptions.MaxBodyBytes is effectively unbounded for an in-memory body")
	}
	if settled.maxBodyBytes > math.MaxInt64/retainedResponseMultiplier {
		return fmt.Errorf("httprest: HandlerOptions.MaxBodyBytes is too large to account for simultaneous response representations")
	}
	if settled.maxWriteBytes <= 0 {
		return fmt.Errorf("httprest: HandlerOptions.MaxWriteBytes is %d; a file-content request needs a positive bound", settled.maxWriteBytes)
	}
	if settled.maxWriteBytes > settled.maxBodyBytes {
		return fmt.Errorf("httprest: HandlerOptions.MaxWriteBytes is %d, above the protocol body bound of %d", settled.maxWriteBytes, settled.maxBodyBytes)
	}
	if settled.maxConcurrentBodies <= 0 {
		return fmt.Errorf("httprest: HandlerOptions.MaxConcurrentBodies is %d; retained request bodies need a positive concurrency bound", settled.maxConcurrentBodies)
	}
	if settled.maxWaitingBodies < 0 {
		return fmt.Errorf("httprest: HandlerOptions.MaxWaitingBodies is %d; request-body admission waiters need a finite non-negative bound", settled.maxWaitingBodies)
	}
	if settled.maxInFlightBodyBytes < settled.maxBodyBytes {
		return fmt.Errorf("httprest: HandlerOptions.MaxInFlightBodyBytes is %d, smaller than the per-body bound of %d", settled.maxInFlightBodyBytes, settled.maxBodyBytes)
	}
	if settled.maxConcurrentResponses <= 0 {
		return fmt.Errorf("httprest: HandlerOptions.MaxConcurrentResponses is %d; retained responses need a positive concurrency bound", settled.maxConcurrentResponses)
	}
	responseReservation := retainedResponseMultiplier * settled.maxBodyBytes
	if settled.maxInFlightResponseBytes < responseReservation {
		return fmt.Errorf("httprest: HandlerOptions.MaxInFlightResponseBytes is %d, smaller than one response reservation of %d", settled.maxInFlightResponseBytes, responseReservation)
	}
	if settled.maxWaitingResponses < 0 {
		return fmt.Errorf("httprest: HandlerOptions.MaxWaitingResponses is %d; response admission waiters need a finite non-negative bound", settled.maxWaitingResponses)
	}
	if settled.maxFrameBytes < minimumMaxFrameBytes {
		return fmt.Errorf("httprest: HandlerOptions.MaxFrameBytes is %d; at least %d bytes are required to carry stream faults within the same bound", settled.maxFrameBytes, minimumMaxFrameBytes)
	}
	if settled.maxFrameBytes > maximumMaxFrameBytes {
		return fmt.Errorf("httprest: HandlerOptions.MaxFrameBytes is %d, above the bounded maximum of %d", settled.maxFrameBytes, maximumMaxFrameBytes)
	}
	if settled.maxFrameBytes > math.MaxInt64/retainedFrameMultiplier {
		return fmt.Errorf("httprest: HandlerOptions.MaxFrameBytes is too large to account for simultaneous frame representations")
	}
	if uint64(settled.maxFrameBytes) > uint64(^uint(0)>>1) {
		return fmt.Errorf("httprest: HandlerOptions.MaxFrameBytes is too large for this process")
	}
	if settled.maxConcurrentSnapshotFrames <= 0 {
		return fmt.Errorf("httprest: HandlerOptions.MaxConcurrentSnapshotFrames is %d; retained snapshot frames need a positive concurrency bound", settled.maxConcurrentSnapshotFrames)
	}
	frameReservation := retainedFrameMultiplier * settled.maxFrameBytes
	if settled.maxInFlightSnapshotFrameBytes < frameReservation {
		return fmt.Errorf("httprest: HandlerOptions.MaxInFlightSnapshotFrameBytes is %d, smaller than one frame reservation of %d", settled.maxInFlightSnapshotFrameBytes, frameReservation)
	}
	if settled.maxWaitingSnapshotFrames < 0 {
		return fmt.Errorf("httprest: HandlerOptions.MaxWaitingSnapshotFrames is %d; snapshot-frame admission waiters need a finite non-negative bound", settled.maxWaitingSnapshotFrames)
	}
	if _, err := retainedEventFrameBytes(settled.replication.MaxSubscriptions, settled.maxFrameBytes); err != nil {
		return err
	}
	if _, err := retainedSnapshotCursorBytes(settled.replication.Snapshots, settled.maxFrameBytes); err != nil {
		return err
	}
	return nil
}

func retainedSnapshotCursorBytes(snapshots int, maxFrameBytes int64) (int64, error) {
	if snapshots < 0 || maxFrameBytes <= 0 {
		return 0, fmt.Errorf("httprest: snapshot cursor accounting needs non-negative snapshots and positive frame bytes")
	}
	if int64(snapshots) > math.MaxInt64/maxFrameBytes {
		return 0, fmt.Errorf("httprest: snapshot cursor accounting overflows for %d snapshots of %d bytes each", snapshots, maxFrameBytes)
	}
	return int64(snapshots) * maxFrameBytes, nil
}

func retainedEventFrameBytes(subscriptions int, maxFrameBytes int64) (int64, error) {
	if subscriptions < 0 || maxFrameBytes <= 0 {
		return 0, fmt.Errorf("httprest: change-stream frame accounting needs non-negative subscriptions and positive frame bytes")
	}
	if int64(subscriptions) > math.MaxInt64/retainedFrameMultiplier/maxFrameBytes {
		return 0, fmt.Errorf("httprest: change-stream frame accounting overflows for %d subscriptions of %d bytes each", subscriptions, maxFrameBytes)
	}
	return int64(subscriptions) * retainedFrameMultiplier * maxFrameBytes, nil
}

// DialOptions configure the resources retained by a Storage obtained through Dial.
type DialOptions struct {
	// Lock control admission is independent of bulk bodies and replication. Zero selects
	// the corresponding default; each active call reserves four fixed control bodies.
	MaxConcurrentLockControls int
	MaxWaitingLockControls    int

	// Silence is how long a stream may say nothing before it is treated as no longer
	// delivered. Zero selects DefaultSilence.
	Silence time.Duration

	// MaxBodyBytes is the largest non-streaming request or response body held by the
	// client except for file-content requests. A read response above it fails with EFBIG;
	// an oversized protocol message fails with EIO because its answer cannot be decoded.
	// Replication streams use their per-frame bound. Zero selects DefaultMaxBodyBytes.
	MaxBodyBytes int64

	// MaxFrameBytes is the largest encoded replication frame retained by one stream.
	// Zero selects DefaultMaxFrameBytes.
	MaxFrameBytes int64

	// MaxWriteBytes is the largest file-content request sent by Write. A larger write
	// fails with EFBIG before a request is sent. Zero selects MaxBodyBytes; a non-zero
	// value must not exceed MaxBodyBytes.
	MaxWriteBytes int64

	// These bounds apply to non-streaming responses, including fixed results.
	// Retained-file request encoding uses an independent pool with the same bounds.
	// Each pool reserves four times MaxBodyBytes per operation for simultaneous
	// encoded and decoded representations.
	MaxConcurrentResponses   int
	MaxInFlightResponseBytes int64
	MaxWaitingResponses      int
}

// DefaultDialOptions returns the bounds Dial uses. MaxWriteBytes remains zero so it
// continues to inherit MaxBodyBytes if the returned protocol limit is changed.
func DefaultDialOptions() DialOptions {
	return DialOptions{
		MaxConcurrentLockControls: DefaultMaxConcurrentLockControls,
		MaxWaitingLockControls:    DefaultMaxWaitingLockControls,
		Silence:                   DefaultSilence,
		MaxBodyBytes:              DefaultMaxBodyBytes,
		MaxFrameBytes:             DefaultMaxFrameBytes,
		MaxConcurrentResponses:    DefaultMaxConcurrentResponses,
		MaxInFlightResponseBytes:  DefaultMaxInFlightResponseBytes,
		MaxWaitingResponses:       DefaultMaxWaitingResponses,
	}
}

func (o DialOptions) settle() (DialOptions, error) {
	if o.MaxConcurrentLockControls == 0 {
		o.MaxConcurrentLockControls = DefaultMaxConcurrentLockControls
	}
	if o.MaxWaitingLockControls == 0 {
		o.MaxWaitingLockControls = DefaultMaxWaitingLockControls
	}
	if err := checkLockControlLimits(o.MaxConcurrentLockControls, o.MaxWaitingLockControls); err != nil {
		return DialOptions{}, err
	}
	if o.Silence == 0 {
		o.Silence = DefaultSilence
	}
	if o.MaxBodyBytes == 0 {
		o.MaxBodyBytes = DefaultMaxBodyBytes
	}
	if o.MaxFrameBytes == 0 {
		o.MaxFrameBytes = DefaultMaxFrameBytes
	}
	if o.MaxWriteBytes == 0 {
		o.MaxWriteBytes = o.MaxBodyBytes
	}
	if o.MaxConcurrentResponses == 0 {
		o.MaxConcurrentResponses = DefaultMaxConcurrentResponses
	}
	if o.MaxInFlightResponseBytes == 0 {
		o.MaxInFlightResponseBytes = DefaultMaxInFlightResponseBytes
	}
	if o.MaxWaitingResponses == 0 {
		o.MaxWaitingResponses = DefaultMaxWaitingResponses
	}
	if o.Silence < 0 {
		return DialOptions{}, fmt.Errorf("httprest: a stream allowed to say nothing for %v is a stream nothing is watching", o.Silence)
	}
	if o.MaxBodyBytes < 0 {
		return DialOptions{}, fmt.Errorf("httprest: DialOptions.MaxBodyBytes is %d; a retained HTTP body needs a positive bound", o.MaxBodyBytes)
	}
	if o.MaxBodyBytes == math.MaxInt64 {
		return DialOptions{}, fmt.Errorf("httprest: DialOptions.MaxBodyBytes is effectively unbounded for an in-memory body")
	}
	if o.MaxBodyBytes > math.MaxInt64/retainedResponseMultiplier {
		return DialOptions{}, fmt.Errorf("httprest: DialOptions.MaxBodyBytes is too large to account for simultaneous response representations")
	}
	if o.MaxFrameBytes < minimumMaxFrameBytes {
		return DialOptions{}, fmt.Errorf("httprest: DialOptions.MaxFrameBytes is %d; at least %d bytes are required to decode stream faults", o.MaxFrameBytes, minimumMaxFrameBytes)
	}
	if o.MaxFrameBytes > maximumMaxFrameBytes {
		return DialOptions{}, fmt.Errorf("httprest: DialOptions.MaxFrameBytes is %d, above the bounded maximum of %d", o.MaxFrameBytes, maximumMaxFrameBytes)
	}
	if uint64(o.MaxFrameBytes) > uint64(^uint(0)>>1) {
		return DialOptions{}, fmt.Errorf("httprest: DialOptions.MaxFrameBytes is too large for this process")
	}
	if o.MaxWriteBytes <= 0 {
		return DialOptions{}, fmt.Errorf("httprest: DialOptions.MaxWriteBytes is %d; a file-content request needs a positive bound", o.MaxWriteBytes)
	}
	if o.MaxWriteBytes > o.MaxBodyBytes {
		return DialOptions{}, fmt.Errorf("httprest: DialOptions.MaxWriteBytes is %d, above the protocol body bound of %d", o.MaxWriteBytes, o.MaxBodyBytes)
	}
	if o.MaxConcurrentResponses <= 0 {
		return DialOptions{}, fmt.Errorf("httprest: DialOptions.MaxConcurrentResponses is %d; retained responses need a positive concurrency bound", o.MaxConcurrentResponses)
	}
	reservation := retainedResponseMultiplier * o.MaxBodyBytes
	if o.MaxInFlightResponseBytes < reservation {
		return DialOptions{}, fmt.Errorf("httprest: DialOptions.MaxInFlightResponseBytes is %d, smaller than one response reservation of %d", o.MaxInFlightResponseBytes, reservation)
	}
	if o.MaxWaitingResponses < 0 {
		return DialOptions{}, fmt.Errorf("httprest: DialOptions.MaxWaitingResponses is %d; response admission waiters need a finite non-negative bound", o.MaxWaitingResponses)
	}
	return o, nil
}
