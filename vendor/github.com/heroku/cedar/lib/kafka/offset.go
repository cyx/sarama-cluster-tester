package kafka

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
)

// TopicOffsets are a mapping of the partition => offsets to start a consumer.
type TopicOffsets map[int32]int64

// NewTopicOffsets returns a TopicOffsets with all the partitions initialized to defaultOffset.
func NewTopicOffsets(partitions []int32, defaultOffset int64) TopicOffsets {
	to := make(TopicOffsets)
	for _, p := range partitions {
		to[p] = defaultOffset
	}
	return to
}

// MarkOffset assigns the offset to the partition and implements OffsetManager.
func (to TopicOffsets) MarkOffset(partition int32, offset int64) error {
	to[partition] = offset
	return nil
}

// Offsets implements OffsetManager.
func (to TopicOffsets) Offsets() TopicOffsets {
	return to
}

// Copy TopicOffsets by value.
func (to TopicOffsets) Copy() TopicOffsets {
	out := make(TopicOffsets)
	for k, v := range to {
		out[k] = v
	}
	return out
}

// UnmarshalOffsets parses a serialized form of offsets and hydrates onto a
// TopicOffsets instance.
func UnmarshalOffsets(s string, o TopicOffsets) error {
	for _, part := range strings.Split(s, ",") {
		var (
			partition int32
			offset    int64
		)

		if _, err := fmt.Sscanf(part, "%d:%d", &partition, &offset); err != nil {
			return errors.Wrapf(err, "invalid offsets: %q", s)
		}

		o[partition] = offset
	}
	return nil
}

// MarshalOffsets takes a TopicOffsets instance and produces the wire format.
func MarshalOffsets(o TopicOffsets) string {
	parts := []string{}

	for partition, offset := range o {
		parts = append(parts, fmt.Sprintf("%d:%d", partition, offset))
	}

	return strings.Join(parts, ",")
}
