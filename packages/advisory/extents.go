package advisory

import "math"

func byteLast(start, length uint64) (uint64, bool) {
	if length == 0 || length-1 > math.MaxUint64-start {
		return 0, false
	}
	return start + length - 1, true
}

func bytesOverlap(aStart, aLength, bStart, bLength uint64) bool {
	aLast, aValid := byteLast(aStart, aLength)
	bLast, bValid := byteLast(bStart, bLength)
	return aValid && bValid && aStart <= bLast && bStart <= aLast
}

func boundaryOverlaps(start, length, cut uint64) bool {
	_, valid := byteLast(start, length)
	return valid && cut > start && cut-start < length
}
