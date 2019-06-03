package gomessagestore

// prevent weirdness with pointers
func copyAndAppend(i []*MessageEnvelope, vals ...*MessageEnvelope) []*MessageEnvelope {
	j := make([]*MessageEnvelope, len(i), len(i)+len(vals))
	copy(j, i)
	return append(j, vals...)
}
