package gomessagestore

func (ms *msgStore) CreateProjector() Projector {
	return CreateProjector(ms.repo)
}
