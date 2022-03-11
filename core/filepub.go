package core

type LogFilePublisher struct {
	logfile         *LogFile
	offsetListeners []OffsetListener
	msgChannel      chan []byte
	msgAckChannel   chan bool
	running         bool
}

func NewLogFilePublisher(logfile *LogFile) *LogFilePublisher {
	return &LogFilePublisher{
		logfile:       logfile,
		msgChannel:    make(chan []byte, 1),
		msgAckChannel: make(chan bool, 1),
	}
}

/**
 * Start the publisher
 */
func (l *LogFilePublisher) Start() {
	l.running = true
	for l.running {
		nextMsg := <-l.msgChannel
		if nextMsg == nil {
			l.running = false
			// can stop
			close(l.msgChannel)
			l.msgChannel = nil
			l.msgAckChannel <- true
		} else {
			l.msgAckChannel <- true
			l.logfile.Publish(nextMsg)
		}
	}
}

func (l *LogFilePublisher) Publish(message []byte) {
	l.msgChannel <- message
	<-l.msgAckChannel // wait for ack before doing anything else
}

func (l *LogFilePublisher) Stop() {
	l.msgChannel <- nil
	<-l.msgAckChannel // wait for ack before doing anything else
}
