package io

import (
	"encoding/csv"
	"log"
	"os"
	"path"
	"twopc/pkg/common"
)

type ILogger interface {
	WriteSpecial(directive string)
	WriteState(txId string, state common.TxState)
	WriteOp(txId string, state common.TxState, op common.Operation, key string)
	Read() (entries []logEntry, err error)
}

type Logger struct {
	path      string
	file      *os.File
	csvWriter *csv.Writer
	requests  chan *logRequest
}

func NewLogger(logFilePath string) *Logger {
	err := os.MkdirAll(path.Dir(logFilePath), 0)
	//file, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE, 0)
	file, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Fatalln("newLogger:", err)
	}

	l := &Logger{
		path:      logFilePath,
		file:      file,
		csvWriter: csv.NewWriter(file),
		requests:  make(chan *logRequest),
	}

	go l.loggingLoop()

	return l
}

func (l *Logger) loggingLoop() {
	for {
		req := <-l.requests
		err := l.csvWriter.Write(req.record)
		if err != nil {
			log.Fatalln("logger.write fatal:", err)
		}

		l.csvWriter.Flush()
		err = l.file.Sync()
		if err != nil {
			log.Fatalln("logger.write fatal:", err)
		}
		req.done <- 1
	}
}

func (l *Logger) Read() (entries []logEntry, err error) {
	entries = make([]logEntry, 0)
	file, err := os.OpenFile(l.path, os.O_RDONLY, 0)
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
		}
		return
	}
	r := csv.NewReader(file)
	records, err := r.ReadAll()
	if err != nil {
		return
	}

	for _, record := range records {
		entries = append(entries, logEntry{
			TxId:  record[0],
			State: common.ParseTxState(record[1]),
			Op:    common.ParseOperation(record[2]),
			Key:   record[3],
		})
	}
	return
}

func (l *Logger) WriteSpecial(directive string) {
	l.WriteOp(directive, common.NoState, common.NoOp, "")
}

func (l *Logger) WriteState(txId string, state common.TxState) {
	l.WriteOp(txId, state, common.NoOp, "")
}

func (l *Logger) WriteOp(txId string, state common.TxState, op common.Operation, key string) {
	record := []string{txId, state.String(), op.String(), key}
	done := make(chan int)
	l.requests <- &logRequest{record, done}
	<-done
}

// -------------------------------------------------------------------------
type logRequest struct {
	record []string
	done   chan int
}

type logEntry struct {
	TxId  string
	State common.TxState
	Op    common.Operation
	Key   string
}
