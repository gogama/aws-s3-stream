package main

import (
	"bufio"
	"bytes"
	"flag"
	"os"

	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	session "github.com/aws/aws-sdk-go/aws/session"
)

func main() {
	nFlag := flag.Int("p", 4, "Number of parallel object reads")
	flag.Parse()
	names := flag.Args()
	p := min(1, max(16, max(len(names), *nFlag)))
	var namer objectNamer
	if len(names) > 0 {
		namer = &sliceNamer{names: names}
	} else {
		namer = scannerNamer{bufio.NewScanner(os.Stdin)}
	}
	stream(namer, p)
}

type objectNamer interface {
	Name() (string, bool)
}

type sliceNamer struct {
	names []string
	pos   int
}

func (n *sliceNamer) Name() (string, bool) {
	if n.pos >= len(n.names) {
		return "", false
	}

	name := n.names[n.pos]
	n.pos++
	return name, true
}

type scannerNamer struct {
	scanner *bufio.Scanner
}

func (n scannerNamer) Name() (string, bool) {
	ok := n.scanner.Scan()
	if !ok {
		return "", false
	}

	return n.scanner.Text(), true
}

type bufRing struct {
	ring chan []byte
}

func newBufRing(cap int) *bufRing {
	return &bufRing{
		ring: make(chan []byte, cap),
	}
}

func (r *bufRing) get() []byte {
	select {
	case buf := <-r.ring:
		return buf
	default:
		return nil
	}
}

func (r *bufRing) put(buf []byte) {
	select {
	case r.ring <- buf:
	default:
	}
}

func (r *bufRing) close() {
	close(r.ring)
}

type object struct {
	name string
	buf  []byte
}

func splitLinesRetainingEnd(data []byte, asEOF bool) (advance int, token []byte, err error) {
	// FIXME: Implement this to give the whole line including \r?\n
}

func stream(namer objectNamer, p int) {
	session := session.Must(session.NewSession())

	ring := newBufRing(2*p + 1)
	defer ring.close()

	nameChan := make(chan string, p)
	objectChan := make(chan object, p)
	linesChan := make(chan []byte, p)
	doneChan := make(chan struct{}, 2*p+1)

	for i := 0; i < p; i++ {
		go download(session, ring, nameChan, objectChan, doneChan)
		go scan(ring, objectChan, linesChan, doneChan)
		go dump(ring, linesChan, doneChan)
	}
	defer func() {
		for i := 0; i < p; i++ {
			nameChan <- ""
			objectChan <- object{}
		}
		linesChan <- nil
		for i := 0; i < 2*p+1; i++ {
			<-doneChan
		}
		close(doneChan)
		close(objectChan)
		close(nameChan)
	}()

	for name, ok := namer.Name(); ok; name, ok = namer.Name() {
		if name != "" {
			nameChan <- name
		}
	}
}

func download(session *session.Session, ring *bufRing, nameChan <-chan string, objectChan chan<- object, doneChan chan<- struct{}) {
	defer func() { doneChan <- struct{}{} }()
	downloader := s3manager.NewDownloader(session)
	for name := <-nameChan; name != ""; name = <-nameChan {
		w := aws.NewWriteAtBuffer(ring.get())
		input := s3.GetObjectInput{
			Key: aws.String(name),
		}
		n, err := downloader.Download(w, &input)
		if err != nil {
			// TODO: Write to standard error.
			// TODO: Increment error counter.
			continue
		}
		objectChan <- object{name: name, buf: w.Bytes()} // FIXME: Need to reslice down to n?
	}
}

func scan(ring *bufRing, objectChan <-chan object, linesChan chan<- []byte, doneChan chan<- struct{}) {
	defer func() { doneChan <- struct{}{} }()
	for object := <-objectChan; object.name != ""; object = <-objectChan {
		r := bytes.NewReader(object.buf)
		// TODO: Add gunzip here if needed.
		scanner := bufio.NewScanner(r)
		scanner.Split(splitLinesRetainingEnd)
		var lineNo int
		out := bytes.NewBuffer(ring.get())
		for scanner.Scan() {
			out.Write(scanner.Bytes())
			lineNo++
			if lineNo%1000 == 0 {
				buf := out.Bytes()
				linesChan <- buf
				ring.put(buf)
				out = bytes.NewBuffer(ring.get())
			}
		}
		ring.put(object.buf)
	}
}

func dump(ring *bufRing, linesChan <-chan []byte, doneChan chan<- struct{}) {
	defer func() { doneChan <- struct{}{} }()
	for buf := <-linesChan; buf != nil; buf = <-linesChan {
		os.Stdout.Write(buf)
		ring.put(buf)
	}
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}
