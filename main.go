package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	session "github.com/aws/aws-sdk-go/aws/session"
)

func main() {
	cFlag := flag.Int("c", 4, "Number of concurrent object reads")
	pFlag := flag.String("p", "", "Default S3 prefix to read relative keys from")
	flag.Parse()
	names := flag.Args()
	concurrent := min(1, max(16, max(len(names), *cFlag)))
	var namer objectNamer
	if len(names) > 0 {
		namer = &sliceNamer{names: names}
	} else {
		namer = scannerNamer{bufio.NewScanner(os.Stdin)}
	}
	bucket := ""
	prefix := ""
	if len(*pFlag) > 0 {
		var err error
		bucket, prefix, err = splitS3Name(*pFlag)
		if err != nil {
			println(err.Error())
			os.Exit(1)
		}
	}
	stream(namer, concurrent, aws.String(bucket), prefix)
	if nerr > 0 {
		println(nerr, "errors.")
		os.Exit(1)
	}
}

var nerr uint64

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
	case r.ring <- buf[:0]:
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

func (o *object) hasGZipExtension() bool {
	return len(o.name) > 3 && o.name[:3] == ".gz"
}

func (o *object) hasAnyExtension() bool {
	lastSlash := strings.LastIndexByte(o.name, '/')
	lastDot := strings.LastIndexByte(o.name, '.')
	return lastDot > lastSlash
}

func (o *object) hasGZipMagicNumber() bool {
	return len(o.buf) >= 10 && o.buf[0] == 0x1f && o.buf[1] == 0x8b
}

func (o *object) isGZipped() bool {
	return (o.hasGZipExtension() || o.hasAnyExtension()) && o.hasGZipMagicNumber()
}

func splitS3Name(name string) (bucket string, path string, err error) {
	if len(name) == 0 {
		return "", "", errors.New("empty object name")
	}

	if len(name) >= 5 && name[0:5] == "s3://" {
		path = name[5:]
		if len(path) == 0 {
			return "", "", fmt.Errorf("missing bucket in S3 URL: '%s'", name)
		}
		i := strings.IndexByte(path, '/')
		if i == 0 {
			return "", "", fmt.Errorf("missing bucket in S3 URL: '%s'", name)
		}
		if i == -1 {
			return path, "", nil
		}

		bucket = path[:i]
		path = path[i+1:]
		return
	}

	return "", name, nil
}

func splitLinesRetainingEnd(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.IndexByte(data, '\n'); i >= 0 {
		// We have a full newline-terminated line.
		return i + 1, data[0 : i+1], nil
	}
	// If we're at EOF, we have a final, non-terminated line. Return it.
	if atEOF {
		return len(data), data, nil
	}
	// Request more data.
	return 0, nil, nil
}

func stream(namer objectNamer, parallel int, defaultBucket *string, defaultPrefix string) {
	s := session.Must(session.NewSession())

	ring := newBufRing(2*parallel + 2)
	defer ring.close()

	nameChan := make(chan string, parallel)
	defer close(nameChan)
	objectChan := make(chan object, parallel)
	defer close(objectChan)
	linesChan := make(chan []byte, parallel)
	defer close(linesChan)
	errorChan := make(chan error, parallel)
	defer close(errorChan)

	wgDownload := new(sync.WaitGroup)
	for i := 0; i < parallel; i++ {
		wgDownload.Add(1)
		go download(s, defaultBucket, defaultPrefix, ring, nameChan, objectChan, errorChan, wgDownload)
	}

	wgScan := new(sync.WaitGroup)
	for i := 0; i < parallel; i++ {
		wgScan.Add(1)
		go scan(ring, objectChan, linesChan, errorChan, wgScan)
	}

	wgOutput := new(sync.WaitGroup)
	wgOutput.Add(1)
	go dump(ring, linesChan, wgOutput)
	wgOutput.Add(1)
	go feedback(errorChan, wgOutput)

	defer func() {
		for i := 0; i < parallel; i++ {
			nameChan <- ""
		}
		wgDownload.Wait()
		for i := 0; i < parallel; i++ {
			objectChan <- object{}
		}
		wgScan.Wait()
		linesChan <- nil
		errorChan <- nil
		wgOutput.Wait()
	}()

	for name, ok := namer.Name(); ok; name, ok = namer.Name() {
		if name != "" {
			nameChan <- name
		}
	}
}

func download(
	s *session.Session,
	defaultBucket *string,
	defaultPrefix string,
	ring *bufRing,
	nameChan <-chan string,
	objectChan chan<- object,
	errorChan chan<- error,
	wg *sync.WaitGroup,
) {
	defer func() { wg.Done() }()
	downloader := s3manager.NewDownloader(s)
	for name := <-nameChan; name != ""; name = <-nameChan {
		bucket, key, err := splitS3Name(name)
		if err != nil {
			errorChan <- err
			continue
		} else if key == "" {
			errorChan <- fmt.Errorf("missing object key: '%s'", name)
			continue
		}

		w := aws.NewWriteAtBuffer(ring.get())
		input := s3.GetObjectInput{
			Bucket: defaultBucket,
		}
		if bucket != "" {
			input.Bucket = aws.String(bucket)
			input.Key = aws.String(key)
		} else {
			input.Key = aws.String(defaultPrefix + key)
		}
		n, err := downloader.Download(w, &input)
		if err != nil {
			errorChan <- err
			ring.put(w.Bytes())
			continue
		}
		objectChan <- object{name: name, buf: w.Bytes()[:n]}
	}
}

func scan(ring *bufRing, objectChan <-chan object, linesChan chan<- []byte, errorChan chan<- error, wg *sync.WaitGroup) {
	defer func() { wg.Done() }()
	for obj := <-objectChan; obj.name != ""; obj = <-objectChan {
		scanOne(obj, ring, linesChan, errorChan)
	}
}

func scanOne(obj object, ring *bufRing, linesChan chan<- []byte, errorChan chan<- error) {
	var r io.Reader
	r = bytes.NewReader(obj.buf)
	if obj.isGZipped() {
		r2, err := gzip.NewReader(r)
		if err != nil {
			errorChan <- err
			ring.put(obj.buf)
			return
		}
		defer func() { _ = r2.Close() }()
		r = r2
	}

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
			out = bytes.NewBuffer(ring.get())
		}
	}

	ring.put(obj.buf)
	if scanner.Err() != nil {
		errorChan <- scanner.Err()
		return
	}
	if lineNo%1000 != 0 {
		buf := out.Bytes()
		linesChan <- buf
	}
}

func dump(ring *bufRing, linesChan <-chan []byte, wg *sync.WaitGroup) {
	defer func() { wg.Done() }()
	for buf := <-linesChan; buf != nil; buf = <-linesChan {
		if len(buf) == 0 || buf[len(buf)-1] != '\n' {
			buf = append(buf, '\n')
		}
		buf2 := buf
		n, err := os.Stdout.Write(buf2)
		for n < len(buf2) && errors.Unwrap(err) == io.ErrShortWrite {
			buf2 = buf2[n:]
			n, err = os.Stdout.Write(buf2)
		}
		ring.put(buf)
	}
}

func feedback(errorChan <-chan error, wg *sync.WaitGroup) {
	defer func() { wg.Done() }()
	for err := <-errorChan; err != nil; err = <-errorChan {
		println(err.Error())
		nerr++
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
