package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/urfave/cli"
	"google.golang.org/api/iterator"
)

type PathType int

const (
	DocumentPath   PathType = 0
	CollectionPath PathType = 1
)

func (p PathType) String() string {
	switch p {
	case DocumentPath:
		return "Document"
	case CollectionPath:
		return "Collection"
	default:
		return "UNKNOWN"
	}
}

type CopyOption struct {
	merge     bool
	overwrite bool
	path      string // Path to copy from source document
}

func pathType(p string) PathType {
	return PathType(len(strings.Split(strings.Trim(p, "/"), "/")) % 2)
}

func copyCommandAction(c *cli.Context) error {
	argsLength := len(c.Args())

	if argsLength != 2 {
		return cli.NewExitError("Wrong number of arguments", 85)
	}

	sourceCollectionOrDocumentPath := strings.Trim(c.Args().Get(0), "/")
	targetCollectionOrDocumentPath := strings.Trim(c.Args().Get(1), "/")

	merge := c.Bool("merge")
	overwrite := c.Bool("overwrite")
	path := c.String("path")

	sc := c.String("src-credentials")
	dc := c.String("dest-credentials")

	sp := c.String("src-projectid")
	dp := c.String("dest-projectid")

	dd := c.String("dest-database")

	if sc == "" {
		sc = credentials
	}

	if dc == "" {
		dc = credentials
	}

	if sp == "" {
		sp = projectId
	}

	if dp == "" {
		dp = projectId
	}

	if dd == "" {
		dd = database
	}

	option := CopyOption{
		merge:     merge,
		overwrite: overwrite,
		path:      path,
	}

	sType := pathType(sourceCollectionOrDocumentPath)
	tType := pathType(targetCollectionOrDocumentPath)

	if sType != tType {
		return cli.NewExitError(fmt.Sprintf("Can't copy from %s to %s", sType.String(), tType.String()), 87)
	}

	sourceClient, err := createClientWithProjectId(sc, sp)
	if err != nil {
		return cliClientError(err)
	}

	targetClient, err := createClientWithProjectIdAndDatabase(dc, dp, dd)
	if err != nil {
		return cliClientError(err)
	}

	if sType == CollectionPath {
		log.Println("copying collection")
		copyCollection(
			sourceClient.Collection(sourceCollectionOrDocumentPath),
			targetClient.Collection(targetCollectionOrDocumentPath),
			option,
		)
	}

	if sType == DocumentPath {
		log.Println("copying document")
		copyDocument(
			sourceClient.Doc(sourceCollectionOrDocumentPath),
			targetClient.Doc(targetCollectionOrDocumentPath),
			option,
		)
	}

	log.Println("Done")

	return nil
}

func copyDocument(source, target *firestore.DocumentRef, option CopyOption) {
	client := NewCopyClient(200, option)

	client.Run(
		NewDocumentCopyJob(source, target),
		NewCollectionIterationJob(source, target),
	)
}

func copyCollection(source, target *firestore.CollectionRef, option CopyOption) {
	client := NewCopyClient(200, option)

	client.Run(NewDocumentIterationJob(source, target))
}

type CopyClient struct {
	jobQueue      []CopyJob
	workerQueue   []chan CopyJob
	jobChannel    chan CopyJob
	workerChannel chan chan CopyJob
	WorkerCount   int
	Option        CopyOption
}

func NewCopyClient(workerCount int, option CopyOption) CopyClient {
	return CopyClient{
		jobQueue:      make([]CopyJob, 0),
		workerQueue:   make([]chan CopyJob, 0),
		jobChannel:    make(chan CopyJob),
		workerChannel: make(chan chan CopyJob),
		WorkerCount:   workerCount,
		Option:        option,
	}
}

func (client *CopyClient) Run(seeds ...CopyJob) {
	out := make(chan []CopyJob)

	go func() {
		// submit initial jobs
		for _, j := range seeds {
			client.submitJob(j)
		}

		for {
			// read jobs from workers
			jobs := <-out
			for _, j := range jobs {
				client.submitJob(j)
			}
		}
	}()

	// create works to handle jobs
	client.createWorkers(out)

	// match workers with jobs
	done := client.scheduleJobs()

	<-done
}

func (client *CopyClient) submitJob(job CopyJob) {
	client.jobChannel <- job
}

func (client *CopyClient) workerReady(w chan CopyJob) {
	client.workerChannel <- w
}

func (client *CopyClient) workerInput() chan CopyJob {
	return make(chan CopyJob)
}

func (client *CopyClient) createWorkers(workerOutput chan []CopyJob) {
	for i := 0; i < client.WorkerCount; i++ {
		worker := &CopyWorker{
			Overwrite: client.Option.overwrite,
			Merge:     client.Option.merge,
			Option:    client.Option,
		}
		in := client.workerInput()
		go func(in chan CopyJob, worker *CopyWorker) {
			for {
				client.workerReady(in)
				j := <-in
				j.worker = worker
				jobs, err := j.worker.handleJob(j)
				if err != nil {
					continue
				}
				workerOutput <- jobs
			}
		}(in, worker)
	}
}

func (client *CopyClient) scheduleJobs() <-chan struct{} {
	done := make(chan struct{})
	go func() {
		j := <-client.jobChannel
		client.jobQueue = append(client.jobQueue, j)

		for {
			var activeJob CopyJob
			var activeWorker chan CopyJob

			if len(client.jobQueue) > 0 && len(client.workerQueue) > 0 {
				activeJob = client.jobQueue[0]
				activeWorker = client.workerQueue[0]
			}

			select {
			case jj := <-client.jobChannel:
				client.jobQueue = append(client.jobQueue, jj)
			case w := <-client.workerChannel:
				client.workerQueue = append(client.workerQueue, w)
			case activeWorker <- activeJob:
				client.jobQueue = client.jobQueue[1:]
				client.workerQueue = client.workerQueue[1:]
			case <-time.After(time.Second * 2):
				// no more jobs
				if len(client.jobQueue) == 0 && len(client.workerQueue) == client.WorkerCount {
					done <- struct{}{}
					return
				}
			}
		}
	}()
	return done
}

var ops int32

type CopyWorker struct {
	Overwrite bool
	Merge     bool
	Option    CopyOption
}

func (w *CopyWorker) handleJob(j CopyJob) ([]CopyJob, error) {
	var result []CopyJob

	if j.name == "iterateCollection" {
		return w.handleCollectionIterationJob(j)
	}

	if j.name == "iterateDocument" {
		return w.handleDocumentIterationJob(j)
	}

	if j.name == "copyDocument" {
		return w.handleCopyDocumentJob(j)
	}

	return result, nil
}

func (w *CopyWorker) handleCollectionIterationJob(j CopyJob) ([]CopyJob, error) {
	var result []CopyJob

	if next, err := j.collectionIterator.Next(); err != nil {
		if err == iterator.Done {
			return result, nil
		}
		return result, err
	} else {
		result = append(result,
			j,
			NewDocumentIterationJob(next, j.targetDocumentRef.Collection(next.ID)),
		)
		return result, nil
	}
}

func (w *CopyWorker) handleDocumentIterationJob(j CopyJob) ([]CopyJob, error) {
	var result []CopyJob

	if next, err := j.documentRefIterator.Next(); err != nil {
		if err == iterator.Done {
			return result, nil
		}
		return result, err
	} else {
		result = append(result,
			j,
			NewCollectionIterationJob(next, j.targetCollectionRef.Doc(next.ID)),
			NewDocumentCopyJob(next, j.targetCollectionRef.Doc(next.ID)),
		)
		return result, nil
	}
}

func (w *CopyWorker) handleCopyDocumentJob(j CopyJob) ([]CopyJob, error) {
	atomic.AddInt32(&ops, 1)
	if ops%100 == 0 {
		log.Printf("Copied %d documents", ops)
	}

	ctx := context.Background()
	snap, err := j.documentRef.Get(ctx)
	if err != nil {
		return nil, fmt.Errorf("Failed to get document %s: %v", j.documentRef.Path, err)
	}

	data := snap.Data()
	if w.Option.path != "" {
		// Handle path-based copying
		if data == nil {
			return nil, fmt.Errorf("Source document is empty")
		}

		pathParts := strings.Split(w.Option.path, ".")
		if len(pathParts) == 0 {
			return nil, fmt.Errorf("Invalid path: %s", w.Option.path)
		}

		value, err := getValueAtPath(data, pathParts)
		if err != nil {
			return nil, fmt.Errorf("Failed to get value at path %s: %v", w.Option.path, err)
		}

		// Create a new map with just the path data
		newData := make(map[string]interface{})
		err = setValueAtPath(newData, pathParts, value)
		if err != nil {
			return nil, fmt.Errorf("Failed to set value at path %s: %v", w.Option.path, err)
		}
		data = newData
	}

	var writeErr error
	if !w.Overwrite {
		_, err := j.targetDocumentRef.Get(ctx)
		if err == nil {
			return nil, fmt.Errorf("Document %s already exists", j.targetDocumentRef.Path)
		}
	}

	if w.Merge {
		_, writeErr = j.targetDocumentRef.Set(ctx, data, firestore.MergeAll)
	} else {
		_, writeErr = j.targetDocumentRef.Set(ctx, data)
	}

	if writeErr != nil {
		return nil, fmt.Errorf("Failed to write document %s: %v", j.targetDocumentRef.Path, writeErr)
	}

	return nil, nil
}

type CopyJob struct {
	name                string
	documentRef         *firestore.DocumentRef
	collectionIterator  *firestore.CollectionIterator
	targetDocumentRef   *firestore.DocumentRef
	documentRefIterator *firestore.DocumentRefIterator
	targetCollectionRef *firestore.CollectionRef
	worker              *CopyWorker
}

func NewCollectionIterationJob(documentRef *firestore.DocumentRef, targetDocumentRef *firestore.DocumentRef) CopyJob {
	return CopyJob{
		name:               "iterateCollection",
		collectionIterator: documentRef.Collections(context.Background()),
		targetDocumentRef:  targetDocumentRef,
	}
}

func NewDocumentIterationJob(collectionRef *firestore.CollectionRef, targetCollectionRef *firestore.CollectionRef) CopyJob {
	return CopyJob{
		name:                "iterateDocument",
		documentRefIterator: collectionRef.DocumentRefs(context.Background()),
		targetCollectionRef: targetCollectionRef,
	}
}

func NewDocumentCopyJob(documentRef *firestore.DocumentRef, targetDocumentRef *firestore.DocumentRef) CopyJob {
	return CopyJob{
		name:              "copyDocument",
		documentRef:       documentRef,
		targetDocumentRef: targetDocumentRef,
	}
}

// getValueAtPath retrieves a value from a nested map using a path
func getValueAtPath(data map[string]interface{}, pathParts []string) (interface{}, error) {
	current := data
	for i, part := range pathParts {
		if i == len(pathParts)-1 {
			if val, ok := current[part]; ok {
				return val, nil
			}
			return nil, fmt.Errorf("path part %s not found", part)
		}

		next, ok := current[part]
		if !ok {
			return nil, fmt.Errorf("path part %s not found", part)
		}

		nextMap, ok := next.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("path part %s is not a map", part)
		}
		current = nextMap
	}
	return nil, fmt.Errorf("invalid path")
}

// setValueAtPath sets a value in a nested map using a path
func setValueAtPath(data map[string]interface{}, pathParts []string, value interface{}) error {
	current := data
	for i, part := range pathParts {
		if i == len(pathParts)-1 {
			current[part] = value
			return nil
		}

		next, ok := current[part]
		if !ok {
			next = make(map[string]interface{})
			current[part] = next
		}

		nextMap, ok := next.(map[string]interface{})
		if !ok {
			return fmt.Errorf("path part %s is not a map", part)
		}
		current = nextMap
	}
	return nil
}
