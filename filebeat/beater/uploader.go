package beater

import (
	"context"
	"io/ioutil"
	"path/filepath"
	"strings"

	"github.com/elastic/beats/v7/filebeat/storage"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/fsnotify/fsnotify"
	"github.com/panjf2000/ants/v2"
	"k8s.io/apimachinery/pkg/util/sets"
)

type uploader struct {
	log  *logp.Logger
	dir  string
	done chan struct{}
}

func newUploader(dir string) *uploader {
	return &uploader{
		dir:  dir,
		log:  logp.NewLogger("uploader"),
		done: make(chan struct{}),
	}
}

func fileTrigger(dir string, evtCh chan string, errCh chan error, stop <-chan struct{}) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	if err = watcher.Add(dir); err != nil {
		return err
	}

	events := sets.NewString()

	go func() {
		defer watcher.Close()
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if event.Op&fsnotify.Create == fsnotify.Create {
					if events.Has(event.Name) {
						continue
					}
					if strings.HasSuffix(event.Name, ".swp") {
						continue
					}
					events.Insert(event.Name)
					evtCh <- event.Name
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				logp.Warn("Error watching file trigger: %v %v", dir, err)
				errCh <- err
			case signal := <-stop:
				logp.Info("Shutting down file watcher: %v %v", dir, signal)
				return
			}
		}
	}()
	return nil
}

func (u *uploader) Start(provider string) error {
	u.log.Infof("Start file uploader")

	evtCh := make(chan string, 1)
	errCh := make(chan error)

	if err := fileTrigger(u.dir, evtCh, errCh, u.done); err != nil {
		return err
	}

	storager, err := storage.GetStorageProvider(provider)
	if err != nil {
		return err
	}

	uploadFunc := func(filePath interface{}) {
		path, ok := filePath.(string)
		if !ok {
			return
		}
		fileName := filepath.Base(path)

		data, err := ioutil.ReadFile(path)
		if err != nil {
			return
		}

		resp, err := storager.UploadObject(context.TODO(), "logs", fileName, string(data))
		if !successCode(resp.AzureResponse.StatusCode()) {
			u.log.Errorf("status code is %d", resp.AzureResponse.StatusCode())
		}
		if err != nil {
			u.log.Errorf("upload file failed: %v", err)
		}
		u.log.Infof("upload file %s succeed", fileName)
	}

	p, err := ants.NewPoolWithFunc(100, uploadFunc)
	if err != nil {
		return err
	}
	defer p.Release()

	for {
		select {
		case filePath := <-evtCh:
			err := p.Invoke(filePath)
			if err != nil {
				return err
			}
		case err := <-errCh:
			return err
		case <-u.done:
			return nil
		}
	}
}

func (u *uploader) Stop() {
	u.log.Info("Stopping uploader")
	defer u.log.Info("uploader stopped")

	close(u.done)
}

func successCode(code int) bool {
	return code >= 200 && code < 300
}
