package beater

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

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

func (u *uploader) Start(provider, container string) error {
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

	type uploadParam struct {
		filePath string
		isLast   bool
	}

	uploadFunc := func(args interface{}) {
		param, ok := args.(uploadParam)
		if !ok {
			return
		}
		fileName := filepath.Base(param.filePath)

		data, err := ioutil.ReadFile(param.filePath)
		if err != nil {
			return
		}

		if param.isLast {
			data = append(data, []byte("--- END OF NEBULA IMPORTER ---\n")...)
		}

		resp, err := storager.UploadObject(context.TODO(), container, fileName, data)
		if !successCode(resp.AzureResponse.StatusCode()) {
			u.log.Errorf("status code is %d", resp.AzureResponse.StatusCode())
			return
		}
		if err != nil {
			u.log.Errorf("upload file failed: %v", err)
			return
		}
		u.log.Infof("upload file %s succeed", fileName)
	}

	p, err := ants.NewPoolWithFunc(1, uploadFunc)
	if err != nil {
		return err
	}
	defer p.Release()

	tick := time.NewTicker(time.Second * 2)
	defer tick.Stop()

	var updated time.Time
	var lastFile string

	for {
		select {
		case filePath := <-evtCh:
			err := p.Invoke(uploadParam{filePath: filePath, isLast: false})
			if err != nil {
				return err
			}
			updated = time.Now()
			lastFile = filePath
		case <-tick.C:
			u.log.Info("----- tick !!! -----")
			s := time.Now().Sub(updated).Seconds()
			if !updated.IsZero() && s > 5 {
				u.log.Info("recheck the last log file again")
				uploadFunc(uploadParam{filePath: lastFile, isLast: true})
				u.log.Infof("no rotate log file created for %v seconds", s)
				u.log.Info("--- EXIT FILEBEAT ---")
				os.Exit(0)
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
