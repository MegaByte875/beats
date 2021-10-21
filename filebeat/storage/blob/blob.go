package client

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"sync"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"

	"github.com/elastic/beats/v7/filebeat/storage"
)

type AuthConfig struct {
	StorageAccount   string
	StorageAccessKey string
}

func init() {
	storage.RegisterStorageProvider(storage.AzureStorage, func() storage.Storage {
		return NewBlobService()
	})
}

var _ storage.Storage = (*storageService)(nil)

type storageService struct {
	accountName string
	accountKey  string

	initOnce   sync.Once
	initError  error
	credential *azblob.SharedKeyCredential
	pipeline   pipeline.Pipeline
	serviceURL azblob.ServiceURL
}

func NewBlobService() storage.Storage {
	return &storageService{
		accountName: "x",
		accountKey:  "J",
	}
}

func (c *storageService) init() error {
	c.initOnce.Do(func() {
		c.credential, c.initError = azblob.NewSharedKeyCredential(c.accountName, c.accountKey)
		if c.initError != nil {
			return
		}
		c.pipeline = azblob.NewPipeline(c.credential, azblob.PipelineOptions{})
		var u *url.URL
		u, c.initError = url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", c.accountName))
		if c.initError != nil {
			return
		}
		c.serviceURL = azblob.NewServiceURL(*u, c.pipeline)
	})
	return c.initError
}

func (c *storageService) CreateContainer(ctx context.Context, containerName string) (*storage.ContainerCreateResponse, error) {
	ctrURL, err := c.getContainerURL(containerName)
	if err != nil {
		return nil, err
	}

	resp, err := ctrURL.Create(
		ctx,
		azblob.Metadata{},
		azblob.PublicAccessContainer)
	if err != nil {
		return nil, err
	}

	return &storage.ContainerCreateResponse{AzureResponse: resp}, err
}

func (c *storageService) UploadObject(ctx context.Context, containerName, blobName string, data []byte) (*storage.UploadResponse, error) {
	blobURL, err := c.getBlockBlobURL(containerName, blobName)
	if err != nil {
		return nil, err
	}

	resp, err := blobURL.Upload(
		ctx,
		bytes.NewReader(data),
		azblob.BlobHTTPHeaders{
			ContentType: "text/plain",
		},
		azblob.Metadata{},
		azblob.BlobAccessConditions{},
		azblob.DefaultAccessTier,
		nil,
		azblob.ClientProvidedKeyOptions{},
	)
	if err != nil {
		return nil, err
	}

	return &storage.UploadResponse{AzureResponse: resp}, nil
}

func (c *storageService) getContainerURL(containerName string) (azblob.ContainerURL, error) {
	if err := c.init(); err != nil {
		return azblob.ContainerURL{}, err
	}

	return c.serviceURL.NewContainerURL(containerName), nil
}

func (c *storageService) getBlockBlobURL(containerName, blobName string) (azblob.BlockBlobURL, error) {
	containerURL, err := c.getContainerURL(containerName)
	if err != nil {
		return azblob.BlockBlobURL{}, err
	}
	return containerURL.NewBlockBlobURL(blobName), nil
}
