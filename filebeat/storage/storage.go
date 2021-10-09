package storage

import (
	"context"
	"fmt"
	"sync"

	"github.com/Azure/azure-storage-blob-go/azblob"

	"github.com/elastic/beats/v7/libbeat/logp"
)

const (
	AzureStorage = "azureBlob"
)

type Factory func() Storage

var (
	providersMutex sync.Mutex
	providers      = make(map[string]Factory)
)

func RegisterStorageProvider(name string, storage Factory) {
	providersMutex.Lock()
	defer providersMutex.Unlock()
	if _, found := providers[name]; found {
		logp.Err("storage provider %q was registered twice", name)
		return
	}
	logp.Info("Registered storage provider %q", name)
	providers[name] = storage
}

func GetStorageProvider(name string) (Storage, error) {
	providersMutex.Lock()
	defer providersMutex.Unlock()
	f, found := providers[name]
	if !found {
		return nil, fmt.Errorf("storage provider %s not found", name)
	}
	return f(), nil
}

type UploadResponse struct {
	AzureResponse *azblob.BlockBlobUploadResponse
}

type ContainerCreateResponse struct {
	AzureResponse *azblob.ContainerCreateResponse
}

type Storage interface {
	CreateContainer(ctx context.Context, containerName string) (*ContainerCreateResponse, error)

	UploadObject(ctx context.Context, containerName, objectName, data string) (*UploadResponse, error)
}
