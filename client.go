package main

import (
	"context"
	"os"

	firestore "cloud.google.com/go/firestore"
	firebase "firebase.google.com/go"
	"google.golang.org/api/option"
)

// create client or fails.
//
// NOTE: If FIRESTORE_EMULATOR_HOST is set, it will set a
// default projectid if none has been set.

func createClient(credentials string) (*firestore.Client, error) {
	return createClientWithProjectId(credentials, projectId)
}

func createClientWithProjectId(credentials string, projectId string) (*firestore.Client, error) {
	if os.Getenv("FIRESTORE_EMULATOR_HOST") != "" {
		if projectId == "" {
			projectId = "henry-glcoud-production"
		}
		return firestore.NewClient(context.Background(), projectId)
	}

	if database == "" {
		database = firestore.DefaultDatabaseID
	}
	if projectId == "" {
		projectId = "henry-glcoud-production"
	}

	options := make([]option.ClientOption, 0)
	if credentials != "" {
		options = append(options, option.WithCredentialsFile(credentials))
	}

	return firestore.NewClientWithDatabase(context.Background(), projectId, database, options...)
}

func createClientWithProjectIdAndDatabase(credentialsFile string, projectId string, databaseName string) (*firestore.Client, error) {
	ctx := context.Background()
	options := []option.ClientOption{}

	if os.Getenv("FIRESTORE_EMULATOR_HOST") != "" {
		if projectId == "" {
			projectId = "henry-glcoud-production"
		}
		return firestore.NewClient(ctx, projectId)
	}

	if projectId == "" {
		projectId = "henry-glcoud-production"
	}

	if databaseName == "" {
		databaseName = firestore.DefaultDatabaseID
	}

	if credentialsFile != "" {
		options = append(options, option.WithCredentialsFile(credentialsFile))
	}

	client, err := firestore.NewClientWithDatabase(ctx, projectId, databaseName, options...)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func getConfigWithProjectId(projectId string) *firebase.Config {
	config := firebase.Config{}
	if projectId != "" {
		config.ProjectID = projectId
	}
	return &config
}
