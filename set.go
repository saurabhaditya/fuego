package main

import (
	"context"
	"fmt"
	"time"

	firestore "cloud.google.com/go/firestore"
	"github.com/urfave/cli"
)

func setData(
	client *firestore.Client,
	collectionPath string,
	documentPath string,
	id string,
	data string,
	merge bool) error {

	object, err := unmarshallData(data)
	if err != nil {
		return err
	}

	transformExtendedJsonMapToFirestoreMap(object, client)
	transformToTime(object)

	var options []firestore.SetOption
	if merge {
		options = append(options, firestore.MergeAll)
	}

	if collectionPath != "" {
		_, err = client.
			Collection(collectionPath).
			Doc(id).
			Set(context.Background(), object, options...)
	} else {
		_, err = client.
			Doc(documentPath).
			Set(context.Background(), object, options...)
	}

	if err != nil {
		return err
	}

	return nil
}

// transform matching string to time.Time
func transformToTime(m map[string]interface{}) {
	for k, v := range m {
		switch v := v.(type) {
		case string:
			// parse "2025-01-13T20:36:32.320825Z" into time.Time
			if !rfc3339regex.MatchString(v) {
				continue
			}
			t, err := time.Parse(time.RFC3339, v)
			if err != nil {
				fmt.Println("Unexpected error parsing rfc3339", v)
				continue
			}
			m[k] = t
		case []interface{}:
			for _, item := range v {
				if itemMap, ok := item.(map[string]interface{}); ok {
					transformToTime(itemMap)
				}
			}
		case map[string]interface{}:
			transformToTime(v)
		}
	}
}

func setCommandAction(c *cli.Context) error {
	argsLength := len(c.Args())

	if argsLength < 2 || argsLength > 3 {
		return cli.NewExitError("Wrong number of arguments", 85)
	}

	merge := c.Bool("merge")

	var collectionPath, id, data, documentPath string

	if argsLength == 3 {
		collectionPath = c.Args().First()
		id = c.Args().Get(1)
		data = c.Args().Get(2)
	} else {
		documentPath = c.Args().First()
		data = c.Args().Get(1)
	}

	client, err := createClient(credentials)
	if err != nil {
		return cliClientError(err)
	}

	err = setData(client, collectionPath, documentPath, id, data, merge)
	if err != nil {
		return cli.NewExitError(fmt.Sprintf("Failed to write data. \n%v", err), 85)
	}

	if collectionPath != "" {
		fmt.Fprintf(c.App.Writer, "%v\n", id)
	} else {
		fmt.Fprintf(c.App.Writer, "%v\n", documentPath)
	}
	defer client.Close()
	return nil
}
