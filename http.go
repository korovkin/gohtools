package gohtools

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

func HttpGetJson(url string, o interface{}, username string, password string) error {
	var err error = nil
	body := []byte{}

	if strings.HasPrefix(url, "http") {
		req, err := http.NewRequest("GET", url, nil)
		CheckNotFatal(err)
		if err != nil {
			return err
		}

		if username != "" || password != "" {
			req.SetBasicAuth(username, password)
		}

		client := http.Client{}
		resp, err := client.Do(req)
		CheckNotFatal(err)
		if err != nil {
			return err
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			err = errors.New(fmt.Sprintf("ERROR: HTTP: CODE: %d", resp.StatusCode))
			CheckNotFatal(err)
			return err
		}

		defer resp.Body.Close()
		body, err = ioutil.ReadAll(resp.Body)
		CheckNotFatal(err)
		if err != nil {
			return err
		}
	} else if strings.HasPrefix(url, "file://") {
		body, err = ioutil.ReadFile(url[len("file://"):])
		CheckNotFatal(err)
		if err != nil {
			return err
		}
	} else {
		err = errors.New("unknown URL: " + url)
		CheckNotFatal(err)
		return err
	}

	err = FromJsonBytes(body, o)
	CheckNotFatal(err)
	if err != nil {
		return err
	}

	return nil
}

func HttpGetXML(url string, o interface{}) error {
	resp, err := http.Get(url)
	CheckNotFatal(err)
	if err != nil {
		return err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return errors.New(fmt.Sprintf("ERROR: HTTP: CODE: %d", resp.StatusCode))
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	CheckNotFatal(err)
	if err != nil {
		return err
	}

	err = xml.Unmarshal(body, o)
	CheckNotFatal(err)
	if err != nil {
		return err
	}

	return nil
}

func IsValidURL(urlString string) error {
	_, err := url.Parse(urlString)
	CheckFatal(err)
	return err
}
