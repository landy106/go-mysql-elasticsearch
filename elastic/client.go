package elastic

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/juju/errors"
)

// Client is the client to communicate with ES.
// Although there are many Elasticsearch clients with Go, I still want to implement one by myself.
// Because we only need some very simple usages.
type Client struct {
	Protocol string
	Addr     string
	User     string
	Password string

	httpClient *http.Client
}

// ClientConfig is the configuration for the client.
type ClientConfig struct {
	HTTPS    bool
	Addr     string
	User     string
	Password string
}

// NewClient creates the Cient with configuration.
func NewClient(conf *ClientConfig) *Client {
	c := new(Client)

	c.Addr = conf.Addr
	c.User = conf.User
	c.Password = conf.Password

	if conf.HTTPS {
		c.Protocol = "https"
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
		c.httpClient = &http.Client{Transport: tr}
	} else {
		c.Protocol = "http"
		c.httpClient = &http.Client{}
	}

	return c
}

// ResponseItem is the ES item in the response.
type ResponseItem struct {
	Index   string         `json:"_index"`
	ID      string         `json:"_id"`
	Version int            `json:"_version"`
	Found   bool           `json:"found"`
	Source  map[string]any `json:"_source"`
}

// Response is the ES response
type Response struct {
	Code int
	ResponseItem
}

// https://www.elastic.co/guide/en/elasticsearch/reference/8.17/docs-bulk.html
const (
	ActionCreate = "create"
	ActionUpdate = "update"
	ActionDelete = "delete"
	ActionIndex  = "index"
)

// BulkRequest is used to send multi request in batch.
type BulkRequest struct {
	Action string
	Index  string
	ID     string

	Data map[string]any
}

func (r *BulkRequest) bulk(buf *bytes.Buffer) error {
	meta := make(map[string]map[string]string)
	metaData := make(map[string]string)

	if len(r.Index) > 0 {
		metaData["_index"] = r.Index
	}

	if len(r.ID) > 0 {
		metaData["_id"] = r.ID
	}

	meta[r.Action] = metaData

	data, err := json.Marshal(meta)
	if err != nil {
		return errors.Trace(err)
	}

	buf.Write(data)
	buf.WriteByte('\n')

	switch r.Action {
	case ActionDelete:
		//nothing to do
	case ActionUpdate:
		doc := map[string]any{
			"doc": r.Data,
		}
		data, err = json.Marshal(doc)
		if err != nil {
			return errors.Trace(err)
		}

		buf.Write(data)
		buf.WriteByte('\n')
	default:
		//for create and index
		data, err = json.Marshal(r.Data)
		if err != nil {
			return errors.Trace(err)
		}

		buf.Write(data)
		buf.WriteByte('\n')
	}

	return nil
}

// BulkResponse is the response for the bulk request.
type BulkResponse struct {
	Code   int
	Took   int  `json:"took"`
	Errors bool `json:"errors"`

	Items []map[string]*BulkResponseItem `json:"items"`
}

// BulkResponseItem is the item in the bulk response.
type BulkResponseItem struct {
	Index   string          `json:"_index"`
	ID      string          `json:"_id"`
	Version int             `json:"_version"`
	Result  string          `json:"result"`
	Status  int             `json:"status"`
	Error   json.RawMessage `json:"error"`
}

// DoRequest sends a request with body to ES.
func (c *Client) DoRequest(method string, url string, body *bytes.Buffer) (*http.Response, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, errors.Trace(err)
	}
	req.Header.Add("Content-Type", "application/json")

	if len(c.User) > 0 && len(c.Password) > 0 {
		req.SetBasicAuth(c.User, c.Password)
	}
	resp, err := c.httpClient.Do(req)

	return resp, err
}

// Do sends the request with body to ES.
func (c *Client) Do(method string, url string, body map[string]any) (*Response, error) {
	bodyData, err := json.Marshal(body)
	if err != nil {
		return nil, errors.Trace(err)
	}

	buf := bytes.NewBuffer(bodyData)
	if body == nil {
		buf = bytes.NewBuffer(nil)
	}

	resp, err := c.DoRequest(method, url, buf)
	if err != nil {
		return nil, errors.Trace(err)
	}

	defer resp.Body.Close()

	ret := new(Response)
	ret.Code = resp.StatusCode

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(data) > 0 {
		err = json.Unmarshal(data, &ret.ResponseItem)
	}

	return ret, errors.Trace(err)
}

// DoBulk sends the bulk request to the ES.
func (c *Client) DoBulk(url string, items []*BulkRequest) (*BulkResponse, error) {
	var buf bytes.Buffer

	for _, item := range items {
		if err := item.bulk(&buf); err != nil {
			return nil, errors.Trace(err)
		}
	}

	resp, err := c.DoRequest("POST", url, &buf)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer resp.Body.Close()

	ret := new(BulkResponse)
	ret.Code = resp.StatusCode

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(data) > 0 {
		err = json.Unmarshal(data, &ret)
	}

	return ret, errors.Trace(err)
}

// DeleteIndex deletes the index.
func (c *Client) DeleteIndex(index string) error {
	reqURL := fmt.Sprintf("%s://%s/%s", c.Protocol, c.Addr,
		url.QueryEscape(index))

	r, err := c.Do("DELETE", reqURL, nil)
	if err != nil {
		return errors.Trace(err)
	}

	if r.Code == http.StatusOK || r.Code == http.StatusNotFound {
		return nil
	}

	return errors.Errorf("Error: %s, code: %d", http.StatusText(r.Code), r.Code)
}

// GetDocument gets the item by id.
func (c *Client) GetDocument(index string, id string) (*Response, error) {
	reqURL := fmt.Sprintf("%s://%s/%s/_doc/%s", c.Protocol, c.Addr,
		url.QueryEscape(index),
		url.QueryEscape(id))

	return c.Do("GET", reqURL, nil)
}

// AddDocument add or overwrite a document
func (c *Client) AddDocument(index string, id string, data map[string]any) error {
	reqURL := fmt.Sprintf("%s://%s/%s/_doc/%s", c.Protocol, c.Addr,
		url.QueryEscape(index),
		url.QueryEscape(id))

	r, err := c.Do("PUT", reqURL, data)
	if err != nil {
		return errors.Trace(err)
	}

	if r.Code == http.StatusOK || r.Code == http.StatusCreated {
		return nil
	}

	return errors.Errorf("Error: %s, code: %d", http.StatusText(r.Code), r.Code)
}

// HasDocument checks whether id exists or not.
func (c *Client) HasDocument(index string, id string) (bool, error) {
	reqURL := fmt.Sprintf("%s://%s/%s/_doc/%s", c.Protocol, c.Addr,
		url.QueryEscape(index),
		url.QueryEscape(id))

	r, err := c.Do("HEAD", reqURL, nil)
	if err != nil {
		return false, err
	}

	return r.Code == http.StatusOK, nil
}

// DeleteDocument deletes the document by id.
func (c *Client) DeleteDocument(index string, id string) error {
	reqURL := fmt.Sprintf("%s://%s/%s/_doc/%s", c.Protocol, c.Addr,
		url.QueryEscape(index),
		url.QueryEscape(id))

	r, err := c.Do("DELETE", reqURL, nil)
	if err != nil {
		return errors.Trace(err)
	}

	if r.Code == http.StatusOK || r.Code == http.StatusNotFound {
		return nil
	}

	return errors.Errorf("Error: %s, code: %d", http.StatusText(r.Code), r.Code)
}

// Bulk sends the bulk request.
// only support parent in 'Bulk' related apis
func (c *Client) Bulk(items []*BulkRequest) (*BulkResponse, error) {
	reqURL := fmt.Sprintf("%s://%s/_bulk", c.Protocol, c.Addr)

	return c.DoBulk(reqURL, items)
}

// IndexBulk sends the bulk request for index.
func (c *Client) IndexBulk(index string, items []*BulkRequest) (*BulkResponse, error) {
	reqURL := fmt.Sprintf("%s://%s/%s/_bulk", c.Protocol, c.Addr,
		url.QueryEscape(index))

	return c.DoBulk(reqURL, items)
}
