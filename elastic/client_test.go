package elastic

import (
	"flag"
	"fmt"
	"testing"

	. "github.com/pingcap/check"
)

var host = flag.String("host", "127.0.0.1", "Elasticsearch host")
var port = flag.Int("port", 9200, "Elasticsearch port")

func Test(t *testing.T) {
	TestingT(t)
}

type elasticTestSuite struct {
	c *Client
}

var _ = Suite(&elasticTestSuite{})

func (s *elasticTestSuite) SetUpSuite(c *C) {
	cfg := new(ClientConfig)
	cfg.Addr = fmt.Sprintf("%s:%d", *host, *port)
	cfg.User = ""
	cfg.Password = ""
	s.c = NewClient(cfg)
}

func (s *elasticTestSuite) TearDownSuite(c *C) {

}

func makeTestData(arg1 string, arg2 string) map[string]any {
	m := make(map[string]any)
	m["name"] = arg1
	m["content"] = arg2

	return m
}

func (s *elasticTestSuite) TestSimple(c *C) {
	index := "dummy"

	err := s.c.AddDocument(index, "1", makeTestData("abc", "hello world"))
	c.Assert(err, IsNil)

	exists, err := s.c.HasDocument(index, "1")
	c.Assert(err, IsNil)
	c.Assert(exists, Equals, true)

	r, err := s.c.GetDocument(index, "1")
	c.Assert(err, IsNil)
	c.Assert(r.Code, Equals, 200)
	c.Assert(r.ID, Equals, "1")

	err = s.c.DeleteDocument(index, "1")
	c.Assert(err, IsNil)

	exists, err = s.c.HasDocument(index, "1")
	c.Assert(err, IsNil)
	c.Assert(exists, Equals, false)

	items := make([]*BulkRequest, 10)

	for i := range 10 {
		id := fmt.Sprintf("%d", i)
		req := new(BulkRequest)
		req.Action = ActionIndex
		req.Index = index
		req.ID = id
		req.Data = makeTestData(fmt.Sprintf("abc %d", i), fmt.Sprintf("hello world %d", i))
		items[i] = req
	}
	_, err = s.c.Bulk(items)
	c.Assert(err, IsNil)

	for i := range 10 {
		id := fmt.Sprintf("%d", i)
		req := new(BulkRequest)
		req.Action = ActionDelete
		req.Index = index
		req.ID = id
		items[i] = req
	}
	_, err = s.c.Bulk(items)
	c.Assert(err, IsNil)

}
