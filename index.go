package main

import (
	"encoding/json"
	"flag"
	"github.com/mattbaird/elastigo/lib"
	// Native engine

	"log"
)

var (
	API_KEY  = flag.String("key", "", "API Key")
	DOMAIN   = flag.String("domain", "127.0.0.1", "ElasticSearch IP")
	PORT     = flag.String("port", "9200", "ElasticSearch Port")
	USERNAME = flag.String("username", "", "ElasticSearch Username")
	PASSWORD = flag.String("password", "", "ElasticSearch Password")
	DB_HOST  = flag.String("db_host", "127.0.0.1", "Database IP")
	DB_USER  = flag.String("db_user", "", "Database User")
	DB_PASS  = flag.String("db_pass", "", "Database Password")
	DB_NAME  = flag.String("db_name", "", "Database Name")
)

type IndexResponse struct {
	Successful int
	Failed     int
	Updated    int
	Inserted   int
	Errors     []string
}

func main() {

	flag.Parse()

	resp := IndexCategories()
	log.Println(resp.Errors)

	err := IndexParts()
	log.Println(err)

	qry := map[string]interface{}{
		"query": map[string]interface{}{
			"query_string": map[string]string{"query": "ball mount"},
		},
	}
	search(qry)
}

func newEsConnection() *elastigo.Conn {
	con := elastigo.NewConn()
	con.Domain = *DOMAIN
	con.Port = *PORT
	con.Username = *USERNAME
	con.Password = *PASSWORD

	return con
}

func search(query map[string]interface{}) {

	con := newEsConnection()

	var args map[string]interface{}
	res, e := con.Search("curt", "", args, query)
	if e != nil {
		return
	}

	js, err := json.Marshal(res.Hits.Hits)
	if err != nil {
		return
	}

	log.Println(string(js))
}
