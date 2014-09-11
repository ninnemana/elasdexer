package main

import (
	"encoding/json"
	"flag"
	"github.com/mattbaird/elastigo/lib"

	"github.com/ziutek/mymysql/mysql"
	_ "github.com/ziutek/mymysql/native" // Native engine

	"log"
	"net/http"
	"net/url"
	"strconv"
	"time"
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

	resp := indexCategories()
	log.Println(resp.Errors)

	err := indexParts()
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

func indexCategories() IndexResponse {

	var resp IndexResponse

	ids, err := getCategoryIds()
	if err != nil {
		resp.Errors = append(resp.Errors, err.Error())
		return resp
	}

	errors := make([]string, 0)
	successfulTransactions := 0
	failedTransactions := 0
	updateCount := 0
	con := newEsConnection()
	ch := make(chan int)
	for _, i := range ids {
		go func(catID int) {
			cat, err := getCategory(catID)
			if err != nil {
				errors = append(errors, err.Error())
				failedTransactions++
			} else {
				if cat.CategoryId > 0 {
					baseResponse, err := con.Exists("curt", "category", strconv.Itoa(cat.CategoryId), nil)
					if baseResponse.Exists && err == nil {
						con.Update("curt", "category", strconv.Itoa(cat.CategoryId), nil, cat)
						updateCount++
					}
					// add single struct entity
					con.Index("curt", "category", strconv.Itoa(cat.CategoryId), nil, cat)
					successfulTransactions++
				}
			}
			ch <- 0
		}(i)
	}

	for _, _ = range ids {
		<-ch
	}

	resp = IndexResponse{
		Successful: successfulTransactions,
		Failed:     failedTransactions,
		Updated:    updateCount,
		Inserted:   successfulTransactions - updateCount,
		Errors:     errors,
	}

	return resp
}

func getCategory(catID int) (c Category, err error) {
	res, err := http.Get("http://goapi.curtmfg.com/category/" + strconv.Itoa(catID) + "?key=" + *API_KEY)
	if err != nil {
		return
	}
	defer res.Body.Close()

	decoder := json.NewDecoder(res.Body)
	err = decoder.Decode(&c)

	return
}

func getCategoryIds() (nums []int, err error) {
	db := mysql.New("tcp", "", *DB_HOST, *DB_USER, *DB_PASS, *DB_NAME)
	err = db.Connect()
	if err != nil {
		return
	}

	rows, _, err := db.Query("select distinct catID from Categories")
	if err != nil {
		return
	}

	for _, row := range rows {
		nums = append(nums, row.Int(0))
	}
	return
}

func indexParts() error {

	nums, err := getPartNumbers()
	if err != nil {
		return err
	}

	errors := make([]string, 0)
	successfulTransactions := 0
	failedTransactions := 0
	updateCount := 0

	con := newEsConnection()
	segments := (len(nums) - 1) / 5

	for i := 0; i < segments; i++ {
		segmentNumbers := nums[(i * 5) : (i*5)+5]
		ch := make(chan int)
		for _, n := range segmentNumbers {
			go func(id int) {
				part, err := getPart(id)
				if err != nil {
					errors = append(errors, err.Error())
					failedTransactions++
				} else {
					if part.PartId > 0 && part.Status > 0 {
						baseResponse, err := con.Exists("curt", "part", strconv.Itoa(part.PartId), nil)
						if baseResponse.Exists && err == nil {
							con.Update("curt", "part", strconv.Itoa(part.PartId), nil, part)
							updateCount++
						}
						// add single struct entity
						con.Index("curt", "part", strconv.Itoa(part.PartId), nil, part)
						successfulTransactions++
					}
				}
				ch <- 0
			}(n)
		}

		for _, _ = range segmentNumbers {
			<-ch
		}
	}

	log.Printf("Successful Part Transactions: %d\n", successfulTransactions)
	log.Printf("Failed Part Transactions: %d\n", failedTransactions)
	log.Printf("Total Part Records Updated: %d\n", updateCount)
	log.Printf("Total Part Records Insert: %d\n", successfulTransactions-updateCount)
	return nil
}

func getPart(partID int) (p Part, err error) {
	res, err := http.Get("http://goapi.curtmfg.com/part/" + strconv.Itoa(partID) + "?key=" + *API_KEY)
	if err != nil {
		return
	}
	defer res.Body.Close()
	decoder := json.NewDecoder(res.Body)
	err = decoder.Decode(&p)

	return
}

func getPartNumbers() (nums []int, err error) {
	db := mysql.New("tcp", "", *DB_HOST, *DB_USER, *DB_PASS, *DB_NAME)
	err = db.Connect()
	if err != nil {
		return
	}

	rows, _, err := db.Query("select distinct partID from Part where status = 800 || status = 900")
	if err != nil {
		return
	}

	for _, row := range rows {
		nums = append(nums, row.Int(0))
	}
	return
}

type Part struct {
	PartId, Status, PriceCode, RelatedCount int
	AverageReview                           float64
	DateModified, DateAdded                 time.Time
	ShortDesc, PartClass                    string
	InstallSheet                            *url.URL
	Attributes                              []Attribute
	VehicleAttributes                       []string
	Vehicles                                []Vehicle
	Content                                 []Content
	Pricing                                 []Pricing
	Reviews                                 []Review
	Images                                  []Image
	Related                                 []int
	Categories                              []ExtendedCategory
	Videos                                  []PartVideo
	Packages                                []Package
	Customer                                CustomerPart
}

type Vehicle struct {
	ID                    int
	Year                  int
	Make, Model, Submodel string
	Configuration         []string
}

type PagedParts struct {
	Parts  []Part
	Paging []Paging
}

type Paging struct {
	CurrentIndex int
	PageCount    int
}

type CustomerPart struct {
	Price         float64
	CartReference int
}

type Attribute struct {
	Key, Value string
}

type Content struct {
	Key, Value string
}

type PartVideo struct {
	YouTubeVideoId, Type string
	IsPrimary            bool
	TypeIcon             *url.URL
}

type Image struct {
	Size, Sort    string
	Height, Width int
	Path          *url.URL
}

type Review struct {
	Rating                           int
	Subject, ReviewText, Name, Email string
	CreatedDate                      time.Time
}

type Package struct {
	Height, Width, Length, Quantity   float64
	Weight                            float64
	DimensionUnit, DimensionUnitLabel string
	WeightUnit, WeightUnitLabel       string
	PackageUnit, PackageUnitLabel     string
}

type Pricing struct {
	Type     string
	Price    float64
	Enforced bool
}

type Category struct {
	CategoryId, ParentId, Sort   int
	DateAdded                    time.Time
	Title, ShortDesc, LongDesc   string
	ColorCode, FontCode          string
	Image                        *url.URL
	IsLifestyle, VehicleSpecific bool
}

type ExtendedCategory struct {

	// Replicate of the Category struct
	CategoryId, ParentId, Sort   int
	DateAdded                    time.Time
	Title, ShortDesc, LongDesc   string
	ColorCode, FontCode          string
	Image                        *url.URL
	IsLifestyle, VehicleSpecific bool

	// Extension for more detail
	SubCategories []Category
	Content       []Content
}
