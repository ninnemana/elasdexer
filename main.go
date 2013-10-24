package main

import (
	"encoding/json"
	"flag"
	"github.com/mattbaird/elastigo/api"
	"github.com/mattbaird/elastigo/core"
	"github.com/ziutek/mymysql/mysql"
	_ "github.com/ziutek/mymysql/native" // Native engine
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

var (
	API_KEY = flag.String("key", "", "API Key")
	DOMAIN  = flag.String("domain", "", "ElasticSearch IP")
	DB_HOST = flag.String("db_host", "", "Database IP")
	DB_USER = flag.String("db_user", "", "Database User")
	DB_PASS = flag.String("db_pass", "", "Database Password")
	DB_NAME = flag.String("db_name", "", "Database Name")
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
	api.Domain = *DOMAIN
	api.Port = "9200"
	// go func() {
	// 	err := indexCategories()
	// 	if err != nil {
	// 		log.Println(err)
	// 	}
	// }()
	// if e := indexParts(); e != nil {
	// 	log.Println(e)
	// }
	search("ball mount")
}

func search(query string) {
	res, e := core.SearchUri("curt", "", query, "", 0)
	for _, hit := range res.Hits.Hits {
		log.Println(string(hit.Source))
	}
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
	for _, i := range ids {
		cat, err := getCategory(i)
		if err != nil {
			errors = append(errors, err.Error())
			failedTransactions++
		} else {
			if cat.CategoryId > 0 {
				exists, err := core.Exists(true, "curt", "part", strconv.Itoa(cat.CategoryId))
				if exists && err == nil {
					updateCount++
				}
				// add single struct entity
				core.Index(true, "curt", "category", strconv.Itoa(cat.CategoryId), cat)
				successfulTransactions++
			}
		}
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
	buf, err := ioutil.ReadAll(res.Body)
	res.Body.Close()

	err = json.Unmarshal(buf, &c)
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
	for _, n := range nums {
		part, err := getPart(n)
		if err != nil {
			errors = append(errors, err.Error())
			failedTransactions++
		} else {
			if part.PartId > 0 {
				exists, err := core.Exists(true, "curt", "part", strconv.Itoa(part.PartId))
				if exists && err == nil {
					updateCount++
				}
				// add single struct entity
				core.Index(true, "curt", "part", strconv.Itoa(part.PartId), part)
				successfulTransactions++
			}
		}
	}

	for _, e := range errors {
		log.Println(e)
	}
	log.Printf("Successful Part Transactions: %d", successfulTransactions)
	log.Printf("Failed Part Transactions: %d", failedTransactions)
	log.Printf("Total Part Records Updated: %d", updateCount)
	log.Printf("Total Part Records Insert: %d", successfulTransactions-updateCount)
	return nil
}

func getPart(partID int) (p Part, err error) {
	res, err := http.Get("http://goapi.curtmfg.com/part/" + strconv.Itoa(partID) + "?key=" + *API_KEY)
	if err != nil {
		return
	}
	buf, err := ioutil.ReadAll(res.Body)
	res.Body.Close()

	err = json.Unmarshal(buf, &p)
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
