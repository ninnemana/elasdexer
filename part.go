package main

import (
	"encoding/json"
	"github.com/ziutek/mymysql/mysql"
	_ "github.com/ziutek/mymysql/native"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

func IndexParts() error {

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
							return
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
	Categories                              []Category
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
