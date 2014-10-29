package main

import (
	"encoding/json"
	"github.com/ziutek/mymysql/mysql"
	_ "github.com/ziutek/mymysql/native"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type Category struct {
	CategoryId, ParentId, Sort   int
	DateAdded                    time.Time
	Title, ShortDesc, LongDesc   string
	ColorCode, FontCode          string
	Image                        *url.URL
	IsLifestyle, VehicleSpecific bool
	SubCategories                []Category
	Content                      []Content
}

func IndexCategories() IndexResponse {

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
	for _, catID := range ids {
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
