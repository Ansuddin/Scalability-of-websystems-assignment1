package sentinel

import (
	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"google.golang.org/api/iterator"
	"google.golang.org/appengine"
	"net/http"
)

type result struct {
	Granule_id string
	Base_url   string
}

type images struct {
	Images []string
}

func getImagesURL(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)

	// GET PARAMS FROM REQUEST
	lat := r.FormValue("lat")
	lng := r.FormValue("lng")
	if lat == "" || lng == "" {
		http.Error(w, "Bad request 400", http.StatusBadRequest)
		return
	}

	// BIGQUERY
	projID := "anud-178408"

	bigclient, err := bigquery.NewClient(ctx, projID)
	if err != nil {
		return
	}

	query := fmt.Sprintf("SELECT granule_id, base_url FROM `sentinel.sentinel_2_index_copy` WHERE  (%s between south_lat and north_lat) and (%s between west_lon and east_lon) LIMIT 1", lat, lng)

	q := bigclient.Query(query)
	it, err := q.Read(ctx)
	if err != nil {
		return
	}
	var res result
	err1 := it.Next(&res)
	if err1 != nil {
		fmt.Fprintf(w, "Error")
		return
	}

	//Create string for prefix
	string1 := res.Base_url[32:len(res.Base_url)]
	objectString := string1 + "/GRANULE/" + res.Granule_id + "/IMG_DATA/"

	//TESTING CLOUD STORAGE ACCESS
	client, err := storage.NewClient(ctx)
	if err != nil {
		return
	}
	iter := client.Bucket("gcp-public-data-sentinel-2").Objects(ctx, &storage.Query{
		Prefix: objectString,
	})

	var imagesString images
	for {
		attrs, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return
		}
		imagesString.Images = append(imagesString.Images, attrs.MediaLink)
	}

	// Marshal
	images, _ := json.Marshal(imagesString)
	w.Write(images)

}

func init() {
	r := mux.NewRouter()
	r.HandleFunc("/images", getImagesURL).Methods("GET")
	http.Handle("/", r)
}
