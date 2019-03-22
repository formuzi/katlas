package splunk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"strconv"
	"strings"
	"testing"
	"time"
	"github.com/intuit/katlas/service/db"
	"github.com/intuit/katlas/service/cfg"
	"github.com/intuit/katlas/service/util"
	"github.com/intuit/katlas/service/apis"
	"os"
)

type jsonWriter struct {
	buf bytes.Buffer
}

func (w *jsonWriter) Write(data []byte) (int, error) {
	json.Indent(&w.buf, data, "", "\t")
	w.buf.WriteTo(os.Stdout)
	dc := db.NewDGClient(cfg.ServerCfg.DgraphHost)
	defer dc.Close()
	entitySvc := apis.NewEntityService(dc)

	var resp map[string]interface{}
	json.Unmarshal(data, &resp)
	if _, ok := resp["lastrow"]; ok {
		return w.buf.Len(), nil
	}

	raw := resp["result"].(map[string]interface{})["_raw"].(string)
	clusterName := resp["result"].(map[string]interface{})["clustername"].(string)
	var entity map[string]interface{}
	json.Unmarshal([]byte(raw), &entity)
	createTimestamp := entity["metadata"].(map[string]interface{})["creationTimestamp"].(string)
	t, _ := time.Parse(time.RFC3339, createTimestamp)


	switch resp["result"].(map[string]interface{})["verb"] {
	case "create":
		// get object
		reqObj := entity["requestObject"].(map[string]interface{})
		kind := strings.ToLower(reqObj["kind"].(string))
		respObj, _ := json.Marshal(entity["responseObject"])
		e, err := util.BuildEntityData(clusterName, kind, respObj, false)
		if err != nil {
			log.Errorf("something wrong to build entity %v", err)
			return 0, err
		}
		e.(map[string]interface{})["name"] = e.(map[string]interface{})["name"].(string) + "_" + strconv.FormatInt(t.Unix(), 10)
		e.(map[string]interface{})["userdata"] = map[string]interface{}{"verb": "create"}
		uid, err := entitySvc.CreateEntity(kind, e.(map[string]interface{}))

		fmt.Println(uid)
		break
	case "update":
		break
	case "patch":
		// get object
		reqObj := entity["requestObject"].(map[string]interface{})
		kind := strings.ToLower(entity["responseObject"].(map[string]interface{})["kind"].(string))
		respObj, _ := json.Marshal(entity["responseObject"])
		e, err := util.BuildEntityData(clusterName, kind, respObj, false)
		if err != nil {
			log.Errorf("something wrong to build entity %v", err)
			return 0, err
		}
		e.(map[string]interface{})["name"] = e.(map[string]interface{})["name"].(string) + "_" + strconv.FormatInt(t.Unix(), 10)
		reqObj["verb"] = "patch"
		e.(map[string]interface{})["userdata"] = reqObj
		uid, err := entitySvc.CreateEntity(kind, e.(map[string]interface{}))
		fmt.Println(uid)
		break
	case "delete":
		kind := strings.ToLower(entity["responseObject"].(map[string]interface{})["kind"].(string))
		respObj, _ := json.Marshal(entity["responseObject"])
		e, err := util.BuildEntityData(clusterName, kind, respObj, false)
		if err != nil {
			log.Errorf("something wrong to build entity %v", err)
			return 0, err
		}
		e.(map[string]interface{})["name"] = e.(map[string]interface{})["name"].(string) + "_" + strconv.FormatInt(t.Unix(), 10)
		e.(map[string]interface{})["userdata"] = map[string]interface{}{"verb": "delete"}
		uid, err := entitySvc.CreateEntity(kind, e.(map[string]interface{}))
		fmt.Println(uid)

		break
	default:
		return 0, fmt.Errorf("unknow verb for parsing data")
	}
	return w.buf.Len(), nil
}

func TestSearch(t *testing.T) {

	ctx, _ := context.WithCancel(context.Background())

	client := NewClient("", "", `https://splunk-cto-e2e-search-rest.platform.intuit.net`)
	if err := client.Login(ctx); err != nil {
		log.Fatalln(err)
	}

	w := &jsonWriter{}

	if err := client.Search(ctx, `source=kube-apiserver-audit verb=create OR verb=update OR verb=patch OR verb=delete level=RequestResponse| spath "responseStatus.code" | search "responseStatus.code"=20* | fields clustername verb`, "-24h", w); err != nil {
		log.Fatalln(err)
	}

}
