package resources

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/intuit/katlas/service/apis"
	"github.com/intuit/katlas/service/db"
	"github.com/intuit/katlas/service/metrics"
	"github.com/intuit/katlas/service/util"
	"github.com/mitchellh/mapstructure"
	"reflect"
	"strings"
)

// ServerResource handle http request
type ServerResource struct {
	EntitySvc *apis.EntityService
	QuerySvc  *apis.QueryService
	MetaSvc   *apis.MetaService
	QSLSvc    *apis.QSLService
	// TODO:
	// add metadata service, audit service and spec service after API ready
}

// EntityGetHandler REST API for get Entity
func (s ServerResource) EntityGetHandler(w http.ResponseWriter, r *http.Request) {
	metrics.KatlasNumReqCount.Inc()
	//Set Access-Control-Allow-Origin header now so that it will be present
	//even if an error is returned (otherwise the error also causes a CORS
	//exception in the browser/client)
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
	vars := mux.Vars(r)
	uid := vars[util.UID]
	start := time.Now()
	code := http.StatusOK
	defer func() {
		metrics.DgraphGetEntityLatencyHistogram.WithLabelValues(fmt.Sprintf("%d", code)).Observe(time.Since(start).Seconds())
	}()
	obj, err := s.EntitySvc.GetEntity(uid)
	if err != nil {
		metrics.KatlasNumReqErr.Inc()
		metrics.KatlasNumReqErr5xx.Inc()
		code = http.StatusInternalServerError
		w.WriteHeader(code)
		w.Write([]byte(fmt.Sprintf("{\"status\": %v, \"error\": \"%s\"}", code, trim(err.Error()))))
		return
	}
	// object not found
	if len(obj) == 0 {
		metrics.KatlasNumReqErr.Inc()
		metrics.KatlasNumReqErr4xx.Inc()
		code = http.StatusNotFound
		w.WriteHeader(code)
		w.Write([]byte(fmt.Sprintf("{\"status\": %v, \"error\": \"entity with id %s not found\"}", code, uid)))
		return
	}
	obj["status"] = code
	ret, _ := json.Marshal(obj)
	w.Write(ret)
	metrics.KatlasNumReq2xx.Inc()
}

// MetaGetHandler REST API for get metadata
func (s ServerResource) MetaGetHandler(w http.ResponseWriter, r *http.Request) {
	metrics.KatlasNumReqCount.Inc()
	//Set Access-Control-Allow-Origin header now so that it will be present
	//even if an error is returned (otherwise the error also causes a CORS
	//exception in the browser/client)
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
	vars := mux.Vars(r)
	name := strings.ToLower(vars[util.Name])
	obj, err := s.MetaSvc.GetMetadata(name)
	if err != nil {
		metrics.KatlasNumReqErr.Inc()
		metrics.KatlasNumReqErr5xx.Inc()
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf("{\"status\": %v, \"error\": \"%s\"}", http.StatusInternalServerError, trim(err.Error()))))
		return
	}
	if obj != nil {
		metrics.KatlasNumReq2xx.Inc()
		ret := []byte(fmt.Sprintf("{\"status\": %v, \"objects\": [", http.StatusOK))
		meta, _ := json.Marshal(obj)
		ret = append(ret, meta...)
		ret = append(ret, []byte("]}")...)
		w.Write(ret)
		return
	}
	metrics.KatlasNumReqErr.Inc()
	metrics.KatlasNumReqErr4xx.Inc()
	w.WriteHeader(http.StatusNotFound)
	w.Write([]byte(fmt.Sprintf("{\"status\": %v, \"error\": \"metadata %s not found\"}", http.StatusNotFound, name)))
}

// MetaDeleteHandler REST API for delete metadata
func (s ServerResource) MetaDeleteHandler(w http.ResponseWriter, r *http.Request) {
	metrics.KatlasNumReqCount.Inc()
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
	vars := mux.Vars(r)
	name := vars[util.Name]
	err := s.MetaSvc.DeleteMetadata(name)
	if err != nil {
		metrics.KatlasNumReqErr.Inc()
		metrics.KatlasNumReqErr4xx.Inc()
		w.WriteHeader(http.StatusConflict)
		w.Write([]byte(fmt.Sprintf("{\"status\": %v, \"error\": \"%s\"}", http.StatusConflict, trim(err.Error()))))
		return
	}
	msg := map[string]interface{}{
		"status": http.StatusOK,
		"objects": []map[string]interface{}{
			{
				"name":    name,
				"objtype": "metadata",
			},
		},
	}
	ret, _ := json.Marshal(msg)
	w.Write(ret)
	metrics.KatlasNumReq2xx.Inc()
}

// EntityDeleteHandler REST API for delete Entity
func (s ServerResource) EntityDeleteHandler(w http.ResponseWriter, r *http.Request) {
	metrics.KatlasNumReqCount.Inc()
	//Set Access-Control-Allow-Origin header now so that it will be present
	//even if an error is returned (otherwise the error also causes a CORS
	//exception in the browser/client)
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
	vars := mux.Vars(r)
	meta := vars[util.Metadata]
	rid := vars[util.ResourceID]

	start := time.Now()
	code := http.StatusOK
	defer func() {
		metrics.DgraphDeleteEntityLatencyHistogram.WithLabelValues(fmt.Sprintf("%d", code)).Observe(time.Since(start).Seconds())
	}()

	err := s.EntitySvc.DeleteEntityByResourceID(meta, rid)
	if err != nil {
		metrics.KatlasNumReqErr.Inc()
		metrics.KatlasNumReqErr5xx.Inc()
		code = http.StatusInternalServerError
		w.WriteHeader(code)
		w.Write([]byte(fmt.Sprintf("{\"status\": %v, \"error\": \"%s\"}", code, trim(err.Error()))))
		return
	}
	msg := map[string]interface{}{
		"status": code,
		"objects": []map[string]interface{}{
			{
				"resourceid": rid,
				"objtype":    meta,
			},
		},
	}
	ret, _ := json.Marshal(msg)
	w.Write(ret)
	metrics.KatlasNumReq2xx.Inc()
}

// EntityCreateHandler REST API for create Entity
func (s ServerResource) EntityCreateHandler(w http.ResponseWriter, r *http.Request) {
	metrics.KatlasNumReqCount.Inc()
	//Set Access-Control-Allow-Origin header now so that it will be present
	//even if an error is returned (otherwise the error also causes a CORS
	//exception in the browser/client)
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
	vars := mux.Vars(r)
	meta := vars[util.Metadata]
	clusterName := r.Header.Get(util.ClusterName)
	code := http.StatusOK
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Error(err)
		code = http.StatusBadRequest
		w.WriteHeader(code)
		w.Write([]byte(fmt.Sprintf("{\"status\": %v, \"error\": \"%s\"}", code, trim(err.Error()))))
		return
	}
	payload, err := util.BuildEntityData(clusterName, meta, body, false)
	if err != nil {
		metrics.KatlasNumReqErr.Inc()
		metrics.KatlasNumReqErr4xx.Inc()
		log.Error(err)
		code = http.StatusBadRequest
		w.WriteHeader(code)
		w.Write([]byte(fmt.Sprintf("{\"status\": %v, \"error\": \"%s\"}", code, trim(err.Error()))))
		return
	}
	start := time.Now()
	defer func() {
		metrics.DgraphCreateEntityLatencyHistogram.WithLabelValues(fmt.Sprintf("%d", code)).Observe(time.Since(start).Seconds())
	}()

	uid, err := s.EntitySvc.CreateEntity(meta, payload.(map[string]interface{}))
	if err != nil {
		metrics.KatlasNumReqErr.Inc()
		metrics.KatlasNumReqErr5xx.Inc()
		log.Error(err)
		code = http.StatusInternalServerError
		w.WriteHeader(code)
		w.Write([]byte(fmt.Sprintf("{\"status\": %v, \"error\": \"%s\"}", code, trim(err.Error()))))
		return
	}
	msg := map[string]interface{}{
		"status": code,
		"objects": []map[string]interface{}{
			{
				"uid":     uid,
				"objtype": meta,
			},
		},
	}
	ret, _ := json.Marshal(msg)
	w.Write(ret)
	metrics.KatlasNumReq2xx.Inc()
}

// EntityUpdateHandler REST API for update Entity
func (s ServerResource) EntityUpdateHandler(w http.ResponseWriter, r *http.Request) {
	metrics.KatlasNumReqCount.Inc()
	//Set Access-Control-Allow-Origin header now so that it will be present
	//even if an error is returned (otherwise the error also causes a CORS
	//exception in the browser/client)
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	vars := mux.Vars(r)
	meta := vars[util.Metadata]
	uuid := vars[util.UID]
	code := http.StatusOK
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Error(err)
		code = http.StatusBadRequest
		w.WriteHeader(code)
		w.Write([]byte(fmt.Sprintf("{\"status\": %v, \"error\": \"%s\"}", code, trim(err.Error()))))
		return
	}
	payload := make(map[string]interface{}, 0)
	err = json.Unmarshal(body, &payload)
	if err != nil {
		metrics.KatlasNumReqErr.Inc()
		metrics.KatlasNumReqErr4xx.Inc()
		log.Error(err)
		code = http.StatusBadRequest
		w.WriteHeader(code)
		w.Write([]byte(fmt.Sprintf("{\"status\": %v, \"error\": \"%s\"}", code, trim(err.Error()))))
		return
	}
	start := time.Now()
	defer func() {
		metrics.DgraphUpdateEntityLatencyHistogram.WithLabelValues(fmt.Sprintf("%d", code)).Observe(time.Since(start).Seconds())
	}()

	err = s.EntitySvc.UpdateEntity(uuid, payload)
	if err != nil {
		metrics.KatlasNumReqErr.Inc()
		metrics.KatlasNumReqErr5xx.Inc()
		log.Error(err)
		code = http.StatusInternalServerError
		w.WriteHeader(code)
		w.Write([]byte(fmt.Sprintf("{\"status\": %v, \"error\": \"%s\"}", code, trim(err.Error()))))
		return
	}
	msg := map[string]interface{}{
		"status": code,
		"objects": []map[string]interface{}{
			{
				"uid":     uuid,
				"objtype": meta,
			},
		},
	}
	ret, _ := json.Marshal(msg)
	w.Write(ret)

	metrics.KatlasNumReq2xx.Inc()
}

// EntitySyncHandler REST API to sync entities
func (s ServerResource) EntitySyncHandler(w http.ResponseWriter, r *http.Request) {
	//Set Access-Control-Allow-Origin header now so that it will be present
	//even if an error is returned (otherwise the error also causes a CORS
	//exception in the browser/client)
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
	vars := mux.Vars(r)
	meta := vars[util.Metadata]
	clusterName := r.Header.Get(util.ClusterName)
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Error(err)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf("{\"status\": %v, \"error\": \"%s\"}", http.StatusBadRequest, trim(err.Error()))))
		return
	}
	payload, err := util.BuildEntityData(clusterName, meta, body, true)
	if err != nil {
		metrics.KatlasNumReqErr.Inc()
		metrics.KatlasNumReqErr4xx.Inc()
		log.Error(err)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf("{\"status\": %v, \"error\": \"%s\"}", http.StatusBadRequest, trim(err.Error()))))
		return
	}
	metrics.KatlasNumReqCount.Inc()
	// return status code 202 directly
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(fmt.Sprintf("{\"status\": %v, \"message\": \"%s sync request accepted\"}", http.StatusAccepted, meta)))
	// process by goroutine
	go func() {
		err = s.EntitySvc.SyncEntities(meta, payload.([]map[string]interface{}))
		if err != nil {
			metrics.KatlasNumReqErr.Inc()
			metrics.KatlasNumReqErr5xx.Inc()
			log.Error(err)
			return
		}
		log.Debugf("%s sync completed, total %d synced", meta, len(payload.([]map[string]interface{})))
		metrics.KatlasNumReq2xx.Inc()
	}()
}

// QueryHandler REST API for get Query Response
func (s ServerResource) QueryHandler(w http.ResponseWriter, r *http.Request) {
	metrics.KatlasNumReqCount.Inc()
	//Set Access-Control-Allow-Origin header now so that it will be present
	//even if an error is returned (otherwise the error also causes a CORS
	//exception in the browser/client)
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
	queryMap := r.URL.Query()

	code := http.StatusOK
	start := time.Now()
	defer func() {
		metrics.KatlasQueryLatencyHistogram.WithLabelValues("katlas", "*", "None", "dev", "containers", "GET", fmt.Sprintf("%d", code), "/**").Observe(time.Since(start).Seconds())
	}()

	obj, err := s.QuerySvc.GetQueryResult(queryMap)
	if err != nil {
		metrics.KatlasNumReqErr5xx.Inc()
		metrics.KatlasNumReqErr.Inc()
		code = http.StatusInternalServerError
		w.WriteHeader(code)
		w.Write([]byte(fmt.Sprintf("{\"status\": %v, \"error\": \"%s\"}", code, trim(err.Error()))))
		return
	}
	obj["status"] = code
	ret, _ := json.Marshal(obj)
	w.Write(ret)

	metrics.KatlasNumReq2xx.Inc()
}

// MetaCreateHandler REST API for create Metadata
func (s ServerResource) MetaCreateHandler(w http.ResponseWriter, r *http.Request) {
	metrics.KatlasNumReqCount.Inc()
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Error(err)
	}
	var payload interface{}
	err = json.Unmarshal(body, &payload)
	if err != nil {
		metrics.KatlasNumReqErr.Inc()
		metrics.KatlasNumReqErr4xx.Inc()
		log.Error(err)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf("{\"status\": %v, \"error\": \"%s\"}", http.StatusBadRequest, trim(err.Error()))))
		return
	}
	var msg map[string]interface{}
	if reflect.TypeOf(payload).Kind() == reflect.Slice {
		var rets []map[string]interface{}
		for _, p := range payload.([]interface{}) {
			uid, err := s.MetaSvc.CreateMetadata(p.(map[string]interface{}))
			if err != nil {
				metrics.KatlasNumReqErr.Inc()
				metrics.KatlasNumReqErr5xx.Inc()
				log.Error(err)
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(fmt.Sprintf("{\"status\": %v, \"error\": \"%s\"}", http.StatusInternalServerError, trim(err.Error()))))
				return
			}

			rets = append(rets, map[string]interface{}{
				"uid":     uid,
				"objtype": p.(map[string]interface{})[util.Name],
			})
		}
		msg = map[string]interface{}{
			"status":  http.StatusOK,
			"objects": rets,
		}
		metrics.KatlasNumReq2xx.Inc()
	} else {
		uid, err := s.MetaSvc.CreateMetadata(payload.(map[string]interface{}))
		if err != nil {
			metrics.KatlasNumReqErr.Inc()
			metrics.KatlasNumReqErr5xx.Inc()
			log.Error(err)
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(fmt.Sprintf("{\"status\": %v, \"error\": \"%s\"}", http.StatusInternalServerError, trim(err.Error()))))
			return
		}
		msg = map[string]interface{}{
			"status": http.StatusOK,
			"objects": []map[string]interface{}{
				{
					"uid":     uid,
					"objtype": payload.(map[string]interface{})[util.Name],
				},
			},
		}
		metrics.KatlasNumReq2xx.Inc()
	}
	ret, _ := json.Marshal(msg)
	w.Write(ret)
}

// MetaUpdateHandler REST API for update Metadata
func (s ServerResource) MetaUpdateHandler(w http.ResponseWriter, r *http.Request) {
	metrics.KatlasNumReqCount.Inc()
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
	vars := mux.Vars(r)
	name := vars[util.Name]
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Error(err)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf("{\"status\": %v, \"error\": \"%s\"}", http.StatusBadRequest, trim(err.Error()))))
		return
	}
	var payload interface{}
	err = json.Unmarshal(body, &payload)
	if err != nil {
		metrics.KatlasNumReqErr.Inc()
		metrics.KatlasNumReqErr4xx.Inc()
		log.Error(err)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf("{\"status\": %v, \"error\": \"%s\"}", http.StatusBadRequest, trim(err.Error()))))
		return
	}
	err = s.MetaSvc.UpdateMetadata(name, payload.(map[string]interface{}))
	if err != nil {
		metrics.KatlasNumReqErr.Inc()
		metrics.KatlasNumReqErr5xx.Inc()
		log.Error(err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf("{\"status\": %v, \"error\": \"%s\"}", http.StatusInternalServerError, trim(err.Error()))))
		return
	}
	msg := map[string]interface{}{
		"status": http.StatusOK,
		"objects": []map[string]interface{}{
			{
				"name":    name,
				"objtype": "metadata",
			},
		},
	}

	ret, _ := json.Marshal(msg)
	w.Write(ret)

	metrics.KatlasNumReq2xx.Inc()
}

// SchemaUpsertHandler REST API for create Schema
func (s ServerResource) SchemaUpsertHandler(w http.ResponseWriter, r *http.Request) {
	metrics.KatlasNumReqCount.Inc()
	defer s.MetaSvc.RemoveSchemaCache(db.LruCache)
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Error(err)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf("{\"status\": %v, \"error\": \"%s\"}", http.StatusBadRequest, trim(err.Error()))))
		return
	}
	var payload interface{}
	err = json.Unmarshal(body, &payload)
	if err != nil {
		metrics.KatlasNumReqErr.Inc()
		metrics.KatlasNumReqErr4xx.Inc()
		log.Error(err)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf("{\"status\": %v, \"error\": \"%s\"}", http.StatusBadRequest, trim(err.Error()))))
		return
	}
	var msg map[string]interface{}
	if reflect.TypeOf(payload).Kind() == reflect.Slice {
		var predicates []db.Schema
		err := mapstructure.Decode(payload, &predicates)
		if err != nil {
			metrics.KatlasNumReqErr.Inc()
			metrics.KatlasNumReqErr4xx.Inc()
			log.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(fmt.Sprintf("{\"status\": %v, \"error\": \"%s\"}", http.StatusBadRequest, trim(err.Error()))))
			return
		}
		names := make([]string, 0)
		for _, p := range predicates {
			err := s.MetaSvc.CreateSchema(p)
			if err != nil {
				log.Error(err)
				metrics.KatlasNumReqErr.Inc()
				metrics.KatlasNumReqErr5xx.Inc()
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(fmt.Sprintf("{\"status\": %v, \"error\": \"%s\"}", http.StatusInternalServerError, trim(err.Error()))))
				return
			}
			names = append(names, p.Predicate)
		}
		msg = map[string]interface{}{
			"status":  http.StatusOK,
			"message": fmt.Sprintf("%v upsert successfully", names),
		}
		metrics.KatlasNumReq2xx.Inc()
	} else {
		var predicate db.Schema
		err := mapstructure.Decode(payload, &predicate)
		if err != nil {
			metrics.KatlasNumReqErr.Inc()
			metrics.KatlasNumReqErr4xx.Inc()
			log.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(fmt.Sprintf("{\"status\": %v, \"error\": \"%s\"}", http.StatusBadRequest, trim(err.Error()))))
			return
		}
		err = s.MetaSvc.CreateSchema(predicate)
		if err != nil {
			metrics.KatlasNumReqErr.Inc()
			metrics.KatlasNumReqErr5xx.Inc()
			log.Error(err)
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(fmt.Sprintf("{\"status\": %v, \"error\": \"%s\"}", http.StatusInternalServerError, trim(err.Error()))))
			return
		}
		msg = map[string]interface{}{
			"status":  http.StatusOK,
			"message": fmt.Sprintf("%s upsert successfully", predicate.Predicate),
		}
		metrics.KatlasNumReq2xx.Inc()
	}
	ret, _ := json.Marshal(msg)
	w.Write(ret)
}

// SchemaDropHandler remove db schema
func (s ServerResource) SchemaDropHandler(w http.ResponseWriter, r *http.Request) {
	metrics.KatlasNumReqCount.Inc()
	defer s.MetaSvc.RemoveSchemaCache(db.LruCache)
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
	vars := mux.Vars(r)
	predicate := vars[util.Name]
	err := s.MetaSvc.DropSchema(predicate)
	if err != nil {
		metrics.KatlasNumReqErr.Inc()
		metrics.KatlasNumReqErr5xx.Inc()
		log.Error(err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf("{\"status\": %v, \"error\": \"%s\"}", http.StatusInternalServerError, trim(err.Error()))))
		return
	}
	msg := map[string]interface{}{
		"status":  http.StatusOK,
		"message": fmt.Sprintf("schema %s drop successfully", predicate),
	}
	ret, _ := json.Marshal(msg)
	w.Write(ret)

	metrics.KatlasNumReq2xx.Inc()
}

// QSLHandler handles requests for QSL
func (s *ServerResource) QSLHandler(w http.ResponseWriter, r *http.Request) {

	metrics.KatlasNumReqCount.Inc()

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
	vars := mux.Vars(r)

	// get query for count only
	query, err := s.QSLSvc.CreateDgraphQuery(vars[util.Query], true)
	if err != nil {
		metrics.KatlasNumReqErr.Inc()
		if err.Error() == "Failed to connect to dgraph to get metadata" {
			metrics.KatlasNumReqErr5xx.Inc()
			w.Write([]byte(fmt.Sprintf("{\"status\": %v, \"error\": \"%s\"}", http.StatusInternalServerError, trim(err.Error()))))
			return
		}
		// code: 400
		metrics.KatlasNumReqErr4xx.Inc()
		w.Write([]byte(fmt.Sprintf("{\"status\": %v, \"error\": \"%s\"}", http.StatusBadRequest, trim(err.Error()))))
		return
	}

	response, err := s.QSLSvc.DBclient.ExecuteDgraphQuery(query)
	if err != nil {
		metrics.KatlasNumReqErr.Inc()
		metrics.KatlasNumReqErr5xx.Inc()
		fmt.Println(err.Error())
		fmt.Println(trim(err.Error()))
		w.Write([]byte(fmt.Sprintf("{\"status\": %v, \"error\": \"%s\"}", http.StatusInternalServerError, trim(err.Error()))))
		return
	}
	total := apis.GetTotalCnt(response)

	// get query with pagination
	query, err = s.QSLSvc.CreateDgraphQuery(vars[util.Query], false)
	log.Infof("dgraph query for %#v:\n %s", vars[util.Query], query)
	if err != nil {
		metrics.KatlasNumReqErr.Inc()
		metrics.KatlasNumReqErr4xx.Inc()
		w.Write([]byte(fmt.Sprintf("{\"status\": %v, \"error\": \"%s\"}", http.StatusBadRequest, trim(err.Error()))))
		return
	}
	start := time.Now()
	code := http.StatusOK
	defer func() {
		//metrics.KatlasQueryLatencyHistogram.WithLabelValues(fmt.Sprintf("%d", code)).Observe(time.Since(start).Seconds())
		metrics.KatlasQueryLatencyHistogram.WithLabelValues("katlas", "*", "None", "dev", "containers", "GET", fmt.Sprintf("%d", code), "/**").Observe(time.Since(start).Seconds())
	}()

	response, err = s.QSLSvc.DBclient.ExecuteDgraphQuery(query)
	if err != nil {
		metrics.DgraphNumQSLErr.Inc()
		code = http.StatusInternalServerError
		metrics.KatlasNumReqErr.Inc()
		metrics.KatlasNumReqErr5xx.Inc()
		w.Write([]byte(fmt.Sprintf("{\"status\": %v, \"error\": \"%s\"}", http.StatusInternalServerError, trim(err.Error()))))
		return
	}
	log.Infof("[elapsedtime: %s]response for query %#v", time.Since(start), vars[util.Query])
	response[util.Count] = total
	response["status"] = http.StatusOK
	ret, err := json.Marshal(response)
	if err != nil {
		metrics.DgraphNumQSLErr.Inc()
		code = http.StatusInternalServerError
		metrics.KatlasNumReqErr.Inc()
		metrics.KatlasNumReqErr5xx.Inc()
		w.Write([]byte(fmt.Sprintf("{\"status\": %v, \"error\": \"%s\"}", http.StatusInternalServerError, trim(err.Error()))))
		return
	}
	w.Write(ret)

	metrics.KatlasNumReq2xx.Inc()
}

func trim(str string) string {
	// remove newline
	str = strings.Replace(strings.Replace(str, "\n", " ", -1), "\\n", " ", -1)
	// remove quotation mark
	str = strings.Replace(str, "\"", "", -1)
	// remove slash
	return strings.Replace(str, "\\", "", -1)
}

// TODO:
// Add more supported rest APIs
