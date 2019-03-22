package util

import (
	"encoding/json"
	"github.com/cenkalti/backoff"
	"github.com/intuit/katlas/service/metrics"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/api/apps/v1beta2"
	core_v1 "k8s.io/api/core/v1"
	ext_v1beta1 "k8s.io/api/extensions/v1beta1"
	"reflect"
	"strings"
	"time"
)

// OptionContext to define options when CRUD entities
type OptionContext struct {
	// is replace field when update
	ReplaceListOrEdge bool
}

// NewBackOff creates an instance of ExponentialBackOff using default values.
func NewBackOff() *backoff.ExponentialBackOff {
	b := &backoff.ExponentialBackOff{
		InitialInterval:     100 * time.Microsecond,
		RandomizationFactor: 0.5,
		Multiplier:          1.5,
		MaxInterval:         500 * time.Microsecond,
		MaxElapsedTime:      5 * time.Minute,
		Clock:               backoff.SystemClock,
	}
	b.Reset()
	return b
}

//RegisterHistogramMetrics ...Register histogram with prometheus
func RegisterHistogramMetrics() {
	prometheus.MustRegister(metrics.KatlasQueryLatencyHistogram)
	prometheus.MustRegister(metrics.DgraphCreateEntityLatencyHistogram)
	prometheus.MustRegister(metrics.DgraphUpdateEntityLatencyHistogram)
	prometheus.MustRegister(metrics.DgraphDeleteEntityLatencyHistogram)
	prometheus.MustRegister(metrics.DgraphGetEntityLatencyHistogram)
}

//ReadCounter ...Extract float64 Value from the prometheus Counter metric
func ReadCounter(m prometheus.Counter) float64 {
	pb := &dto.Metric{}
	m.Write(pb)
	return pb.GetCounter().GetValue()
}

// BuildEntityData - build entity
func BuildEntityData(clusterName string, meta string, body []byte, isArray bool) (interface{}, error) {
	switch meta {
	case Namespace:
		if isArray {
			list := make([]map[string]interface{}, 0)
			data := []core_v1.Namespace{}
			err := json.Unmarshal(body, &data)
			if err != nil {
				return nil, err
			}
			for _, d := range data {
				namespace := map[string]interface{}{
					ObjType:         Namespace,
					Name:            d.ObjectMeta.Name,
					CreationTime:    d.ObjectMeta.CreationTimestamp,
					Cluster:         clusterName,
					ResourceVersion: d.ResourceVersion,
					K8sObj:          K8sObj,
					Labels:          d.ObjectMeta.GetLabels(),
					Asset:           getValues(&data, AssetID, "GetAnnotations"),
				}
				list = append(list, namespace)
			}
			return list, nil
		}
		data := core_v1.Namespace{}
		err := json.Unmarshal(body, &data)
		if err != nil {
			return nil, err
		}
		return map[string]interface{}{
			ObjType:         Namespace,
			Name:            data.ObjectMeta.Name,
			CreationTime:    data.ObjectMeta.CreationTimestamp,
			Cluster:         clusterName,
			ResourceVersion: data.ResourceVersion,
			K8sObj:          K8sObj,
			Labels:          data.ObjectMeta.GetLabels(),
			Asset:           getValues(&data, AssetID, "GetAnnotations"),
		}, nil
	case Deployment:
		if isArray {
			list := make([]map[string]interface{}, 0)
			data := []v1beta2.Deployment{}
			err := json.Unmarshal(body, &data)
			if err != nil {
				return nil, err
			}
			for _, d := range data {
				deployment := map[string]interface{}{
					ObjType:           Deployment,
					Cluster:           clusterName,
					Name:              d.ObjectMeta.Name,
					CreationTime:      d.ObjectMeta.CreationTimestamp,
					Namespace:         d.ObjectMeta.Namespace,
					NumReplicas:       d.Spec.Replicas,
					AvailableReplicas: d.Status.AvailableReplicas,
					Strategy:          d.Spec.Strategy.Type,
					ResourceVersion:   d.ResourceVersion,
					Labels:            d.ObjectMeta.GetLabels(),
					K8sObj:            K8sObj,
				}
				// creata application from labels
				appList := createAppNameList(&d)
				if len(appList) > 0 {
					deployment[Application] = appList
				}
				list = append(list, deployment)
			}
			return list, nil
		}
		data := v1beta2.Deployment{}
		err := json.Unmarshal(body, &data)
		if err != nil {
			return nil, err
		}
		deployment := map[string]interface{}{
			ObjType:           Deployment,
			Cluster:           clusterName,
			Name:              data.ObjectMeta.Name,
			CreationTime:      data.ObjectMeta.CreationTimestamp,
			Namespace:         data.ObjectMeta.Namespace,
			NumReplicas:       data.Spec.Replicas,
			AvailableReplicas: data.Status.AvailableReplicas,
			Strategy:          data.Spec.Strategy.Type,
			ResourceVersion:   data.ResourceVersion,
			Labels:            data.ObjectMeta.GetLabels(),
			K8sObj:            K8sObj,
		}
		// creata application from labels
		appList := createAppNameList(&data)
		if len(appList) > 0 {
			deployment[Application] = appList
		}
		return deployment, nil
	case Ingress:
		if isArray {
			list := make([]map[string]interface{}, 0)
			data := []ext_v1beta1.Ingress{}
			err := json.Unmarshal(body, &data)
			if err != nil {
				return nil, err
			}
			for _, d := range data {
				ingress := map[string]interface{}{
					ObjType:         Ingress,
					Cluster:         clusterName,
					Name:            d.ObjectMeta.Name,
					Namespace:       d.ObjectMeta.Namespace,
					CreationTime:    d.ObjectMeta.CreationTimestamp,
					DefaultBackend:  d.Spec.Backend,
					TSL:             d.Spec.TLS,
					Rules:           d.Spec.Rules,
					ResourceVersion: d.ObjectMeta.ResourceVersion,
					Labels:          d.ObjectMeta.GetLabels(),
					K8sObj:          K8sObj,
				}
				// creata application from labels
				appList := createAppNameList(&d)
				if len(appList) > 0 {
					ingress[Application] = appList
				}
				list = append(list, ingress)
			}
			return list, nil
		}
		data := ext_v1beta1.Ingress{}
		err := json.Unmarshal(body, &data)
		if err != nil {
			return nil, err
		}
		ingress := map[string]interface{}{
			ObjType:         Ingress,
			Cluster:         clusterName,
			Name:            data.ObjectMeta.Name,
			Namespace:       data.ObjectMeta.Namespace,
			CreationTime:    data.ObjectMeta.CreationTimestamp,
			DefaultBackend:  data.Spec.Backend,
			TSL:             data.Spec.TLS,
			Rules:           data.Spec.Rules,
			ResourceVersion: data.ObjectMeta.ResourceVersion,
			Labels:          data.ObjectMeta.GetLabels(),
			K8sObj:          K8sObj,
		}
		appList := createAppNameList(&data)
		if len(appList) > 0 {
			ingress[Application] = appList
		}
		return ingress, nil
	case Pod:
		if isArray {
			list := make([]map[string]interface{}, 0)
			data := []core_v1.Pod{}
			err := json.Unmarshal(body, &data)
			if err != nil {
				return nil, err
			}
			for _, d := range data {
				pod := map[string]interface{}{
					ObjType:         Pod,
					Name:            d.ObjectMeta.Name,
					Namespace:       d.ObjectMeta.Namespace,
					CreationTime:    d.ObjectMeta.CreationTimestamp,
					Phase:           d.Status.Phase,
					NodeName:        d.Spec.NodeName,
					IP:              d.Status.PodIP,
					Containers:      d.Spec.Containers,
					Volumes:         d.Spec.Volumes,
					Labels:          d.ObjectMeta.GetLabels(),
					Cluster:         clusterName,
					ResourceVersion: d.ObjectMeta.ResourceVersion,
					K8sObj:          K8sObj,
					StartTime:       d.Status.StartTime,
				}
				if len(d.ObjectMeta.OwnerReferences) > 0 {
					pod[Owner] = d.ObjectMeta.OwnerReferences[0].Name
					pod[OwnerType] = strings.ToLower(d.ObjectMeta.OwnerReferences[0].Kind)
				}
				list = append(list, pod)
			}
			return list, nil
		}
		data := core_v1.Pod{}
		err := json.Unmarshal(body, &data)
		if err != nil {
			return nil, err
		}
		pod := map[string]interface{}{
			ObjType:         Pod,
			Name:            data.ObjectMeta.Name,
			Namespace:       data.ObjectMeta.Namespace,
			CreationTime:    data.ObjectMeta.CreationTimestamp,
			Phase:           data.Status.Phase,
			NodeName:        data.Spec.NodeName,
			IP:              data.Status.PodIP,
			Containers:      data.Spec.Containers,
			Volumes:         data.Spec.Volumes,
			Labels:          data.ObjectMeta.GetLabels(),
			Cluster:         clusterName,
			ResourceVersion: data.ObjectMeta.ResourceVersion,
			K8sObj:          K8sObj,
			StartTime:       data.Status.StartTime,
		}
		if len(data.ObjectMeta.OwnerReferences) > 0 {
			pod[Owner] = data.ObjectMeta.OwnerReferences[0].Name
			pod[OwnerType] = strings.ToLower(data.ObjectMeta.OwnerReferences[0].Kind)
		}
		return pod, nil
	case ReplicaSet:
		if isArray {
			list := make([]map[string]interface{}, 0)
			data := []v1beta2.ReplicaSet{}
			err := json.Unmarshal(body, &data)
			if err != nil {
				return nil, err
			}
			for _, d := range data {
				replicaset := map[string]interface{}{
					ObjType:         ReplicaSet,
					Name:            d.ObjectMeta.Name,
					CreationTime:    d.ObjectMeta.CreationTimestamp,
					Namespace:       d.ObjectMeta.Namespace,
					NumReplicas:     d.Spec.Replicas,
					PodSpec:         d.Spec.Template.Spec,
					Owner:           d.ObjectMeta.OwnerReferences[0].Name,
					Cluster:         clusterName,
					ResourceVersion: d.ObjectMeta.ResourceVersion,
					Labels:          d.ObjectMeta.GetLabels(),
					K8sObj:          K8sObj,
				}
				list = append(list, replicaset)
			}
			return list, nil
		}
		data := v1beta2.ReplicaSet{}
		err := json.Unmarshal(body, &data)
		if err != nil {
			return nil, err
		}
		return map[string]interface{}{
			ObjType:         ReplicaSet,
			Name:            data.ObjectMeta.Name,
			CreationTime:    data.ObjectMeta.CreationTimestamp,
			Namespace:       data.ObjectMeta.Namespace,
			NumReplicas:     data.Spec.Replicas,
			PodSpec:         data.Spec.Template.Spec,
			Owner:           data.ObjectMeta.OwnerReferences[0].Name,
			Cluster:         clusterName,
			ResourceVersion: data.ObjectMeta.ResourceVersion,
			Labels:          data.ObjectMeta.GetLabels(),
			K8sObj:          K8sObj,
		}, nil
	case Service:
		if isArray {
			list := make([]map[string]interface{}, 0)
			data := []core_v1.Service{}
			err := json.Unmarshal(body, &data)
			if err != nil {
				return nil, err
			}
			for _, d := range data {
				service := map[string]interface{}{
					ObjType:         Service,
					Name:            d.ObjectMeta.Name,
					Namespace:       d.ObjectMeta.Namespace,
					CreationTime:    d.ObjectMeta.CreationTimestamp,
					Selector:        d.Spec.Selector,
					Labels:          d.ObjectMeta.GetLabels(),
					ClusterIP:       d.Spec.ClusterIP,
					ServiceType:     d.Spec.Type,
					Ports:           d.Spec.Ports,
					Cluster:         clusterName,
					ResourceVersion: d.ObjectMeta.ResourceVersion,
					K8sObj:          K8sObj,
				}
				// creata application from labels
				appList := createAppNameList(&d)
				if len(appList) > 0 {
					service[Application] = appList
				}
				list = append(list, service)
			}
			return list, nil
		}
		data := core_v1.Service{}
		err := json.Unmarshal(body, &data)
		if err != nil {
			return nil, err
		}
		service := map[string]interface{}{
			ObjType:         Service,
			Name:            data.ObjectMeta.Name,
			Namespace:       data.ObjectMeta.Namespace,
			CreationTime:    data.ObjectMeta.CreationTimestamp,
			Selector:        data.Spec.Selector,
			Labels:          data.ObjectMeta.GetLabels(),
			ClusterIP:       data.Spec.ClusterIP,
			ServiceType:     data.Spec.Type,
			Ports:           data.Spec.Ports,
			Cluster:         clusterName,
			ResourceVersion: data.ObjectMeta.ResourceVersion,
			K8sObj:          K8sObj,
		}
		// creata application from labels
		appList := createAppNameList(&data)
		if len(appList) > 0 {
			service[Application] = appList
		}
		return service, nil
	case StatefulSet:
		if isArray {
			list := make([]map[string]interface{}, 0)
			data := []appsv1.StatefulSet{}
			err := json.Unmarshal(body, &data)
			if err != nil {
				return nil, err
			}
			for _, d := range data {
				statefulset := map[string]interface{}{
					ObjType:         StatefulSet,
					Name:            d.ObjectMeta.Name,
					CreationTime:    d.ObjectMeta.CreationTimestamp,
					Namespace:       d.ObjectMeta.Namespace,
					NumReplicas:     d.Spec.Replicas,
					Cluster:         clusterName,
					ResourceVersion: d.ObjectMeta.ResourceVersion,
					Labels:          d.ObjectMeta.GetLabels(),
					K8sObj:          K8sObj,
				}
				list = append(list, statefulset)
			}
			return list, nil
		}
		data := appsv1.StatefulSet{}
		err := json.Unmarshal(body, &data)
		if err != nil {
			return nil, err
		}
		return map[string]interface{}{
			ObjType:         StatefulSet,
			Name:            data.ObjectMeta.Name,
			CreationTime:    data.ObjectMeta.CreationTimestamp,
			Namespace:       data.ObjectMeta.Namespace,
			NumReplicas:     data.Spec.Replicas,
			Cluster:         clusterName,
			ResourceVersion: data.ObjectMeta.ResourceVersion,
			Labels:          data.ObjectMeta.GetLabels(),
			K8sObj:          K8sObj,
		}, nil
	default:
		var data interface{}
		if isArray {
			data = []map[string]interface{}{}
		} else {
			data = map[string]interface{}{}
		}
		err := json.Unmarshal(body, &data)
		if err != nil {
			return nil, err
		}
		return data, nil
	}
}

func getValues(data interface{}, key, method string) string {
	vals := []reflect.Value{}
	switch data.(type) {
	case *core_v1.Service:
		vals = reflect.ValueOf(&data.(*core_v1.Service).ObjectMeta).MethodByName(method).Call(nil)
	case *core_v1.Namespace:
		vals = reflect.ValueOf(&data.(*core_v1.Namespace).ObjectMeta).MethodByName(method).Call(nil)
	case *v1beta2.Deployment:
		vals = reflect.ValueOf(&data.(*v1beta2.Deployment).ObjectMeta).MethodByName(method).Call(nil)
	case *ext_v1beta1.Ingress:
		vals = reflect.ValueOf(&data.(*ext_v1beta1.Ingress).ObjectMeta).MethodByName(method).Call(nil)
	case *core_v1.Pod:
		vals = reflect.ValueOf(&data.(*core_v1.Pod).ObjectMeta).MethodByName(method).Call(nil)
	case *v1beta2.ReplicaSet:
		vals = reflect.ValueOf(&data.(*v1beta2.ReplicaSet).ObjectMeta).MethodByName(method).Call(nil)
	case *appsv1.StatefulSet:
		vals = reflect.ValueOf(&data.(*appsv1.StatefulSet).ObjectMeta).MethodByName(method).Call(nil)
	}
	if len(vals) > 0 {
		if val, ok := vals[0].Interface().(map[string]string)[key]; ok {
			return val
		}
	}
	return ""
}

func createAppNameList(obj interface{}) []interface{} {
	appList := make([]interface{}, 0)
	for _, key := range []string{App, K8sApp} {
		val := getValues(obj, key, "GetLabels")
		if val != "" {
			appList = append(appList, val)
		}
	}
	return appList
}
