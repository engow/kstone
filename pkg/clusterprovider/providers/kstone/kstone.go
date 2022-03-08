/*
 * Tencent is pleased to support the open source community by making TKEStack
 * available.
 *
 * Copyright (C) 2012-2023 Tencent. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package kstone

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	kstonev1alpha2 "tkestack.io/kstone/pkg/apis/kstone/v1alpha2"
	"tkestack.io/kstone/pkg/clusterprovider"
	"tkestack.io/kstone/pkg/controllers/util"
	"tkestack.io/kstone/pkg/etcd"
	platformscheme "tkestack.io/kstone/pkg/generated/clientset/versioned/scheme"
)

const (
	providerName              = kstonev1alpha2.EtcdClusterKstone
	AnnoImportedURI           = "importedAddr"
	defaultEmptyResourceValue = "0"
)

var (
	once     sync.Once
	instance *EtcdClusterKstone
)

// EtcdClusterKstone is responsible for synchronizing kstone.tkestack.io/etcdcluster to kstone-etcd-operator
type EtcdClusterKstone struct {
	name kstonev1alpha2.EtcdClusterType
	ctx  *clusterprovider.ClusterContext
}

// init registers a kstone etcd cluster(generated by kstone-etcd-provider) provider
func init() {
	clusterprovider.RegisterEtcdClusterFactory(
		providerName,
		func(cluster *clusterprovider.ClusterContext) (clusterprovider.Cluster, error) {
			return initEtcdClusterKstoneInstance(cluster)
		},
	)
}

func initEtcdClusterKstoneInstance(ctx *clusterprovider.ClusterContext) (clusterprovider.Cluster, error) {
	once.Do(func() {
		instance = &EtcdClusterKstone{
			name: kstonev1alpha2.EtcdClusterKstone,
			ctx: &clusterprovider.ClusterContext{
				Clientbuilder: ctx.Clientbuilder,
				Client:        ctx.Clientbuilder.DynamicClientOrDie(),
			},
		}
	})
	return instance, nil
}

func (c *EtcdClusterKstone) BeforeCreate(cluster *kstonev1alpha2.EtcdCluster) error {
	return nil
}

// Create creates an etcd cluster
func (c *EtcdClusterKstone) Create(cluster *kstonev1alpha2.EtcdCluster) error {
	etcdRes := schema.GroupVersionResource{Group: "etcd.tkestack.io", Version: "v1alpha1", Resource: "etcdclusters"}
	etcdcluster := map[string]interface{}{
		"apiVersion": "etcd.tkestack.io/v1alpha1",
		"kind":       "EtcdCluster",
		"metadata": map[string]interface{}{
			"name":        cluster.Name,
			"namespace":   cluster.Namespace,
			"annotations": cluster.Annotations,
		},
		"spec": c.generateEtcdSpec(cluster),
	}

	etcdclusterRequest := &unstructured.Unstructured{
		Object: etcdcluster,
	}

	err := controllerutil.SetOwnerReference(cluster, etcdclusterRequest, platformscheme.Scheme)
	if err != nil {
		return err
	}

	_, err = c.ctx.Client.Resource(etcdRes).
		Namespace(cluster.Namespace).
		Create(context.TODO(), etcdclusterRequest, metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}

	return nil
}

// AfterCreate handles etcdcluster after created
func (c *EtcdClusterKstone) AfterCreate(cluster *kstonev1alpha2.EtcdCluster) error {
	if cluster.Annotations["scheme"] == "https" {
		cluster.Annotations["certName"] = fmt.Sprintf("%s/%s-etcd-client-cert", cluster.Namespace, cluster.Name)
	}

	cluster.Annotations["importedAddr"] = fmt.Sprintf(
		"%s://%s-etcd.%s.svc.cluster.local:2379",
		cluster.Annotations["scheme"],
		cluster.Name,
		cluster.Namespace,
	)
	// update extClientURL
	extClientURL := ""
	for i := 0; i < int(cluster.Spec.Size); i++ {
		key := fmt.Sprintf("%s-etcd-%d:2379", cluster.Name, i)
		value := fmt.Sprintf(
			"%s-etcd-%d.%s-etcd-headless.%s.svc.cluster.local:2379",
			cluster.Name,
			i,
			cluster.Name,
			cluster.Namespace,
		)
		if i < int(cluster.Spec.Size)-1 {
			extClientURL += fmt.Sprintf("%s->%s,", key, value)
		} else {
			extClientURL += fmt.Sprintf("%s->%s", key, value)
		}
	}
	cluster.Annotations["extClientURL"] = extClientURL
	return nil
}

// BeforeUpdate handles etcdcluster before updated
func (c *EtcdClusterKstone) BeforeUpdate(cluster *kstonev1alpha2.EtcdCluster) error {
	return nil
}

// Update updates cluster of kstone-etcd-operator
func (c *EtcdClusterKstone) Update(cluster *kstonev1alpha2.EtcdCluster) error {
	etcdRes := schema.GroupVersionResource{Group: "etcd.tkestack.io", Version: "v1alpha1", Resource: "etcdclusters"}
	etcd, err := c.ctx.Client.Resource(etcdRes).
		Namespace(cluster.Namespace).
		Get(context.TODO(), cluster.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	err = c.updateEtcdSpec(etcd, cluster)
	if err != nil {
		return err
	}

	_, updateErr := c.ctx.Client.Resource(etcdRes).
		Namespace(cluster.Namespace).
		Update(context.TODO(), etcd, metav1.UpdateOptions{})
	if updateErr != nil {
		klog.Error(updateErr.Error())
		return updateErr
	}
	return nil
}

// Equal checks etcdcluster, if not equal, sync etcdclusters.etcd.tkestack.io
// if equal, nothing to do
func (c *EtcdClusterKstone) Equal(cluster *kstonev1alpha2.EtcdCluster) (bool, error) {
	etcdRes := schema.GroupVersionResource{Group: "etcd.tkestack.io", Version: "v1alpha1", Resource: "etcdclusters"}
	etcd, err := c.ctx.Client.Resource(etcdRes).
		Namespace(cluster.Namespace).
		Get(context.TODO(), cluster.Name, metav1.GetOptions{})
	if err != nil {
		return true, err
	}

	oldSize, _, _ := unstructured.NestedInt64(etcd.Object, "spec", "size")
	if int64(cluster.Spec.Size) != oldSize {
		klog.Info("size is different")
		return false, nil
	}

	oldVersion, _, _ := unstructured.NestedString(etcd.Object, "spec", "version")
	if strings.TrimLeft(oldVersion, "v") != strings.TrimLeft(cluster.Spec.Version, "v") {
		klog.Info("version is different")
		return false, nil
	}

	oldStorage, _, _ := unstructured.NestedString(
		etcd.Object,
		"spec",
		"template",
		"persistentVolumeClaimSpec",
		"resources",
		"requests",
		"storage",
	)
	if strings.TrimRight(oldStorage, "Gi") != strconv.Itoa(int(cluster.Spec.DiskSize)) {
		klog.Info("storage is different")
		return false, nil
	}

	if !c.resourceEqual(cluster, etcd) {
		return false, nil
	}

	oldEnvObject, _, _ := unstructured.NestedSlice(etcd.Object, "spec", "template", "env")
	oldEnv := make([]corev1.EnvVar, 0)
	oldEnvBytes, err := json.Marshal(oldEnvObject)
	if err != nil {
		return true, err
	}
	err = json.Unmarshal(oldEnvBytes, &oldEnv)
	if err != nil {
		return true, err
	}
	if len(oldEnv) == 0 && len(cluster.Spec.Env) == 0 {
		return true, nil
	}
	if !reflect.DeepEqual(oldEnv, cluster.Spec.Env) {
		klog.Info("env is different")
		return false, nil
	}

	return true, nil
}

// AfterUpdate handles etcdcluster after updated
func (c *EtcdClusterKstone) AfterUpdate(cluster *kstonev1alpha2.EtcdCluster) error {
	return nil
}

// BeforeDelete handles etcdcluster before deleted
func (c *EtcdClusterKstone) BeforeDelete(cluster *kstonev1alpha2.EtcdCluster) error {
	return nil
}

// Delete handles delete
func (c *EtcdClusterKstone) Delete(cluster *kstonev1alpha2.EtcdCluster) error {
	return nil
}

// AfterDelete handles etcdcluster after deleted
func (c *EtcdClusterKstone) AfterDelete(cluster *kstonev1alpha2.EtcdCluster) error {
	return nil
}

// Status checks etcd member and returns new status
func (c *EtcdClusterKstone) Status(config *etcd.ClientConfig, cluster *kstonev1alpha2.EtcdCluster) (kstonev1alpha2.EtcdClusterStatus, error) {
	var phase kstonev1alpha2.EtcdClusterPhase

	status := cluster.Status

	annotations := cluster.Annotations
	if annotations == nil {
		annotations = make(map[string]string)
	}

	// endpoints
	endpoints := clusterprovider.GetStorageMemberEndpoints(cluster)

	if len(endpoints) == 0 {
		if addr, found := annotations[AnnoImportedURI]; found {
			endpoints = append(endpoints, addr)
			status.ServiceName = addr
		} else {
			status.Phase = kstonev1alpha2.EtcdCluterCreating
			return status, nil
		}
	}

	members, err := clusterprovider.GetRuntimeEtcdMembers(
		endpoints,
		cluster.Annotations[util.ClusterExtensionClientURL],
		config,
	)
	if err != nil || len(members) == 0 || int(cluster.Spec.Size) != len(members) {
		if status.Phase == kstonev1alpha2.EtcdClusterRunning {
			status.Phase = kstonev1alpha2.EtcdClusterUnknown
		}
		return status, err
	}

	status.Members, phase = clusterprovider.GetEtcdClusterMemberStatus(members, config)
	if status.Phase == kstonev1alpha2.EtcdClusterRunning || phase != kstonev1alpha2.EtcdClusterUnknown {
		status.Phase = phase
	}
	return status, err
}

// updateEtcdSpec update spec
func (c *EtcdClusterKstone) updateEtcdSpec(etcd *unstructured.Unstructured, cluster *kstonev1alpha2.EtcdCluster) error {
	newSpec := c.generateEtcdSpec(cluster)

	spec, found, err := unstructured.NestedMap(etcd.Object, "spec")
	if err != nil || !found || spec == nil {
		return fmt.Errorf("get spec error")
	}

	if err = unstructured.SetNestedField(etcd.Object, newSpec, "spec"); err != nil {
		klog.Error(err.Error())
		return err
	}

	return nil
}

// generateEtcdSpec generate spec with etcdcluster
func (c *EtcdClusterKstone) generateEtcdSpec(cluster *kstonev1alpha2.EtcdCluster) map[string]interface{} {
	extraServerCertSANsStr := cluster.Annotations["extraServerCertSANs"]
	extraServerCertSANList := make([]interface{}, 0)
	for _, certSAN := range strings.Split(extraServerCertSANsStr, ",") {
		temp := strings.TrimSpace(certSAN)
		if temp == "" {
			continue
		}
		extraServerCertSANList = append(extraServerCertSANList, temp)
	}
	if len(extraServerCertSANList) == 0 {
		extraServerCertSANList = nil
	}

	labels := make(map[string]interface{}, len(cluster.Labels))
	for k, v := range cluster.Labels {
		labels[k] = v
	}
	annotations := make(map[string]interface{}, len(cluster.Annotations))
	for k, v := range cluster.Annotations {
		annotations[k] = v
	}
	env := make([]interface{}, 0)
	envBytes, _ := json.Marshal(cluster.Spec.Env)
	_ = json.Unmarshal(envBytes, &env)

	spec := map[string]interface{}{
		"size":    int64(cluster.Spec.Size),
		"version": cluster.Spec.Version,
		"template": map[string]interface{}{
			"extraArgs": []interface{}{
				"logger=zap",
			},
			"labels":      labels,
			"annotations": annotations,
			"env":         env,
			"persistentVolumeClaimSpec": map[string]interface{}{
				"accessModes": []interface{}{
					"ReadWriteOnce",
				},
				"resources": map[string]interface{}{
					"requests": map[string]interface{}{
						"storage": fmt.Sprintf("%dGi", cluster.Spec.DiskSize),
					},
				},
			},
		},
	}

	resources := make(map[string]interface{})
	if cluster.Spec.Resources.Limits != nil {
		resources["limits"] = map[string]interface{}{
			"cpu":    cluster.Spec.Resources.Limits.Cpu().String(),
			"memory": cluster.Spec.Resources.Limits.Memory().String(),
		}
	}
	if cluster.Spec.Resources.Requests != nil {
		resources["requests"] = map[string]interface{}{
			"cpu":    cluster.Spec.Resources.Requests.Cpu().String(),
			"memory": cluster.Spec.Resources.Requests.Memory().String(),
		}
	}

	spec["template"].(map[string]interface{})["resources"] = resources

	if cluster.Annotations["scheme"] == "https" {
		autoTLSCert := map[string]interface{}{
			"autoGenerateClientCert": true,
			"autoGeneratePeerCert":   true,
			"autoGenerateServerCert": true,
			"extraServerCertSANs":    extraServerCertSANList,
		}
		if cluster.Spec.AuthConfig.TLSSecret != "" {
			autoTLSCert["externalCASecret"] = cluster.Spec.AuthConfig.TLSSecret
		}

		spec["secure"] = map[string]interface{}{
			"tls": map[string]interface{}{
				"autoTLSCert": autoTLSCert,
			},
		}
		spec["template"].(map[string]interface{})["extraArgs"] = []interface{}{
			"logger=zap",
			"client-cert-auth=true",
		}
	}
	return spec
}

// resourceEqual checks if old resource is equal to desired resource
// Note that if Resources.Limits.Cpu or other resource is not exists,
// kubernetes will check and return a default Quantity Object
// So there will be no nil pointer panic
func (c *EtcdClusterKstone) resourceEqual(cluster *kstonev1alpha2.EtcdCluster, etcd *unstructured.Unstructured) bool {
	resourceTypeNames := []corev1.ResourceName{corev1.ResourceRequestsCPU, corev1.ResourceRequestsMemory, corev1.ResourceLimitsCPU, corev1.ResourceLimitsMemory}
	for _, resourceTypeName := range resourceTypeNames {
		r := strings.Split(string(resourceTypeName), ".")
		resourceType, resourceName := r[0], r[1]
		oldResource := c.getOldResource(etcd, resourceType, resourceName)
		desiredResource := c.getDesiredResource(cluster, resourceTypeName)
		if oldResource != desiredResource {
			info := fmt.Sprintf("%s %s is different", resourceName, resourceType)
			klog.Info(info)
			return false
		}
	}
	return true
}

func (c *EtcdClusterKstone) getOldResource(etcd *unstructured.Unstructured, resourceType, resourceName string) string {
	resource, _, _ := unstructured.NestedString(etcd.Object, "spec", "template", "resources", resourceType, resourceName)
	if resource == "" {
		resource = defaultEmptyResourceValue
	}
	return resource
}

func (c *EtcdClusterKstone) getDesiredResource(cluster *kstonev1alpha2.EtcdCluster, resourceTypeName corev1.ResourceName) string {
	switch resourceTypeName {
	case corev1.ResourceLimitsCPU:
		return cluster.Spec.Resources.Limits.Cpu().String()
	case corev1.ResourceLimitsMemory:
		return cluster.Spec.Resources.Limits.Memory().String()
	case corev1.ResourceRequestsCPU:
		return cluster.Spec.Resources.Requests.Cpu().String()
	case corev1.ResourceRequestsMemory:
		return cluster.Spec.Resources.Requests.Memory().String()
	}
	return ""
}
