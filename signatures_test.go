package sim

import (
	"context"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// TestStageSignaturesTakeNoClusterHandle is the README's locality claim stated
// as types: Identify and both Generate methods accept no context.Context, no
// client.Client, and no cluster handle, so they cannot read global state.
//
// A cluster handle is anything that is, holds, or points at the in-memory
// Cluster, a NodeReader, a state.Cluster, or a client. Checking the parameter
// types reflectively, one level into structs and through pointers, slices and
// maps, catches a handle smuggled in as a field of an argument as well as one
// passed directly.
//
// What this cannot see is a package-level variable. evictionCostCtx in
// disruption.go is one: a context.Context that EvictionCost reads, which stage 1
// and 2 reach through NodeDisruptionCost. It is built once from upstream's
// default feature gates and carries nothing else, so it holds no cluster state.
func TestStageSignaturesTakeNoClusterHandle(t *testing.T) {
	ctxType := reflect.TypeOf((*context.Context)(nil)).Elem()
	clientType := reflect.TypeOf((*client.Client)(nil)).Elem()
	clientReader := reflect.TypeOf((*client.Reader)(nil)).Elem()
	forbidden := map[reflect.Type]string{
		reflect.TypeOf(Cluster{}):    "*Cluster",
		reflect.TypeOf(NodeReader{}): "*NodeReader",
	}

	var check func(path string, ty reflect.Type, depth int)
	check = func(path string, ty reflect.Type, depth int) {
		switch {
		case ty == ctxType || (ty.Kind() == reflect.Interface && ty.Implements(ctxType) && ty.NumMethod() > 0):
			t.Errorf("%s is a context.Context", path)
			return
		case ty.Kind() == reflect.Interface && ty.NumMethod() > 0 && (ty.Implements(clientReader) || ty == clientType):
			t.Errorf("%s is a controller-runtime client", path)
			return
		}
		if name, ok := forbidden[ty]; ok {
			t.Errorf("%s is a cluster handle (%s)", path, name)
			return
		}
		// Upstream's state.Cluster is the cluster handle the README names.
		if ty.Name() == "Cluster" && strings.HasSuffix(ty.PkgPath(), "controllers/state") {
			t.Errorf("%s is a state.Cluster", path)
			return
		}
		switch ty.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Array, reflect.Map:
			if ty.Kind() == reflect.Map {
				check(path+"[key]", ty.Key(), depth)
			}
			check(path+"[]", ty.Elem(), depth)
		case reflect.Struct:
			// Only the simulator's own argument types are opened. Upstream's API types
			// (NodePool, InstanceType, Pod) are data, and opening them would walk all
			// of Kubernetes' object model.
			if depth > 0 || ty.PkgPath() != reflect.TypeOf(NodeSnapshot{}).PkgPath() {
				return
			}
			for i := 0; i < ty.NumField(); i++ {
				f := ty.Field(i)
				check(path+"."+f.Name, f.Type, depth+1)
			}
		case reflect.Func:
			t.Errorf("%s is a function; a callback can close over cluster state", path)
		}
	}

	for _, tc := range []struct {
		name string
		fn   reflect.Type
		// want is the full parameter list, receiver first for a method, so a new
		// parameter fails here even if it is not a handle and makes someone look.
		want []reflect.Type
	}{
		{"Identify", reflect.TypeOf(Identify), []reflect.Type{
			reflect.TypeOf([]NodeSnapshot{}), reflect.TypeOf(PoolTotals{}), reflect.TypeOf(float64(0)),
		}},
		{"ConsolidationGenerator.Generate", reflect.TypeOf(ConsolidationGenerator.Generate), []reflect.Type{
			reflect.TypeOf(ConsolidationGenerator{}), reflect.TypeOf(NodeSnapshot{}), reflect.TypeOf([]NodeSnapshot{}),
			reflect.TypeOf(&Catalog{}), reflect.TypeOf(PoolTotals{}),
		}},
		{"ProvisioningGenerator.Generate", reflect.TypeOf(ProvisioningGenerator.Generate), []reflect.Type{
			reflect.TypeOf(ProvisioningGenerator{}), reflect.TypeOf([]*corev1.Pod{}), reflect.TypeOf(&Catalog{}),
		}},
	} {
		if tc.fn.NumIn() != len(tc.want) {
			t.Errorf("%s takes %d parameters, want %d", tc.name, tc.fn.NumIn(), len(tc.want))
		}
		for i := 0; i < tc.fn.NumIn() && i < len(tc.want); i++ {
			if tc.fn.In(i) != tc.want[i] {
				t.Errorf("%s parameter %d is %v, want %v", tc.name, i, tc.fn.In(i), tc.want[i])
			}
		}
		for i := 0; i < tc.fn.NumIn(); i++ {
			check(tc.name+" param "+tc.fn.In(i).String(), tc.fn.In(i), 0)
		}
	}
}
