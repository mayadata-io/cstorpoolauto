package recommendation

import (
	"sort"

	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"mayadata.io/cstorpoolauto/types"
	bdutil "mayadata.io/cstorpoolauto/util/blockdevice"
)

// cStorPoolClusterRecommendationRequest is used to request the device
// recommendation for a given raid type.
type cStorPoolClusterRecommendationRequest struct {
	Request types.CStorPoolClusterRecommendationRequest
	Data    Data
}

// Data is the input data requested for the specifix recommendation.
type Data struct {
	BlockDeviceList *unstructured.UnstructuredList
}

// NewRequestForDevice returns a device request object after validation.
func NewRequestForDevice(request *types.CStorPoolClusterRecommendationRequest, data *Data) (
	*cStorPoolClusterRecommendationRequest, error) {

	if request.Spec.PoolCapacity.IsZero() {
		return nil, errors.New(
			"Unable to create device recommendation request: Got zero pool capacity")
	}
	if data.BlockDeviceList == nil {
		return nil, errors.New(
			"Unable to create device recommendation request: Got nil block device list")
	}

	err := request.Spec.DataConfig.Validate()
	if err != nil {
		return nil, errors.Wrap(err, "Unable to create capacity recommendation request")
	}

	cspcrr := cStorPoolClusterRecommendationRequest{
		Request: types.CStorPoolClusterRecommendationRequest{
			Spec: types.CStorPoolClusterRecommendationRequestSpec{
				PoolCapacity: request.Spec.PoolCapacity,
				DataConfig:   request.Spec.DataConfig,
			},
		},
		Data: *data,
	}

	return &cspcrr, nil
}

// GetRecommendation returns recommended block devices for all nodes and device types.
func (r *cStorPoolClusterRecommendationRequest) GetRecommendation() map[string]types.CStorPoolClusterRecommendation {

	cStorPoolClusterRecommendation := make(map[string]types.CStorPoolClusterRecommendation)

	if len(r.Data.BlockDeviceList.Items) == 0 {
		return cStorPoolClusterRecommendation
	}

	if err := r.Request.Spec.DataConfig.Validate(); err != nil {
		return cStorPoolClusterRecommendation
	}

	availableBlockDeviceList := unstructured.UnstructuredList{}
	availableBlockDeviceList.Object = r.Data.BlockDeviceList.Object
	for _, bd := range r.Data.BlockDeviceList.Items {
		isEligible, err := bdutil.IsEligibleForCStorPool(bd)
		if err == nil && isEligible {
			availableBlockDeviceList.Items = append(availableBlockDeviceList.Items, bd)
		}
	}
	if len(availableBlockDeviceList.Items) == 0 {
		return cStorPoolClusterRecommendation
	}

	deviceTypeNodeBlockDeviceMap := bdutil.GetTopologyMapGroupByDeviceTypeAndBlockSize(availableBlockDeviceList)
	if len(deviceTypeNodeBlockDeviceMap) == 0 {
		return cStorPoolClusterRecommendation
	}

	for kind, nodeBlockDeviceListMap := range deviceTypeNodeBlockDeviceMap {

		nodeCapacityBlockDeviceMap := nodeCapacityBlockDevices{}

		for nodeName, blockDeviceList := range nodeBlockDeviceListMap {

			// capacityBlockDevicesMap contains block device capacity and all block devices
			// of that capacity in one node.
			capacityBlockDevicesMap := nodeCapacityBlockDeviceMap.getOrDefault(nodeName)

			for _, blockDevice := range blockDeviceList {
				capacity, found := blockDevice.Capacity.AsInt64()
				if !found {
					// TODO handle this case
					continue
				}

				blockDevices := capacityBlockDevicesMap.getOrDefault(capacity)
				capacityBlockDevicesMap.update(capacity, append(blockDevices, blockDevice))

			}

			// update nodeCapacityBlockDeviceMap for each node.
			nodeCapacityBlockDeviceMap.update(nodeName, capacityBlockDevicesMap)
		}

		cStorPoolClusterRecommendationValue := nodeCapacityBlockDeviceMap.getDeviceRecommendtion(r.Request.Spec.PoolCapacity, r.Request.Spec.DataConfig)
		cStorPoolClusterRecommendationValue.RequestSpec = r.Request.Spec
		cStorPoolClusterRecommendation[kind] = cStorPoolClusterRecommendationValue
	}

	return cStorPoolClusterRecommendation
}

// nodeCapacityBlockDevice contains key with node name and value with capacityBlockDevice
type nodeCapacityBlockDevices map[string]capacityBlockDevices

// getDeviceRecommendtion returns the recommended block devices on a node with the given configuration.
func (ncb nodeCapacityBlockDevices) getDeviceRecommendtion(poolCapacity resource.Quantity, raidConfig types.RaidGroupConfig) types.CStorPoolClusterRecommendation {

	poolCapacityInt, ok := poolCapacity.AsInt64()
	if !ok {
		return types.CStorPoolClusterRecommendation{}
	}

	poolInstances := []types.PoolInstanceConfig{}

	for nodeName, capacityBlockDevices := range ncb {

		poolInstance := capacityBlockDevices.getPoolInstance(poolCapacityInt, raidConfig)
		poolInstance.Node.Name = nodeName
		poolInstance.Capacity = poolCapacity

		poolInstances = append(poolInstances, poolInstance)

	}

	CStorPoolClusterRecommendation := types.CStorPoolClusterRecommendation{
		Spec: types.CStorPoolClusterRecommendationSpec{
			PoolInstances: poolInstances,
		},
	}

	return CStorPoolClusterRecommendation
}

// capacityBlockDevices contains a key with capacity of block device and
// value with all the block devices of that capacity.
type capacityBlockDevices map[int64][]bdutil.MetaInfo

func (cbd capacityBlockDevices) getPoolInstance(poolCapacityInt int64, raidConfig types.RaidGroupConfig) types.PoolInstanceConfig {
	// To sort map storing (ascending order) keys in a seperate data structure.
	// Note: map cannot be sorted.
	capacityKeys := make([]int64, 0, len(cbd))
	for capacity := range cbd {
		capacityKeys = append(capacityKeys, capacity)
	}
	sort.SliceStable(capacityKeys, func(i, j int) bool {
		return capacityKeys[i] < capacityKeys[j]
	})

	prevDataDevices := []types.Reference{}
	for _, capacity := range capacityKeys {
		dataDevices := []types.Reference{}

		blockDevices := cbd[capacity]
		count := int64(len(blockDevices))

		if count < raidConfig.GroupDeviceCount {
			continue
		}

		noOfRaidGroup := count / raidConfig.GroupDeviceCount

		maxCapacity := noOfRaidGroup * raidConfig.GetDataDeviceCount() * capacity

		// If required pool capacity is greater than the max capacity of
		// the current block devices then skip this device.
		if maxCapacity < poolCapacityInt {
			continue
		}

		// Calculate the no of block devices to return to client.
		noOfBlockDevices := (poolCapacityInt / capacity) * raidConfig.GroupDeviceCount
		if poolCapacityInt < capacity {
			noOfBlockDevices = raidConfig.GroupDeviceCount
		}
		for i := 0; i < int(noOfBlockDevices); i++ {
			dataDevices = append(dataDevices, *blockDevices[i].Identity)
		}

		// Lets say someone requested for a 100GB pool with mirror type and the node
		// contains both 50GB and 100GB of block devices.
		// This will break the loop once suitable block device is found and return the last block devices.
		// In this case 100GB.
		if len(prevDataDevices) != 0 && (poolCapacityInt < capacity) {
			break
		}

		// maintaining the prevDataDevices that means previous capacity suitable
		// block devices for requested pool capacity.
		prevDataDevices = dataDevices
	}

	poolInstance := types.PoolInstanceConfig{
		BlockDevices: types.BlockDeviceTopology{
			DataDevices: prevDataDevices,
		},
	}

	return poolInstance
}

// getOrDefault returns the value from the map for given key.
// It returns default value if key is absent
func (ncb nodeCapacityBlockDevices) getOrDefault(key string) capacityBlockDevices {
	value, found := ncb[key]
	if !found {
		value = capacityBlockDevices{}
		ncb[key] = value
	}
	return value
}

// updates the map with given key and value
func (ncb nodeCapacityBlockDevices) update(key string, value capacityBlockDevices) {
	ncb[key] = value
}

// getOrDefault returns the value from the map for given key.
// It returns default value if key is absent
func (cbd capacityBlockDevices) getOrDefault(key int64) []bdutil.MetaInfo {
	value, found := cbd[key]
	if !found {
		value = make([]bdutil.MetaInfo, 0)
		cbd[key] = value
	}
	return value
}

// updates the map with given key and value
func (cbd capacityBlockDevices) update(key int64, value []bdutil.MetaInfo) {
	cbd[key] = value
}
