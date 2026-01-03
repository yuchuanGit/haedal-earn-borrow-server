package vault

import (
	"haedal-earn-borrow-server/common/mydb"
	"log"
)

func QueryVaultAll() []VaultModel {
	var vms []VaultModel
	con := mydb.GetDbConnection()
	sql := "select vault_id,vault_name,asset_type,htoken_type,asset_decimals from vault "
	rs, err := con.Query(sql)
	if err != nil {
		log.Printf("QueryVaultAll 查询失败: %v", err)
		defer con.Close()
		return vms
	}
	for rs.Next() {
		var vm VaultModel
		errScan := rs.Scan(&vm.VaultId, &vm.VaultName, &vm.AssetType, &vm.HtokenType, &vm.AssetDecimals)
		if errScan != nil {
			log.Printf("QueryVaultAll scan失败: %v", err)
			defer con.Close()
			return vms
		}
		vms = append(vms, vm)
	}
	defer con.Close()
	return vms
}

type VaultModel struct {
	VaultId           string
	VaultName         *string
	AssetType         string
	HtokenType        string
	AssetDecimals     float64 // 存入精度
	TotalShares       string  // 存入总的份额
	AssetReserve      string  // 总的闲置数量
	SupplyCap         string  // Vault 最大存入数量
	MaxDeposit        string  // Vault 单次最大存入数量
	MinDeposit        string  // Vault 单次最小存入数量
	ManagementFeeBps  string
	PerformanceFeeBps string
}
