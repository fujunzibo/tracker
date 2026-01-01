package hyper

import (
	"testing"
)

func TestGetPortfolio(t *testing.T) {
	client := DefaultClient()
	portfolio, err := client.GetPortfolio("0x019d4248690664e2a87a4aa2a26f83c8e63de7c4")
	if err != nil {
		t.Fatalf("获取 portfolio 失败: %v", err)
	}
	for _, item := range portfolio {
		t.Logf("item: %v\n", item)
	}
}

func TestGetUserRole(t *testing.T) {
	client := DefaultClient()
	role, err := client.GetUserRole("0x9e02aca9865e1859bb7865f6f64801e804a173df")
	if err != nil {
		t.Fatalf("获取 user role 失败: %v", err)
	}
	t.Logf("user role: %v\n", role)
	role, err = client.GetUserRole("0xb83de012dba672c76a7dbbbf3e459cb59d7d6e36")
	if err != nil {
		t.Fatalf("获取 user role 失败: %v", err)
	}
	t.Logf("user role: %v\n", role)
}

func TestGetAvailableValuts(t *testing.T) {
	vaults, err := GetAvailableValuts()
	if err != nil {
		t.Fatalf("获取 vaults 失败: %v", err)
	}
	t.Logf("vaults count: %d\n", len(vaults))
	t.Logf("vaults: %v\n", vaults)
}

func TestGetUserFills(t *testing.T) {
	client := DefaultClient()
	fills, err := client.GetUserFills("0xf40097a3c8d0b7f63ea6421becff2ca6324d2214")
	if err != nil {
		t.Fatalf("获取 fills 失败: %v", err)
	}
	t.Logf("fills count: %d\n", len(fills))
	for _, fill := range fills {
		t.Logf("fill: %v\n", fill)
	}
}
