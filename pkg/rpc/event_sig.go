package rpc

import (
	"encoding/hex"

	"golang.org/x/crypto/sha3"
)

// CalculateEventSignature 计算事件签名哈希
// 事件签名格式：EventName(type1,type2,...)
// 例如：Fill(address,string,uint256,uint256,int256,uint256,uint256)
// 返回：事件签名的 keccak256 哈希的完整32字节（64个十六进制字符）
// 注意：在 eth_getLogs 的 topics[0] 中使用时，通常只需要前4字节，但某些实现可能需要完整哈希
func CalculateEventSignature(eventSignature string) string {
	// 使用 golang.org/x/crypto/sha3 的 NewLegacyKeccak256
	// Keccak-256 是 Ethereum 使用的哈希算法
	hasher := sha3.NewLegacyKeccak256()
	hasher.Write([]byte(eventSignature))
	hashBytes := hasher.Sum(nil)
	// 返回完整的32字节哈希（64个十六进制字符）
	return "0x" + hex.EncodeToString(hashBytes)
}

// 常见的 Hyperliquid 事件签名（需要根据实际合约定义调整）
const (
	// Trade 事件 - 需要根据实际合约定义调整
	// 示例：Trade(address indexed user, uint256 pnl, uint256 fee, uint256 time)
	TradeEventSignature = "Trade(address,uint256,uint256,uint256)"
	
	// ClosePosition 事件 - 需要根据实际合约定义调整
	// 示例：ClosePosition(address indexed user, uint256 pnl, uint256 time)
	ClosePositionEventSignature = "ClosePosition(address,uint256,uint256)"
	
	// Fill 事件 - 需要根据实际合约定义调整
	// 示例：Fill(address indexed user, string coin, uint256 price, uint256 size, int256 closedPnl, uint256 fee, uint256 time)
	FillEventSignature = "Fill(address,string,uint256,uint256,int256,uint256,uint256)"
)

// GetTradeEventSignature 获取 Trade 事件签名哈希
func GetTradeEventSignature() string {
	return CalculateEventSignature(TradeEventSignature)
}

// GetClosePositionEventSignature 获取 ClosePosition 事件签名哈希
func GetClosePositionEventSignature() string {
	return CalculateEventSignature(ClosePositionEventSignature)
}

// GetFillEventSignature 获取 Fill 事件签名哈希
// 直接返回预定义的哈希值：0x7e127339796e67610e206087b33783c5139049a464c8d551e127a518cc342c16
func GetFillEventSignature() string {
	return "0x7e127339796e67610e206087b33783c5139049a464c8d551e127a518cc342c16"
}
