package rpc

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"
)

// ABIDecoder ABI 解码器
type ABIDecoder struct {
	data string // 十六进制数据（不含 0x 前缀）
	pos  int    // 当前读取位置（字节）
}

// NewABIDecoder 创建新的 ABI 解码器
func NewABIDecoder(data string) (*ABIDecoder, error) {
	// 移除 0x 前缀
	data = strings.TrimPrefix(data, "0x")
	
	// 确保是偶数长度（每两个字符代表一个字节）
	if len(data)%2 != 0 {
		return nil, fmt.Errorf("invalid hex data length: %d", len(data))
	}
	
	return &ABIDecoder{
		data: data,
		pos:  0,
	}, nil
}

// readBytes 读取指定数量的字节
func (d *ABIDecoder) readBytes(count int) ([]byte, error) {
	if d.pos+count*2 > len(d.data) {
		return nil, fmt.Errorf("read beyond data length: pos=%d, count=%d, dataLen=%d", d.pos, count, len(d.data))
	}
	
	hexStr := d.data[d.pos : d.pos+count*2]
	bytes, err := hex.DecodeString(hexStr)
	if err != nil {
		return nil, fmt.Errorf("decode hex: %w", err)
	}
	
	d.pos += count * 2
	return bytes, nil
}

// readUint256 读取 uint256（32 字节）
func (d *ABIDecoder) readUint256() (*big.Int, error) {
	bytes, err := d.readBytes(32)
	if err != nil {
		return nil, err
	}
	return new(big.Int).SetBytes(bytes), nil
}

// readInt256 读取 int256（32 字节，有符号）
func (d *ABIDecoder) readInt256() (*big.Int, error) {
	bytes, err := d.readBytes(32)
	if err != nil {
		return nil, err
	}
	value := new(big.Int).SetBytes(bytes)
	// 检查符号位（最高位）
	if len(bytes) > 0 && bytes[0]&0x80 != 0 {
		// 负数，需要转换为补码
		maxUint256 := new(big.Int)
		maxUint256.Exp(big.NewInt(2), big.NewInt(256), nil)
		value.Sub(value, maxUint256)
	}
	return value, nil
}

// readStringAtOffset 在指定位置读取 string（动态类型）
// offset 是相对于数据开始位置的字节偏移
func (d *ABIDecoder) readStringAtOffset(offset int64) (string, error) {
	// 保存当前位置
	savedPos := d.pos
	
	// 跳转到 offset 位置
	d.pos = int(offset) * 2
	
	// 读取长度
	lengthBytes, err := d.readBytes(32)
	if err != nil {
		return "", err
	}
	length := new(big.Int).SetBytes(lengthBytes).Int64()
	
	// 读取字符串数据（需要对齐到 32 字节）
	var coin string
	if length > 0 {
		paddedLength := int((length + 31) / 32 * 32)
		dataBytes, err := d.readBytes(paddedLength)
		if err != nil {
			return "", err
		}
		coin = string(dataBytes[:length])
	}
	
	// 恢复位置
	d.pos = savedPos
	
	return coin, nil
}

// DecodeFillEvent 解码 Fill 事件数据
// 事件定义：Fill(address indexed user, string coin, uint256 price, uint256 size, int256 closedPnl, uint256 fee, uint256 time)
// Topics[0] = 事件签名
// Topics[1] = user 地址（indexed）
// Data 编码顺序（由于 string 是动态类型）：
//   - coin_offset (uint256): 指向 coin 字符串的位置（相对于数据开始）
//   - price (uint256)
//   - size (uint256)
//   - closedPnl (int256)
//   - fee (uint256)
//   - time (uint256)
//   - coin_length (uint256): coin 字符串长度（在 offset 位置）
//   - coin_data (bytes): coin 字符串数据（对齐到32字节）
func DecodeFillEvent(logData string) (*FillEventData, error) {
	decoder, err := NewABIDecoder(logData)
	if err != nil {
		return nil, fmt.Errorf("create decoder: %w", err)
	}
	
	data := &FillEventData{}
	
	// 1. 读取 coin_offset (uint256) - 指向 coin 字符串的位置
	coinOffset, err := decoder.readUint256()
	if err != nil {
		return nil, fmt.Errorf("read coin offset: %w", err)
	}
	
	// 2. 读取 price (uint256)
	price, err := decoder.readUint256()
	if err != nil {
		return nil, fmt.Errorf("read price: %w", err)
	}
	data.Price = bigIntToFloat64(price, 18)
	
	// 3. 读取 size (uint256)
	size, err := decoder.readUint256()
	if err != nil {
		return nil, fmt.Errorf("read size: %w", err)
	}
	data.Size = bigIntToFloat64(size, 18)
	
	// 4. 读取 closedPnl (int256)
	closedPnl, err := decoder.readInt256()
	if err != nil {
		return nil, fmt.Errorf("read closedPnl: %w", err)
	}
	data.ClosedPnl = bigIntToFloat64(closedPnl, 18)
	
	// 5. 读取 fee (uint256)
	fee, err := decoder.readUint256()
	if err != nil {
		return nil, fmt.Errorf("read fee: %w", err)
	}
	data.Fee = bigIntToFloat64(fee, 18)
	
	// 6. 读取 time (uint256) - 通常是秒或毫秒
	timeValue, err := decoder.readUint256()
	if err != nil {
		return nil, fmt.Errorf("read time: %w", err)
	}
	data.Time = timeValue.Int64()
	
	// 7. 读取 coin 字符串（根据 offset 跳转到实际位置）
	coin, err := decoder.readStringAtOffset(coinOffset.Int64())
	if err != nil {
		return nil, fmt.Errorf("read coin: %w", err)
	}
	data.Coin = coin
	
	return data, nil
}

// FillEventData Fill 事件数据
type FillEventData struct {
	Coin      string  // 币种
	Price     float64 // 价格
	Size      float64 // 数量
	ClosedPnl float64 // 已实现盈亏
	Fee       float64 // 手续费
	Time      int64   // 时间戳（毫秒）
}

// bigIntToFloat64 将 big.Int 转换为 float64，考虑小数位
func bigIntToFloat64(value *big.Int, decimals int) float64 {
	divisor := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil))
	valueFloat := new(big.Float).SetInt(value)
	result, _ := new(big.Float).Quo(valueFloat, divisor).Float64()
	return result
}
