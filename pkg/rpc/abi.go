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

// readAddress 读取地址（20字节，32字节格式，左补0）
func (d *ABIDecoder) readAddress() (string, error) {
	bytes, err := d.readBytes(32)
	if err != nil {
		return "", err
	}
	// 地址是后20字节（40个十六进制字符）
	addressBytes := bytes[12:32] // 跳过前12字节，取后20字节
	return "0x" + hex.EncodeToString(addressBytes), nil
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
	
	// 8. 尝试读取  地址（如果 Data 字段中包含）
	// 注意：根据 Fill 事件定义，Data 字段只包含：coin, price, size, closedPnl, fee, time
	//   地址可能不在 Fill 事件的 Data 字段中
	// 但为了完整性，我们尝试在读取完所有已知字段后，检查是否还有剩余数据
	// 如果  地址在 Data 字段中，它可能在 coin 字符串之后的位置
	
	// 记录解码后的位置和数据长度，用于调试
	remainingData := len(decoder.data) - decoder.pos
	
	// 如果还有剩余数据，尝试读取  地址
	//   地址可能在 coin 字符串之后
	if remainingData >= 64 {
		// 尝试读取一个地址（32字节）
		counterparty, err := decoder.readAddress()
		if err == nil && counterparty != "" && counterparty != "0x0000000000000000000000000000000000000000" {
			data.Counterparty = counterparty
		}
	}
	
	// 如果仍然没有找到  地址，尝试从 coin 字符串之后的位置查找
	// 重新创建解码器，精确定位到 coin 字符串之后
	if data.Counterparty == "" {
		decoder2, err := NewABIDecoder(logData)
		if err == nil {
			// 1. 读取 coin_offset
			coinOffset2, err := decoder2.readUint256()
			if err == nil {
				coinOffsetValue := coinOffset2.Int64()
				
				// 2. 跳过 price, size, closedPnl, fee, time (5 * 32 = 160 字节 = 320 个十六进制字符)
				decoder2.pos += 320
				
				// 3. 跳转到 coin 字符串位置
				if coinOffsetValue > 0 && int(coinOffsetValue)*2 < len(decoder2.data) {
					decoder2.pos = int(coinOffsetValue) * 2
					
					// 4. 读取 coin 字符串长度
					coinLengthBytes, err := decoder2.readBytes(32)
					if err == nil {
						coinLength := new(big.Int).SetBytes(coinLengthBytes).Int64()
						
						// 5. 计算 coin 字符串占用的字节数（对齐到32字节）
						coinPaddedLength := int((coinLength + 31) / 32 * 32)
						
						// 6. 跳过 coin 字符串（长度字段 + 数据）
						decoder2.pos += (32 + coinPaddedLength) * 2
						
						// 7. 尝试读取  地址（在 coin 字符串之后）
						if decoder2.pos+64 <= len(decoder2.data) {
							counterparty, err := decoder2.readAddress()
							if err == nil && counterparty != "" && counterparty != "0x0000000000000000000000000000000000000000" {
								data.Counterparty = counterparty
							}
						}
					}
				}
			}
		}
	}
	
	return data, nil
}

// FillEventData Fill 事件数据
type FillEventData struct {
	Coin         string  // 币种
	Price        float64 // 价格
	Size         float64 // 数量
	ClosedPnl    float64 // 已实现盈亏
	Fee          float64 // 手续费
	Time         int64   // 时间戳（毫秒）
	Counterparty string  //   地址（如果存在，从 Data 字段解码）
}

// bigIntToFloat64 将 big.Int 转换为 float64，考虑小数位
func bigIntToFloat64(value *big.Int, decimals int) float64 {
	divisor := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil))
	valueFloat := new(big.Float).SetInt(value)
	result, _ := new(big.Float).Quo(valueFloat, divisor).Float64()
	return result
}
