# QuickNode RPC 配置说明

## 一、配置 QuickNode RPC URL

### 1.1 编辑配置文件

编辑 `etc/tracker.yaml` 文件，在 `follow` 部分添加 `rpcUrl` 配置：

```yaml
follow:
  rpcUrl: "https://blissful-wiser-flower.hype-mainnet.quiknode.pro/bf764c7e480d0ddff5c8960cdf48b58afa14feb5/evm"
  # ... 其他配置
```

### 1.2 使用您自己的 QuickNode RPC URL

如果您有自己的 QuickNode 账户，可以使用您自己的 RPC URL：

```yaml
follow:
  rpcUrl: "https://your-endpoint.quiknode.pro/your-api-key/evm"
```

## 二、当前实现状态

### 2.1 已实现的功能

✅ **RPC 客户端框架**
- 已创建 `pkg/rpc/client.go`，实现了基本的 EVM RPC 调用
- 支持 `eth_getLogs`、`eth_call`、`eth_blockNumber` 等方法
- 已集成到 `query-account` 命令中

✅ **自动切换逻辑**
- 当 Hyperliquid API 返回 `null` 时，自动尝试从链上获取数据
- 当 API 调用失败时，也会尝试使用 QuickNode RPC 作为备用

✅ **数据计算逻辑**
- 已实现从链上交易记录计算 `total_pnl` 和 `total_fee`
- 已实现 7 日内交易次数统计

### 2.2 待完善的功能

⚠️ **需要 Hyperliquid 合约信息**

当前实现还需要以下信息才能完全工作：

1. **合约地址**
   - Hyperliquid 的 Clearinghouse 合约地址
   - Exchange 合约地址
   - 或其他相关合约地址

2. **事件签名**
   - 交易事件的签名哈希（如 `Trade` 事件）
   - 平仓事件的签名哈希（如 `ClosePosition` 事件）

3. **事件解析逻辑**
   - 需要根据 Hyperliquid 的事件结构解析日志数据
   - 提取 `ClosedPnl`、`Fee`、`Time` 等字段

## 三、如何获取合约信息

### 3.1 查找合约地址

可以通过以下方式查找 Hyperliquid 的合约地址：

1. **查看 Hyperliquid 官方文档**
   - 访问 Hyperliquid 的开发者文档
   - 查找合约部署信息

2. **使用区块浏览器**
   - 在 Hyperliquid 链的区块浏览器中搜索
   - 查找相关的合约地址

3. **分析链上交易**
   - 通过分析已知的交易，找到调用的合约地址

### 3.2 获取事件签名

事件签名是事件名称和参数类型的 keccak256 哈希的前 4 字节。

例如，如果事件定义为：
```solidity
event Trade(address indexed user, uint256 pnl, uint256 fee, uint256 time);
```

可以使用以下方法计算签名：
```bash
# 使用 web3.js 或 ethers.js
keccak256("Trade(address,uint256,uint256,uint256)").slice(0, 10)
```

## 四、使用步骤

### 4.1 配置 RPC URL

1. 编辑 `etc/tracker.yaml`
2. 在 `follow.rpcUrl` 中填入您的 QuickNode RPC URL
3. 保存文件

### 4.2 运行程序

```bash
./tracker-linux query-account --config etc/tracker.yaml
```

### 4.3 当前行为

- ✅ 如果配置了 RPC URL，程序会尝试从链上获取数据
- ⚠️ 但由于缺少合约地址和事件签名，当前会返回空数据
- ✅ 程序会记录调试日志，显示 RPC 调用状态

## 五、完善实现

要完全实现链上数据获取，需要：

### 5.1 修改 `pkg/rpc/client.go`

在 `QueryUserFillsFromChain` 函数中：

```go
// 1. 添加合约地址常量
const HyperliquidClearinghouseAddress = "0x..." // 需要填入实际地址

// 2. 添加事件签名常量
const TradeEventSignature = "0x..." // 需要填入实际签名

// 3. 完善查询逻辑
req := GetLogsRequest{
    FromBlock: "0x" + fromBlock.Text(16),
    ToBlock:   "0x" + toBlock.Text(16),
    Address:   HyperliquidClearinghouseAddress,
    Topics: []string{
        TradeEventSignature,
        AddressToTopic(userAddress),
    },
}

// 4. 实现事件解析
for _, log := range logs {
    // 解析 log.Data 和 log.Topics
    // 提取 ClosedPnl, Fee, Time 等字段
    fill := ChainFill{
        Time:      parseTime(log),
        ClosedPnl: parsePnl(log),
        Fee:       parseFee(log),
    }
    fills = append(fills, fill)
}
```

### 5.2 获取合约 ABI（可选）

如果需要获取账户状态（`total_equity`、`available_balance`、`used_margin`），还需要：

1. 获取合约 ABI
2. 实现合约函数调用
3. 解析返回数据

## 六、测试

### 6.1 测试 RPC 连接

可以先用简单的 RPC 调用测试连接：

```go
// 测试获取最新区块号
blockNumber, err := rpcClient.GetBlockNumber()
if err != nil {
    log.Fatal("RPC 连接失败:", err)
}
log.Println("最新区块号:", blockNumber)
```

### 6.2 查看日志

运行程序时，查看日志输出：

- 如果看到 "QuickNode RPC 客户端已创建"，说明配置成功
- 如果看到 "从链上查询交易记录失败"，可能是缺少合约信息
- 如果看到 "从链上获取数据完成"，说明查询成功

## 七、注意事项

1. **RPC 速率限制**
   - QuickNode 可能有速率限制
   - 大量查询时需要注意控制频率

2. **区块范围**
   - 当前实现查询最近 10000 个区块
   - 可以根据需要调整范围

3. **性能考虑**
   - 链上查询比 API 查询慢
   - 建议优先使用 API，链上作为备用

4. **数据准确性**
   - 需要正确解析事件数据
   - 建议与 API 数据对比验证

## 八、总结

**当前状态：**
- ✅ RPC 客户端框架已实现
- ✅ 自动切换逻辑已实现
- ⚠️ 需要 Hyperliquid 合约地址和事件签名才能完全工作

**下一步：**
1. 获取 Hyperliquid 合约地址
2. 获取事件签名哈希
3. 实现事件日志解析逻辑
4. 测试和验证数据准确性

配置好 RPC URL 后，程序框架已就绪，只需要补充合约信息即可完全使用。
