# Hyperliquid 链上数据获取说明

## QuickNode RPC 节点

```
https://blissful-wiser-flower.hype-mainnet.quiknode.pro/bf764c7e480d0ddff5c8960cdf48b58afa14feb5/evm
```

## 一、通过 EVM RPC 可以获取的数据类型

### 1.1 基础链上数据

#### 1. 区块数据
- **区块高度** (Block Number)
- **区块时间戳** (Block Timestamp)
- **区块哈希** (Block Hash)
- **交易数量** (Transaction Count)
- **Gas 使用量** (Gas Used)
- **区块奖励** (Block Reward)

#### 2. 交易数据
- **交易哈希** (Transaction Hash)
- **发送地址** (From Address)
- **接收地址** (To Address)
- **交易金额** (Value)
- **Gas 价格** (Gas Price)
- **Gas 限制** (Gas Limit)
- **交易状态** (Status: Success/Failed)
- **交易时间戳** (Timestamp)

#### 3. 交易回执 (Transaction Receipt)
- **交易状态** (Status)
- **Gas 使用量** (Gas Used)
- **日志** (Logs)
- **合约地址** (Contract Address，如果是合约调用)

### 1.2 Hyperliquid 合约事件日志 (Event Logs)

通过查询合约事件日志，可以获取以下数据：

#### 1. 交易事件 (Trade Events)
- **交易对** (Trading Pair/Coin)
- **交易方向** (Buy/Sell, Long/Short)
- **交易价格** (Price)
- **交易数量** (Size)
- **交易时间** (Timestamp)
- **交易者地址** (Trader Address)
- **订单 ID** (Order ID)
- **已实现盈亏** (Realized PnL)
- **手续费** (Fee)

#### 2. 开仓事件 (Open Position Events)
- **用户地址** (User Address)
- **币种** (Coin)
- **开仓方向** (Long/Short)
- **开仓价格** (Entry Price)
- **开仓数量** (Size)
- **杠杆倍数** (Leverage)
- **保证金** (Margin)
- **开仓时间** (Timestamp)

#### 3. 平仓事件 (Close Position Events)
- **用户地址** (User Address)
- **币种** (Coin)
- **平仓价格** (Exit Price)
- **平仓数量** (Size)
- **已实现盈亏** (Realized PnL)
- **平仓时间** (Timestamp)

#### 4. 清算事件 (Liquidation Events)
- **被清算用户** (Liquidated User)
- **清算币种** (Coin)
- **清算价格** (Liquidation Price)
- **清算数量** (Size)
- **清算时间** (Timestamp)
- **清算方式** (Method)

#### 5. 资金变动事件 (Funding Events)
- **用户地址** (User Address)
- **资金费用** (Funding Fee)
- **资金费率** (Funding Rate)
- **时间戳** (Timestamp)

#### 6. 存款/提款事件 (Deposit/Withdraw Events)
- **用户地址** (User Address)
- **金额** (Amount)
- **币种** (Token)
- **时间戳** (Timestamp)
- **交易哈希** (Transaction Hash)

### 1.3 合约状态查询 (Contract State)

通过调用合约的 view 函数，可以获取：

#### 1. 用户账户状态
- **账户余额** (Account Balance)
- **总权益** (Total Equity)
- **可用保证金** (Available Margin)
- **已用保证金** (Used Margin)
- **未实现盈亏** (Unrealized PnL)
- **已实现盈亏** (Realized PnL)

#### 2. 持仓信息
- **持仓币种** (Coin)
- **持仓方向** (Long/Short)
- **持仓数量** (Position Size)
- **开仓价格** (Entry Price)
- **当前价格** (Mark Price)
- **未实现盈亏** (Unrealized PnL)
- **杠杆倍数** (Leverage)
- **清算价格** (Liquidation Price)

#### 3. 市场数据
- **交易对列表** (Trading Pairs)
- **当前价格** (Current Price)
- **24小时交易量** (24h Volume)
- **持仓量** (Open Interest)
- **资金费率** (Funding Rate)
- **标记价格** (Mark Price)
- **指数价格** (Index Price)

## 二、Hyperliquid 主要合约地址

### 2.1 核心合约

根据 Hyperliquid 的架构，主要合约包括：

1. **Clearinghouse 合约** (清算所合约)
   - 处理交易清算
   - 管理用户账户状态
   - 存储持仓信息

2. **Exchange 合约** (交易所合约)
   - 处理订单匹配
   - 记录交易事件

3. **Vault 合约** (金库合约)
   - 管理资金池
   - 处理存款/提款

4. **Oracle 合约** (预言机合约)
   - 提供价格数据
   - 更新标记价格

## 三、通过 RPC 获取数据的方法

### 3.1 使用 eth_getLogs 获取事件日志

```json
{
  "jsonrpc": "2.0",
  "method": "eth_getLogs",
  "params": [{
    "fromBlock": "0x0",
    "toBlock": "latest",
    "address": "0x合约地址",
    "topics": ["0x事件签名哈希"]
  }],
  "id": 1
}
```

**可以获取：**
- 所有交易记录
- 开仓/平仓记录
- 清算记录
- 资金变动记录

### 3.2 使用 eth_call 调用合约函数

```json
{
  "jsonrpc": "2.0",
  "method": "eth_call",
  "params": [{
    "to": "0x合约地址",
    "data": "0x函数调用数据"
  }, "latest"],
  "id": 1
}
```

**可以获取：**
- 用户账户状态
- 持仓信息
- 市场数据
- 价格信息

### 3.3 使用 eth_getTransactionReceipt 获取交易详情

```json
{
  "jsonrpc": "2.0",
  "method": "eth_getTransactionReceipt",
  "params": ["0x交易哈希"],
  "id": 1
}
```

**可以获取：**
- 交易详细信息
- 交易中的事件日志
- Gas 使用情况

### 3.4 使用 eth_getBlockByNumber 获取区块数据

```json
{
  "jsonrpc": "2.0",
  "method": "eth_getBlockByNumber",
  "params": ["0x区块号", true],
  "id": 1
}
```

**可以获取：**
- 区块中的所有交易
- 区块时间戳
- 区块哈希

## 四、具体可获取的数据列表

### 4.1 用户交易数据

| 数据项 | 说明 | 获取方式 |
|--------|------|---------|
| **交易哈希** | 每笔交易的唯一标识 | `eth_getTransactionReceipt` |
| **交易时间** | 交易发生的时间戳 | 从区块时间戳获取 |
| **交易对/币种** | 交易的币种 | 从事件日志解析 |
| **交易方向** | 买入/卖出，开多/开空 | 从事件日志解析 |
| **交易价格** | 成交价格 | 从事件日志解析 |
| **交易数量** | 成交数量 | 从事件日志解析 |
| **已实现盈亏** | 平仓时的盈亏 | 从事件日志解析 |
| **手续费** | 交易手续费 | 从事件日志解析 |
| **订单 ID** | 订单标识 | 从事件日志解析 |

### 4.2 用户持仓数据

| 数据项 | 说明 | 获取方式 |
|--------|------|---------|
| **持仓币种** | 持有的币种 | `eth_call` 调用合约 |
| **持仓方向** | 多/空 | `eth_call` 调用合约 |
| **持仓数量** | 持仓大小 | `eth_call` 调用合约 |
| **开仓价格** | 开仓时的价格 | `eth_call` 调用合约 |
| **当前价格** | 标记价格 | `eth_call` 调用合约 |
| **未实现盈亏** | 当前未平仓的盈亏 | `eth_call` 调用合约 |
| **杠杆倍数** | 使用的杠杆 | `eth_call` 调用合约 |
| **清算价格** | 触发清算的价格 | `eth_call` 调用合约 |
| **保证金** | 使用的保证金 | `eth_call` 调用合约 |

### 4.3 用户账户数据

| 数据项 | 说明 | 获取方式 |
|--------|------|---------|
| **总权益** | 账户总价值 | `eth_call` 调用合约 |
| **可用余额** | 可用资金 | `eth_call` 调用合约 |
| **已用保证金** | 持仓占用的保证金 | `eth_call` 调用合约 |
| **累计盈亏** | 历史累计盈亏 | 从事件日志累计计算 |
| **累计手续费** | 历史累计手续费 | 从事件日志累计计算 |

### 4.4 市场数据

| 数据项 | 说明 | 获取方式 |
|--------|------|---------|
| **当前价格** | 实时价格 | `eth_call` 调用合约 |
| **24小时交易量** | 24小时成交量 | `eth_call` 调用合约 |
| **持仓量** | 未平仓合约总量 | `eth_call` 调用合约 |
| **资金费率** | 当前资金费率 | `eth_call` 调用合约 |
| **标记价格** | 标记价格 | `eth_call` 调用合约 |
| **指数价格** | 指数价格 | `eth_call` 调用合约 |

### 4.5 历史统计数据

| 数据项 | 说明 | 获取方式 |
|--------|------|---------|
| **历史交易记录** | 所有历史交易 | `eth_getLogs` 查询事件 |
| **历史开仓记录** | 所有开仓记录 | `eth_getLogs` 查询事件 |
| **历史平仓记录** | 所有平仓记录 | `eth_getLogs` 查询事件 |
| **历史清算记录** | 所有清算记录 | `eth_getLogs` 查询事件 |
| **历史资金变动** | 存款/提款记录 | `eth_getLogs` 查询事件 |
| **历史价格数据** | 价格历史 | 从事件日志提取 |

## 五、实现难度和限制

### 5.1 实现难度

| 数据类型 | 难度 | 说明 |
|---------|------|------|
| **事件日志查询** | ⭐⭐⭐ | 需要知道合约地址和事件签名 |
| **合约状态查询** | ⭐⭐⭐⭐ | 需要合约 ABI 和函数签名 |
| **历史数据重建** | ⭐⭐⭐⭐⭐ | 需要遍历大量区块和事件 |

### 5.2 限制

1. **需要合约地址和 ABI**
   - Hyperliquid 的合约地址
   - 合约的 ABI（Application Binary Interface）
   - 事件签名哈希

2. **数据量大**
   - 需要遍历大量区块
   - 需要处理大量事件日志
   - 需要大量 RPC 调用

3. **性能问题**
   - RPC 调用有速率限制
   - 大量查询可能很慢
   - 需要优化查询策略

4. **数据准确性**
   - 需要正确解析事件数据
   - 需要处理数据格式转换
   - 需要验证数据完整性

## 六、与 API 数据的对比

### 6.1 API 数据（当前使用）

| 数据 | 来源 | 优点 | 缺点 |
|------|------|------|------|
| **用户权益** | `GetUserAccountSummary` | 实时、准确 | 可能返回 null |
| **交易记录** | `GetUserFills` | 完整历史 | 可能返回 null |
| **持仓信息** | `GetUserAccountSummary` | 实时状态 | 可能返回 null |

### 6.2 链上数据（RPC）

| 数据 | 来源 | 优点 | 缺点 |
|------|------|------|------|
| **交易记录** | 事件日志 | 不可篡改、完整 | 需要解析、量大 |
| **账户状态** | 合约调用 | 实时、准确 | 需要 ABI、复杂 |
| **历史数据** | 区块查询 | 完整历史 | 查询慢、成本高 |

## 七、推荐方案

### 7.1 混合方案（推荐）

1. **优先使用 API**
   - 速度快
   - 数据格式友好
   - 实现简单

2. **API 失败时使用链上数据**
   - 作为备用数据源
   - 确保数据完整性
   - 提高可靠性

3. **定期同步链上数据**
   - 验证 API 数据准确性
   - 补充缺失数据
   - 建立数据备份

### 7.2 实现步骤

1. **获取合约信息**
   - 查找 Hyperliquid 合约地址
   - 获取合约 ABI
   - 确定事件签名

2. **实现 RPC 客户端**
   - 使用 `go-ethereum` 库
   - 实现事件日志查询
   - 实现合约调用

3. **数据解析和存储**
   - 解析事件日志
   - 转换数据格式
   - 存储到数据库

## 八、总结

通过 QuickNode RPC 节点可以获取 Hyperliquid 的**所有链上数据**，包括：

✅ **交易记录** - 完整的历史交易数据  
✅ **持仓信息** - 实时持仓状态  
✅ **账户状态** - 账户余额、权益等  
✅ **市场数据** - 价格、交易量等  
✅ **历史统计** - 所有历史数据  

但实现需要：
- 合约地址和 ABI
- 事件签名
- 大量 RPC 调用
- 数据解析逻辑

**建议**：继续使用 API 作为主要数据源，链上数据作为备用和验证手段。
