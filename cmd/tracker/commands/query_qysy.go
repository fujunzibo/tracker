package commands

import (
	"context"
	"hyper-tracker/dao"
	"hyper-tracker/internal/config"
	"hyper-tracker/pkg/hyper"
	"hyper-tracker/pkg/log"
	"log/slog"
	"math"
	"time"

	"github.com/spf13/cobra"
)

var queryQysyCmd = &cobra.Command{
	Use:   "query-qysy",
	Short: "查询用户权益损益数据",
	Long: `从数据库读取钱包地址，通过 Hyperliquid API 查询以下数据并保存到 tb_user_qysy 表：
  1. 用户权益
  2. 用户损益(累计亏损)
  3. 整体盈亏比(所有盈利订单累计/所有亏损订单累计)
  4. 手续费占比(手续费累计/总亏损)
  5. 7日内平均交易次数

使用示例：
  tracker query-qysy --config etc/tracker.yaml`,
	Run: func(cmd *cobra.Command, args []string) {
		runQueryQysy()
	},
}

func init() {
	rootCmd.AddCommand(queryQysyCmd)
}

func runQueryQysy() {
	// 加载配置
	cfg, err := config.LoadConfig(GetConfigFile())
	if err != nil {
		exitWithError(err)
	}

	// 初始化日志
	log.Setup(&cfg.Log)

	// 初始化数据库
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = dao.InitDB(ctx, cfg.DSN)
	if err != nil {
		slog.Error("初始化数据库连接失败", "error", err)
		exitWithError(err)
	}

	// 创建表（如果不存在）
	err = dao.CreateUserQysyTable()
	if err != nil {
		slog.Error("创建表失败", "error", err)
		exitWithError(err)
	}
	slog.Info("表创建/检查完成")

	// 获取所有地址
	addresses, err := dao.GetAllAddresses()
	if err != nil {
		slog.Error("获取地址列表失败", "error", err)
		exitWithError(err)
	}
	slog.Info("获取到地址列表", "count", len(addresses))

	// 创建 Hyperliquid 客户端
	client := hyper.WithLocalAddrsAndProxies(cfg.Network.LocalAddrs, cfg.Network.Proxies)

	// 批量处理
	batchSize := 10
	var results []*dao.UserQysy
	total := len(addresses)
	
	for i, address := range addresses {
		slog.Info("处理地址", "index", i+1, "total", total, "address", address)
		
		data, err := queryUserData(client, address)
		if err != nil {
			slog.Error("查询用户数据失败", "address", address, "error", err)
			continue
		}
		
		results = append(results, data)
		
		// 批量保存
		if len(results) >= batchSize {
			err = dao.BatchSaveUserQysy(results)
			if err != nil {
				slog.Error("批量保存失败", "error", err)
			} else {
				slog.Info("批量保存成功", "count", len(results))
			}
			results = results[:0]
		}
		
		// 避免请求过快
		time.Sleep(100 * time.Millisecond)
	}
	
	// 保存剩余数据
	if len(results) > 0 {
		err = dao.BatchSaveUserQysy(results)
		if err != nil {
			slog.Error("保存剩余数据失败", "error", err)
		} else {
			slog.Info("保存剩余数据成功", "count", len(results))
		}
	}
	
	slog.Info("查询完成", "total", total)
}

// queryUserData 查询单个用户的数据
func queryUserData(client *hyper.Client, address string) (*dao.UserQysy, error) {
	data := &dao.UserQysy{
		Address: address,
		UpdatedAt: time.Now(),
	}
	
	// 1. 查询用户权益（账户价值）
	summary, err := client.GetUserAccountSummary(address)
	if err != nil {
		if err == hyper.ErrNull {
			// 用户不存在或没有数据，返回默认值
			return data, nil
		}
		return nil, err
	}
	
	// 获取账户价值（用户权益）
	if summary.MarginSummary.AccountValue != nil {
		accountValue, err := summary.MarginSummary.AccountValue.Float64()
		if err == nil {
			data.UserEquity = accountValue
		}
	}
	
	// 2. 查询用户交易记录
	fills, err := client.GetUserFills(address)
	if err != nil {
		if err == hyper.ErrNull {
			// 没有交易记录，返回已有数据
			return data, nil
		}
		return nil, err
	}
	
	// 计算7天前的时间戳
	sevenDaysAgo := time.Now().AddDate(0, 0, -7).UnixMilli()
	
	var (
		totalProfit    float64 // 所有盈利订单累计
		totalLoss      float64 // 所有亏损订单累计（总亏损）
		totalFee       float64 // 手续费累计
		tradeCount7d   int     // 7日内交易次数
	)
	
	// 遍历所有交易记录
	for _, fill := range fills {
		// 获取已实现盈亏
		closedPnl, err := fill.ClosedPnl.Float64()
		if err != nil {
			continue
		}
		
		// 获取手续费
		fee, err := fill.Fee.Float64()
		if err == nil {
			totalFee += math.Abs(fee) // 手续费取绝对值
		}
		
		// 统计盈亏
		if closedPnl > 0 {
			totalProfit += closedPnl
		} else if closedPnl < 0 {
			totalLoss += math.Abs(closedPnl) // 累计亏损取绝对值
		}
		
		// 统计7日内的交易次数
		if fill.Time >= sevenDaysAgo {
			tradeCount7d++
		}
	}
	
	// 2. 用户损益(累计亏损)
	data.TotalLoss = totalLoss
	
	// 3. 整体盈亏比(所有盈利订单累计/所有亏损订单累计)
	if totalLoss > 0 {
		data.ProfitLossRatio = totalProfit / totalLoss
	} else if totalProfit > 0 {
		// 如果只有盈利没有亏损，设为最大值
		data.ProfitLossRatio = math.MaxFloat64
	} else {
		data.ProfitLossRatio = 0
	}
	
	// 4. 手续费占比(手续费累计/总亏损)
	if totalLoss > 0 {
		data.FeeRatio = totalFee / totalLoss
	} else {
		data.FeeRatio = 0
	}
	
	// 5. 7日内平均交易次数
	data.AvgTradeCount7d = float64(tradeCount7d) / 7.0
	
	return data, nil
}

