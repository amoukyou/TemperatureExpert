# Temperature Expert - Polymarket 温度市场专家分析工具

Polymarket 温度预测市场交易者分析平台。通过链上交易数据分析，识别温度预测专家、检测套利策略、追踪专家持仓。

**在线访问**: https://tero.market/pm

## 数据规模

| 指标 | 数量 |
|------|------|
| 温度事件 | 2,070+ |
| 子市场(conditionId) | 16,552+ |
| 链上交易记录 | 7,235,965+ |
| 参与钱包 | 114,600+ |
| 覆盖城市 | 40 |

## 功能

### 📊 市场概览
- 各城市交易量对比（柱状图）
- 盈亏比例统计
- 城市热度排行表（含参与人数、交易笔数、交易量、事件数）
- 时间范围过滤（7天/14天/30天/全部）

### 🔍 搜索筛选
- **10+ 个自定义过滤器**：A类盈亏、总盈亏、信念比、曲线评分、B类占比、近期活跃度等
- **快捷预设**：专家模式、狙击手模式
- **排行榜**：按 A类盈亏、ROI、胜率、曲线评分等排序
- **每日活跃度图表**：跟随过滤器联动
- **批量收藏**：全选/多选加入收藏夹

### 👤 个人详情
- 资产走势曲线（天气交易累计盈亏）
- 曲线质量指标：夏普比率、盈利因子、最大回撤、恢复因子
- **B类策略标签**：每笔交易标注套利策略编号（如 B1.001, B4.002）
- 事件级盈亏计算
- 交易结果判定（有利/不利）

### ⭐ 专家收藏夹
- 批次分组管理（创建/重命名/删除）
- 批量添加
- 导出 CSV
- 数据库持久化

### 📡 跟踪专家
- 实时查询收藏专家的当前温度市场持仓
- 按城市/日期分组展示
- 显示均价、现价、盈亏

### 📖 策略图鉴
11 种已识别的套利策略，分三大类：

**A类 - 问题技巧**（需要气象知识）
- A1 气象预报套利
- A2 温度阶梯 (Laddering)
- A3 不对称重注
- A4 气象模型更新窗口

**B类 - PM平台通用技巧**（与温度无关）
- B1 扫尾盘等判决
- B2 总价>$1套利 (Overround)
- B3 总价<$1套利 (Underround)
- B4 Yes+No双买
- B5 SPLIT做市
- B6 子集/逻辑矛盾
- B7 未知B类模式

**C类 - 跨平台技巧**
- C1 跨平台对冲套利

## 关键指标

| 指标 | 说明 |
|------|------|
| A类盈亏 | 排除所有B类标签交易后的盈亏 |
| A类胜率 | 仅统计A类交易且已结算事件的胜率 |
| 信念比 | (结算+近似结算)/总回收，越高=越持有到底 |
| B类占比 | B类交易金额占总交易额比例 |
| 曲线评分 | 综合夏普(30%)+盈利因子(25%)+恢复因子(20%)+回撤比(15%)+连胜率(10%) |
| 近似结算 | ≥$0.98卖出，等同于结算 |

## B类策略检测规则

| 标签 | 触发条件 |
|------|---------|
| B1 | BUY ≥$0.95 |
| B2 | 5分钟内SELL ≥70%选项的Yes + 仓位均匀(max/min≤3x) |
| B3 | 5分钟内BUY ≥70%选项的Yes + 仓位均匀(max/min≤3x) |
| B4 | 5分钟内同一conditionId买Yes+买No + 金额比0.5x-2x |
| B5 | 整个事件只有SELL无BUY（SPLIT做市） |
| B7 | 5分钟内≥5笔逻辑订单 或 买入≥3个不同选项(非B3) |

## 技术架构

```
Frontend (HTML/JS/Chart.js)
    ↓ fetch API
Flask Backend (pm_server.py, port 8899)
    ↓ SQLite queries
Database (pm_temperature.db, ~3GB)
    ↓
Nginx reverse proxy (/pm → :8899)
    ↓
https://tero.market/pm
```

### 数据来源
- **Gamma API**: 事件和市场元数据
- **Data API**: 交易记录 (trades, takerOnly=false)
- **Activity API**: 用户活动（用于发现缺失的conditionId）
- **结算数据**: 从outcomePrices推导

### 数据补全策略
通过 top N 活跃钱包的 Activity API 反向发现缺失的 conditionId，再批量拉取交易。Gamma API 分页不稳定，不能作为唯一数据源。

## 项目文件

| 文件 | 说明 |
|------|------|
| `pm_server.py` | Flask 后端 API |
| `pm_temperature_dashboard.html` | 前端主页面 |
| `pm_strategies.html` | 策略图鉴页面 |
| `pm_temp_active_markets.json` | 活跃市场 conditionId 映射 |
| `full_recalc.py` | 全量重算脚本（B类标签+钱包统计+曲线指标） |
| `backfill_fast.py` | 多线程数据补全脚本 |

## API 端点

| 端点 | 说明 |
|------|------|
| `GET /api/stats` | 总体统计 |
| `GET /api/wallets?sort=&page=&min_pnl_a=...` | 排行榜（服务端过滤分页） |
| `GET /api/wallet/<addr>` | 钱包详情 |
| `GET /api/wallet/<addr>/trades` | 交易记录（含btag） |
| `GET /api/cities?days=` | 城市统计 |
| `GET /api/daily?days=` | 每日统计 |
| `GET /api/groups` | 收藏夹 CRUD |
| `GET /api/filtered_stats` | 过滤后的汇总统计 |
| `GET /api/filtered_daily` | 过滤后的日活数据 |

## 部署

需要 Python 3.9+、Flask、SQLite3。

```bash
pip install flask flask-cors
python3 pm_server.py  # 启动在 :8899
```

Nginx 配置示例：
```nginx
location /pm {
    rewrite ^/pm(/.*)$ $1 break;
    rewrite ^/pm$ / break;
    proxy_pass http://127.0.0.1:8899;
    proxy_read_timeout 120s;
}
```

## 更新日志

### v0.4 (2026-03-22)
- **PnL 数据修复**：参考 [@runes_leo 的分析](https://x.com/runes_leo/status/2034075926699343910)，增加 REDEEM/SPLIT/MERGE 活动采集
- **翻页修复**：Activity API 改用 `end` 游标翻页（修复 offset>3000 截断 bug）
- **结算数据补全**：通过 slug 搜索修复 1,027 个合成事件的结算结果（总 winners 855→1,881）
- **新增 `fetch_activities.py`**：采集非 TRADE 类型活动（REDEEM/SPLIT/MERGE/REWARD）

### v0.3 (2026-03-22)
- **架构优化**：`full_recalc.py` 改为流式处理（内存 5GB→50MB）
- **增量模式**：只处理 `needs_retag=1` 的脏钱包，日常更新 <60 秒
- **数据库索引**：添加复合索引，去重查询提速 100x
- **A5 极限反转策略**：新增策略图鉴条目（$0.001 买 No 赌市场过度自信）

### v0.2 (2026-03-21)
- **数据大幅补全**：通过 top 200 钱包 Activity API 反向发现缺失 conditionId
- **数据量**：461→2,070 事件，50 万→740 万交易
- **B 类策略检测严格化**：B2/B3/B4 加入 5 分钟时间窗口 + 仓位均衡约束
- **曲线评分系统**：夏普比率、盈利因子、最大回撤、恢复因子
- **后端 API**：Flask + SQLite，替代静态 JSON
- **部署上线**：https://tero.market/pm

### v0.1 (2026-03-21)
- 初始版本：静态 HTML + JSON 数据
- 11 种 B 类策略检测（B1-B7）
- 城市过滤器、自定义筛选、收藏夹
- 策略图鉴（A1-A4, B1-B7, C1）

## License

MIT
