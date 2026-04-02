# SGE 溢价、盘面、美债、政治战争监控

这是一个本地运行的监控面板，当前围绕四个核心模块组织：

- `SGE 溢价`
- `盘面预警`
- `十年期美债反转`
- `政治与战争预警`

系统同时集成了：

- `RSS 源管理`
- `RSS ML 训练与状态查看`
- `钉钉推送配置与发送记录`
- `系统状态与运行记录`

前端入口默认是 `http://127.0.0.1:8000`。  
数据默认持久化到 [data/monitor.db](C:/Users/25376/Documents/sge溢价监控/data/monitor.db)。

## 快速上手

### Windows

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

也可以直接使用：

```powershell
start_server.bat
```

启动后访问：

```text
http://127.0.0.1:8000
```

## 当前页面结构

前端当前主要页面为：

- `总览`
- `SGE 溢价`
- `盘面预警`
- `十年期美债反转`
- `政治与战争预警`
- `RSS 源`
- `推送设置`
- `系统状态`

## 页面与接口对照

### 总览

- `GET /api/status`
- `GET /api/reversal/status`
- `GET /api/us10y/status`
- `GET /api/notification/logs`

### SGE 溢价

- `GET /api/status`
- `GET /api/history?range=1D`
- `PUT /api/settings`
- `POST /api/run-once`

### 盘面预警

- `GET /api/reversal/status`
- `GET /api/reversal/history?range=1D`
- `GET /api/reversal/history?range=1W&stride=20`
- `PUT /api/reversal/settings`
- `POST /api/reversal/run-once`
- `POST /api/reversal/test-alert`

### 十年期美债反转

- `GET /api/us10y/status`
- `GET /api/us10y/history?range=1D`
- `GET /api/us10y/history?range=1W&stride=20`
- `PUT /api/reversal/settings`
- `POST /api/us10y/run-once`

### 政治与战争预警

- `GET /api/reversal/events`
- `GET /api/rss-ml/status`
- `PUT /api/rss-ml/config`
- `POST /api/rss-ml/train`
- `GET /api/rss-ml/train-status`
- `POST /api/rss-ml/train-control`
- `POST /api/reversal/rss-run-once`
- `POST /api/reversal/rss-bulk-fill`
- `POST /api/reversal/rss-dedup`

### RSS 源

- `GET /api/settings`
- `PUT /api/settings`

### 推送设置

- `GET /api/settings`
- `PUT /api/settings`
- `GET /api/notification/logs`

### 系统状态

- `GET /api/status`
- `GET /api/reversal/status`
- `GET /api/us10y/status`

## 功能概览

### 1. SGE 溢价

程序抓取以下实时数据：

- `nf_AU0`
- `hf_XAU`
- `USDCNY`

并按下面公式计算溢价：

```text
伦敦金折算人民币/克 = 伦敦金(美元/盎司) * USDCNY / 31.1034768
溢价(元/克) = 沪金(元/克) - 伦敦金折算人民币/克
```

当沪金和国际金都处于交易时段，且溢价超过阈值时，可通过钉钉机器人发送 `SGE 溢价预警`。

### 2. 盘面预警

盘面预警围绕黄金反转样本构建，综合以下条件：

- `price`：盘面反弹，价格高于最近低点一定比例，且重新站上短均线
- `political`：RSS 中命中停火、谈判、缓和等关键词
- `war`：RSS 中命中复航、恢复出口、航运恢复等关键词
- `us10y`：美债回落信号可作为联动信息参与状态展示

当前盘面预警是 `4级`：

- `1级`：最强信号
- `2级`：较强信号
- `3级`：弱信号
- `4级`：仅记录，不推送

后端当前只对 `1级 / 2级` 进入推送流程，`3级 / 4级` 只记录。

### 3. 十年期美债反转

支持多期限监控：

- `5Y`
- `10Y`
- `20Y`

可配置参数：

- 采样频率（秒）
- 回落窗口（小时）
- 回落阈值（bp）
- 预警冷却（秒）
- 重复发送抑制（小时）
- 启用期限（5Y / 10Y / 20Y）

推送逻辑包含两层限制：

- `预警冷却`：距离上次成功发送不足指定秒数时不再发送
- `重复发送抑制`：触发时若过去 N 小时内已经成功发送过，则直接取消发送

默认重复发送抑制为 `4` 小时，可在前端页面直接调整。

### 4. 政治与战争预警

这一页主要承接 RSS 事件流与新闻语义判断，支持：

- RSS 抓取入库
- 事件流展示
- 关键词命中展示
- 去重
- RSS ML 配置、训练、暂停、恢复、取消
- 训练状态与 Loss / Accuracy 曲线展示

## 图表与交互

当前图表行为：

- 支持 `1H / 1D / 1W` 切换
- `1W` 下黄金与美债历史会自动下采样，减少前端渲染压力
- 美债联动图鼠标悬浮时统一返回四个值：
- `Gold`
- `US 5Y`
- `US 10Y`
- `US 20Y`
- 美债联动图支持纵轴滚轮缩放

对应接口也支持长周期采样：

- `GET /api/reversal/history?range=1W&stride=20`
- `GET /api/us10y/history?range=1W&stride=20`

## 默认配置

### SGE 溢价

| 配置项 | 默认值 |
| --- | --- |
| 溢价阈值 | `20` |
| 采样频率 | `60s` |
| 预警冷却 | `900s` |
| 请求超时 | `10s` |

### 盘面预警

| 配置项 | 默认值 |
| --- | --- |
| 冷却 | `1800s` |
| 价格回看 | `360 分钟` |
| 最小反弹 | `1.2%` |
| 短均线窗口 | `15` |
| 信号有效窗口 | `180 分钟` |

### 美债

| 配置项 | 默认值 |
| --- | --- |
| 采样频率 | `60s` |
| 回落窗口 | `24h` |
| 回落阈值 | `1.0bp` |
| 预警冷却 | `1800s` |
| 重复发送抑制 | `4h` |
| 默认期限 | `10Y` |

## API 摘要

### 基础状态

- `GET /api/status`
- `GET /api/settings`
- `PUT /api/settings`
- `POST /api/run-once`

### 盘面预警

- `GET /api/reversal/status`
- `GET /api/reversal/history?range=1D`
- `GET /api/reversal/history?range=1W&stride=20`
- `GET /api/reversal/events`
- `PUT /api/reversal/settings`
- `POST /api/reversal/run-once`
- `POST /api/reversal/test-alert`
- `POST /api/reversal/rss-run-once`
- `POST /api/reversal/rss-bulk-fill`
- `POST /api/reversal/rss-dedup`

### 美债

- `GET /api/us10y/status`
- `GET /api/us10y/history?range=1D`
- `GET /api/us10y/history?range=1W&stride=20`
- `POST /api/us10y/run-once`

### RSS ML

- `GET /api/rss-ml/status`
- `PUT /api/rss-ml/config`
- `POST /api/rss-ml/train`
- `GET /api/rss-ml/train-status`
- `POST /api/rss-ml/train-control`
- `POST /api/rss-ml/clear-samples`
- `POST /api/rss-ml/sync-csv`

## 说明

- 数据库文件默认位于 [data/monitor.db](C:/Users/25376/Documents/sge溢价监控/data/monitor.db)
- 美债数据源按优先级尝试：`Eastmoney -> 新浪 -> FRED`
- `1W` 图表接口支持 `stride` 参数，前端默认在长周期下启用采样
- 推送发送记录会写入 `notification_logs`
- 美债重复发送抑制只针对“成功发送过”的历史记录生效
