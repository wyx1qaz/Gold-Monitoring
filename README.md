# SGE 溢价与黄金反转监控

本项目会从新浪财经轻量抓取以下实时数据：

- `nf_AU0` 沪金连续
- `hf_XAU` 伦敦金（现货黄金）
- `USDCNY` 美元人民币汇率

并按下面公式计算沪金相对伦敦金的溢价：

```text
伦敦金折算人民币/克 = 伦敦金(美元/盎司) * USDCNY / 31.1034768
溢价(元/克) = 沪金(元/克) - 伦敦金折算人民币/克
```

程序现在包含两条监控链路，并统一在新的前端中台页面里管理：

- `SGE 溢价监控`：当且仅当沪金和伦敦金都处在开盘时段，且溢价超过设定阈值时，通过钉钉机器人 Webhook 推送预警。
- `黄金反转监控`：从新浪盘面抓取现货金价格，并结合 RSS 事件源识别三类反转锚点。
  - `price`：盘面反弹，现价高于最近低点一定比例，且重新站上短均线。
  - `political`：最近 RSS 出现停火、谈判、斡旋等政治缓和关键词。
  - `war`：最近 RSS 出现复航、恢复装船、重启出口、护航等战争进度关键词。

前端支持：

- 多级菜单：`总览`、`SGE 溢价`、`黄金反转`、`RSS 源`、`推送设置`、`系统状态`
- 可配置 RSS 源地址列表
- 可配置 RSS 抓取频率
- 可配置多组 `webhook + secret` 推送目标
- RSS 分类展示、关键词展示和手动测试推送

反转信号分三级：

- `一级`：`price + political + war` 三者同时触发
- `二级`：三者中任意两项触发
- `三级`：三者中任意一项触发

所有抓取记录、溢价记录、RSS 事件和预警记录都会落到本地 SQLite。

## 运行

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

浏览器打开 `http://127.0.0.1:8000`。

## 默认配置

- 阈值：`20` 元/克
- 抓取频率：`60` 秒
- RSS 抓取频率：`300` 秒
- 预警冷却：`900` 秒
- 请求超时：`10` 秒
- 黄金反转冷却：`1800` 秒
- 反转盘面回看：`360` 分钟
- 反转最小反弹：`1.2%`
- 反转短均线窗口：`15` 个样本
- RSS 信号有效期：`180` 分钟

这些配置都会持久化保存。原有页面仍可改 SGE 参数；黄金反转参数可以通过 API 更新。

## 反转监控 API

- `GET /api/reversal/status`：查看最新反转样本、最近告警、最近 RSS 事件和运行状态
- `GET /api/reversal/history?range=1D`：查看最近 1H / 1D / 1W 的反转样本
- `GET /api/reversal/events?event_type=political`：查看最近 RSS 命中的政治或战争事件
- `PUT /api/reversal/settings`：更新反转监控配置
- `POST /api/reversal/test-alert`：发送一级 / 二级 / 三级测试推送
- `POST /api/reversal/run-once`：仅执行一次黄金反转监控

示例：

```bash
curl -X PUT http://127.0.0.1:8000/api/reversal/settings ^
  -H "Content-Type: application/json" ^
  -d "{\"dingtalk_webhook\":\"https://oapi.dingtalk.com/robot/send?access_token=...\",\"dingtalk_at_user_ids\":[\"user123\"],\"rss_feed_urls\":[\"https://feeds.bbci.co.uk/news/world/middle_east/rss.xml\"]}"
```

## 说明

- 沪金开盘时段按上期所黄金常见交易时段处理：`09:00-11:30`、`13:30-15:00`、`21:00-02:30`。
- 伦敦金开盘时段按纽约时间 `周日 17:00` 开盘、`周五 17:00` 收盘处理，自动兼容夏令时。
- 节假日未接入交易所日历，因此法定休市日仍可能被判定为交易日时间段。
- 数据库存放在 `data/monitor.db`。
- RSS 默认源是公开 RSS，可按你的使用习惯替换成更贴近交易的源。
