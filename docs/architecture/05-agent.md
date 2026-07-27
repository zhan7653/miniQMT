# 决策 Agent（`fundlab/agent`）

`fundlab/agent` 是 JSON 决策文件契约的仓库内生产者。无论确定性策略还是 LLM
策略，进入交易内核的唯一产物仍是：

```text
data/agent/decisions/<account_id>/<YYYY-MM-DD>.json
        ↓
fundlab.strategies.FileIntentSource
        ↓
PortfolioIntent
```

Agent 不直接调用模拟内核，更不连接实盘。没有决策文件表示持有；已有但无效的文件
会失败关闭，不能降级成沉默。

## 当前策略

### `momentum-rotation`

`paper-agent` 的确定性基线。它使用点时复权收盘价计算 60 会话动量，在声明的
`risk_on` / `risk_off` 权重之间选择。历史不足、参数拼错或权重越界都不落盘。

### `dividend-value`

`paper-dividend` 的红利价值 Agent，也是第一个 LLM 策略。它不是“让模型随意选股”，
而是确定性代码包围一次受限的语义排序：

1. `CanonicalMarketData` 在明确 `as_of` 下读取全股票宇宙、原始收盘/成交额和
   `known_date <= as_of` 的现金分红记录。
2. 代码排除停牌、ST、流动性不足、连续分红不足和股息率不足的股票，计算 TTM
   股息率、连续分红年数、近年每股分红序列及波动度。
3. 只把前 50 个合格候选、白名单资料、最近的有界记忆和当前组合事实发给 LLM。
4. LLM 必须严格返回 10 个候选 ID、逐项理由、`hold|rebalance` 和可选机会列表。
5. 代码再次验证候选归属、数量、机会邮件 6% 股息率门槛、等权、5% 现金、15%
   单股上限和章程硬规则；任何越界都不生成文件、不发邮件。

当前发布快照（`snap-2a502eb188874c6ac7bbfb7f`，数据截至 2026-07-24）的只读实测：
5,201 只股票中，2,882 只通过三年/流动性等硬筛，334 只继续通过“五年连续分红、
股息率至少 4%”战术门槛，足以形成候选池。该宇宙带幸存者偏差，因此这里只证明
当前向前模拟可运行，不据此宣称历史回测收益无偏。

## 章程与战术

`config/agents/dividend-value.yaml` 是版本化 charter（“道”），内容哈希进入评估上下文
与策略配置证据。v1 硬规则：

- 只买股票且排除 ST；
- 连续现金分红至少 3 年，股息率不得低于 2%；
- 持仓 5–20 只，单股不超过 15%；
- 现金不少于 5%；
- 每周研究、低换手，机会可以只提示而不交易。

`config/fundlab.yaml` 是可调战术（“术”）：选 10 只、至少 5 年连续分红、股息率至少
4%、20 日平均成交额至少 2,000 万元、现金 5%、候选池 50、组合变更冷却 28 天。
构造策略时就校验“术”没有突破“道”，否则拒绝启动。

## 周度研究与组合变更

每日脚本仍会在发布前后调用 `agent decide --all`，但 dividend 策略只有在当前
`as_of` 是该自然周最后一个开市日时才评估（通常周五；节假日周可为周四）。同一
ISO 周、同一配置哈希的评估记录存在后，后续调用幂等跳过。

LLM 每周都可以给出研究结论，但只有以下情况允许写新组合决策：

- 账户尚未建仓；
- 当前持仓违反 charter，代码要求退出/恢复边界；
- 距离上一份组合决策至少 28 天且 LLM 建议 `rebalance`。

有未完成订单时不叠加新组合。冷却期内发现的机会仍可发邮件，但不调仓。持仓违反
硬规则时，代码可以用 LLM 已严格选出的合格篮子强制恢复边界，即使模型建议 hold。

## Responses 中转契约

配置与当前 Codex provider 对齐：`gpt-5.6-sol`、OpenAI-compatible Responses 协议、
`medium` reasoning、`max_output_tokens=32768`、300 秒超时、瞬时失败最多重试两次。
实际请求固定为：

```text
POST <agent.llm.base_url>/responses
Authorization: Bearer $FUNDLAB_LLM_API_KEY
Content-Type: application/json; charset=utf-8
```

请求使用 `store: false`；结构化输出按 Responses API 放在 `text.format`，而不是旧的
`response_format`。详见 OpenAI 的
[Responses Structured Outputs 迁移说明](https://developers.openai.com/api/docs/guides/migrate-to-responses#6-update-structured-outputs-definitions)。

响应只接受 `status=completed` 且恰好一个正式
`output[].content[].type=output_text`；拒绝、非 JSON、schema 字段变化、候选越界均失败。
不解析 Markdown 代码块，不从自由文本提取 JSON，也不降级到另一个模型或协议。

## 资料库、记忆与邮件

- **资料库**：`data/agent/library/` 下仅 `.md`/`.txt`，且必须在
  `agent.library.documents` 明确列名；每份最多 40,000 字符，总计最多 120,000。
  内容作为不可信引用数据，模型提示明确禁止执行其中的指令。
- **记忆**：`data/agent/memory/<account>.jsonl` 追加记录评估、模型/response ID、token
  用量、上下文哈希、选择理由、决策落盘和邮件结果。输入只取最近 20 条，并继续受
  单条 20,000、总计 120,000 字符限制。损坏的 JSONL 不会被静默喂给模型。
- **并发**：每账户跨进程文件锁包围完整评估，避免计划任务与手动 Web/CLI 同时调用
  LLM 或重复发信；交易决策文件另有原子无覆盖写入保护。
- **邮件**：只有通过代码复核的新机会/风险才进入邮件；证据哈希已成功发送过则不
  重复。机会需股息率至少 6%，持仓硬规则风险不受此门槛限制。SMTP 失败不会撤销
  已验证的交易决策。

所有秘密只来自环境变量：

```text
FUNDLAB_LLM_API_KEY
FUNDLAB_SMTP_HOST
FUNDLAB_SMTP_PORT       # 可选，默认 465 / SSL
FUNDLAB_SMTP_USER
FUNDLAB_SMTP_PASSWORD
```

## 灰度启用

`paper-dividend` 已加入模拟账户，但策略初始是 `scheduled: false`。因此账户会随每日
管线推进并保持现金，`agent decide --all` 不会调用外部模型。先配置密钥后运行：

```powershell
uv run fundlab agent decide --account-id paper-dividend --force-review --dry-run
```

这会真实调用中转并完整验证结果，但不写决策、记忆或邮件。确认后可去掉
`--dry-run` 做一次人工投递，或把 `scheduled` 改为 `true` 交给周度节奏。Web API
同样支持 body 字段 `force_review`；仪表盘提供“忽略周度节奏，立即评估”复选框。

CLI 完整接口：

```text
fundlab agent decide (--account-id X | --all)
  [--target-date D] [--overwrite] [--dry-run] [--force-review]
```

`--force-review` 只允许单账户 review-cadence 策略；`--all` 只执行
`scheduled: true` 的策略。

## 明确延期

本期不抓取新闻，不启用 hosted web search，也不让 Agent 修改 charter、战术参数或
提示词。记忆只是可审计事实和后续上下文，不是自主学习通道。

## 验证

- `test_agent_decision.py`：共享时点、策略、服务、文件幂等与并发写入契约；
- `test_dividend_value_agent.py`：章程边界、候选限制、冷却/强制退出、资料与记忆上限、
  `/responses` 请求形状、严格解析、瞬时重试、灰度开关、hold 邮件与零决策文件；
- `test_cli.py` / `test_web_api.py`：CLI 与 Web 共享服务入口；
- 正式快照候选只读冒烟：不调用模型、不写数据，验证实际候选数量大于 10。
