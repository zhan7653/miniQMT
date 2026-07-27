# 交易内核与策略(fundlab/trading + fundlab/strategies)

## 职责与边界

本层是 canonical 路径第二条持久边界的实现:**组合意图(PortfolioIntent)→ 风控评估 → 单一每日交易内核 → 哈希链账本**。它消费上游已发布的固定快照(通过 `fundlab.marketdata.portal` 的 `CanonicalMarketData` / `MarketSession` / `PointInTimeMarketView`),把"想要的目标权重"变成"可审计的成交与估值事件",并将全部证据以哈希链形式落入 SQLite 账本。

边界纪律:

- **策略层只产意图,不产订单**。`fundlab/strategies` 里的意图源(IntentSource)只能返回不可变的 `PortfolioIntent`;把意图变成订单、成交的权力只属于 `TradingKernel`。
- **历史模拟与每日模拟共用同一内核**,唯一区别是时钟(`HistoricalClock` 跑一个日期区间,`DailyClock` 跑单个交易日),经济逻辑没有第二份实现。
- 本层不抓取数据、不做对账;所有市场输入必须来自已发布快照,估值与撮合只用快照里的原始 OHLCV 与公司行动。

## 核心概念

- **PortfolioIntent(组合意图)**:某账户在某个决策日、基于某个快照,想要达到的目标权重集合。身份字段(`snapshot_id`、`strategy_id/version/config_hash`、`observation_hash`)全部必填且非空,`intent_id` 由内容稳定摘要派生。
- **RiskPolicy / assess_intent(风控评估)**:未知标的或不允许的资产类型直接整体拒绝;超限仓位裁剪到 `max_position_weight`;总权重超出 `1 - minimum_cash_weight` 视为杠杆违规拒绝。
- **TradingKernel(交易内核)**:纯函数式的单日推进器,`PortfolioState` 进、`SessionResult`/`IntentResult` 出,自身无 IO、无隐藏状态。
- **LedgerEvent(账本事件)**:一切发生过的事(意图接收、风控、下单、成交、公司行动、估值、质量降级)都是一条带冻结 payload 的事件,由 `TradingRepository` 串成哈希链。
- **RunBinding(运行绑定)**:一次模拟运行的完整身份 = 账户 + 模式 + `snapshot_id` + 策略三元组哈希 + 执行/风控/费用三个策略哈希 + 初始状态哈希 + 起止日期 + seed。`binding_hash` 是幂等与复现的键。
- **incomplete_reasons(失败即阻断的温和形态)**:内核遇到无法精确建模的事实(缺收盘价、股息税未建模、碎股)不猜数字,而是在状态里累积原因并发出 `simulation_marked_incomplete` 事件,`SimulationFeedback.quality` 随之变为 `incomplete`。

## 文件地图

### fundlab/trading(约 2470 行)

| 文件 | 行数 | 职责 |
|---|---|---|
| `__init__.py` | ~40 | 公开面:re-export 全部规范契约(含从 strategies 兼容性 re-export 的 `StaticAllocationSource`) |
| `intent.py` | ~160 | `PortfolioIntent`、`RiskPolicy`、`RiskAssessment`、`assess_intent`;`decimal_value` 数值净化入口 |
| `fees.py` | ~130 | 费用引擎:`FeeRule`(生效区间/资产类型/交易所匹配 + 优先级)、`FeeSchedule.calculate`(佣金含最低、印花税仅卖出、过户/经手/规费),规则歧义直接抛错;`trusted_for_simulation` 未置真会把整轮模拟标记 incomplete |
| `state.py` | ~230 | 全部冻结 dataclass:`PositionLot`(逐笔持仓含 `sellable_on` T+N)、`Order`、`Fill`、`Entitlement`(公司行动权益)、`Valuation`、`LedgerEvent`、`PortfolioState`(带 `state_hash`)、`SessionResult`/`IntentResult` |
| `kernel.py` | ~910 | `TradingKernel`:`process_session`(单日推进)与 `submit_intent`(意图→次日订单);撮合含滑点冲击模型、涨跌停封板判断、流动性参与率上限、T+1 可卖约束、现金可负担量二分;公司行动含现金分红、送股成本分摊、拆股/缩股原子应用、配股明确放弃 |
| `repository.py` | ~530 | `TradingRepository`:SQLite 持久化账户/运行/账本事件/检查点;账本哈希链写入(`append_session`)、终局哈希(`complete_run`)、链校验(`verify_run`);触发器强制事件与检查点不可改删、终局运行不可改 |
| `runtime.py` | ~280 | `IntentSource` 协议、`HistoricalClock`/`DailyClock`、`SimulationService`(整轮编排:开始运行→逐日推进→意图校验→完成/失败) |
| `schedule.py` | ~30 | 再平衡日历:`scheduled_trading_days` / `is_rebalance_day`(daily/weekly/monthly 取周期末交易日) |
| `reporting.py` | ~170 | `build_simulation_feedback`:从账本重建一轮运行的反馈(收益、最大回撤、成交率、换手、费用、滑点、分红),`quality` 直接由 `incomplete_reasons` 决定 |

### fundlab/strategies(约 255 行)

| 文件 | 行数 | 职责 |
|---|---|---|
| `__init__.py` | ~20 | 意图源包入口,re-export `StaticAllocationSource`、`FileIntentSource`、`AgentDecision(Error)`、`load_agent_decision`(`IntentSource` 协议本体定义在 `fundlab/trading/runtime.py`) |
| `static.py` | ~60 | `StaticAllocationSource`:固定目标权重,每个交易日重申同一权重;`config_hash` 绑定权重内容 |
| `agent_file.py` | ~180 | `AgentDecision` / `load_agent_decision` / `FileIntentSource`:外部 Agent 的 JSON 决策文件契约(详见下文) |

## 关键流程

### 一个交易日在内核里的完整事件序列

`SimulationService._run` 对每个交易日执行"先结算当日、后决策次日"的两段:

```mermaid
sequenceDiagram
    participant S as SimulationService
    participant K as TradingKernel
    participant I as IntentSource
    participant R as TradingRepository
    S->>K: process_session(state, market)
    Note over K: 1 费用表信任检查(不信任→标记 incomplete)
    Note over K: 2 公司行动开盘前:除权日(送股成本分摊/拆股原子应用)、支付日(cash_dividend_paid/配股放弃)、到账日(share_distribution_listed)
    Note over K: 3 订单清扫:过期→order_expired;今日到期→按 卖先买后 顺序撮合
    Note over K: 4 撮合:开盘价+滑点冲击模型,受涨跌停/流动性参与率/T+1 可卖/现金约束;产出 fill_created,剩余量当日 order_expired
    Note over K: 5 收盘估值 portfolio_valued(缺价→incomplete)
    Note over K: 6 登记日捕获权益 corporate_action_entitlement
    K-->>S: SessionResult(state, events, fills, valuation)
    S->>I: decide(account_id, PointInTimeMarketView(day), state)
    I-->>S: PortfolioIntent | None(None=持有)
    S->>K: submit_intent(state, intent, market, next_session)
    Note over K: portfolio_intent_received → risk_assessed →<br/>按收盘价把批准权重换算成 T+1 单日订单(order_created)
    K-->>S: IntentResult(state 含 pending_orders)
    S->>R: append_session(run_id, 全部事件, state, valuation) — 哈希链落库
```

要点:订单一律是**次日单日有效**(`execution_date == expiry_date == next_session`),以当日收盘价定量、次日开盘价撮合;卖单排在买单前执行以先释放现金;买入量用二分搜索找到"金额+费用 ≤ 现金"的最大合规申报数量;申报数量必须满足 `buy_lot`/`quantity_step` 规则,碎股仅允许"清仓全卖"(`odd_lot_sell_all`)。

### 账本哈希链与可复现性

- `append_session` 为每条事件计算 `event_hash = stable_digest({run_id, sequence, 事件内容, previous_hash})`;链头锚定在 `initial_state_hash`。
- `complete_run` 计算 `result_hash = stable_digest({binding_hash, event_chain_head, final_state_hash})`——一轮运行的最终指纹同时绑定了"用什么配置跑"和"逐条发生了什么"。
- `verify_run` 从链头重放全链校验每个哈希与序号连续性;`SimulationFeedback.event_chain_head` 就是校验通过后的链头,反馈本身也有 `feedback_hash`。
- 幂等:`begin_run` 发现相同 `binding_hash` 已有 complete 运行时直接复用(`SimulationOutcome.reused=True`),不会重跑;`run_daily` 对"账户头已在目标日"的重复调用同样校验绑定后复用,绑定不同则报错要求显式从更早父运行重放。
- 账户推进是**父子链**:`RunBinding.parent_run_id` 指向上一轮,`complete_run(promote=True)` 在同一事务里检查账户头未被并发移动后才推进 `selected_run_id`。

## 对外接口

- **CLI**(`fundlab/cli.py`):`fundlab account create/show` 管理账户;`fundlab simulate --account-id ... --weight INSTRUMENT=WEIGHT (--date | --start-date/--end-date)` 用 `StaticAllocationSource` 驱动共享内核,结束后打印 `build_simulation_feedback` 结果;`--allow-untrusted-fees` 才允许未验证费用表(仍会标记 incomplete)。
- **每日管线**(`fundlab/pipeline/daily.py`):`_advance_accounts` 为配置的每个模拟账户构建 `SimulationService`,从账户头逐日 `run_daily` 推进到快照数据头(`published_end`);`_intent_source` 按账户策略选择 `StaticAllocationSource`(paper-1)或 `FileIntentSource`(paper-agent),后者从 `settings.daily.agent_decision_root` 读取决策文件。单账户异常不放大为管线崩溃,而是记为 `blocked` 并写入日报。
- **Web 控制台**(`fundlab/web/service.py`):只读消费 `TradingRepository` 与 `build_simulation_feedback` 展示账户头与运行反馈。
- **外部 Agent**:不 import 任何代码,只在 `<decision_root>/<account_id>/<YYYY-MM-DD>.json` 落一个 JSON 文件即可参与当日决策。字段:`account_id`、`decision_date`(必须与路径一致)、`target_weights`(非空、非负、字符串数值)、`reason`(必填)、`agent_id`(可选,默认 `agent-file`)。语义:**无文件 = 持有**(一等结果,不是错误);**文件存在但无效 = 抛 `AgentDecisionError` 使运行失败**(沉默与损坏在证据里必须可区分);决策内容哈希被绑入 `FileIntentSource.config_hash`,进而进入 `RunBinding`——重放同一天换了决策内容会得到不同绑定,不可能悄悄执行另一份决策。

## 不变量与约束

- **失败即阻断**:未知涨跌停状态(`PriceLimitState.UNKNOWN`)直接抛错;费用规则缺失或歧义抛错;卖出超过已结算持仓、买入超过现金抛错;无法精确建模的事实降级为 `incomplete_reasons` 而非猜测数字。
- **不可变性**:所有领域对象是 frozen dataclass,payload 经 `deep_freeze`;SQLite 触发器保证账本事件、检查点、终局运行不可 UPDATE/DELETE,事件只能写入 `running` 状态的运行。
- **哈希绑定无处不在**:意图绑定快照与策略配置;订单/成交/持仓批次 ID 由内容摘要派生(同输入同 ID);`SimulationService._validate_intent` 强制意图的账户、日期、快照、策略三元组与运行绑定一致。
- **幂等**:同一 `binding_hash` 的完成运行被复用;重试产生新的 `attempt`,`(binding_hash, attempt)` 唯一。
- **保守撮合**:涨停封板不买、跌停封板不卖;成交量参与率上限(默认 5%)+ 二次方冲击滑点;买价向上取整到 tick、卖价向下取整;T+N 可卖约束按逐笔批次的 `sellable_on` 执行。
- **公司行动守恒**:拆股/送股全程保持总成本不变(逐批次分摊、末批吃余数);配股执行显式"不参与"策略(`subscribe_rights=True` 直接拒绝构造);股息税不建模但显式标记。
- **金额纪律**:一切金额过 `money()` 量化到分,NAV 量化到 1e-8,禁止浮点直接入账。

## 测试对应(tests/canonical)

- `test_trading_kernel.py`(10 例):历史/每日双时钟经济等价、次日开盘撮合与真实费用、科创板 200 股最小申报与碎股清仓、T+1 与涨跌停保守性、现金分红与税未建模标记、拆股/缩股成本守恒、同日因子确认拆股原子应用、账本与终局运行不可变、回撤跨父运行连续。
- `test_agent_file_source.py`(6 例):无文件=持有、落文件即执行且内容哈希入绑定、决策内容改变 `config_hash`、畸形/错配文件失败即阻断、账户钉扎拒绝外账户、Decimal 权重规范化。
- `test_runtime_surface.py`:`fundlab.trading` 公开面只含规范契约、strategies 包只暴露意图源、已删除的旧模块保持不可导入。
- `test_daily_pipeline.py`:管线侧覆盖账户推进、`TradingRepository` 集成与阻断上报。