# Alert Routing Engine (本地版)

一套完全在本地进程内运行的告警路由引擎：接收来自不同来源的**触发（firing）/恢复（resolved）**事件，
按指纹与标签分组、去重，套用静默/抑制规则，再投递到内存通知通道。**不依赖任何外部服务**。

## 快速验证

```bash
go build ./...
go vet ./...
go test ./... -race -count=1      # 并发场景用 -race 跑通
go run ./cmd/demo                 # 端到端演示（含重复/孤立恢复/乱序/静默/抑制/失败重试/多轮故障）
```

测试中的时间行为全部由 `clock.FakeClock` 驱动：只有显式 `Advance` 才会推进时间、触发退避定时器，
**不靠 `time.Sleep` 等待被测行为**；`runtime.Gosched()` 只用于让出 CPU、放大并发窗口。

### 多轮故障 / 多通道并发看哪些用例

| 用例 | 验证点 |
|---|---|
| `state.TestRoundHistoryPreservedAcrossRounds` / `TestRoundHistoryIsolatedInClone` | 轮次冻结进历史、轮号/闩锁/载荷互不覆盖、快照隔离 |
| `engine.TestMultiRoundFaultNotifiesEachRoundAndKeepsHistory` | 恢复后再触发开新轮；每轮 firing/recovery 各通知一次；轮内重复仍是 `duplicate`；通道顺序 `f1,r1,f2,r2,…` |
| `engine.TestNewRoundRejudgedAgainstCurrentRules` | 新一轮按到达时规则重判（上一轮通知不顶新一轮；重载不追溯；静音轮恢复无通知；规则删除后再开轮再通知） |
| `engine.TestConcurrentFlappingAcrossProducersAndChannels` | 多生产者并发推触发/恢复，两个本地通道各自对每轮**正好**一次 firing、一次 recovery，且顺序与状态演进一致 |
| `engine.TestSlowGroupDoesNotBlockOtherGroups` | 同一通道上一个分组卡在投递中，另一个分组不被堵死 |
| 原有 `TestDuplicate…` / `TestOrphanResolve…` / `…OutOfOrder…` / `TestReload…` / `TestDeliveryFailure…` / `TestConcurrentProducersNoDuplicates` | 重复、孤立恢复、乱序、规则重载、失败重试不回归 |

建议并发用例用 `go test ./engine/ -race -count=10` 反复跑；日志中每个输入事件都会打印
`round=<轮号> outcome=<结论> rule=<规则ID> reason=<判定依据> messages=<通知数>`，可直接对照状态演进。

## 包结构

| 包       | 职责                                                                 |
|----------|----------------------------------------------------------------------|
| `alert`  | 事件模型、事件种类、**判定结论 `Outcome`**、确定性分组键 `GroupKey`   |
| `clock`  | 可注入时钟：`RealClock` 与手动推进的 `FakeClock`（退避定时器确定性） |
| `rule`   | 静默规则、抑制规则及**不可变快照 `*Matchers`**                        |
| `state`  | 指纹 -> 状态表；**按轮次（round）跟踪**，所有变更在写锁内原子完成    |
| `notify` | 本地通道接口、按 `(通道, 分组)` 串行的分发器、重试退避、丢弃原因      |
| `engine` | 摄入、状态流转、规则套用、投递编排；`Recorder` 记录判定与投递结果    |
| `cmd/demo` | 本地端到端示例                                                      |

## 分组与去重语义

- **指纹 `Fingerprint`** 标识同一条告警条件，触发与恢复共用同一指纹（由来源方保证）。
  去重、状态机都以指纹为键；与路由分组键是两个概念。
- **分组键 `GroupKey(labels, groupBy)`**：只取 `groupBy` 中声明的标签，按标签名排序后拼接，
  结果与 map 遍历顺序、与未列入标签均无关；`groupBy` 为空时所有告警落入同一个全局分组。
  分组键只用于“同一分组内投递串行化”。
- **轮内去重闩锁 `Entry.Notified`**：去重范围是**当前轮**。一轮 pending 期间只发一次 firing：
  - 已通知且仍 pending → `duplicate`，刷新 `LastSeen`/标签，**绝不二次通知**；
  - 之前一直被静默/抑制（闩锁未置位）、本次仍命中规则 → 继续 `silenced`/`suppressed`；
  - 之前被静默/抑制、本次规则已不再命中 → 此刻补发该轮**唯一一次** firing 通知（闩锁置位）。
  新一轮开启时闩锁重置，旧闩锁只冻结在 `History` 中，不会让新轮被当成重复（详见“多轮故障”）。

## 状态流转与事件结论（互不相同，不互相吞掉）

一条指纹的生命周期由一到多个**故障轮次（round）**组成（见下一节）；下图描述单轮状态机：

```
            firing(新)                 firing(重复)
 (不存在) ───────────────▶ pending ───────────────▶ pending（duplicate）
                              │  ▲                    │
              resolved(匹配)  │  │                    │ resolved
                              ▼  │                    ▼
                           resolved ◀────────── 该轮关闭；再次 firing = 新一轮
                              ▲
            resolved(无触发) ─┘  orphan_resolve（仅记录，不开轮）
```

| 输入情形 | 结论 `Outcome` | 通知 | 状态处理 |
|---|---|---|---|
| 新 firing（第 1 轮），无规则命中 | `notified` | firing | 建 pending，第 1 轮，轮内闩锁置位 |
| 上一轮已 resolved 后再次 firing | `notified` | **新一轮的 firing** | 旧轮冻结进 `History`，开启下一轮，独立闩锁、按**本次到达时**的规则重判 |
| 同一轮内重复 firing（已通知、pending） | `duplicate` | 无 | 仅刷新观测字段 |
| firing 到达的是**孤立恢复**留下的 resolved 记录 | `out_of_order` | 无 | **不复活、不开轮、不改载荷**（与“正常恢复后的新一轮”严格区分） |
| resolved 先于任何 firing | `orphan_resolve` | 无 | 记为仅记录的 resolved（轮次 0、`Orphan`）；后续重复恢复=duplicate，后续 firing=out_of_order，三类结论互不相同 |
| resolved 匹配某轮 pending 且该轮曾通知 | `resolved_notification` | 该轮 resolution | 该轮 pending→resolved |
| resolved 匹配某轮 pending 但该轮全程被静音（从未通知） | `resolved_no_notification` | 无 | 该轮 pending→resolved |
| 已 resolved 后再来 resolved（无论孤立轮还是正常轮） | `duplicate` | 无 | 仅刷新观测字段，不开新轮 |

被**静默或抑制**的告警不发任何通知，但在状态表中正常跟踪（`Silenced/SilenceBy`、
`Suppressed/SuppressBy` 可观测），恢复、重复、乱序的结论照常产生。

## 多轮故障（flapping）

- **一轮（round）**：同一条指纹从一次 firing 到与之匹配的 resolved 为一轮，轮次从 1 开始。
- **多轮**：一轮 resolved 之后再次到来的 firing **不是**乱序、**不是**重复，而是开启下一轮：
  - 旧轮在新一轮开启的瞬间被**冻结**进 `Entry.History`（此后不再被修改，不被覆盖）；
  - 新轮拿到自己独立的触发/恢复通知闩锁，因此**每一轮都会重新通知一次 firing、resolved 时再通知一次 recovery**；
    上一轮通知过不会让新一轮被当成重复事件，多个轮次的通知互不顶替；
  - 新轮是否被静默/抑制，只看**该轮事件到达时**抓到的规则快照与当时的在途源告警状态，与上一轮结论无关。
- **状态可观测**：`state.Entry` 顶层字段始终是“当前/最新一轮”，`Entry.Round` 是当前轮号；
  `Entry.AllRounds()` 按轮次顺序返回历史轮 + 最新轮，`Entry.RoundByNumber(n)` 取指定轮。
  每个 `state.Round` 带 `FiringNotified`/`ResolveNotified` 两个轮内通知标记、状态、时间和载荷。
- **通知带轮号**：`notify.Notification.Round` 标识该通知属于哪一轮，通知 ID 含 `r<轮号>`，
  投递侧的有效性复查也按“通知自己的那一轮”进行——新一轮的状态变化不会把上一轮的 recovery 判成 stale。

### 与孤立恢复 / 乱序 / 重复的兼容边界

| 情形 | 结论 | 是否开新一轮 |
|---|---|---|
| 正常恢复后再来 firing（抖动、指标反复越界） | `notified`（新轮） | 是，轮号 +1 |
| 从来没触发过、先收到 resolved，之后再来 firing | 先 `orphan_resolve`，后 `out_of_order` | **否**：孤立恢复不代表真实故障轮，永远不能“触发新一轮” |
| resolved 之后重复来的 resolved | `duplicate` | 否 |
| 同一轮 pending 期间重复 firing | `duplicate` | 否（轮内去重） |
| 一轮全程被静音后 resolved，再 firing | 先 `resolved_no_notification`，后按新轮重判（可能 `notified`/`silenced`/`suppressed`） | 是，且新轮通知与旧轮无关 |

即：只有“**真实发生过、已正常关闭的一轮**”之后的 firing 才开新轮；孤立恢复、重复事件的既有语义
完全保留，不会被“新一轮”逻辑吞掉或改变含义。

## 静默与抑制：优先级与判定顺序

每个事件在**进入引擎时抓取一份规则快照**，对该事件的整个判定只使用这一份快照。判定顺序：

1. **静默（Silence）优先**：标签包含规则 `Equal` 全部键值，且事件时刻落在半开区间
   `[StartsAt, EndsAt)` 内 → `silenced`，记录规则 ID，不再评估抑制。
2. **抑制（Inhibit）**：目标标签包含 `TargetEqual`，且存在**另一条**处于 pending 的源告警
   （源标签包含 `SourceEqual`，且规则 `Equal` 中列出的标签在源/目标上取值相同；告警不会抑制自己）
   → `suppressed`。
3. 都不命中 → `notified`。

因此当一条告警同时满足静默与抑制时，结论稳定为 `silenced` 且携带静默规则 ID。

## 规则重载的兼容规则

- `Engine.Reload` 只原子替换一个不可变快照指针；旧快照仍被正在处理的事件持有，自然过期回收。
- **在途事件**：从进入到完成始终用同一快照，结论不会在处理中途翻转；在途投递（含退避重试）不会因重载被取消或重发。
- **已有轮次**：历史轮与当前轮的状态（含各轮 `FiringNotified`/`ResolveNotified` 闩锁、pending/resolved）
  完全不随重载变化；因此“重载前已通知”的**同一轮**重复 firing 仍是 `duplicate`，**不会冒出重复通知**。
- **后续轮次/事件**：使用新快照。新一轮在它自己的 firing 到达时重新走一遍静默/抑制判定：
  - 上一轮正常恢复、重载加了静默后再触发 → 新一轮 `silenced`（旧轮的通知不受影响、不追溯改判）；
  - 新一轮全程被静音后恢复 → `resolved_no_notification`；之后再删静默、再触发 → 又一轮 `notified`；
  - 静默按**事件时刻**匹配，规则窗口到期等价于重载删除：时间推进越过 `EndsAt` 后后续 firing 不再被静音。
- 通知通道也可 `ReplaceChannels`：已在途的重试若发现通道被移除，按 `channel_gone` 收尾；
  通道替换不丢在途告警（按 `channel_gone` 明确收尾），也不会冒出重复通知。

## 并发、投递顺序与重试

- 多个生产者可并发 `Ingest`：状态判定在状态表写锁内原子完成。
- **同一指纹**额外持有一把“指纹锁”，覆盖“判定 + 投递”整个区间。多个来源同时推同一条告警的
  触发/恢复/再触发时，同一指纹的事件被串行处理；配合分发器的 `(通道, 分组)` 锁，可保证每个通道上：
  - 同一分组不会出现并发投递，也不会重复投递（一轮严格一次 firing、一次 recovery）；
  - 顺序与状态演进一致：**第 N 轮 firing 在第 N 轮 recovery 之前，第 N+1 轮 firing 不会排在第 N 轮 recovery 之前**
    （通道上看到的序列恒为 `f1,r1,f2,r2,…`）。
- **不同指纹/不同分组不互相阻塞**：指纹锁按指纹独立、分组锁按 `(通道, 分组)` 独立；某个分组通道慢，
  只阻塞同一分组的投递，其他分组、其他通道照常并行（见 `TestSlowGroupDoesNotBlockOtherGroups`）。
- `Dispatch` 默认在调用方 goroutine 内同步完成各通道投递（按通道名排序），使“投递结论”与
  “判定时刻状态”一致；需要异步背压不阻塞摄入时可用 `DispatchAsync` + `Idle()` 等待结算。
- 失败重试：指数退避 `BaseBackoff * 2^(attempt-1)`（上限 `MaxBackoff`），时钟可注入；
  重试只重复“发送”动作，**不触碰状态机、不产生额外判定**。
- 每次尝试前都会复查通道集合与一个 `revalidate` 条件；该条件按**通知所属轮次**复查
  （firing 通知要求该轮仍 pending，recovery 通知要求该轮已 resolved；历史轮冻结在 `History` 中可查），
  因此相邻轮次的状态翻转互不误伤。

### 投递失败 vs 主动丢弃：三类终态明确区分

`notify.Result.DropReason`：

| 终态 | 含义 | 与其他的区别 |
|---|---|---|
| `retry_exhausted` | 达到 `MaxAttempts` 仍发送失败（携带最后一次错误、实际尝试次数） | 传输层失败 |
| `stale` | 尝试前复查发现状态已变化（如告警已恢复，firing 通知不再适用） | 业务上作废，**0 次或未耗尽尝试**，无传输错误含义 |
| `channel_gone` | 引擎停止，或重试期间通道被 `ReplaceChannels` 移除 | 配置变更导致 |

## 可观测性

`engine.Recorder` 记录每个输入事件的 `Decision`（输入、所属轮次 `Round`、结论、命中规则 ID、文字理由、
产生的通知）和每个通知的终态 `Result`（通知上带 `Round`）。单测通过 `t.Logf` 打印
**输入事件 + 轮次 + 判定依据（命中哪条规则/为何去重/为何开新轮）**；轮次历史可通过
`State()[fp].AllRounds()` / `RoundByNumber(n)` 直接断言。
