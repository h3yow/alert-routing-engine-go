# Alert Routing Engine (本地版)

一套完全在本地进程内运行的告警路由引擎：接收来自不同来源的**触发（firing）/恢复（resolved）**事件，
按指纹与标签分组、去重，套用静默/抑制规则，再投递到内存通知通道。**不依赖任何外部服务**。

## 快速验证

```bash
go build ./...
go vet ./...
go test ./... -race -count=1      # 并发场景用 -race 跑通
go run ./cmd/demo                 # 端到端演示（含多轮抖动/重复/孤立恢复/乱序/静默/抑制/失败重试）
```

测试中的时间行为全部由 `clock.FakeClock` 驱动：只有显式 `Advance` 才会推进时间、触发退避定时器，
**不靠 `time.Sleep` 等待被测行为**；`runtime.Gosched()` 只用于让出 CPU、放大并发窗口。

## 包结构

| 包       | 职责                                                                 |
|----------|----------------------------------------------------------------------|
| `alert`  | 事件模型、事件种类、**判定结论 `Outcome`**、确定性分组键 `GroupKey`   |
| `clock`  | 可注入时钟：`RealClock` 与手动推进的 `FakeClock`（退避定时器确定性） |
| `rule`   | 静默规则、抑制规则及**不可变快照 `*Matchers`**                        |
| `state`  | 指纹 -> 状态表；**多轮故障 `Round` 历史与每轮通知闩锁**；变更在写锁内原子完成 |
| `notify` | 本地通道接口、按 `(通道, 分组)` 串行的分发器、重试退避、丢弃原因      |
| `engine` | 摄入、状态流转、规则套用、投递编排；`Recorder` 记录判定与投递结果    |
| `cmd/demo` | 本地端到端示例                                                      |

## 分组与去重语义

- **指纹 `Fingerprint`** 标识同一条告警条件，触发与恢复共用同一指纹（由来源方保证）。
  去重、状态机都以指纹为键；与路由分组键是两个概念。
- **分组键 `GroupKey(labels, groupBy)`**：只取 `groupBy` 中声明的标签，按标签名排序后拼接，
  结果与 map 遍历顺序、与未列入标签均无关；`groupBy` 为空时所有告警落入同一个全局分组。
  分组键只用于“同一分组内投递串行化”。
- **去重闩锁 `Entry.Notified`**：一条 pending 告警只发出一次 firing 通知。重复 firing：
  - 已通知且仍 pending → `duplicate`，刷新 `LastSeen`/标签，**绝不二次通知**；
  - 之前一直被静默/抑制（闩锁未置位）、本次仍命中规则 → 继续 `silenced`/`suppressed`；
  - 之前被静默/抑制、本次规则已不再命中 → 此刻补发**唯一一次** firing 通知（闩锁置位）。

## 多轮故障（flapping / 抖动反复越界）

- **一轮（round）**：同一条告警（同一指纹）从一次 firing 到与之匹配的 resolved 为一轮。
  恢复之后再次到达的 firing **开启新一轮**，而不是被当成旧告警的重复或乱序。
- 状态 `state.Entry` 里保留：
  - `RoundNo`：当前轮号（pending 时是开放轮；resolved 时是刚关闭的轮；只有孤立恢复时为 0）；
  - `Rounds []Round`：**从第 1 轮起的完整历史**，每轮独立保存 `FiredAt/ResolvedAt`、
    本轮载荷，以及两个独立闩锁 `FiringNotified`/`ResolveNotified`；**历史轮次永不被新一轮覆盖**。
  - 扁平字段 `Notified/Silenced/...` 是“当前轮”的镜像，便于像旧版本一样直接观察。
- **每轮独立通知**：第 N 轮各通道恰好收到一次 firing 通知；该轮恢复时再各收到一次 resolution
  通知。新一轮不会因为上一轮已通知而被判 `duplicate`，不同轮次的通知也不会互相顶替。
  通知的 `Round` 字段与判定的 `Decision.Round` 标明它属于哪一轮（孤立恢复等无轮次事件为 0）。
- **新一轮按它到达时的规则/状态重新判定**：静默、抑制、闩锁都不跨轮继承。上一轮在途的投递不受
  规则重载追溯改判；新一轮此刻是否静默/抑制/通知，完全由它自己进入时抓到的快照决定。
- 一整轮都被静音（闩锁从未置位）后恢复：结论仍是 `resolved_no_notification`，该轮不发恢复通知；
  下一轮仍是干净的新轮次，照常可以通知。

## 状态流转与事件结论（互不相同，不互相吞掉）

```
 (不存在)
   │ firing(新)                 ┌──────── 新一轮 firing（上一轮已正常关闭）
   ▼                            │  开启 round N+1：独立闩锁、独立通知
 pending ──resolved(匹配)──▶ resolved(round N 关闭)
   │  ▲ firing(同轮重复)          │
   │  └──── duplicate ◀──────────┤ firing(孤立恢复之后到达)
   │                             └──── out_of_order（不开轮、不通知、不改载荷）
   ▼
 resolved ◀──── resolved(无触发)：orphan_resolve（OrphanResolved=true，无轮次）
   ▲
   └ resolved(已 resolved 再来)：duplicate（无论该 resolved 来自孤立还是已关闭轮次）
```

| 输入情形 | 结论 `Outcome` | 轮次 | 通知 | 状态处理 |
|---|---|---|---|---|
| 新 firing，无规则命中 | `notified` | 开轮 | firing | 建 pending 轮，本轮闩锁置位 |
| **已正常关闭的轮次之后再来 firing（抖动再触发）** | `notified`/`silenced`/`suppressed` | **开新一轮** | 按新一轮判定 | 新轮追加到 `Rounds`，历史保留 |
| 同一开放轮内重复 firing（已通知、pending） | `duplicate` | 当前轮 | 无 | 仅刷新观测字段 |
| firing 到达在**孤立恢复**之后 | `out_of_order` | 无（0） | 无 | **不复活、不开轮、不改载荷** |
| resolved 先于任何 firing | `orphan_resolve` | 无（0） | 无 | 记为 resolved（`OrphanResolved`），重复恢复=duplicate，后续 firing=out_of_order |
| resolved 匹配开放轮且该轮曾通知 | `resolved_notification` | 当前轮 | resolution | 关闭该轮，记 `ResolvedAt` |
| resolved 匹配开放轮但该轮全程被静音 | `resolved_no_notification` | 当前轮 | 无 | 关闭该轮，不发恢复通知 |
| 已 resolved 后再来 resolved | `duplicate` | 最近轮（或 0） | 无 | 仅刷新观测字段 |

被**静默或抑制**的告警不发任何通知，但在状态表中正常跟踪（`Silenced/SilenceBy`、
`Suppressed/SuppressBy` 可观测），恢复、重复、乱序的结论照常产生。

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
- **在途事件**：从进入到完成始终用同一快照，结论不会在处理中途翻转。
- **已有轮次**：状态（含每轮 `FiringNotified/ResolveNotified` 闩锁、开放/关闭）完全不随重载变化；
  因此“重载前已通知”的开放轮，重载后来重复 firing 仍是 `duplicate`，**不会冒出重复通知**。
- **新一轮**：是否通知按它自己进入时的规则与状态判断。例如第 1 轮正常通知后恢复，重载新增静默，
  则第 2 轮到达时 `silenced`（其恢复为 `resolved_no_notification`）；删除静默后第 3 轮又会正常通知。
  第 1 轮已在途的投递不会被追溯改判，也不会因此丢失或重复。
- 静默按**事件时刻**匹配，规则窗口到期等价于重载删除：时间推进越过 `EndsAt` 后后续 firing 不再被静音。
- 通知通道也可 `ReplaceChannels`：已在途的重试若发现通道被移除，按 `channel_gone` 收尾。
- 在途投递本身不会因规则重载而被取消或重发。

## 并发、投递顺序与重试

- 多个生产者可并发 `Ingest`：状态判定在状态表写锁内原子完成。
- **按指纹串行（多轮顺序保证）**：同一指纹的事件由独立的 per-key 互斥锁串起“状态流转 + 各通道投递”
  整段过程。因此即使多个来源并发推同一指纹的 firing/resolved/再触发，通道看到的顺序也严格等于状态
  演进顺序——每轮 firing 在该轮 resolved 之前，下一轮 firing 不会排在上一轮 resolved 之前。
- **每通道对每轮恰好一次 firing + 一次 resolution**；同一开放轮内并发的重复 firing/resolved 只有一个
  产生通知，其余得到 `duplicate`，不会多投。
- 分发器对每个 `(通道名, 分组键)` 使用独立互斥锁，**同一分组在单个通道内不会出现并发或重复投递**；
  不同分组、不同通道可并发，某个分组投递慢（退避重试）不会堵死其他分组。
- per-fingerprint 锁只包裹该指纹，不同指纹之间不互斥，因此慢分组只影响落在同一 `(通道,分组)` 的
  投递（这是顺序保证的必要代价），不会拖垮无关告警。
- `Dispatch` 默认在调用方 goroutine 内同步完成各通道投递（按通道名排序），使“投递结论”与
  “判定时刻状态”一致；需要异步背压不阻塞摄入时可用 `DispatchAsync` + `Idle()` 等待结算。
- 失败重试：指数退避 `BaseBackoff * 2^(attempt-1)`（上限 `MaxBackoff`），时钟可注入；
  重试只重复“发送”动作，**不触碰状态机、不产生额外判定、不额外开/关轮次**。
- 每次尝试前都会复查通道集合与一个**按轮次**的 `revalidate` 条件：firing 通知只在所属轮仍开放时有效，
  resolution 通知只在所属轮已关闭时有效；进入下一轮不会把上一轮的通知误判为 `stale`。

### 投递失败 vs 主动丢弃：三类终态明确区分

`notify.Result.DropReason`：

| 终态 | 含义 | 与其他的区别 |
|---|---|---|
| `retry_exhausted` | 达到 `MaxAttempts` 仍发送失败（携带最后一次错误、实际尝试次数） | 传输层失败 |
| `stale` | 尝试前按轮次复查发现该通知已不再适用（firing 所属轮已关闭，或 resolution 所属轮已重开） | 业务上作废，**0 次或未耗尽尝试**，无传输错误含义 |
| `channel_gone` | 引擎停止，或重试期间通道被 `ReplaceChannels` 移除 | 配置变更导致 |

## 可观测性与本地验证

`engine.Recorder` 记录每个输入事件的 `Decision`（输入、**所属轮次 `Round`**、结论、命中规则 ID、
文字理由、产生的通知）和每个通知的终态 `Result`。单测通过 `t.Logf` 打印
**输入事件 + 轮次 + 判定依据（命中哪条规则/为何去重/开了第几轮）**。

多轮与并发行为的本地验证：

```bash
# 多轮顺序、孤立/乱序兼容、重载下新一轮重判、失败重试不重复
go test ./engine/ -race -count=1 -v -run 'TestMultiRound|TestOrphan|TestRound2|TestReload|TestDelivery'
# 多生产者并发 firing/resolved、多通道投递计数与顺序、组内无并发投递
go test ./engine/ -race -count=1 -v -run 'TestConcurrent'
# 轮次历史累积/不被覆盖/克隆隔离
go test ./state/  -race -count=1 -v -run 'TestRounds'
# 全部（务必带 -race；FakeClock 驱动时间，无 sleep）
go test ./... -race -count=1
# 端到端演示：抖动再触发在两个通道各重发 firing + resolution
go run ./cmd/demo
```

关键断言口径（可对照 `engine/rounds_test.go`）：

- `TestMultiRoundReopensAfterRecovery`：3 轮，每轮 firing/resolution 各投递一次，
  `Entry.Rounds` 完整保留三轮且各自两个闩锁都置位、时间顺序正确；
- `TestOrphanAndOutOfOrderSurviveMultiRound`：孤立恢复→重复恢复→乱序 firing，三类结论仍互不相同，
  且都不开轮、不通知；
- `TestRound2EvaluatedAgainstReloadedRules`：重载新增静默只影响新一轮，上一轮在途投递不丢不重；
- `TestConcurrentMultiRoundMultiChannel`：12 生产者 × 10 轮（firing/resolved 分两阶段屏障并发），
  每个通道对每轮恰好 1 firing + 1 resolution，顺序为 `r1.fire<r1.resolve<r2.fire...`，组内峰值并发 ≤ 1；
- `TestConcurrentDistinctGroupsStayParallel`：两个分组各自 25 轮，互不阻塞，计数精确。
