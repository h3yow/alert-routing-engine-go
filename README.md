# Alert Routing Engine (本地版)

一套完全在本地进程内运行的告警路由引擎：接收来自不同来源的**触发（firing）/恢复（resolved）**事件，
按指纹与标签分组、去重，套用静默/抑制规则，再投递到内存通知通道。**不依赖任何外部服务**。

## 快速验证

```bash
go build ./...
go vet ./...
go test ./... -race -count=1      # 并发场景用 -race 跑通
go run ./cmd/demo                 # 端到端演示（含重复/孤立恢复/乱序/静默/抑制/失败重试）
```

测试中的时间行为全部由 `clock.FakeClock` 驱动：只有显式 `Advance` 才会推进时间、触发退避定时器，
**不靠 `time.Sleep` 等待被测行为**；`runtime.Gosched()` 只用于让出 CPU、放大并发窗口。

## 包结构

| 包       | 职责                                                                 |
|----------|----------------------------------------------------------------------|
| `alert`  | 事件模型、事件种类、**判定结论 `Outcome`**、确定性分组键 `GroupKey`   |
| `clock`  | 可注入时钟：`RealClock` 与手动推进的 `FakeClock`（退避定时器确定性） |
| `rule`   | 静默规则、抑制规则及**不可变快照 `*Matchers`**                        |
| `state`  | 指纹 -> 状态表；所有变更在写锁内原子完成                             |
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

## 状态流转与事件结论（互不相同，不互相吞掉）

```
            firing(新)                 firing(重复)
 (不存在) ───────────────▶ pending ───────────────▶ pending（duplicate）
                              │  ▲                    │
              resolved(匹配)  │  │ firing(已恢复后到达)│ resolved
                              ▼  └──── out_of_order ◀─┤
                           resolved ◀─────────────────┘
                              ▲
            resolved(无触发) ─┘  orphan_resolve（记录为 resolved）
```

| 输入情形 | 结论 `Outcome` | 通知 | 状态处理 |
|---|---|---|---|
| 新 firing，无规则命中 | `notified` | firing | 建 pending，闩锁置位 |
| 重复 firing（已通知、pending） | `duplicate` | 无 | 仅刷新观测字段 |
| firing 到达时已是 resolved | `out_of_order` | 无 | **不复活、不改载荷** |
| resolved 先于任何 firing | `orphan_resolve` | 无 | 记为 resolved（后续重复恢复=duplicate，后续 firing=out_of_order，三类结论互不相同） |
| resolved 匹配 pending 且曾通知 | `resolved_notification` | resolution | pending→resolved |
| resolved 匹配 pending 但全程被静音（从未通知） | `resolved_no_notification` | 无 | pending→resolved |
| 已 resolved 后再来 resolved | `duplicate` | 无 | 仅刷新观测字段 |

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
- **已有告警**：状态（含 `Notified` 闩锁、pending/resolved）完全不随重载变化；
  因此“重载前已通知”的告警，重载后来重复 firing 仍是 `duplicate`，**不会冒出重复通知**。
- **后续事件**：使用新快照。例如新加静默后，新告警立即 `silenced`；删除静默后，
  之前一直被静音（闩锁未置位）的告警再来一次 firing 时会补发唯一一次通知。
- 静默按**事件时刻**匹配，规则窗口到期等价于重载删除：时间推进越过 `EndsAt` 后后续 firing 不再被静音。
- 通知通道也可 `ReplaceChannels`：已在途的重试若发现通道被移除，按 `channel_gone` 收尾。
- 在途投递本身不会因规则重载而被取消或重发。

## 并发、投递顺序与重试

- 多个生产者可并发 `Ingest`：状态判定在状态表写锁内原子完成，重复 firing 全局只会产生一次通知。
- 分发器对每个 `(通道名, 分组键)` 使用独立互斥锁，**同一分组不会出现并发投递**；不同分组、不同通道可并发。
- `Dispatch` 默认在调用方 goroutine 内同步完成各通道投递（按通道名排序），使“投递结论”与
  “判定时刻状态”一致；需要异步背压不阻塞摄入时可用 `DispatchAsync` + `Idle()` 等待结算。
- 失败重试：指数退避 `BaseBackoff * 2^(attempt-1)`（上限 `MaxBackoff`），时钟可注入；
  重试只重复“发送”动作，**不触碰状态机、不产生额外判定**。
- 每次尝试前都会复查通道集合与一个 `revalidate` 条件（当前状态是否仍与该通知相符）。

### 投递失败 vs 主动丢弃：三类终态明确区分

`notify.Result.DropReason`：

| 终态 | 含义 | 与其他的区别 |
|---|---|---|
| `retry_exhausted` | 达到 `MaxAttempts` 仍发送失败（携带最后一次错误、实际尝试次数） | 传输层失败 |
| `stale` | 尝试前复查发现状态已变化（如告警已恢复，firing 通知不再适用） | 业务上作废，**0 次或未耗尽尝试**，无传输错误含义 |
| `channel_gone` | 引擎停止，或重试期间通道被 `ReplaceChannels` 移除 | 配置变更导致 |

## 可观测性

`engine.Recorder` 记录每个输入事件的 `Decision`（输入、结论、命中规则 ID、文字理由、产生的通知）
和每个通知的终态 `Result`。单测通过 `t.Logf` 打印**输入事件 + 判定依据（命中哪条规则/为何去重）**。
