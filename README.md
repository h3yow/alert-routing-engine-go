# 告警路由引擎（本地，无外部服务）

一个纯本地运行的告警路由引擎：接收不同来源的告警「触发 / 恢复」事件，按标签分组、去重，
依据规则做抑制与静默，再把通知并发投递到本地内存通知通道。整个过程不依赖任何真实外部服务，
时间行为通过可注入时钟确定性复现，测试不使用 `sleep`。

## 快速开始

```go
clk := alertrouter.RealClock() // 生产用真实时钟；测试用 newFakeClock(...)
eng := alertrouter.NewEngine(
    alertrouter.Config{GroupBy: []string{"service", "severity"}},
    []alertrouter.Notifier{myLocalChannel},
    clk,
)
defer eng.Close()

dec := eng.Submit(alertrouter.AlertEvent{
    Fingerprint: "node1.disk",                 // 同一条告警的稳定身份
    Labels:      alertrouter.LabelSet{"service": "db", "severity": "page"},
    Status:      alertrouter.EventFiring,
})
fmt.Println(dec.Reason) // notified / duplicate / suppressed / silenced / ...
```

`Notifier` 是一个接口，生产环境可接入任意本地实现（写日志、写本地队列、写屏幕等）；
测试中提供了记录投递、可注入失败的内存假通道。

## 1. 分组、去重与状态流转语义

### 身份与分组
- **身份（Fingerprint）**：每条告警的稳定唯一标识，由上游对完整标签集哈希得到。
  引擎以指纹作为去重与状态跟踪的键，同一条告警的重复事件指纹相同。
- **分组键（GroupKey）**：按配置的 `GroupBy` 标签（有序）拼接得到。
  分组只决定通知如何聚合与串行投递，不影响单条告警的状态判定。

### 单条告警状态机
引擎为每个指纹维护一个状态，事件进入后得到**互斥**的结论：

| 当前状态 | 到达事件 | 新状态 | 结论（Decision.Reason） |
| --- | --- | --- | --- |
| 不存在 | Firing | firing | `notified`（经规则判定后投递） |
| 不存在 | Resolved | —（不落库） | `orphan_resolve`（孤立恢复） |
| firing | Firing | firing | `duplicate`（重复触发，去重不通知） |
| firing | Resolved | resolved | `resolved`（正常恢复，对应上了触发） |
| resolved | Resolved | resolved | `out_of_order`（乱序/过期重复恢复，不通知） |
| resolved | Firing | firing | `notified`（新一轮事故，重新通知） |

三种"不通知"结论彼此不同、可区分，互不吞掉：

- **孤立恢复 `orphan_resolve`**：此前从未见过触发。不创建状态，因此之后真正的触发仍按新事故处理。
- **乱序恢复 `out_of_order`**：已经恢复过，又来一条恢复（过期/重放）。状态保持 resolved。
- **重复触发 `duplicate`**：触发期间重复到达，去重丢弃。

> 状态机在进入投递之前就已同步落定，因此后续投递成败、重试都不会改变告警状态。

## 2. 抑制与静默

两者都**只抑制通知，不影响状态跟踪**（被抑制/静默的告警，其恢复仍能正确对应状态）。

- **静默（Silence）**：规则 `Match` 的所有标签都与告警相等即命中。空匹配器不命中任何告警。
- **抑制（Inhibit）**：当存在一条**其他**触发中的告警命中 `Source`，目标告警命中 `Target`，
  且二者在 `Equal` 列出的标签上取值一致时，目标被抑制。告警不能抑制自身。

**优先级：静默 > 抑制。** 一条告警同时命中两者时，结论记为 `silenced`（并给出命中的规则名），
不会再报 `suppressed`。判定时取的是"当前触发中告警"的快照。

## 3. 规则热重载的兼容规则

`ReloadRules` 整体原子替换规则集（快照语义）：

1. 进行中的 `Submit` 使用进入时取到的规则快照，结论不被重载改变；
2. 已在状态机中的告警**不丢弃**，已排队/在途的投递**不取消、不补发**，不会因重载产生重复通知；
3. 新规则只对重载之后的新判定生效（包括已有告警之后的重复/恢复事件）；
4. 可在任意时刻反复加载、清空规则。

## 4. 并发与投递

- 多个生产者可并发 `Submit`；状态机、规则快照、投递队列均并发安全。
- 每个**分组**有一个串行 worker：同组通知严格串行，**同分组绝不会并发重复投递**；
  不同分组、不同通知通道之间并发。
- 同组、同状态的并发通知会**聚合**：
  - 已有**排队中**的同状态通知时，新告警按指纹并入该批（相同指纹不重复）；
  - 同状态通知**正在投递**时，新告警挂起为下一批，投递结束后串行送出——不丢任何告警。
  这种"主动聚合"的同步结论是 `delivery_coalesced`。
- 每个通道独立投递、独立重试，互不影响；重试采用指数退避
  `backoff = BaseBackoff × 2^(attempt-1)`，达到 `MaxAttempts` 后判定失败。

### 丢弃原因可区分

| 原因 | 含义 | 在哪里得到 |
| --- | --- | --- |
| `duplicate` | 同指纹重复触发，状态层去重 | `Submit` 同步返回 |
| `delivery_coalesced` | 主动聚合进同组同状态的投递（非错误） | `Submit` 同步返回 |
| `suppressed` / `silenced` | 命中规则不通知 | `Submit` 同步返回 |
| `orphan_resolve` / `out_of_order` | 孤立恢复 / 乱序恢复 | `Submit` 同步返回 |
| **投递失败** | 重试耗尽仍失败（`Delivered=false`，带 error 与尝试次数） | `DrainResults()` 异步收集 |

投递失败（`DeliveryResult.Delivered == false`）与主动聚合丢弃（`delivery_coalesced`）来源不同、互不混淆；
重试过程不触碰状态机。`Close()` 会等待所有在途投递（含重试）排空后再返回，不丢在途告警。

## 5. 确定性时间

- `Clock` 接口注入时间：生产用真实时钟，测试用 `fakeClock`。
- 假时钟只在调用 `Add(d)` 时前进，并在到期点**同步**触发到期定时器；
  截止时刻已过才登记的定时器立即触发，退避不会丢失。
- 测试用 `waitSettled()` 等待投递全部结束，用 `clk.waitPending(n)` 精确等待第 n 个退避定时器登记后再推进时间，
  因此重试节奏可逐拍确定性复现，**全程没有 `sleep`**。

## 6. 本地验证

```bash
# 全部单测
go test ./...

# 并发场景用竞态检测器跑（多轮，确认不 flaky）
go test -race -count=10 ./...

# 查看带输入与判定依据的详细日志
go test -race -v ./...
```

测试覆盖：重复触发去重、正常/孤立/乱序恢复三种互斥结论、抑制、静默及二者优先级、
运行中规则重载（在途不丢、状态不丢、无重复通知）、投递失败按退避重试后成功 / 重试耗尽丢弃、
主动聚合丢弃与投递失败原因区分、多生产者 × 多通道并发（含同分组无并发重叠投递的检测）、
关闭排空在途投递。每条用例日志都会打印输入事件与判定依据（reason、分组、命中规则）。

## 代码结构

| 文件 | 职责 |
| --- | --- |
| `alert.go` | 事件/标签类型，分组键计算 |
| `state.go` | 按指纹的状态机与去重、互斥结论 |
| `rules.go` | 抑制/静默规则匹配与优先级 |
| `dispatcher.go` | 分组串行、聚合去重、多通道并发、退避重试 |
| `engine.go` | 串联状态机→规则→投递，规则原子热重载 |
| `clock.go` | 真实时钟与确定性假时钟 |
