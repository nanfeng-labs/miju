# 示例

[miju](../README.zh-CN.md) · [English](README.md)

每个可运行示例均包含独立的 `Cargo.toml` 和 `src/main.rs`，展示所需 feature 和执行过程的注释。共享辅助库与这些示例放在同一目录下。

## 运行示例

在仓库根目录运行：

```sh
cargo run -p basic --locked
```

具体包名及必要的 `--bin` 参数见[运行与退出行为](#运行与退出行为)。部分示例会自动退出，其他示例需要按 **Ctrl+C** 关闭。

## 任务处理与上下文

| 示例 | 场景 |
| --- | --- |
| [`basic`](basic) | 使用速率限制、并发、重试、panic 捕获、任务 spawn 和 tracing 处理内存任务。 |
| [`deps-injection`](deps-injection) | 提取任务元数据，通过 `Data` 共享计数器，并通过 `FromRequest` 提供自定义参数。 |
| [`task-context`](task-context) | 通过克隆的 `TaskContext` 取消任务，等待执行结束通知，并观察 `Killed` 状态。 |
| [`worker-context`](worker-context) | 查询正在执行的任务，并通过共享的 `WorkerContext` 从外部观测 worker。 |
| [`advanced`](advanced) | 组合客户端注入中间件、共享 API 额度与缓存、后台维护、panic 捕获及重试。 |

## 执行与关闭

| 示例 | 场景 |
| --- | --- |
| [`catch-panic`](catch-panic) | 捕获邮件任务中主动触发的 panic，每项任务重试一次，并观测 worker 错误事件。 |
| [`graceful-shutdown`](graceful-shutdown) | 响应 Ctrl+C，完成当前批次，并在关闭前模拟保存任务状态。 |
| [`long-running`](long-running) | 通过 `TaskRunner` 并发执行数据导出、收集结果，并在关闭时等待执行中的任务完成。 |
| [`retries`](retries) | 对普通错误应用指数退避并最多重试三次，同时排除 `AbortError`。 |
| [`unique-jobs`](unique-jobs) | 使用相同幂等键提交两个不同载荷，仅执行第一个任务。 |
| [`unmonitored-worker`](unmonitored-worker) | 以 20 个任务的并发上限独立运行 worker，待有限批次全部完成后自动停止。 |
| [`monitor`](monitor) | 注册单个及五个带索引的 worker，汇集事件，并配置有次数上限的重启策略与关闭期限。 |
| [`monitor-context`](monitor-context) | 持续入队并运行可取消任务，同时从外部观察 worker 状态。 |

## 工作流

| 示例 | 场景 |
| --- | --- |
| [`workflow-stepped`](workflow-stepped) | 在三个步骤间传递不同类型的结果，配置后续步骤的延迟，并向各 handler 注入共享后端。 |
| [`workflow-graph`](workflow-graph) | 将入口参数分发给三个图节点，以类型化元组收集依赖结果，并输出 DOT 图。 |
| [`workflow-graph-dynamic`](workflow-graph-dynamic) | 生成 1,000 个依赖节点，校验 DAG，并通过节点句柄向量收集结果。 |

## 可观测性

| 示例 | 场景 |
| --- | --- |
| [`tracing`](tracing) | 对比后端与 worker 的本地 span，以及通过任务元数据恢复的 producer trace。 |

## 共享示例支持

[`email-service`](email-service) 是邮件示例共用的库，不是可直接运行的二进制程序。它提供可序列化的载荷、三种地址校验结果、示例邮件、应用错误及 HTML 提交表单。投递为模拟操作，不会发送邮件。运行 `cargo test -p email-service --locked` 可检查错误分类。

## 运行与退出行为

- **`basic`** 处理完任务后自动退出。
- **`advanced`** 入队十封邮件，十次 API 额度允许五次带校验的模拟投递，随后中间件暂停接收。Handler 在投递后主动取消任务，因此会出现预期的“任务已取消，无法完成”错误。缓存刷新在任务执行结束通知后独立运行，取消也会触发该通知。额度不会补充；等待刷新日志后按 Ctrl+C。初始载荷均为不同地址，因此运行时走缓存未命中路径；命中有效地址只扣一次额度，命中无效地址直接失败。取消不能撤销投递，过早退出也可能丢弃后台刷新，详见[示例说明](advanced/README.md)。
- **`deps-injection`** 处理十个任务后退出。
- **`task-context`** 在三秒后请求取消任务，等待 worker 将状态更新为 `Killed` 后自动停止。取消会产生预期的任务错误事件，被取消 handler 的后续代码不会执行。
- **`worker-context`** 观测一个任务五秒后停止，并等待外部观察任务结束。
- **`catch-panic`** 持续运行，直到按下 Ctrl+C。两个任务各 panic 两次：panic 捕获层装箱错误，此处重试策略不会解包，因此会重试一次。输出共包含四条 panic hook 信息和两个 worker 最终错误；不会发送邮件。
- **`graceful-shutdown`** 持续运行，直到按下 Ctrl+C。它在每个五秒批次结束后检查关闭状态，再用五秒模拟检查点保存，不会实际写入数据。
- **`long-running`** 并发模拟三个耗时八至十二秒的导出操作。Worker 持续运行，直到按下 Ctrl+C，此时会等待执行中的导出完成并收集结果。
- **`retries`** 处理三个地址：一个成功执行一次，一个失败并累计执行四次，一个在首次执行时中止。错误均为预期结果。退避使用一秒下限、五秒上限及随机抖动；所有尝试完成后，按 Ctrl+C 停止 worker。
- **`unique-jobs`** 为同一收件人提交两次任务，但只处理第一个主题。内存后端在整个生命周期内保留幂等键，任务完成后也不会释放；这不提供跨应用重启的持久去重。按 Ctrl+C 退出。
- **`unmonitored-worker`** 以 20 个任务的并发上限处理 100 个耗时五秒的任务，全部完成后停止 worker；此时 `run_until` 仍会等待 Ctrl+C，收到信号后进程才退出。提前按 Ctrl+C 会等待活动任务完成，尚未开始的任务不会继续执行。
- **`workflow-stepped`** 依次执行欢迎、活动、完成三个步骤，对应 ID 为 1、2、3，随后自动停止。`delay_for(1s)` 会记录调度时间，但内存后端不会等待该时间，后续步骤仍立即执行；实际等待需要支持定时调度的后端。Worker 通过 spawn 执行任务，并发上限为二；步骤间的依赖仍决定执行顺序。
- **`workflow-graph`** 将 42、43、44 分别交给名称、年龄、地址编码三个入口节点，构成一次图执行的三个输入。汇总节点收到 `("42", 63, 144)`，输出总和 249 后停止 worker。输出的 DOT 图可使用 Graphviz 渲染。
- **`workflow-graph-dynamic`** 从源节点的值 1 开始，1,000 个动态生成的分支分别加一；汇总节点收到 1,000 个结果，输出总和 2,000 后自动停止。图结构在执行前生成，本示例不展示运行期间修改图。

- **`monitor`** 向两个临时 JSON 后端各提交一个任务。五个上下文 worker 具有独立文件游标，可能读取同一条记录，不能据此实现竞争消费。每个 handler 在一秒后停止自身 worker 并返回预期的业务错误，六个 worker 通常会自动退出；也可按 Ctrl+C 发起关闭，最多等待五秒。业务错误不会触发 worker 重启策略。此处只演示元数据接线，完整 trace 传播由 `tracing` 示例展示。

- **`monitor-context`** 每 500 毫秒向两个临时文件入队，并发上限分别为 30、20。任务随机选择 1–20 秒的耗时：超过七秒时立即取消，否则等待后返回预期错误。独立 future 每秒刷新终端面板，展示名称、运行与就绪状态、任务数、重启数和运行时长；重定向输出为纯文本快照。Ctrl+C 最多等待五秒，随后结束生产与展示并恢复终端。十二秒的 `run_until_executed` 子任务在下次被轮询时检查取消，运行时退出也会丢弃它。此示例不安装 trace provider。

- **`tracing`** 使用 `cargo run -p tracing-example --locked` 运行。输出四组 trace/span ID，在两个任务返回预期错误后退出。携带元数据的 consumer 与 producer 共享 trace ID，但有独立的 span ID；普通 consumer 属于自身 worker 的 trace。保留默认 debug 过滤器或设置 `RUST_LOG=debug`，才能看到任务 span。示例安装 SDK 和 W3C propagator，不配置远端 exporter；内联测试还会校验实际导出的父 span ID。
