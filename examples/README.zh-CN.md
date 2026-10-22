# 示例

[miju](../README.zh-CN.md) · [English](README.md)

每个可运行示例均包含独立的 `Cargo.toml` 和 `src/main.rs`，展示所需 feature 和执行过程的注释。共享辅助库与这些示例放在同一目录下。

## 运行示例

在仓库根目录运行：

```sh
cargo run -p basic --locked
```

将 `basic` 替换为下方任一示例名称即可。部分示例会自动退出，其他示例需要按 **Ctrl+C** 关闭，具体见[运行与退出行为](#运行与退出行为)。

## 任务处理与上下文

| 示例 | 场景 |
| --- | --- |
| [`basic`](basic) | 使用速率限制、并发、重试、panic 捕获、任务 spawn 和 tracing 处理内存任务。 |
| [`deps-injection`](deps-injection) | 提取任务元数据，通过 `Data` 共享计数器，并通过 `FromRequest` 提供自定义参数。 |
| [`task-context`](task-context) | 通过克隆的 `TaskContext` 取消任务，等待执行结束通知，并观察 `Killed` 状态。 |
| [`worker-context`](worker-context) | 查询正在执行的任务，并通过共享的 `WorkerContext` 从外部观测 worker。 |

## 执行与关闭

| 示例 | 场景 |
| --- | --- |
| [`catch-panic`](catch-panic) | 捕获邮件任务中主动触发的 panic，排除中止错误的重试，并观测 worker 错误事件。 |
| [`graceful-shutdown`](graceful-shutdown) | 响应 Ctrl+C，完成当前批次，并在关闭前模拟保存任务状态。 |
| [`long-running`](long-running) | 通过 `TaskRunner` 并发执行数据导出、收集结果，并在关闭时等待执行中的任务完成。 |
| [`retries`](retries) | 对普通错误应用指数退避并最多重试三次，同时排除 `AbortError`。 |
| [`unmonitored-worker`](unmonitored-worker) | 以 20 个任务的并发上限独立运行 worker，待有限批次全部完成后自动停止。 |

## 共享示例支持

[`email-service`](email-service) 是邮件示例共用的库，不是可直接运行的二进制程序。它提供可序列化的载荷、三种地址校验结果、示例邮件、应用错误及 HTML 提交表单。投递为模拟操作，不会发送邮件。运行 `cargo test -p email-service --locked` 可检查错误分类。

## 运行与退出行为

- **`basic`** 处理完任务后自动退出。
- **`deps-injection`** 处理十个任务后退出。
- **`task-context`** 在三秒后请求取消任务，等待 worker 将状态更新为 `Killed` 后自动停止。取消会产生预期的任务错误事件，被取消 handler 的后续代码不会执行。
- **`worker-context`** 观测一个任务五秒后停止，并等待外部观察任务结束。
- **`catch-panic`** 持续运行，直到按下 Ctrl+C。它会先打印 panic hook 信息，再上报捕获到的错误，不会发送邮件。
- **`graceful-shutdown`** 持续运行，直到按下 Ctrl+C。它在每个五秒批次结束后检查关闭状态，再用五秒模拟检查点保存，不会实际写入数据。
- **`long-running`** 并发模拟三个耗时八至十二秒的导出操作。Worker 持续运行，直到按下 Ctrl+C，此时会等待执行中的导出完成并收集结果。
- **`retries`** 处理三个地址：一个成功执行一次，一个失败并累计执行四次，一个在首次执行时中止。错误均为预期结果。退避使用一秒下限、五秒上限及随机抖动；所有尝试完成后，按 Ctrl+C 停止 worker。
- **`unmonitored-worker`** 以 20 个任务的并发上限处理 100 个耗时五秒的任务，全部完成后停止 worker；此时 `run_until` 仍会等待 Ctrl+C，收到信号后进程才退出。提前按 Ctrl+C 会等待活动任务完成，尚未开始的任务不会继续执行。
