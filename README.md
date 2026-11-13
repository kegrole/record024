# pqueue — 优先级采集调度器

纯 Python 标准库实现，无第三方依赖。调度器负责两件事：按优先级排队并去重，
以及在任务越界访问时拦截并记录安全事件。

## 任务模型

每个任务（pqueue/task.py）包含：

- id：任务唯一标识
- key：去重键
- priority：优先级，整数，数值越大越紧急（支持任意多级）
- url：要访问的地址
- allowed\_domains：授权域名清单
- allowed\_path\_prefixes：授权路径前缀清单
- payload：载荷（payload.sleep\_ms 控制模拟请求耗时）

## 优先级与去重语义

- 出队顺序：优先级数值大者先出队；同优先级按入队顺序（FIFO）。
  高优先级任务入队后即排在堆顶，自然插到所有低优先级任务之前。
- 去重：同一个 key 在调度器生命周期内只执行一次。重复提交同 key 的任务
  会被合并进首次提交，merged\_count 加一，不会重复入队、更不会并发跑两遍。

## 访问范围规则

任务执行前先过策略检查（pqueue/policy.py），通过才"发出"请求：

- url 的域名必须精确命中 allowed\_domains，否则记 domain\_not\_allowed；
- url 的路径必须以 allowed\_path\_prefixes 中某个前缀开头，否则记
  path\_prefix\_not\_allowed。

越界请求一律拦截：不进入投递环节，任务状态记为 blocked，并向
blocked\_requests 追加一条 {task\_id, url, reason} 安全事件。

## 背压策略

队列容量有界（--queue-size）。队列满时按 --backpressure 处理：

- block（默认）：提交方阻塞等待空位，每发生一次阻塞记一次 backoff\_events；
- reject：立即拒绝该次提交，rejected\_count 加一。

## 停机策略

close() 之后不再接受新任务（submit 返回 closed）。未开始的任务按
--shutdown 处理：

- drain（默认）：按优先级从高到低全部处理完再退出；
- drop：直接丢弃队列中剩余任务，并计入 dropped\_count，
  保证 executed + dropped == accepted，不存在既没做完也没计数的任务。

已在执行中的任务不受停机模式影响，一律做完。

## 并发

worker 数由 --workers 决定，执行器用计数器跟踪并发度并记录
peak\_concurrency，任何时刻并发执行的任务数不超过 worker 上限。

## 报告

JSON 报告包含六个必需段：executed（每条含 id、key、status、duration\_ms）、
merged\_count、rejected\_count、blocked\_requests、backoff\_events、
peak\_concurrency，另附 accepted\_count 与 dropped\_count 便于对账。

## 用法

    python -m pqueue.cli --tasks demo/tasks.json --workers 4 --queue-size 100
    python -m pqueue.cli --tasks <任务清单> --workers <并发数> \
        --queue-size <队列容量> --out <报告路径> \
        [--backpressure block|reject] [--shutdown drain|drop]

运行结束在 stdout 打印一行摘要：

    accepted=180 executed=180 merged=20 rejected=0 blocked\_requests=2

## 测试

    python -m unittest discover -s tests -v

测试覆盖八条判定口径：唯一执行数与合并数、越界拦截、优先级生效、
并发上限、背压计数、停机语义、报告结构、CLI 与 demo 夹具。
