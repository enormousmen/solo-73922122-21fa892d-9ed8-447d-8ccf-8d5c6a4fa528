# pkg-cache：单进程 HTTP 包缓存代理 + 可控本地源站

面向构建工程的依赖包缓存代理：固定本地源站、按路径 GET 完整包，减少重复
回源流量；源包更新后按路径清除缓存。代理由 **FastAPI + aiohttp** 实现，
单进程单 worker 运行。

## 快速开始

```bash
docker compose up --build
```

* 代理：http://localhost:8080 （下载 API + `/docs` 清除入口）
* 源站：http://localhost:8081 （控制 API + `/docs`，自带 32MiB 包 `pkg-32m.bin` 与 1MiB 包 `pkg-1m.bin`）

打开 http://localhost:8081/docs 即可按页面内的演示流程依次执行换包、下载、查询统计（每个端点都有中文摘要）。

## 使用

```bash
# 下载（首次 MISS 回源，之后 HIT 命中磁盘缓存）
# 响应头 X-Package-Sha256 给出所供版本的 sha256 摘要
curl -D - -OJ http://localhost:8080/packages/pkg-32m.bin

# 按路径清除（也可打开 http://localhost:8080/docs 在 Swagger UI 里执行）
curl -X POST http://localhost:8080/purge/pkg-32m.bin

# 查看代理内部状态
curl http://localhost:8080/cache/status
```

源站控制（换内容 / 延迟 / 断流 / 回源次数）：

```bash
# 查看回源次数与当前版大小、sha256（含子目录里的自定义包；补算中的摘要为 null）
curl http://localhost:8081/control/stats

# 更换包内容（原子发布同路径新版本，返回新摘要；代理在清除前仍发旧包）
curl -X POST http://localhost:8081/control/content/pkg-32m.bin

# 每个 256KiB 块延迟 100ms（模拟慢源站）
curl -X POST http://localhost:8081/control/behavior \
  -H 'Content-Type: application/json' \
  -d '{"chunk_delay_ms":100,"truncate_after_bytes":null,"chunk_size":262144}'

# 发送 200000 字节后断流（模拟上游故障）
curl -X POST http://localhost:8081/control/behavior \
  -H 'Content-Type: application/json' \
  -d '{"chunk_delay_ms":0,"truncate_after_bytes":200000,"chunk_size":65536}'

# 清零回源计数
curl -X POST http://localhost:8081/control/reset
```

### 换包 → 下载 → 查询统计演示（/docs 同流程）

```bash
# 1) 换包：同路径发布新版本（补算旧包摘要期间也允许发布，互不阻塞）
curl -X POST 'http://localhost:8081/control/content/team-x/widget.tar.gz?size=222222'
# -> {"path":"team-x/widget.tar.gz","size":222222,"sha256":"...","generation":2}

# 2) 下载：响应头 X-Package-Sha256 与正文字节一致（始终是当前版的摘要）
curl -D - -o /tmp/widget.tar.gz http://localhost:8081/packages/team-x/widget.tar.gz
sha256sum /tmp/widget.tar.gz   # 与响应头 X-Package-Sha256 相同

# 3) 查询统计：packages 列出每个包当前版的 size 与 sha256（含子目录包）
curl http://localhost:8081/control/stats
```

## 行为要点

* **元数据恢复（源站重启）**：启动时递归登记卷上已有包（含子目录），
  sha256 在后台任务中补算，**开服不等待补算完成**；补算并发由
  `DIGEST_WORKERS`（默认 4）限制。补算未完成时统计中该包摘要为 `null`，
  首个下载请求会单飞等待该版本补算并在响应头带上摘要；同版本后续下载
  直接复用已算好的摘要。
* **换包与晚到摘要**：每个包路径维护单调递增的版本号（generation）。
  换包经临时文件 + `os.replace` 原子发布并立刻登记新版本；旧版本补算
  完成得再晚，也会因版本/文件身份校验失败而被丢弃（日志
  `discard stale digest ...`），不能覆盖新版。换包后新发起的下载与
  `/control/stats` 查询一律以新版为准，统计给出当前版的大小和摘要。
* **单飞回源（代理）**：同路径并发未命中只回源一次；所有下载（包括中途加入的）
  都从字节 0 开始逐块收到原字节，首块在源站首块落盘后即可读出，不等整包。
* **慢客户端隔离（代理）**：填充只写磁盘，各读者按自己的节奏尾随 `.part-<代>` 文件；
  单客户端内存为一个读缓冲，不随包长增长（实测 32MiB 下载代理 RSS 增量约 4MiB）。
* **按路径清除（代理）**：清除返回后首个新下载重新回源，后续共用新填充/缓存；
  已开始的旧下载继续读完旧代文件；旧填充完成也不会重新登记；
  旧文件在最后一个读者结束后回收。
* **失败传播（代理）**：上游断流/非 200 使该代所有下载失败（客户端读到不完整
  响应），半包立即删除、绝不入缓存；下一个请求自动重试回源。
* **重启（代理）**：`proxy-cache` 卷保留完整包；启动时清理 `.part-*` 残件与孤儿
  meta。404 不做负缓存。

## 本地自测（无 Docker 时）

```bash
pip install -r proxy/requirements.txt -r origin/requirements.txt
pip install httpx pytest
python3 -m pytest tests/ -v
```

测试以真实子进程启动源站与代理，覆盖：单飞、首块延迟、慢客户端隔离、
清除语义（含填充中清除）、断流不缓存半包、重启保留/清理、内存有界、
404 与路径穿越；以及源站元数据恢复专项——卷上自定义包（含子目录）重启
恢复、启动不阻塞于补算、同版下载复用摘要、补算中换包且晚到旧摘要被丢弃、
换包后下载与统计均以新版为准、卷外直接替换文件的识别。
`tests/run_server.py` 是测试专用启动器（本沙箱禁止
进程间信号，用控制文件代替 SIGTERM 停服），交付应用本身不含测试钩子。
