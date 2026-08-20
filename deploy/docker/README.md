# Container images

同一个 Dockerfile 通过两个 target 分别构建 Control Plane 和 Worker 内的 Agentlet：

```bash
docker build -f deploy/docker/Dockerfile --target agentd -t ghcr.io/compforge/agentd:latest .
docker build -f deploy/docker/Dockerfile --target agentlet -t ghcr.io/compforge/agentlet:latest .
```

构建上下文必须是仓库根目录。运行镜像只保留 Go 二进制和 SQLite 所需的 C runtime；Sandbox
Engine 使用自己的镜像，不打包进 Agentlet。
