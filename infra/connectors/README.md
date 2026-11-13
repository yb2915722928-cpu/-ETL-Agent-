# 真实 MySQL / Doris / PostgreSQL 与 CSV 验收资源

这些入口从 2026-11-13 已实际运行的独立环境提炼，服务真实数据库，不替代成模拟驱动。初始化只操作 `etl-connector-*` 与 `etl-connectors_*` 新资源，复用既有私有凭据与卷；不要删除控制面卷或将 `.runtime/connectors` 整目录提交。脚本在仓库根运行，须先启动平台以建立 `etl-agent_default` 网络。

```powershell
python scripts/prepare_connector_mysql.py
# 约4.28GB，官方OSS，分段断点续传并SHA512核验
python scripts/download_connector_doris.py
python scripts/build_connector_doris.py
docker compose -f infra/connectors/compose.doris.yaml up -d --wait --wait-timeout 180
python scripts/prepare_connector_doris.py
python scripts/generate_connector_resources.py
```

`--runtime <目录>` 可用于 Python 入口统一指定私有运行目录，默认 `.runtime/connectors`。密码只写本地私有 `secrets.json`、客户端 cnf 与初始化 SQL；失败 setup 日志也留在该忽略目录。白名单生成器只提取非密码字段；后续通过 Maker 表单/HTTP 登记密码到加密 SecretReference，不能把私有 JSON直接返回前端。

MySQL 固定镜像 `mysql@sha256:7dcddc01f13bab2f15cde676d44d01f61fc9f99fe7785e86196dfc07d358ae2b`，实测版本8.0.46。初始化通过镜像官方首次 init 机制创建3条customers、2条orders及customer_public视图；已有卷不重复执行种子SQL，不重置账号。只读账号仅有SELECT/SHOW VIEW。健康检查使用真实只读账号查询，cnf先复制容器/tmp并chmod600，兼容Windows bind mount被MySQL忽略的问题。

Doris 是同容器中的真实FE/BE，官方固定3.0.8归档内部节点报告 `doris-3.0.8-rc01-09b0cc49a6`。构建入口校验Java SHA256和Doris固定SHA512，然后构建运行库并仅向空命名卷解包。已有包含FE/BE安装的卷保留；其他非空不完整卷直接失败，需操作者调查，脚本不会覆盖。

当前构建明确依赖本机这两个已有固定镜像层，而不是通用冷机器一键安装：

- `docker-elasticsearch@sha256:611391cdbf688c003bf01df72748a6e655541908ca4ac7e50a1a6c84cd9d4ff2`：只复用其Ubuntu24.04.3 glibc层，不启动Elasticsearch、不挂其数据。
- `etl-connectors-postgres:17.11-126f44c116f1`：构建脚本验证image ID必须为 `sha256:126f44c116f1541d3c274246bf6193b3247b2cb66d3549b7b11b890d132b4611`，只复制标准TZif文件（tzdata2026f-r0）。这是既有阿里云构建PG镜像固定版本的本地别名。不存在或ID不同时明确失败，不静默改用其他镜像。

[Doris Dockerfile](doris.Dockerfile)固定阿里云Ubuntu运行包，包括libgomp1/gcc-14-base/libgcc-s1/libstdc++6的14.2.0-4ubuntu2~24.04.1、libaio1t64的0.3.113-6build1.1、libnuma1/numactl的2.0.18-1ubuntu0.24.04.1。运行镜像名称为 `etl-connectors-doris-runtime:3.0.8-jdk17.0.20`，原实测image ID为 `sha256:aefac91816d21ec19c706945df914fe2c5605a4b37f5c94de0a248e041dcb0e0`；提炼后显式固定传递依赖的重建产物可能有新ID，不能把原ID冒充重新构建的结果。

Java为Alibaba Dragonwell Standard17.0.20.0.21+8，官方[OSS下载](https://dragonwell.oss-cn-shanghai.aliyuncs.com/17.0.20.0.21%2B8/Alibaba_Dragonwell_Standard_17.0.20.0.21.8_x64_linux.tar.gz)，SHA256 `656a2aa3334082a35a222ac1cfabc6979d89257993d2ba6034ad2fa65e26fc9b`。Doris[官方3.0.8归档](https://apache-doris-releases.oss-accelerate.aliyuncs.com/apache-doris-3.0.8-bin-x64.tar.gz)共4,278,678,794字节，SHA512 `e4441aaf845d9e95ebefa51159316b6248f039658db2e24d0e378f56cf5b94f433a841f52a3b68c651899e214f49b75cac0e49e7ae6e53ed8a166d51dbb09b1f`。

独立网络固定 `172.20.0.0/16`，Doris为172.20.0.10，BE心跳9050。MySQL同时接该网和平台网。若部署机已有冲突子网，需同时修改受管Compose/初始化BE地址，不能只改一个地址后宣称可用。Doris容器限制4GiB/4CPU，FE堆1GiB、BE内存1536MiB/JNI堆512MiB；使用官方SKIP_CHECK_ULIMIT，不更改宿主sysctl。该设置只验证小型合成样本，不代表生产容量/性能结果。

`prepare_connector_doris.py`首次固定凭据后保护默认root/admin，注册BE、等待FE/BE Alive、建立真实UNIQUE KEY单桶单副本表与只读账号。重复执行仍使用原凭据，样本按UNIQUE KEY覆盖相同键，不累计行数。它执行真实只读SELECT及CREATE拒绝检查，再保存不含密码的健康报告；SHOW GRANTS中的Password列仅为Yes/No。

PostgreSQL/CSV 的入口是 `scripts/prepare_postgresql_csv_demo.py`（`--directory` 指定私有运行目录）和 `infra/connectors/postgresql.compose.yaml`，完整命令与格式/项目白名单契约见 [V04说明](../../docs/connectors.md#v04postgresql-与受管-csv)。PG 与控制库使用独立容器、账号和 `etl-connectors_postgres_data` 卷；沿用上文固定 PG 镜像。`--initialize` 已在真实现有源上幂等运行，Compose 已通过配置检查，未重建并行验收环境。

提炼入口已做Python语法及Compose配置核验；已有实例上的真实驱动验证见 [V03边界记录](../../artifacts/issue-4/implementation-verification.md)。不为验证提炼脚本而重建正在被集成验收使用的容器。本说明分别记录已运行的原始依赖和提炼后入口，不把未重新冷启动的整套脚本宣称为新的完整环境验收。
