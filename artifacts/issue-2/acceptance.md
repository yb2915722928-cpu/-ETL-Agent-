# #2 V01 初始化、登录与隔离项目协作验收

验收日期：2026-11-13（Asia/Shanghai）。范围为 [GitHub #2](https://github.com/yb2915722928-cpu/-ETL-Agent-/issues/2)，基线提交 `b4879a74054594623f983bc3659d6e5eae3640bb`。应用实现 `97930f7`，双轴审查修复 `7a9ef32`，集成合并 `e343e4100ef83e6b9c45214ed8f4b180ebdd687d`；随后补齐中文路径构建入口和真实环境启动修正。本验收使用真实 Docker 服务、公开 HTTP 接口与系统 Chrome，没有 SQLite、依赖替身或内部函数断言。

## 功能与证据

| #2 要求 | 已实现与验证 | 证据 |
|---|---|---|
| 最小 Vue/FastAPI/PostgreSQL/Redis 环境与可重复初始化 | 四服务启动；初始化两个隔离租户，每个租户六个独立账号；重复初始化保留既有身份和授权 | [启动说明](../../README.md)、[身份结果](identity.json)、[重建结果](persistence.json) |
| 账号、租户、项目、成员及独立角色持久化 | Maker、数据 Checker、安全 Checker、Operator、Auditor 分别登录，读取所属项目与成员；管理员账号与项目管理权限分离 | [身份结果](identity.json) |
| 服务端隔离与即时权限核验 | 跨租户账号/成员 ID、外部或未加入项目 ID 被拒绝；当前会话在降权、移除成员、禁用账号及撤销租户管理员后立即受限 | [身份结果](identity.json)、[浏览器结果](browser.json) |
| 中文页面反映真实权限 | 通过页面创建账号、授予项目管理员、降为 Auditor、移除成员和禁用；对应控件消失、项目消失、禁用会话退回登录；无页面运行错误 | [管理员页面](admin-project.png)、[Maker 只读](maker-readonly.png)、[账号管理](account-management.png)、[撤权页面](revoked-admin.png)、[移除成员](removed-member.png)、[禁用会话](disabled-login.png) |
| 有序、带操作者的必要身份审计，无敏感明文回传 | PostgreSQL 事务内写变更与事件；租户连续序号、actor 和时间；失败写入不留下成功事件；响应/审计敏感字段扫描通过 | [身份结果](identity.json) |
| 真实依赖健康、版本和资源事实 | PostgreSQL `SELECT 1`、服务端版本、数据库大小；Redis `PING`、服务版本和内存；CPU/内存/磁盘与进程内存 | [身份结果](identity.json)、[重建结果](persistence.json) |
| 服务重建后的真实持久性 | `down` 保留命名卷，重新创建四服务并重复初始化；原账号 ID、原密码、Auditor 成员角色及原审计事件仍有效 | [重建结果](persistence.json) |

## 最少验证执行

1. 按 README 执行 `init_local.py`、阿里云基础镜像及运行镜像构建、`compose up -d --no-build --wait`、两个租户初始化。
2. 执行唯一提交的 HTTP 验证脚本：`python scripts/verify_identity.py --output artifacts/issue-2/identity.json`。五组身份与拒绝分支检查全部通过。
3. 使用临时 Playwright 脚本驱动已安装的系统 Chrome，完成四组中文页面流程并保存六张截图；浏览器版本 `157.0.8103.78`，`page_errors=[]`。浏览器脚本不加入代码库，不下载浏览器。
4. 执行 `docker compose -f compose.yaml -f compose.aliyun.yaml down`，随后 `up -d --no-build --wait`；重复初始化并执行 `python scripts/verify_identity.py --recheck --output artifacts/issue-2/persistence.json`，通过。
5. 只读查询真实数据库确认 `identity_events` 的 `(actor_id, tenant_id)` 与 `(project_id, tenant_id)` 组合外键已创建。重建后四服务均正常，API、PostgreSQL、Redis 显示 healthy。
6. 扫描当前四服务日志与验收文本材料中的已知初始化/验证/数据库密码，命中数均为 0。截图经目视检查不展示密码或 Cookie。

未添加单元测试套件。提交一个标准库公共 HTTP 验证脚本，用于本票必要的功能和持久化复查。

## 环境事实与已解决阻碍

- 实测 PostgreSQL `17.11`、Redis `8.0.4`、Python `3.12.16`、FastAPI `0.115.12`、SQLAlchemy `2.0.41`。镜像标签 `3.22.2` 对应固定 Alpine rootfs，包版本以实际健康结果为准。
- 宿主 Docker Engine `29.6.1`，Compose `5.2.0`，Linux x86_64 / WSL2；容器可见 CPU `20`、总内存 `8127676416` 字节。资源为环境事实，未作性能承诺。
- 用户要求后所有下载走阿里云：rootfs、校验值和 apk 使用 `mirrors.aliyun.com`，pip 使用阿里云 PyPI，npm 使用 `registry.npmmirror.com`。未用外部镜像仓库回退。
- 阿里云 Docker 加速镜像请求曾返回 403，因此采用校验 SHA256 的阿里云 rootfs 本地构建；真实服务已成功运行。
- Compose 5.2 在当前中文目录构建时报 `header key "x-docker-expose-session-sharedkey" contains value with non-printable ASCII characters`。修订为短小的 `build_runtime.py` 直接调用四次 `docker build`，该入口已完整执行成功；Compose 负责 `--no-build` 启停。
- 首次构建发现 Alpine 上 psutil 需编译工具，补齐临时编译依赖并在安装后删除；PostgreSQL 初始化补齐 socket 目录；Redis 允许 Compose 内网连接且不映射宿主端口。失败初始化产生的空卷在创建业务数据前清理，持久化验收使用的卷随后全程保留。

## 双轴审查收口

- **Standards**：发现一项 P2，审计 actor/project 外键原先未包含租户。已按仓库 Scope 约束改为组合外键并实测落库；复核后新增问题 0。
- **Spec**：发现一项 P2，快速切换项目可能让响应覆盖错误成员列表。项目卡片与现有写操作统一受 `busy` 禁用，串行选择；复核后新增问题 0。

V01 仅提供本票必要的身份审计事实；后续 ETL 规划、审批、执行、发布、证据哈希链与签名按对应工单实施。未把后续功能计入本票完成项。
