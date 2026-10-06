# 港口泊位与航道调度

纯Python标准库实现的港口泊位与航道调度原型，使用SQLite持久化，HTTP接口由`http.server`提供。

## 模块结构

- `app.py`：命令行参数、依赖组装和服务启动。
- `src/domain.py`：领域数据类型、错误和基础校验。
- `src/rules.py`：状态转换、靠泊可行性、吃水安全、时间窗冲突和冲突检查。
- `src/repository.py`：SQLite建表、事务和查询。
- `src/service.py`：用例编排、权限检查、乐观并发和审计。
- `src/hydrology.py`：水文复核纯规则（有效水深判定、暂判、计划阶段）。
- `src/hydro_repository.py`：报文收件箱、判定台账、资源占用、复核作业、冲突草稿。
- `src/hydro_service.py`：报文去重入账、确认放行裁决、可恢复重算 saga。
- `src/http_api.py`：HTTP路由与统一错误响应。
- `src/audit.py`：事件时间线。
- `static/index.html`：潮位复核演示页面。
- `tests/`：完整流程、规则计算、失败场景与潮位可恢复复核测试。

## 启动

```bash
python3 app.py --db ./data.db --port 8321
```

默认端口为`8321`，默认数据库位于项目目录。服务启动时自动建表。

## 主要接口

- `GET /health`：健康检查。
- `GET /`：演示页面。
- `GET /api/records`：记录列表，可带`state`和`limit`参数。
- `GET /api/records/{id}`：记录详情。
- `GET /api/records/{id}/audit`：审计时间线。
- `GET /api/stats`：状态统计。
- `POST /api/records`：创建记录，请求体为`{"reference":"...","data":{...}}`。
- `POST /api/records/{id}/actions/{action}`：执行业务动作，请求体为`{"expected_version":1,"data":{...}}`。

除`/health`和`/`外，请求需提供`X-User-Id`、`X-Role`，可选`X-Org`。

## 潮位可恢复复核链路

把「录入水深 / 潮位报文 / 靠泊计划 / 航道通行证+引航员班次」四者接成可恢复复核：

- 报文以 `(测次, report_no)` 唯一约束入账，**同一报文号只入账一次**，重放不触发二次重算。
- 报文未到时按录入水深作**暂判（provisional）**放行；报文迟到/订正到达后，
  未靠泊计划的原判定置为 `superseded` 并按新报文重算：通过则重新放行并重新占用
  通行证/引航班次，不通过则置 `held` 并释放两类资源。
- 已开始靠泊的计划在靠泊瞬间把当时依据冻结（`frozen`），新判定只作 `advisory`，
  结论反转才置 `review` 待人工复核，资源保持占用。
- 重算按「判定 / 对账」两段持久化断点（`recompute_items`），段失败即熔断落盘，
  下次只重试未完成/失败段；重放靠 `(plan, kind, report_no)` 唯一索引幂等，
  不会重复占用资源。
- 两名调度员并发确认时，放行整事务做版本+状态 CAS：只有先到者放行，
  后到者返回 `conflict` 并把其版本保留为冲突草稿（`confirmation_drafts`），不重复占资源。

新增接口（计划与资源角色 `port_controller`，报文角色 `tide_observer`）：

- `POST /api/series`、`GET /api/series`：测次（录入水深）。
- `POST /api/series/{id}/plans`、`GET /api/series/{id}/plans`、`GET /api/plans/{id}`：靠泊计划。
- `POST /api/series/{id}/tide-reports`、`GET /api/series/{id}/tide-reports`：潮位报文入账（自动触发并返回复核作业）。
- `POST /api/plans/release`：确认放行，体为 `{"plan_id":1,"expected_version":1,"data":{...}}`。
- `POST /api/plans/{id}/berth|depart|cancel`：靠泊（冻结依据）/离泊（释放资源）/取消。
- `GET /api/plans/{id}/decisions|reservations|drafts|events`：判定台账、资源占用、冲突草稿、事件。
- `GET /api/jobs`、`GET /api/jobs/{id}`：复核作业及其分段断点。

判定口径：`有效水深 = 录入水深 + 潮位`，要求 `有效水深 >= 吃水 + 0.5m`。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

测试覆盖完整流程、规则计算、重复引用、权限拒绝和版本冲突。
