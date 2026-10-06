# 港口泊位与航道调度

纯Python标准库实现的港口泊位与航道调度原型，使用SQLite持久化，HTTP接口由`http.server`提供。

## 模块结构

- `app.py`：命令行参数、依赖组装和服务启动。
- `src/domain.py`：领域数据类型、错误和基础校验。
- `src/rules.py`：状态转换、靠泊可行性、吃水安全、潮位放行判定和资源槽位。
- `src/repository.py`：SQLite建表、事务和查询。
- `src/recovery.py`：报文迟到/订正后的可恢复重算与已靠泊复核编排。
- `src/service.py`：用例编排、权限检查、乐观并发、冲突草稿和审计。
- `src/http_api.py`：HTTP路由与统一错误响应。
- `src/audit.py`：事件时间线。
- `static/index.html`：最小演示页面。
- `tests/`：完整流程、规则计算、失败场景与可恢复重算测试。

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
- `GET /api/records/{id}/decisions`：该计划全部放行判定（active/held/superseded/retained/completed）。
- `GET /api/stats`：状态统计。
- `POST /api/records`：创建计划，`data`可带`series_id`（潮位测次）、`channel_id`。
- `POST /api/records/{id}/actions/{action}`：执行`confirm/berth/depart/cancel`，请求体为`{"expected_version":1,"data":{...}}`。
- `POST /api/tide-reports`：录入潮位观测报文，`tide_observer`或`port_controller`可用。
- `GET /api/tide-reports?series_id=`：报文列表。
- `GET /api/recompute-runs`、`GET /api/recompute-runs/{id}`：重算运行与逐项状态。
- `POST /api/recompute-runs/{id}/retry`：失败后恢复，只重试未完成项。
- `GET /api/conflict-drafts?status=open`：并发确认落选者保留的冲突草稿。
- `POST /api/conflict-drafts/{id}`：`{"resolution":"reapply|discard"}`重放或放弃草稿。
- `GET /api/reviews?status=open`、`POST /api/reviews/{id}`：已靠泊计划的人工复核队列与处理。
- `GET /api/resource-bookings?status=held`：航道通行证与引航班次占用视图。

除`/health`和`/`外，请求需提供`X-User-Id`、`X-Role`，可选`X-Org`。

### 潮位驱动的可恢复复核链

1. **报文入账**：`tide_reports`按`(series_id, message_no)`唯一，同一测次同一报文号只入账一次，重放返回既有报文且不触发新运行；内容不同的同号报文被拒绝，订正须使用新报文号。
2. **按录入水深先放行**：无生效报文时`confirm`按录入海图水深入账（`basis_kind=entered`），并占用航道通行证与引航班次；报文到达后自动重算。
3. **订正/迟到触发重算**：新报文使该测次旧报文失效，并生成`recompute_runs`；未靠泊计划的原判定置为`superseded`、释放旧占用后按新报文重算——通过则重新占用资源回`confirmed`，不足则不占资源转`held`，此时禁止靠泊。
4. **已靠泊保留依据**：已靠泊计划的当时判定冻结为`retained`，资源不释放、不重算，只进入`reviews`复核队列等待人工处理。
5. **可恢复**：重算按计划逐项独立事务落`pending/failed/done/skipped`；资源冲突等失败整项回滚不留半成品，`retry`只重放未完成项；资源占用以`held`状态部分唯一索引兜底，重放物理上不可能重复占资源。
6. **并发确认**：两名调度员基于同一版本确认时，先到者放行，后到者得到`409 version_conflict`，其原始提交保存在冲突草稿中，可在计划回到可确认状态后`reapply`或直接`discard`。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

测试覆盖完整流程、规则计算、重复引用、权限拒绝、版本冲突，以及潮位报文幂等、迟到/订正重算、已靠泊保留依据入复核、失败项重试不重复占资源、并发确认冲突草稿和HTTP冒烟。
