# 切片交接与污染隔离台

岩芯切片实验室的标本位置/责任链管理：每片切片按 **领取 → 制片 → 交接 → 归还** 记录当前位置与责任人，并支持污染/温控异常的隔离复核闭环。

## 运行

```bash
npm start
# http://localhost:3025
```

数据持久化在 `data/core-slices.json`（原子写入：临时文件 + rename）。旧版数据首次启动时自动幂等迁移。

## 状态与规则

- 切片阶段：`待领取 → 制片中 → 交接中 → 已归还`，任意非隔离阶段可转入 `隔离中`。
- 每片切片始终记录：当前位置（`location`）、当前责任人（`custodian`）、密封状态、温控截止时间。
- **领取**：从样本暂存柜领取；重复或并发领取只成功一次（服务端样本级串行锁 + `requestId` 幂等）。
- **交接**：只有制片中、未污染、密封完好且温控未超时的切片可交接；同一片同时只允许一笔未归还交接。
- **归还**：必须核对原责任人（交接时的接收人）与密封状态，且温控未超时，任一不符即 409 拒绝（密封破损/超时/污染请转隔离）。
- **隔离复核**：原因分 `contaminated`（污染确认）、`temp_exceeded`（温控超时）、`seal_broken`（密封破损）。转入时保存隔离前快照（阶段/位置/责任人/未归还交接）；解除后按快照恢复原流程并重置温控计时。
- 污染确认的切片即使解除隔离也永久禁止交接；历史记录（`history`、`handovers`、`quarantineRecords`）只追加，刷新后位置轨迹与污染原因保持一致。

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/samples` | 样本及切片（含温控剩余、可操作标记等派生字段） |
| POST | `/api/samples` | 创建样本与初始切片（入库待领取） |
| POST | `/api/samples/:sid/slices` | 新增切片 |
| POST | `/api/samples/:sid/slices/:id/claim` | 领取（`person`,`station`,`tempLimitMinutes`,`sealIntact`,`requestId`） |
| POST | `/api/samples/:sid/slices/:id/logs` | 记录制片步骤（仅制片中） |
| POST | `/api/samples/:sid/slices/:id/handovers` | 交接（`toPerson`,`toLocation`,`tempLimitMinutes`,`sealIntact`,`requestId`） |
| POST | `/api/samples/:sid/slices/:id/handovers/:hid/return` | 归还（`person`,`sealIntact`,`toLocation`） |
| POST | `/api/samples/:sid/slices/:id/quarantine` | 转隔离（`reason`,`by`,`detail`） |
| POST | `/api/samples/:sid/slices/:id/quarantine/release` | 解除隔离（`by`,`outcome=cleared|confirmed_contamination`） |
| POST | `/api/samples/:sid/deliver` | 标记交付 |

业务冲突统一返回 `409 { error, message }`。
