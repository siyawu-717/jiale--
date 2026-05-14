# 千川输入表（本目录）

将以下 Excel 放入本目录后，在项目根执行 `python3 scripts/run_jiale_0412_pipeline.py`（或加 `--skip-step2` 仅 Step1）。

| 文件名（默认） | 说明 |
|----------------|------|
| `轩辕0412-0511.xlsx` | 轩辕账号素材与指标；列中含「标签」且含「奥创」的行会记为奥创账号 |
| `奥创0412-0511.xlsx` | 奥创账号素材与指标 |
| `oss_path.xlsx_asr_result.xlsx` | 视频 ID ↔ OSS 与 ASR；需含 **`视频ID`**（与千川素材 ID 一致） |

可通过命令行参数 `--xuanyuan` / `--aochuang` / `--oss` 覆盖路径。

> 默认 `.gitignore` 会忽略 `*.xlsx`，避免把业务数据推上 GitHub；若需提交脱敏样例，可删除根目录 `.gitignore` 中对应规则。
