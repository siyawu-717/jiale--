本目录为 **vendored** 的恢复版核心脚本副本，供克隆仓库后无需再依赖本机「全量明细…_恢复版」路径即可跑 Step1/Step2。

- `build_shangshangzhou_recent30_workflow.py`：商品识别、三分类阈值、`format_detail` 等
- `analyze_need_modify_materials_prompt_v2.py`：需修改 Gemini 分析 + PDF 生成逻辑（由 `run_jiale_0412_pipeline.py` 打补丁后执行）

若你本地仍维护独立恢复版目录，可在 `scripts/run_jiale_0412_pipeline.py` 中保留的 fallback 路径优先使用旧版（当本目录缺少文件时）。
