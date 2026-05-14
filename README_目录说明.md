# 目录说明

这是根据历史对话重建的 `全量明细与需修改全流程 skill` 归档版。

核心入口：

- `skill/SKILL.md`：整套流程总说明
- `prompts/`：数据定位问题、修改建议、脚本建议 1、脚本建议 2 的固定 prompt
- `references/`：流程拆解、表格口径、PDF 产出逻辑、脚本职责
- `给其他AI的标准提需求模板.md`：下次让别的 AI 复跑这套流程时可直接复制使用

目录结构：

- `skill/`
  - `SKILL.md`
  - `agents/openai.yaml`
- `prompts/`
  - `prompt_01_数据定位与修改建议.txt`
  - `prompt_02_完整脚本1.txt`
  - `prompt_03_完整脚本2_微润色.txt`
  - `prompt_04_执行版_诊断+双脚本.txt`
- `references/`
  - `step1_data_and_dashboard.md`
  - `step2_need_modify_analysis.md`
  - `step3_pdf_and_pdfurl.md`
  - `脚本清单.md`

说明：

- 这份归档以“流程、口径、prompt、交付结构”为主。
- 如果原 Python 脚本已经在本机被删除，这份 skill 仍然可以作为完整重建依据。
