# 家乐千川：全量明细 · 看板 · 需修改诊断（0412–0512 流水线）

本仓库对应「轩辕 + 奥创 + OSS(ASR)」输入，产出 **一张总表与分表**、**看板数据**、**需修改 Gemini 分析 + PDF**，并支持将 **样例 PDF 的 OSS URL** 回填到 workflow 的「需修改」sheet。（仓库原名侧重「周创建脚本诊断」，现已扩展为完整千川复盘流水线。）

线上说明与历史描述亦可见：[siyawu-717/jiale--](https://github.com/siyawu-717/jiale--)（本 README 为完整操作版）。

---

## 仓库里有什么

| 路径 | 作用 |
|------|------|
| `scripts/run_jiale_0412_pipeline.py` | **主入口**：Step1 合并输入、写 Excel、看板、MD；Step2 打补丁并调 Gemini + Chrome 出 PDF |
| `scripts/jiale_dashboard_md.py` | 看板 sheet 分段、合并单元格、结论区；`编导读数分析报告.md` 生成 |
| `scripts/merge_need_modify_pdf_oss_urls.py` | 将 **样例 PDF OSS URL** 写入「需修改分析」表和/或 **workflow 总表**（openpyxl 只改需修改 sheet，不破坏看板格式） |
| `recovery_scripts/` | Vendored 的 `format_detail`、三分类、需修改分析**原始**脚本（避免依赖本机其它目录） |
| `prompts/` | Step2 使用的 prompt 文本（由分析脚本读取） |
| `references/` | Step1/2/3 口径说明（给 Cursor Skill 或人工对齐用） |
| `skill/SKILL.md` | Cursor Agent Skill：何时读哪些 reference、交付物清单 |
| `0412-0512输入/` | 放置千川导出 + OSS 表（见该目录下 `README.md`） |
| `output/` | 默认输出目录（默认被 `.gitignore` 忽略；克隆后本地跑一遍即生成） |

---

## 环境准备

1. **Python 3.10+**，安装依赖：

   ```bash
   pip install pandas openpyxl
   ```

2. **Gemini 视频分析**（仅跑 Step2 时需要）  
   - 默认查找 `call_gemini.py`（可通过环境变量 `CALL_GEMINI_PATH` 覆盖）。  
   - 需能访问脚本内配置的 LAS / 代理地址（见 `recovery_scripts/analyze_need_modify_materials_prompt_v2.py` 打补丁前的逻辑）。

3. **Chrome**（生成 PDF 时需要）  
   - 默认 macOS 路径；可用环境变量 `CHROME_BIN` 覆盖。  
   - 无 Chrome 时 Step2 仍可能写出 Excel，但 PDF 会失败。

4. **（可选）Step1 后自动写 PDF OSS 列**  
   - 设置 `JIALE_WORKFLOW_PDF_OSS_BASE` 为 OSS 目录 URL 前缀（与 `merge_need_modify_pdf_oss_urls.py --auto-oss-base` 相同），Step1 结束会自动给 workflow「需修改」补 `样例PDF_OSS_URL`。

---

## 一步在做什么（总流程）

### Step 0：准备输入

把三份表放进 `0412-0512输入/`（或自行传参路径），要求见 `0412-0512输入/README.md`。

### Step 1：合并 + 总表 + 看板 + 编导 MD

执行：

```bash
cd /path/to/本仓库
python3 scripts/run_jiale_0412_pipeline.py
# 或只生成 Step1，跳过 Gemini/PDF：
python3 scripts/run_jiale_0412_pipeline.py --skip-step2
```

**内部步骤概要：**

1. 读轩辕 / 奥创表 → 统一 `素材ID`、名称、账号（轩辕表「标签」含「奥创」→ 账号记奥创）。  
2. 读 OSS 表 → 按 `视频ID` 合并 `oss_path` 与 ASR。  
3. 调用 `recovery_scripts/build_shangshangzhou_recent30_workflow.py` 中的 **`infer_product` / `classify_result` / `format_detail`**：识别商品（回味粉、小面酱、干锅酱、水煮酱）、打「可复制 / 需修改 / 直接放弃」。  
4. **总体明细表**：默认筛 **创建时间** ∈ `--dashboard-create-start`～`--dashboard-create-end`（默认与主窗 **0412–0512** 一致）。  
5. **看板数据**（同一张 sheet 多段）：  
   - **月消耗**：全量素材；标题中 **消耗时间** = 主窗 `CREATE_START`～`CREATE_END`（0412–0512）；创建不筛。  
   - **月创建**：创建时间同上默认窗；消耗文案与月消耗一致。  
   - **周创建**：在创建窗内按 **周一至周日** 切周；每周一段表。  
6. 写 `家乐_workflow_0412-0512_总表与分表.xlsx`（含总明细、历史爆款、三分类分表、看板）。  
7. 写 `编导读数分析报告.md`（以月创建口径为主，按品分节）。  
8. 若设置了 `JIALE_WORKFLOW_PDF_OSS_BASE`，调用 `merge_need_modify_pdf_oss_urls.py` 给「需修改」补 OSS 列。

**常用参数：**

| 参数 | 含义 |
|------|------|
| `--input-dir` | 输入目录，默认 `0412-0512输入` |
| `--out-dir` | 输出根目录，默认 `output/run_0412_0512` |
| `--skip-step2` | 只跑 Step1 |
| `--step2-only` | 只跑 Step2（需已有总表 xlsx） |
| `--dashboard-create-start` / `--end` | 看板/总体明细的**创建**筛选项（旧名 `--global-create-start/end` 仍可用） |
| `--legacy-sample-pdfs` | PDF 按品抽样（默认全量每条需修改出 PDF） |

主窗日期常量（需改周期时编辑 `run_jiale_0412_pipeline.py` 内 `CREATE_START` / `CREATE_END`）。

### Step 2：需修改 → Gemini → PDF / Excel / CSV

在 **未** 加 `--skip-step2` 且存在 `prompts/`、`CALL_GEMINI_PATH` 指向的脚本时：

1. 将 `recovery_scripts/analyze_need_modify_materials_prompt_v2.py` 复制为 `output/run_0412_0512/_analyze_need_modify_generated.py` 并打补丁（工作簿路径、输出目录、prompt 路径、全量 PDF 等）。  
2. 对「需修改」逐条（或走缓存）调 Gemini，写 `need_modify_analysis/需修改分析_Gemini双脚本.xlsx`（及 csv/md）。  
3. Chrome 打印 HTML → `pdf_samples/*.pdf`。

若 Step1 后「需修改」条数增加，务必重跑 Step2（或 `--step2-only`），否则分析表与 PDF 会落后；流水线在条数不一致时会 **stderr 警告**。

### Step 3：PDF 上传 OSS 后回填 URL

将「文件名 → HTTPS URL」准备好后：

```bash
# 只更新 workflow「需修改」sheet（推荐，不破坏看板合并单元格）
python3 scripts/merge_need_modify_pdf_oss_urls.py \
  --workflow-xlsx output/run_0412_0512/家乐_workflow_0412-0512_总表与分表.xlsx \
  --auto-oss-base 'https://你的-bucket/.../家乐0412-0512/' \
  --skip-analysis

# 同时更新「需修改分析」单表 + csv
python3 scripts/merge_need_modify_pdf_oss_urls.py \
  --auto-oss-base 'https://.../'
```

也支持 `--paste` / `--map` 传非规则 URL 列表。

---

## Cursor Skill

将 `skill/SKILL.md` 作为 Cursor Agent Skill 导入后，Agent 会按其中链接阅读 `references/` 下各 step 说明，与本文互补。

---

## 与上游「恢复版」的关系

历史上完整工程另有「恢复版」目录；本仓库在 `recovery_scripts/` 中 **vendored** 两份关键脚本，`run_jiale_0412_pipeline.py` **优先**使用它们。若你本机仍保留原路径且删除了 `recovery_scripts/`，代码会回退到原绝对路径（便于本地迁移期）。

---

## 许可证与数据

- 业务表、OSS 链接、ASR 内容请勿提交到公开仓库；默认 `.gitignore` 已忽略 `0412-0512输入/*.xlsx` 与 `output/**`。  
- 脚本与文档可按你方政策选择许可证（本仓库未默认指定 SPDX，可自行补充 `LICENSE`）。

---

## 常见问题

**Q：看板上「消耗时间」和「创建时间」看不懂？**  
A：见 `references/step1_data_and_dashboard.md` —— 消耗窗固定为千川复盘 **0412–0512**；月创建/周/总体明细的创建窗默认与主窗一致，可用 CLI 单独改成例如 0328–0426 只看上新。

**Q：Step1 后小面酱需修改有 9 条但没有 PDF？**  
A：Step2 结果是旧的；跑 `python3 scripts/run_jiale_0412_pipeline.py --step2-only`。

**Q：`编导读数分析报告.md` 里某品没有爆款？**  
A：当月创建窗口内该品「可复制」样本可能为空；可调 `--dashboard-create-*` 或扩数据。

