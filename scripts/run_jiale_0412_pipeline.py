#!/usr/bin/env python3
"""家乐 0412-0512 输入：轩辕 + 奥创 + OSS(ASR)，无信息维护表。轩辕标签含「奥创」则账号记为奥创。"""
from __future__ import annotations

import argparse
import importlib.util
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pandas as pd

import jiale_dashboard_md as jdm

WORKSPACE = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = WORKSPACE / "0412-0512输入"
DEFAULT_OUT = WORKSPACE / "output" / "run_0412_0512"
# 优先使用仓库内 vendored 恢复版脚本，便于克隆后开箱；本地可继续用旧绝对路径
_RECOVER_LOCAL = WORKSPACE / "recovery_scripts" / "build_shangshangzhou_recent30_workflow.py"
_ANALYZE_LOCAL = WORKSPACE / "recovery_scripts" / "analyze_need_modify_materials_prompt_v2.py"
RECOVER_SCRIPT = (
    _RECOVER_LOCAL
    if _RECOVER_LOCAL.exists()
    else Path(
        "/Users/siya/Desktop/全量明细与需修改全流程_skill_20260428_0328-0426_恢复版/scripts/build_shangshangzhou_recent30_workflow.py"
    )
)
ANALYZE_SCRIPT = (
    _ANALYZE_LOCAL
    if _ANALYZE_LOCAL.exists()
    else Path(
        "/Users/siya/Desktop/全量明细与需修改全流程_skill_20260428_0328-0426_恢复版/scripts/analyze_need_modify_materials_prompt_v2.py"
    )
)
CALL_GEMINI = Path(os.environ.get("CALL_GEMINI_PATH", "/Users/siya/.codex/skills/gemini-las-video/scripts/call_gemini.py"))
CHROME_BIN = Path(os.environ.get("CHROME_BIN", "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"))

CREATE_START = pd.Timestamp("2026-04-12")
CREATE_END = pd.Timestamp("2026-05-12")
# 看板「月创建/周」与「总体明细」的创建筛选默认 = 主窗口 CREATE_*（0412–0512）；可用 CLI 单独改


def load_wf_module() -> Any:
    spec = importlib.util.spec_from_file_location("jiale_wf", RECOVER_SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"找不到恢复版脚本: {RECOVER_SCRIPT}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def load_oss(path: Path, wf: Any) -> tuple[pd.DataFrame, int]:
    raw = pd.read_excel(path, dtype=str)
    if "视频ID" not in raw.columns:
        raise ValueError("OSS 表需包含「视频ID」列，与千川素材ID一致")
    out = raw.rename(columns={"视频ID": "素材ID"}).copy()
    out["素材ID"] = out["素材ID"].map(wf.normalize_id)
    out["oss_path"] = out["oss_path"].map(wf.clean_text)

    def pick_asr(row: pd.Series) -> str:
        for col in ("gemini_asr_result", "tencent_asr_result"):
            if col not in row.index:
                continue
            t = wf.safe_str(row.get(col))
            if t and t.lower() != "nan":
                return t
        return ""

    out["asr_prefill"] = out.apply(pick_asr, axis=1)
    dup = int(out.duplicated(subset=["素材ID"], keep="first").sum())
    out = out.drop_duplicates(subset=["素材ID"], keep="first")
    return out[["素材ID", "oss_path", "asr_prefill"]].copy(), dup


def load_account(path: Path, default_account: str, wf: Any) -> pd.DataFrame:
    df = pd.read_excel(path, dtype=str)
    df["素材ID"] = df["素材ID"].map(wf.normalize_id)
    df["素材名称"] = df["素材名称"].map(wf.clean_text)
    df["账号"] = default_account
    if default_account == "轩辕":
        tag = df["标签"] if "标签" in df.columns else pd.Series([""] * len(df))
        tag = tag.fillna("").astype(str)
        df.loc[tag.str.contains("奥创", na=False), "账号"] = "奥创"
    df["商品"] = df["素材名称"].map(wf.infer_product)
    df["素材创建时间"] = pd.to_datetime(df["素材创建时间"], errors="coerce")

    spend = df["整体消耗"].map(wf.parse_number) if "整体消耗" in df.columns else pd.Series([pd.NA] * len(df))
    gmv = df["整体成交金额"].map(wf.parse_number) if "整体成交金额" in df.columns else pd.Series([pd.NA] * len(df))
    plays = (
        df["视频播放数"].map(wf.parse_number)
        if "视频播放数" in df.columns
        else pd.Series([pd.NA] * len(df))
    )
    show = df["整体展示次数"].map(wf.parse_number) if "整体展示次数" in df.columns else pd.Series([pd.NA] * len(df))
    click = df["整体点击次数"].map(wf.parse_number) if "整体点击次数" in df.columns else pd.Series([pd.NA] * len(df))
    order = df["整体成交订单数"].map(wf.parse_number) if "整体成交订单数" in df.columns else pd.Series([pd.NA] * len(df))
    r3 = df["3秒播放率"].map(wf.parse_percent) if "3秒播放率" in df.columns else pd.Series([pd.NA] * len(df))
    r5 = df["5秒播放率"].map(wf.parse_percent) if "5秒播放率" in df.columns else pd.Series([pd.NA] * len(df))
    r10 = df["10秒播放率"].map(wf.parse_percent) if "10秒播放率" in df.columns else pd.Series([pd.NA] * len(df))

    ctr = (click / show).where(show.notna() & (show != 0), pd.NA)
    if "整体点击率" in df.columns:
        ctr = df["整体点击率"].map(wf.parse_percent)
    cvr = (order / click).where(click.notna() & (click != 0), pd.NA)
    if "整体转化率" in df.columns:
        cvr = df["整体转化率"].map(wf.parse_percent)

    roi = (gmv / spend).where(spend.notna() & (spend != 0), pd.NA)

    out = pd.DataFrame(
        {
            "商品": df["商品"],
            "账号": df["账号"],
            "素材ID": df["素材ID"],
            "素材名称": df["素材名称"],
            "素材创建时间": df["素材创建时间"],
            "奥创视频制作名称": "",
            "消耗": spend,
            "GMV": gmv,
            "播放": plays,
            "展示": show,
            "点击": click,
            "订单": order,
            "ROI": roi,
            "千次播放GMV": (gmv / plays * 1000).where(plays.notna() & (plays != 0), pd.NA),
            "CTR": ctr,
            "CVR": cvr,
            "3s完播率": r3,
            "5s完播率": r5,
            "10s完播率": r10,
        }
    )
    out["发布时间"] = out["素材创建时间"].dt.strftime("%Y-%m-%d").where(out["素材创建时间"].notna(), "")
    return out


def attach_oss_asr(detail: pd.DataFrame, oss: pd.DataFrame, wf: Any) -> pd.DataFrame:
    m = detail.merge(oss, on="素材ID", how="left")
    m["asr"] = m["asr_prefill"].fillna("").map(lambda x: wf.clean_text(x))
    m = m.drop(columns=["asr_prefill"])
    path_ok = m["oss_path"].fillna("").astype(str).str.strip().ne("")
    asr_ok = m["asr"].astype(str).str.strip().ne("")
    err = pd.Series("", index=m.index)
    err = err.mask(~path_ok, "未匹配OSS或无路径")
    err = err.mask(path_ok & ~asr_ok, "OSS无ASR文本")
    m["asr_error"] = err
    return m


def write_step1_excel(
    path: Path,
    detail_master_fmt: pd.DataFrame,
    fmt_win: pd.DataFrame,
    history_fmt: pd.DataFrame,
    sections: list[jdm.DashboardSection],
    conclusion: str,
) -> None:
    copy_df = fmt_win[fmt_win["三类结果"] == "可复制"].copy()
    modify_df = fmt_win[fmt_win["三类结果"] == "需修改"].copy()
    drop_df = fmt_win[fmt_win["三类结果"] == "直接放弃"].copy()
    jdm.write_step1_workbook_v2(
        path,
        detail_master_fmt,
        fmt_win,
        history_fmt,
        copy_df,
        modify_df,
        drop_df,
        sections,
        conclusion,
    )


GENERATE_SAMPLE_PDFS_ORIG = '''def generate_sample_pdfs(df: pd.DataFrame, sample_per_product: int) -> pd.DataFrame:
    candidates = df[df["gemini状态"].isin(["ok", "cached"])].copy()
    candidates = candidates.sort_values(["商品", "GMV"], ascending=[True, False])
    sample_ids = set(candidates.groupby("商品").head(sample_per_product)["素材ID"].tolist())
    for idx, row in df.iterrows():
        material_id = safe_str(row["素材ID"])
        if material_id not in sample_ids:
            continue
        row_dict = row.to_dict()
        write_html(row_dict)
        pdf_path = export_pdf(material_id)
        df.at[idx, "是否生成PDF样例"] = "是"
        df.at[idx, "样例PDF文件名"] = pdf_path.name
        df.at[idx, "样例PDF本地路径"] = str(pdf_path)
    return df


'''

GENERATE_SAMPLE_PDFS_ALL = '''def generate_sample_pdfs(df: pd.DataFrame, sample_per_product: int) -> pd.DataFrame:
    """为每一条需修改记录生成 PDF（不再按商品抽样）。失败行标记为「失败」。"""
    for idx, row in df.iterrows():
        material_id = safe_str(row["素材ID"])
        row_dict = row.to_dict()
        try:
            write_html(row_dict)
            pdf_path = export_pdf(material_id)
            df.at[idx, "是否生成PDF样例"] = "是"
            df.at[idx, "样例PDF文件名"] = pdf_path.name
            df.at[idx, "样例PDF本地路径"] = str(pdf_path)
        except Exception as exc:
            df.at[idx, "是否生成PDF样例"] = "失败"
            df.at[idx, "样例PDF文件名"] = ""
            df.at[idx, "样例PDF本地路径"] = safe_str(exc)
    return df


'''


def patch_analyze_script(
    dst: Path, workflow_xlsx: Path, out_dir: Path, prompts_dir: Path, *, all_pdfs: bool
) -> None:
    src = ANALYZE_SCRIPT.read_text(encoding="utf-8")
    src = src.replace(
        'WORKFLOW_XLSX = DATA_ROOT / "output/上上周新创建素材分析/上上周新创建素材_workflow结果_0405-0412_消耗0321-0419.xlsx"',
        f'WORKFLOW_XLSX = Path(r"{workflow_xlsx}")',
    )
    src = src.replace(
        'OLD_CONTEXT_CACHE_DIR = DATA_ROOT / "output/上上周新创建素材分析/上上周新创建素材_需修改全量分析_20260422/gemini_context_cache"',
        f'OLD_CONTEXT_CACHE_DIR = Path(r"{out_dir / "_empty_old_context"}")',
    )
    src = src.replace(
        'ASR_CACHE_DIR = DATA_ROOT / "output/上上周新创建素材分析/workflow_asr_cache_0405-0412"',
        f'ASR_CACHE_DIR = Path(r"{out_dir / "_empty_asr_cache"}")',
    )
    src = src.replace(
        'RUN_ROOT = ROOT / "交付归档/上上周需修改分析_新prompt双脚本_20260422"',
        f'RUN_ROOT = Path(r"{out_dir.parent}")',
    )
    src = src.replace('PROMPTS_DIR = RUN_ROOT / "prompts"', f'PROMPTS_DIR = Path(r"{prompts_dir}")')
    src = src.replace(
        'OUTPUT_DIR = RUN_ROOT / "outputs"',
        f'OUTPUT_DIR = Path(r"{out_dir}")',
    )
    src = src.replace(
        'CACHE_DIR = OUTPUT_DIR / "cache"',
        f'CACHE_DIR = Path(r"{out_dir / "cache"}")',
    )
    src = src.replace(
        'HTML_DIR = OUTPUT_DIR / "html_samples"',
        f'HTML_DIR = Path(r"{out_dir / "html_samples"}")',
    )
    src = src.replace(
        'PDF_DIR = OUTPUT_DIR / "pdf_samples"',
        f'PDF_DIR = Path(r"{out_dir / "pdf_samples"}")',
    )
    src = src.replace(
        'OUTPUT_XLSX = OUTPUT_DIR / "上上周新创建素材_需修改分析_新prompt双脚本_20260422.xlsx"',
        f'OUTPUT_XLSX = Path(r"{out_dir / "需修改分析_Gemini双脚本.xlsx"}")',
    )
    src = src.replace(
        'OUTPUT_CSV = OUTPUT_DIR / "上上周新创建素材_需修改分析_新prompt双脚本_20260422.csv"',
        f'OUTPUT_CSV = Path(r"{out_dir / "需修改分析_Gemini双脚本.csv"}")',
    )
    src = src.replace(
        'OUTPUT_MD = OUTPUT_DIR / "上上周新创建素材_需修改分析_新prompt双脚本_20260422.md"',
        f'OUTPUT_MD = Path(r"{out_dir / "需修改分析_Gemini双脚本.md"}")',
    )
    src = src.replace(
        '"原视频ASR": cached_asr(row["素材ID"]),',
        '"原视频ASR": safe_str(row.get("asr")) or cached_asr(row["素材ID"]),',
    )
    if all_pdfs:
        if GENERATE_SAMPLE_PDFS_ORIG not in src:
            raise RuntimeError("分析脚本结构已变：找不到 generate_sample_pdfs，无法切换全量 PDF")
        src = src.replace(GENERATE_SAMPLE_PDFS_ORIG, GENERATE_SAMPLE_PDFS_ALL)
    dst.write_text(src, encoding="utf-8")


def run_step2(analyze_py: Path, argv: list[str]) -> None:
    subprocess.run([sys.executable, str(analyze_py), *argv], check=True)


def maybe_apply_workflow_pdf_oss_urls(step1_xlsx: Path) -> None:
    """若设置环境变量 JIALE_WORKFLOW_PDF_OSS_BASE（目录 URL 前缀），在 Step1 写盘后给「需修改」补样例PDF_OSS_URL。"""
    base = os.environ.get("JIALE_WORKFLOW_PDF_OSS_BASE", "").strip()
    if not base:
        return
    merge_py = WORKSPACE / "scripts" / "merge_need_modify_pdf_oss_urls.py"
    subprocess.run(
        [
            sys.executable,
            str(merge_py),
            "--workflow-xlsx",
            str(step1_xlsx),
            "--auto-oss-base",
            base,
            "--skip-analysis",
        ],
        check=True,
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT)
    p.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    p.add_argument("--xuanyuan", type=Path, default=None, help="轩辕 xlsx")
    p.add_argument("--aochuang", type=Path, default=None, help="奥创 xlsx")
    p.add_argument("--oss", type=Path, default=None, help="OSS+ASR xlsx")
    p.add_argument("--skip-step2", action="store_true", help="只生成 Step1 工作簿")
    p.add_argument(
        "--step2-only",
        action="store_true",
        help="跳过 Step1，仅用已有「家乐_workflow_0412-0512_总表与分表.xlsx」重跑 Step2（适合已跑完 Gemini 缓存后只补 PDF）",
    )
    p.add_argument("--max-workers", type=int, default=2)
    p.add_argument("--chunk-size", type=int, default=4)
    p.add_argument("--sample-pdf-per-product", type=int, default=2)
    p.add_argument(
        "--legacy-sample-pdfs",
        action="store_true",
        help="PDF 按商品抽样（旧逻辑）；默认关闭=为每条需修改都生成 PDF",
    )
    p.add_argument(
        "--dashboard-create-start",
        "--global-create-start",
        default=str(CREATE_START.date()),
        dest="dashboard_create_start",
        help="总体明细/月创建/周看板 的创建起始（默认=主窗口 0412；旧参数名 --global-create-start 仍可用）",
    )
    p.add_argument(
        "--dashboard-create-end",
        "--global-create-end",
        default=str(CREATE_END.date()),
        dest="dashboard_create_end",
        help="总体明细/月创建/周看板 的创建结束（默认=主窗口 0512；旧参数名 --global-create-end 仍可用）",
    )
    p.add_argument("--force-refresh", action="store_true")
    return p.parse_args()


def warn_step2_stale(workflow_xlsx: Path, step2_dir: Path) -> None:
    """Step1「需修改」与已有 Step2 行数不一致时提示（常见于新增小面酱等后未重跑 Step2）。"""
    analysis_xlsx = step2_dir / "需修改分析_Gemini双脚本.xlsx"
    if not workflow_xlsx.exists() or not analysis_xlsx.exists():
        return
    try:
        n_need = len(pd.read_excel(workflow_xlsx, sheet_name="需修改"))
        n_old = len(pd.read_excel(analysis_xlsx))
    except Exception:
        return
    if n_need == n_old:
        return
    print(
        f"[warn] Step1「需修改」{n_need} 条，已有 Step2 分析 {n_old} 条（差 {n_need - n_old:+d}）。"
        "「需修改分析」与 pdf_samples 不会随 Step1 自动更新；请去掉 --skip-step2 全量重跑，或执行 "
        "`python3 scripts/run_jiale_0412_pipeline.py --step2-only` 以补 Gemini/PDF。",
        file=sys.stderr,
    )
    try:
        need = pd.read_excel(workflow_xlsx, sheet_name="需修改")
        old = pd.read_excel(analysis_xlsx)
        if "商品" in need.columns and "商品" in old.columns:
            c1 = need["商品"].astype(str).value_counts()
            c2 = old["商品"].astype(str).value_counts()
            parts: list[str] = []
            for p in sorted(set(c1.index) | set(c2.index)):
                a, b = int(c1.get(p, 0)), int(c2.get(p, 0))
                if a != b:
                    parts.append(f"{p} {b}→{a}")
            if parts:
                print(f"[warn] 分商品需修改条数：{'; '.join(parts)}", file=sys.stderr)
    except Exception:
        pass


def main() -> None:
    args = parse_args()
    inp = args.input_dir
    out = args.out_dir
    out.mkdir(parents=True, exist_ok=True)
    all_pdfs = not args.legacy_sample_pdfs

    step1_xlsx = out / "家乐_workflow_0412-0512_总表与分表.xlsx"

    if not args.step2_only:
        xu_p = args.xuanyuan or (inp / "轩辕0412-0511.xlsx")
        ao_p = args.aochuang or (inp / "奥创0412-0511.xlsx")
        oss_p = args.oss or (inp / "oss_path.xlsx_asr_result.xlsx")
        for p in (xu_p, ao_p, oss_p):
            if not p.exists():
                raise FileNotFoundError(p)

        wf = load_wf_module()
        oss_map, dup_oss = load_oss(oss_p, wf)

        raw = pd.concat(
            [
                load_account(xu_p, "轩辕", wf),
                load_account(ao_p, "奥创", wf),
            ],
            ignore_index=True,
        )
        raw = raw[raw["商品"].isin(wf.TARGET_PRODUCTS)].copy()
        if raw.empty:
            raise RuntimeError("两表合并后没有命中目标商品（回味粉/小面酱/干锅酱/水煮酱）")

        detail_all = attach_oss_asr(raw, oss_map, wf)
        dc0 = pd.Timestamp(args.dashboard_create_start)
        dc1 = pd.Timestamp(args.dashboard_create_end)
        spend_lbl = (
            f"{CREATE_START.strftime('%Y-%m-%d')}～{CREATE_END.strftime('%Y-%m-%d')}"
            "（千川导出/复盘消耗窗口；标签 0412–0512；素材级指标）"
        )
        create_lbl = (
            f"{dc0.strftime('%Y-%m-%d')}～{dc1.strftime('%Y-%m-%d')}"
            "（看板「月创建/周」与「总体明细」的创建时间口径）"
        )

        global_mask = detail_all["素材创建时间"].notna() & detail_all["素材创建时间"].dt.normalize().between(
            dc0, dc1, inclusive="both"
        )
        detail_master = detail_all.loc[global_mask].copy()
        detail_master_fmt = jdm.format_master_detail_no_asr(detail_master, wf)

        m_spend = jdm.add_three_class(detail_all.copy(), wf)
        df_spend = jdm.format_board_excel_values(jdm.build_board_rows(m_spend, wf))
        sec_spend = jdm.DashboardSection(
            f"【月消耗看板】消耗时间：{spend_lbl}；创建时间：全部创建时间（本导出中出现的全部素材，不按创建日筛选）。",
            df_spend,
        )

        m_month = detail_all.loc[global_mask].copy()
        m_month_c = jdm.add_three_class(m_month, wf)
        df_month = jdm.format_board_excel_values(jdm.build_board_rows(m_month_c, wf))
        sec_month = jdm.DashboardSection(
            f"【月创建看板】消耗时间：{spend_lbl}；创建时间：{create_lbl}。",
            df_month,
        )

        sections: list[jdm.DashboardSection] = [sec_spend, sec_month]
        week_summaries: list[tuple[str, pd.DataFrame]] = []
        for w0, w1, lab in jdm.week_spans_in_range(dc0, dc1):
            wm = m_month_c["素材创建时间"].notna() & m_month_c["素材创建时间"].dt.normalize().between(
                w0, w1, inclusive="both"
            )
            wdf = m_month_c.loc[wm].copy()
            week_summaries.append((lab, wdf))
            sections.append(
                jdm.DashboardSection(
                    f"【每周新创建看板｜{lab}】消耗时间：{spend_lbl}；创建时间：{w0.strftime('%Y-%m-%d')}～{w1.strftime('%Y-%m-%d')}（周一至周日）。",
                    jdm.format_board_excel_values(jdm.build_board_rows(wdf, wf)),
                )
            )

        conclusion = jdm.build_conclusion_text(m_month_c, week_summaries, spend_lbl, create_lbl, wf)
        need_xlsx = out / "need_modify_analysis" / "需修改分析_Gemini双脚本.xlsx"
        md_text = jdm.build_md_report(
            m_month_c,
            week_summaries,
            spend_lbl,
            create_lbl,
            wf,
            need_xlsx if need_xlsx.exists() else None,
        )
        (out / "编导读数分析报告.md").write_text(md_text, encoding="utf-8")

        win_mask = detail_all["素材创建时间"].notna() & detail_all["素材创建时间"].dt.normalize().between(
            CREATE_START, CREATE_END, inclusive="both"
        )
        detail_win = detail_all.loc[win_mask].copy()
        if detail_win.empty:
            raise RuntimeError(
                f"创建时间在 {CREATE_START.date()}～{CREATE_END.date()} 的素材为空，请检查日期或输入表"
            )

        hist_mask = detail_all.apply(
            lambda r: wf.classify_result(str(r["商品"]), r["GMV"]) == "可复制",
            axis=1,
        )
        detail_hist = detail_all.loc[hist_mask].copy()

        fmt_win = wf.format_detail(detail_win.copy())
        fmt_hist = wf.format_detail(detail_hist.copy())

        mod_need = fmt_win.loc[fmt_win["三类结果"] == "需修改", "商品"].value_counts().sort_index()
        n_modify = int(mod_need.sum())

        write_step1_excel(step1_xlsx, detail_master_fmt, fmt_win, fmt_hist, sections, conclusion)
        maybe_apply_workflow_pdf_oss_urls(step1_xlsx)

        summary_lines = [
            "# 0412-0512 流水线 Step1",
            "",
            f"- 输入目录：`{inp}`",
            f"- OSS 重复素材ID行数（去重前）：{dup_oss}",
            f"- 全量（目标商品）行数：`{len(detail_all)}`",
            f"- 总体明细创建窗口 {create_lbl} 行数：`{len(detail_master)}`",
            f"- 需修改主窗口创建 {CREATE_START.date()}～{CREATE_END.date()} 行数：`{len(detail_win)}`",
            f"- **「需修改」合计**：`{n_modify}`（分商品：`{mod_need.to_dict()}`）",
            f"- 历史跑量爆款（可复制阈值）行数：`{len(detail_hist)}`",
            f"- Step1 输出：`{step1_xlsx}`",
            f"- 编导读数分析报告：`{out / '编导读数分析报告.md'}`",
            "",
            "若需每次 Step1 后自动在「需修改」写入 `样例PDF_OSS_URL`，可设置环境变量 `JIALE_WORKFLOW_PDF_OSS_BASE` 为 OSS 目录 URL 前缀（与 `merge_need_modify_pdf_oss_urls.py --auto-oss-base` 相同）。",
            "若 Step1「需修改」条数大于已有 `need_modify_analysis/需修改分析_Gemini双脚本.xlsx`，请重跑 Step2（勿加 `--skip-step2` 或单独 `--step2-only`）以生成全量 PDF。",
        ]
        (out / "step1_summary.md").write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
        print(step1_xlsx)

        if args.skip_step2:
            warn_step2_stale(step1_xlsx, out / "need_modify_analysis")
            return
    else:
        if not step1_xlsx.exists():
            raise FileNotFoundError(f"--step2-only 需要已有 Step1 文件: {step1_xlsx}")

    if not CALL_GEMINI.exists():
        print(f"[warn] 未找到 call_gemini.py，跳过 Step2: {CALL_GEMINI}", file=sys.stderr)
        return

    prompts = WORKSPACE / "prompts"
    if not prompts.exists():
        raise FileNotFoundError(f"缺少 prompts 目录: {prompts}")

    step2_dir = out / "need_modify_analysis"
    step2_dir.mkdir(parents=True, exist_ok=True)
    warn_step2_stale(step1_xlsx, step2_dir)
    analyze_dst = out / "_analyze_need_modify_generated.py"
    patch_analyze_script(analyze_dst, step1_xlsx, step2_dir, prompts, all_pdfs=all_pdfs)

    extra = [
        "--max-workers",
        str(args.max_workers),
        "--chunk-size",
        str(args.chunk_size),
        "--sample-pdf-per-product",
        str(args.sample_pdf_per_product),
    ]
    if args.force_refresh:
        extra.append("--force-refresh")
    run_step2(analyze_dst, extra)
    print(step2_dir / "需修改分析_Gemini双脚本.xlsx")

    done = out / "ALL_NEED_MODIFY_PDFS_DONE.txt"
    pdf_dir = step2_dir / "pdf_samples"
    n_pdf = len(list(pdf_dir.glob("*.pdf"))) if pdf_dir.exists() else 0
    step2_extra_lines: list[str] = []
    try:
        adf = pd.read_excel(step2_dir / "需修改分析_Gemini双脚本.xlsx")
        gemini_vc = adf["gemini状态"].value_counts().to_dict()
        pdf_ok = adf[adf.get("是否生成PDF样例") == "是"]
        pdf_by_prod = pdf_ok["商品"].value_counts().sort_index().to_dict() if "商品" in pdf_ok.columns else {}
        pdf_fail = int((adf.get("是否生成PDF样例") == "失败").sum()) if "是否生成PDF样例" in adf.columns else 0
        step2_extra_lines = [
            "",
            "## Step2 摘要",
            "",
            f"- gemini 状态统计：`{gemini_vc}`",
            f"- PDF 生成成功（按商品）：`{pdf_by_prod}`",
            f"- PDF 标记为失败行数：`{pdf_fail}`",
        ]
        (out / "step2_summary.md").write_text(
            "\n".join(
                [
                    "# 0412-0512 流水线 Step2",
                    "",
                    f"- 完成时间：`{pd.Timestamp.now()}`",
                    f"- 全量 PDF 模式：`{all_pdfs}`",
                    f"- 分析结果：`{step2_dir / '需修改分析_Gemini双脚本.xlsx'}`",
                    f"- PDF 目录：`{pdf_dir}`（glob 计数 {n_pdf}）",
                    f"- gemini 状态：`{gemini_vc}`",
                    f"- PDF 成功按商品：`{pdf_by_prod}`",
                    f"- PDF 失败行数：`{pdf_fail}`",
                    "",
                ]
            ),
            encoding="utf-8",
        )
    except Exception as exc:
        step2_extra_lines = ["", f"## Step2 摘要（读取结果失败：{exc}）", ""]
        (out / "step2_summary.md").write_text(
            f"# Step2\n\n读取 `需修改分析_Gemini双脚本.xlsx` 失败：{exc}\n",
            encoding="utf-8",
        )

    done.write_text(
        f"step2_finished_at={pd.Timestamp.now()}\n"
        f"all_pdfs_mode={all_pdfs}\n"
        f"pdf_count_glob={n_pdf}\n"
        f"output_xlsx={step2_dir / '需修改分析_Gemini双脚本.xlsx'}\n"
        + "\n".join(step2_extra_lines)
        + "\n",
        encoding="utf-8",
    )

    if not CHROME_BIN.exists():
        print(f"[warn] 未检测到 Chrome，PDF 可能无法生成: {CHROME_BIN}", file=sys.stderr)


if __name__ == "__main__":
    main()
