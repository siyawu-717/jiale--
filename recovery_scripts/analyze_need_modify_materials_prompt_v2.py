#!/usr/bin/env python3
from __future__ import annotations

import argparse
import html
import importlib.util
import json
import re
import subprocess
from pathlib import Path
from typing import Any, Dict, Iterable

import pandas as pd


ROOT = Path("/Users/siya/Desktop/家乐")
DATA_ROOT = ROOT / "家乐千川数据分析"
WORKFLOW_XLSX = DATA_ROOT / "output/上上周新创建素材分析/上上周新创建素材_workflow结果_0405-0412_消耗0321-0419.xlsx"
OLD_CONTEXT_CACHE_DIR = DATA_ROOT / "output/上上周新创建素材分析/上上周新创建素材_需修改全量分析_20260422/gemini_context_cache"
ASR_CACHE_DIR = DATA_ROOT / "output/上上周新创建素材分析/workflow_asr_cache_0405-0412"
CALL_GEMINI = Path("/Users/siya/.codex/skills/gemini-las-video/scripts/call_gemini.py")
RUN_ROOT = ROOT / "交付归档/上上周需修改分析_新prompt双脚本_20260422"
PROMPTS_DIR = RUN_ROOT / "prompts"
OUTPUT_DIR = RUN_ROOT / "outputs"
CACHE_DIR = OUTPUT_DIR / "cache"
HTML_DIR = OUTPUT_DIR / "html_samples"
PDF_DIR = OUTPUT_DIR / "pdf_samples"
OUTPUT_XLSX = OUTPUT_DIR / "上上周新创建素材_需修改分析_新prompt双脚本_20260422.xlsx"
OUTPUT_CSV = OUTPUT_DIR / "上上周新创建素材_需修改分析_新prompt双脚本_20260422.csv"
OUTPUT_MD = OUTPUT_DIR / "上上周新创建素材_需修改分析_新prompt双脚本_20260422.md"
PROMPT_DIAG = PROMPTS_DIR / "prompt_01_数据定位与修改建议.txt"
PROMPT_SCRIPT1 = PROMPTS_DIR / "prompt_02_完整脚本1.txt"
PROMPT_SCRIPT2 = PROMPTS_DIR / "prompt_03_完整脚本2_微润色.txt"
PROMPT_EXEC = PROMPTS_DIR / "prompt_04_执行版_诊断+双脚本.txt"
CHROME_BIN = Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")

SYSTEM_PROMPT = (
    "你是餐饮ToB千川视频优化专家。"
    "你需要看当前视频和对标视频，但必须严格服从给定数据和原视频ASR。"
    "你的输出必须具体、可执行，像业务同学写给编导的修改单。"
    "不要输出分析过程。"
)

KEYWORD_GROUPS: Dict[str, Dict[str, list[str]]] = {
    "回味粉": {
        "牛肉汤": ["牛肉汤", "牛肉粉", "牛骨汤", "骨汤", "羊汤", "馄饨", "汤底", "汤"],
        "肉馅": ["肉馅", "包子", "饺子", "馅", "肉片"],
        "卤味": ["卤", "卤味", "卤汤", "卤肉", "嗦骨头"],
        "烧烤": ["烧烤", "烤", "串", "煎肉", "铁板", "夜市", "五花"],
        "辣椒油": ["辣椒油", "红油", "凉皮"],
        "炒粉": ["炒粉", "河粉", "米粉", "炒饭", "炒菜"],
    },
    "干锅酱": {
        "土豆": ["土豆", "土豆片"],
        "五花肉": ["五花", "五花肉"],
        "虾": ["虾", "大虾", "蘑菇虾"],
        "鸡翅": ["鸡翅", "鸡"],
        "花菜": ["花菜", "包菜", "蔬菜"],
        "麻辣香锅": ["麻辣香锅", "香锅"],
        "干锅": ["干锅"],
    },
    "水煮酱": {
        "肉片": ["水煮肉片", "肉片"],
        "牛肉": ["水煮牛肉", "牛肉"],
        "鱼": ["鱼", "水煮鱼"],
        "虾": ["虾", "大虾"],
        "水煮": ["水煮"],
    },
    "小面酱": {
        "重庆小面": ["重庆小面", "豌杂", "豌杂面", "担担面", "碱面"],
        "拌面": ["拌面", "葱油拌面", "凉面", "燃面"],
        "汤面": ["汤面", "牛肉面", "面汤", "高汤"],
        "小面": ["小面", "小面酱"],
        "浇头": ["浇头", "杂酱", "肉酱", "臊子"],
    },
}


def safe_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def normalize_id(value: Any) -> str:
    text = safe_str(value)
    return text[:-2] if text.endswith(".0") else text


def safe_float(value: Any) -> float:
    text = safe_str(value).replace(",", "")
    if not text or text.lower() == "nan":
        return 0.0
    if text.endswith("%"):
        text = text[:-1]
        try:
            return float(text) / 100
        except ValueError:
            return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def pct_text(value: Any) -> str:
    return f"{safe_float(value) * 100:.2f}%"


def num_text(value: Any) -> str:
    num = safe_float(value)
    if float(num).is_integer():
        return f"{int(num):,}"
    return f"{num:,.2f}"


def normalize_text(text: str) -> str:
    return re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", "", safe_str(text))


def char_ngrams(text: str, n: int = 2) -> set[str]:
    clean = normalize_text(text)
    if not clean:
        return set()
    if len(clean) <= n:
        return {clean}
    return {clean[i : i + n] for i in range(len(clean) - n + 1)}


def jaccard(a: Iterable[str], b: Iterable[str]) -> float:
    sa = set(a)
    sb = set(b)
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


def extract_tags(title: str, product: str) -> set[str]:
    groups = KEYWORD_GROUPS.get(product, {})
    text = safe_str(title)
    tags: set[str] = set()
    for group, keywords in groups.items():
        if any(keyword in text for keyword in keywords):
            tags.add(group)
    if not tags:
        tags.add("通用")
    return tags


def benchmark_score(row: pd.Series, candidate: pd.Series, top_gmv: float) -> float:
    row_tags = extract_tags(row["素材名称"], row["商品"])
    cand_tags = extract_tags(candidate["素材名称"], candidate["商品"])
    overlap = len(row_tags & cand_tags)
    ngram_score = jaccard(char_ngrams(row["素材名称"]), char_ngrams(candidate["素材名称"]))
    same_account_bonus = 0.3 if safe_str(row.get("账号")) == safe_str(candidate.get("账号")) else 0.0
    gmv_bonus = (candidate["GMV"] / top_gmv) if top_gmv > 0 else 0.0
    if row_tags == {"通用"}:
        overlap = 0
    return overlap * 10 + ngram_score * 5 + same_account_bonus + gmv_bonus


def compute_product_means(total_df: pd.DataFrame) -> Dict[str, Dict[str, float]]:
    metric_cols = ["GMV", "ROI", "千次播放GMV", "CTR", "CVR", "3s完播率", "5s完播率", "10s完播率"]
    means: Dict[str, Dict[str, float]] = {}
    for product, sub in total_df.groupby("商品"):
        means[product] = {f"{col}_均值": float(sub[col].mean()) for col in metric_cols}
    return means


def load_workflow_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict[str, Dict[str, float]]]:
    total_df = pd.read_excel(WORKFLOW_XLSX, sheet_name="总明细表", dtype=str)
    need_df = pd.read_excel(WORKFLOW_XLSX, sheet_name="需修改", dtype=str)
    copy_df = pd.read_excel(WORKFLOW_XLSX, sheet_name="可复制", dtype=str)
    for frame in (total_df, need_df, copy_df):
        frame["素材ID"] = frame["素材ID"].map(normalize_id)
        for col in ["GMV", "消耗", "ROI", "播放", "千次播放GMV", "CTR", "CVR", "3s完播率", "5s完播率", "10s完播率"]:
            frame[col] = frame[col].map(safe_float)
    need_df = need_df.drop_duplicates(subset=["素材ID"]).reset_index(drop=True)
    return total_df, need_df, copy_df, compute_product_means(total_df)


def select_benchmark(row: pd.Series, copy_df: pd.DataFrame) -> pd.Series:
    same_product = copy_df[copy_df["商品"] == row["商品"]].copy()
    if same_product.empty and row["商品"] in {"干锅酱", "水煮酱", "小面酱"}:
        same_product = copy_df[copy_df["商品"] == "回味粉"].copy()
    same_product = same_product[same_product["素材ID"] != row["素材ID"]].copy()
    if same_product.empty:
        raise ValueError(f"商品 {row['商品']} 没有可复制对标")
    top_gmv = float(same_product["GMV"].max()) if not same_product.empty else 0.0
    same_product["benchmark_score"] = same_product.apply(lambda cand: benchmark_score(row, cand, top_gmv), axis=1)
    same_product = same_product.sort_values(["benchmark_score", "GMV"], ascending=[False, False])
    return same_product.iloc[0]


def load_call_gemini_module() -> Any:
    spec = importlib.util.spec_from_file_location("skill_call_gemini", CALL_GEMINI)
    if spec is None or spec.loader is None:
        raise RuntimeError("无法加载 call_gemini.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def unwrap_message(payload: Any) -> Dict[str, Any]:
    current = payload
    for _ in range(8):
        if not isinstance(current, dict):
            break
        if "message" in current and isinstance(current["message"], dict):
            current = current["message"]
            continue
        if "result" in current and isinstance(current["result"], dict):
            current = current["result"]
            continue
        if "data" in current and isinstance(current["data"], dict):
            current = current["data"]
            continue
        break
    return current if isinstance(current, dict) else {}


def response_schema() -> Dict[str, Any]:
    return {
        "type": "OBJECT",
        "properties": {
            "message": {
                "type": "OBJECT",
                "properties": {
                    "data_issue": {"type": "STRING"},
                    "fix_advice": {"type": "STRING"},
                    "script_v1": {"type": "STRING"},
                    "script_v2": {"type": "STRING"},
                },
                "required": ["data_issue", "fix_advice", "script_v1", "script_v2"],
            }
        },
        "required": ["message"],
    }


def fill_prompt(template: str, mapping: Dict[str, Any]) -> str:
    prompt = template
    for key, value in mapping.items():
        prompt = prompt.replace(f"{{{{{key}}}}}", safe_str(value))
    return prompt


def old_context_asr(material_id: str) -> str:
    path = OLD_CONTEXT_CACHE_DIR / f"{material_id}.json"
    if not path.exists():
        return ""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return ""
    return safe_str(data.get("current_asr"))


def cached_asr(material_id: str) -> str:
    existing = old_context_asr(material_id)
    if existing:
        return existing
    for path in ASR_CACHE_DIR.glob(f"{material_id}_*.json"):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        payload = data.get("payload")
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                payload = {}
        payload = unwrap_message(payload)
        text = safe_str(payload.get("asr"))
        if text:
            return text
        raw_result = data.get("raw_result", {})
        raw_payload = raw_result.get("result")
        if isinstance(raw_payload, str):
            try:
                raw_payload = json.loads(raw_payload)
            except json.JSONDecodeError:
                raw_payload = {}
        raw_payload = unwrap_message(raw_payload)
        text = safe_str(raw_payload.get("asr"))
        if text:
            return text
    return ""


def build_benchmark_info(common: Dict[str, Any]) -> str:
    lines = [
        f"- 对标素材ID：{safe_str(common.get('对标_素材ID'))}",
        f"- 对标素材名称：{safe_str(common.get('benchmark_素材名称'))}",
        f"- 对标GMV：{num_text(common.get('对标_GMV'))}",
        f"- 对标视频OSS：{safe_str(common.get('对标视频oss'))}",
    ]
    return "\n".join(lines)


def task_prompt(template: str, common: Dict[str, Any], mean_map: Dict[str, float]) -> str:
    mapping = {
        "素材ID": common["素材ID"],
        "素材名称": common["素材名称"],
        "商品": common["商品"],
        "原视频ASR": common["原视频ASR"],
        "3秒播放率": pct_text(common["3s完播率"]),
        "5秒播放率": pct_text(common["5s完播率"]),
        "10秒播放率": pct_text(common["10s完播率"]),
        "CTR": pct_text(common["CTR"]),
        "CVR": pct_text(common["CVR"]),
        "ROI": num_text(common["ROI"]),
        "千次播放GMV": num_text(common["千次播放GMV"]),
        "3秒均值": pct_text(mean_map["3s完播率_均值"]),
        "5秒均值": pct_text(mean_map["5s完播率_均值"]),
        "10秒均值": pct_text(mean_map["10s完播率_均值"]),
        "CTR均值": pct_text(mean_map["CTR_均值"]),
        "CVR均值": pct_text(mean_map["CVR_均值"]),
        "ROI均值": num_text(mean_map["ROI_均值"]),
        "千次播放GMV均值": num_text(mean_map["千次播放GMV_均值"]),
        "对标信息": build_benchmark_info(common),
    }
    return fill_prompt(template, mapping)


def cache_path(material_id: str) -> Path:
    return CACHE_DIR / f"{material_id}.json"


def load_cached_output(material_id: str) -> Dict[str, Any] | None:
    path = cache_path(material_id)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def save_cached_output(material_id: str, payload: Dict[str, Any]) -> None:
    cache_path(material_id).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_result(raw_result: Dict[str, Any]) -> Dict[str, str]:
    payload = raw_result.get("result")
    if isinstance(payload, str):
        payload = json.loads(payload)
    message = unwrap_message(payload)
    return {
        "数据定位问题": safe_str(message.get("data_issue")),
        "具体修改建议": safe_str(message.get("fix_advice")),
        "gemini优化后完整脚本建议": safe_str(message.get("script_v1")),
        "gemini优化后完整脚本建议2（微润色版）": safe_str(message.get("script_v2")),
    }


def render_multiline(text: str) -> str:
    return html.escape(safe_str(text)).replace("\n", "<br>\n")


def render_html(row: Dict[str, Any]) -> str:
    metric_cards = [
        ("发布时间", safe_str(row.get("发布时间")) or "-"),
        ("GMV", num_text(row.get("GMV"))),
        ("消耗", num_text(row.get("消耗"))),
        ("播放", num_text(row.get("播放"))),
        ("ROI", num_text(row.get("ROI"))),
        ("千次播放GMV", num_text(row.get("千次播放GMV"))),
        ("CTR", pct_text(row.get("CTR"))),
        ("CVR", pct_text(row.get("CVR"))),
        ("3s完播率", pct_text(row.get("3s完播率"))),
        ("5s完播率", pct_text(row.get("5s完播率"))),
        ("10s完播率", pct_text(row.get("10s完播率"))),
    ]
    cards_html = "\n".join(
        f'<div class="metric-card"><div class="metric-label">{html.escape(label)}</div><div class="metric-value">{html.escape(value)}</div></div>'
        for label, value in metric_cards
    )
    original_block = (
        f'<div><strong>原视频：</strong>{html.escape(safe_str(row.get("素材名称")))}</div>'
        f'<div><strong>素材ID：</strong>{html.escape(safe_str(row.get("素材ID")))}</div>'
        f'<div><a href="{html.escape(safe_str(row.get("原视频oss")))}" target="_blank" rel="noreferrer">{html.escape(safe_str(row.get("原视频oss")))}</a></div>'
    )
    benchmark_block = (
        f'<div><strong>对标视频：</strong>{html.escape(safe_str(row.get("benchmark_素材名称")) or safe_str(row.get("对标_素材ID")))}</div>'
        f'<div><strong>对标素材ID：</strong>{html.escape(safe_str(row.get("对标_素材ID")) or "-")}</div>'
        f'<div><strong>对标GMV：</strong>{html.escape(num_text(row.get("对标_GMV")))}</div>'
        f'<div><a href="{html.escape(safe_str(row.get("对标视频oss")))}" target="_blank" rel="noreferrer">{html.escape(safe_str(row.get("对标视频oss")))}</a></div>'
    )
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(safe_str(row.get("素材ID")))} - 素材分析卡</title>
  <style>
    :root {{
      --bg: #f4f1e8;
      --paper: #fffdf8;
      --ink: #1d1b18;
      --muted: #73685b;
      --line: #d8cdbd;
      --accent: #b4492d;
      --accent-soft: #f0dfd4;
      --chip: #efe7da;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "PingFang SC", "Noto Sans SC", "Microsoft YaHei", sans-serif;
      background:
        radial-gradient(circle at top left, #efe0cf 0, transparent 28%),
        radial-gradient(circle at top right, #d7e3d1 0, transparent 22%),
        var(--bg);
      color: var(--ink);
      line-height: 1.6;
    }}
    .page {{
      width: min(1080px, calc(100vw - 36px));
      margin: 16px auto 24px;
      padding: 20px;
      background: var(--paper);
      border: 1px solid var(--line);
      box-shadow: 0 18px 60px rgba(70, 49, 28, 0.10);
      border-radius: 22px;
    }}
    .hero {{
      display: grid;
      grid-template-columns: 1.6fr 1fr;
      gap: 14px;
      align-items: start;
      margin-bottom: 14px;
    }}
    .hero-card {{
      padding: 16px;
      border-radius: 16px;
      border: 1px solid var(--line);
      background: linear-gradient(180deg, rgba(255,255,255,0.95), rgba(250,245,238,0.95));
    }}
    .eyebrow {{
      font-size: 12px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: var(--accent);
      font-weight: 700;
      margin-bottom: 8px;
    }}
    h1 {{
      margin: 0 0 8px;
      font-size: 24px;
      line-height: 1.25;
    }}
    .meta {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 8px;
    }}
    .chip {{
      padding: 5px 10px;
      border-radius: 999px;
      background: var(--chip);
      color: var(--muted);
      font-size: 12px;
      border: 1px solid var(--line);
    }}
    .status {{
      background: var(--accent-soft);
      color: var(--accent);
      border-color: #d9b6a4;
      font-weight: 700;
    }}
    .metrics {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 8px;
    }}
    .metric-card {{
      padding: 8px 10px;
      border-radius: 12px;
      border: 1px solid var(--line);
      background: #fff;
      min-height: 60px;
    }}
    .metric-label {{
      color: var(--muted);
      font-size: 11px;
      margin-bottom: 4px;
      line-height: 1.25;
    }}
    .metric-value {{
      font-size: 16px;
      font-weight: 700;
      line-height: 1.15;
      word-break: break-word;
    }}
    .section {{
      margin-top: 12px;
      padding: 14px 16px;
      border-radius: 16px;
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.75);
    }}
    .section h2 {{
      margin: 0 0 8px;
      font-size: 16px;
    }}
    .links {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
    }}
    .link-card {{
      padding: 12px;
      border-radius: 12px;
      background: #fff;
      border: 1px solid var(--line);
      font-size: 12px;
      line-height: 1.45;
    }}
    a {{
      color: var(--accent);
      word-break: break-all;
      text-decoration: none;
    }}
    .copy {{
      white-space: pre-wrap;
      font-size: 13px;
      line-height: 1.5;
    }}
    .script {{
      white-space: pre-wrap;
      background: #fcf8f1;
      border: 1px dashed #d8c8b1;
      padding: 12px;
      border-radius: 12px;
      font-size: 13px;
      line-height: 1.5;
    }}
    @media print {{
      @page {{ size: A4; margin: 10mm; }}
      body {{ background: #fff; }}
      .page {{ width: auto; margin: 0; box-shadow: none; border: none; padding: 0; }}
      .metrics {{ grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 6px; }}
      .metric-card {{ padding: 7px 8px; min-height: 52px; }}
      .metric-label {{ font-size: 10px; margin-bottom: 3px; }}
      .metric-value {{ font-size: 14px; }}
      .section {{ margin-top: 8px; padding: 10px 12px; }}
      .section h2 {{ font-size: 15px; margin-bottom: 6px; }}
      .copy, .script {{ font-size: 12px; line-height: 1.4; }}
      a {{ color: #000; text-decoration: none; }}
    }}
  </style>
</head>
<body>
  <main class="page">
    <section class="hero">
      <div class="hero-card">
        <div class="eyebrow">Material Brief</div>
        <h1>{html.escape(safe_str(row.get("素材名称")))}</h1>
        <div class="meta">
          <span class="chip">{html.escape(safe_str(row.get("商品")))}</span>
          <span class="chip">{html.escape(safe_str(row.get("账号")))}</span>
          <span class="chip">{html.escape(safe_str(row.get("素材ID")))}</span>
          <span class="chip status">需修改样例</span>
        </div>
      </div>
      <div class="hero-card">
        <div class="eyebrow">核心指标</div>
        <div class="metrics">
          {cards_html}
        </div>
      </div>
    </section>

    <section class="section">
      <h2>视频链接</h2>
      <div class="links">
        <div class="link-card">
          <div class="eyebrow">原视频 OSS</div>
          {original_block}
        </div>
        <div class="link-card">
          <div class="eyebrow">对标视频 OSS</div>
          {benchmark_block}
        </div>
      </div>
    </section>

    <section class="section">
      <h2>数据定位问题</h2>
      <div class="copy">{render_multiline(safe_str(row.get("数据定位问题")))}</div>
    </section>

    <section class="section">
      <h2>原视频 ASR</h2>
      <div class="copy">{render_multiline(safe_str(row.get("原视频ASR")))}</div>
    </section>

    <section class="section">
      <h2>Gemini 修改建议</h2>
      <div class="copy">{render_multiline(safe_str(row.get("具体修改建议")))}</div>
    </section>

    <section class="section">
      <h2>gemini优化后完整脚本建议</h2>
      <div class="script">{render_multiline(safe_str(row.get("gemini优化后完整脚本建议")))}</div>
    </section>

    <section class="section">
      <h2>gemini优化后完整脚本建议2（微润色版）</h2>
      <div class="script">{render_multiline(safe_str(row.get("gemini优化后完整脚本建议2（微润色版）")))}</div>
    </section>
  </main>
</body>
</html>
"""


def write_html(row: Dict[str, Any]) -> Path:
    path = HTML_DIR / f"{safe_str(row['素材ID'])}.html"
    path.write_text(render_html(row), encoding="utf-8")
    return path


def export_pdf(material_id: str) -> Path:
    html_path = HTML_DIR / f"{material_id}.html"
    out_path = PDF_DIR / f"{material_id}.pdf"
    subprocess.run(
        [
            str(CHROME_BIN),
            "--headless=new",
            "--disable-gpu",
            f"--print-to-pdf={out_path}",
            html_path.as_uri(),
        ],
        check=True,
    )
    return out_path


def prepare_dirs() -> None:
    for path in [OUTPUT_DIR, CACHE_DIR, HTML_DIR, PDF_DIR]:
        path.mkdir(parents=True, exist_ok=True)


def build_base_records() -> list[Dict[str, Any]]:
    total_df, need_df, copy_df, mean_map_all = load_workflow_data()
    records: list[Dict[str, Any]] = []
    for _, row in need_df.iterrows():
        bench = select_benchmark(row, copy_df)
        common = {
            "商品": row["商品"],
            "账号": row["账号"],
            "素材ID": row["素材ID"],
            "素材名称": row["素材名称"],
            "发布时间": safe_str(row.get("发布时间") or row.get("素材创建时间")),
            "原视频oss": safe_str(row.get("oss_path")),
            "对标_素材ID": safe_str(bench.get("素材ID")),
            "benchmark_素材名称": safe_str(bench.get("素材名称")),
            "对标_GMV": float(bench.get("GMV", 0.0)),
            "对标视频oss": safe_str(bench.get("oss_path")),
            "GMV": float(row.get("GMV", 0.0)),
            "消耗": float(row.get("消耗", 0.0)),
            "ROI": float(row.get("ROI", 0.0)),
            "播放": float(row.get("播放", 0.0)),
            "千次播放GMV": float(row.get("千次播放GMV", 0.0)),
            "CTR": float(row.get("CTR", 0.0)),
            "CVR": float(row.get("CVR", 0.0)),
            "3s完播率": float(row.get("3s完播率", 0.0)),
            "5s完播率": float(row.get("5s完播率", 0.0)),
            "10s完播率": float(row.get("10s完播率", 0.0)),
            "原视频ASR": cached_asr(row["素材ID"]),
            "三类结果": safe_str(row.get("三类结果") or "需修改"),
            "_mean_map": mean_map_all[row["商品"]],
        }
        records.append(common)
    return records


def build_tasks(records: list[Dict[str, Any]], force_refresh: bool) -> tuple[list[Dict[str, Any]], list[Dict[str, Any]]]:
    template = PROMPT_EXEC.read_text(encoding="utf-8")
    tasks: list[Dict[str, Any]] = []
    metas: list[Dict[str, Any]] = []
    for record in records:
        cached = None if force_refresh else load_cached_output(record["素材ID"])
        if cached is not None:
            record.update(cached)
            record["gemini状态"] = "cached"
            record["gemini错误"] = ""
            continue
        prompt = task_prompt(template, record, record["_mean_map"])
        tasks.append(
            {
                "proxy_base": "https://las-operator.runix.ai",
                "tos_base": "https://mogic-collect.tos-cn-beijing.volces.com",
                "model_name": "gemini-2.5-flash",
                "system_prompt": SYSTEM_PROMPT,
                "user_message": prompt,
                "oss_links": [safe_str(record.get("原视频oss")), safe_str(record.get("对标视频oss"))],
                "response_schema": response_schema(),
            }
        )
        metas.append(record)
    return tasks, metas


def run_gemini(tasks: list[Dict[str, Any]], max_workers: int, submit_qps: float, poll_qps: float) -> list[Dict[str, Any]]:
    module = load_call_gemini_module()
    return module.run_video_chat_batch(
        tasks=tasks,
        max_workers=max_workers,
        submit_qps=submit_qps,
        poll_qps=poll_qps,
        force_connection_close=False,
    )


def analyze_records(records: list[Dict[str, Any]], args: argparse.Namespace) -> list[Dict[str, Any]]:
    tasks, metas = build_tasks(records, force_refresh=args.force_refresh)
    if tasks:
        for start in range(0, len(tasks), max(1, args.chunk_size)):
            task_chunk = tasks[start : start + max(1, args.chunk_size)]
            meta_chunk = metas[start : start + max(1, args.chunk_size)]
            print(f"[gemini] chunk {start // max(1, args.chunk_size) + 1} / {(len(tasks) - 1) // max(1, args.chunk_size) + 1}", flush=True)
            result_chunk = run_gemini(task_chunk, max_workers=args.max_workers, submit_qps=args.submit_qps, poll_qps=args.poll_qps)
            for meta, raw in zip(meta_chunk, result_chunk):
                if raw.get("ok"):
                    parsed = parse_result(raw)
                    meta.update(parsed)
                    meta["gemini状态"] = "ok"
                    meta["gemini错误"] = ""
                    save_cached_output(meta["素材ID"], parsed)
                else:
                    meta["数据定位问题"] = ""
                    meta["具体修改建议"] = ""
                    meta["gemini优化后完整脚本建议"] = ""
                    meta["gemini优化后完整脚本建议2（微润色版）"] = ""
                    meta["gemini状态"] = "failed"
                    meta["gemini错误"] = safe_str(raw.get("error") or "gemini_failed")
    for record in records:
        record.pop("_mean_map", None)
        record.setdefault("数据定位问题", "")
        record.setdefault("具体修改建议", "")
        record.setdefault("gemini优化后完整脚本建议", "")
        record.setdefault("gemini优化后完整脚本建议2（微润色版）", "")
        record.setdefault("gemini状态", "cached" if record.get("数据定位问题") else "pending")
        record.setdefault("gemini错误", "")
        record.setdefault("是否生成PDF样例", "")
        record.setdefault("样例PDF文件名", "")
        record.setdefault("样例PDF本地路径", "")
    return records


def generate_sample_pdfs(df: pd.DataFrame, sample_per_product: int) -> pd.DataFrame:
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


def save_outputs(df: pd.DataFrame) -> None:
    ordered_cols = [
        "商品",
        "账号",
        "素材ID",
        "素材名称",
        "发布时间",
        "原视频oss",
        "原视频ASR",
        "对标_素材ID",
        "benchmark_素材名称",
        "对标_GMV",
        "对标视频oss",
        "GMV",
        "消耗",
        "ROI",
        "播放",
        "千次播放GMV",
        "CTR",
        "CVR",
        "3s完播率",
        "5s完播率",
        "10s完播率",
        "数据定位问题",
        "具体修改建议",
        "gemini优化后完整脚本建议",
        "gemini优化后完整脚本建议2（微润色版）",
        "是否生成PDF样例",
        "样例PDF文件名",
        "样例PDF本地路径",
        "gemini状态",
        "gemini错误",
    ]
    df = df[ordered_cols]
    df.to_excel(OUTPUT_XLSX, index=False)
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    lines = [
        "# 上上周需修改素材分析（新prompt双脚本版）",
        "",
        f"- 素材条数：{len(df)}",
        f"- Excel：`{OUTPUT_XLSX}`",
        f"- CSV：`{OUTPUT_CSV}`",
        f"- PDF样例目录：`{PDF_DIR}`",
        f"- HTML样例目录：`{HTML_DIR}`",
        "",
        "gemini状态统计：",
    ]
    for status, count in df["gemini状态"].value_counts(dropna=False).items():
        lines.append(f"- {status}: {count}")
    lines.extend(["", "PDF样例统计："])
    for product, count in df[df["是否生成PDF样例"] == "是"]["商品"].value_counts().items():
        lines.append(f"- {product}: {count}")
    OUTPUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="按新prompt批量分析上上周需修改素材，并生成样例PDF")
    parser.add_argument("--max-workers", type=int, default=2)
    parser.add_argument("--chunk-size", type=int, default=4)
    parser.add_argument("--submit-qps", type=float, default=2.0)
    parser.add_argument("--poll-qps", type=float, default=8.0)
    parser.add_argument("--sample-pdf-per-product", type=int, default=2)
    parser.add_argument("--force-refresh", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    prepare_dirs()
    records = build_base_records()
    records = analyze_records(records, args)
    df = pd.DataFrame(records).sort_values(["商品", "账号", "素材ID"]).reset_index(drop=True)
    df = generate_sample_pdfs(df, sample_per_product=args.sample_pdf_per_product)
    save_outputs(df)
    print(OUTPUT_XLSX)
    print(OUTPUT_CSV)
    print(PDF_DIR)


if __name__ == "__main__":
    main()
