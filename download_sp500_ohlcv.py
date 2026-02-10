"""
S&P 500 3-year OHLCV 다운로드 + 클린 + 티커별 CSV + ZIP

사용법:
    pip install -r requirements.txt
    python download_sp500_ohlcv.py

주요 수정사항 (원본 Colab 대비):
- yf.download() → yf.Ticker().history() 로 변경
  (yf.download()는 단일 티커에도 MultiIndex 컬럼을 반환하여 컬럼 매핑 실패)
- auto_adjust=False 로 변경하여 원본 OHLCV 보존
  (auto_adjust=True 사용 시 TSLA, NVDA, AMZN, GOOGL 등
   주식분할 종목에서 실제 거래가와 다른 보정값이 출력됨)
- Adj Close 컬럼 별도 보관 (분석 시 활용 가능)
- Wikipedia 티커 로드 시 User-Agent 설정으로 403 방지
"""

import os
import glob
import time
import re
import zipfile
import logging

import pandas as pd
import yfinance as yf
import requests
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── 로깅 설정 ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── 1) S&P 500 티커 리스트 로드 (Wikipedia API) ────────────
def load_sp500_tickers() -> list[str]:
    url = "https://en.wikipedia.org/api/rest_v1/page/html/List_of_S%26P_500_companies"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (compatible; sp500-fetch/1.0; "
            "+https://github.com/)"
        )
    }
    html = requests.get(url, headers=headers, timeout=30).text
    tables = pd.read_html(html)
    sp_df = tables[0]

    tickers = sp_df["Symbol"].dropna().astype(str).tolist()
    tickers = [t.replace(".", "-").strip() for t in tickers]  # BRK.B → BRK-B
    tickers = sorted(list(dict.fromkeys(tickers)))
    log.info("S&P 500 티커 수: %d개", len(tickers))
    return tickers


# ── 2) 다운로드 파라미터 ───────────────────────────────────
OUTDIR_RAW = "sp500_ohlcv_3y_raw"
OUTDIR_CLEAN = "sp500_ohlcv_3y_clean"
PERIOD = "3y"
CHUNK_SIZE = 200
MAX_WORKERS = 12
RETRIES = 3
CHUNK_DELAY = 5

OHLCV_COLS_WITH_ADJ = [
    "Date", "Open", "High", "Low", "Close", "Adj Close", "Volume",
]


# ── 3) 단일 티커 다운로드 ─────────────────────────────────
def download_one(ticker: str) -> tuple[str, bool, str]:
    """
    핵심 수정 2가지:

    1) yf.Ticker().history() 사용
       - yf.download()는 최신 yfinance에서 단일 티커에도
         MultiIndex 컬럼 ('Open','AAPL') 을 반환함
       - history()는 항상 단일 인덱스 컬럼을 반환하므로 안전

    2) auto_adjust=False
       - True일 때 과거 가격이 분할/배당 역보정되어
         실제 거래 가격과 전혀 다른 값이 출력됨
       - 예: AMZN 20:1 분할 → 과거가 $3000이 $150으로 보정
       - False로 설정하면 실제 거래가 그대로 보존
       - 보정된 종가가 필요하면 'Adj Close' 컬럼 사용
    """
    out_path = os.path.join(OUTDIR_RAW, f"{ticker}_ohlcv_3y.csv")
    if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        return ticker, True, "SKIP"

    for attempt in range(RETRIES):
        try:
            t = yf.Ticker(ticker)
            data = t.history(period=PERIOD, interval="1d", auto_adjust=False)

            if data is None or data.empty:
                raise ValueError("EMPTY_DATA")

            data = data.reset_index()

            # 컬럼명 표준화 (대소문자/공백 차이 방어)
            col_map = {}
            for c in data.columns:
                name = str(c).strip()
                low = name.lower()
                if low == "date":
                    col_map[c] = "Date"
                elif low == "open":
                    col_map[c] = "Open"
                elif low == "high":
                    col_map[c] = "High"
                elif low == "low":
                    col_map[c] = "Low"
                elif low == "close":
                    col_map[c] = "Close"
                elif low in ("adj close", "adj_close", "adjclose"):
                    col_map[c] = "Adj Close"
                elif low == "volume":
                    col_map[c] = "Volume"
            data = data.rename(columns=col_map)

            # 필요한 컬럼만 추출
            keep = [c for c in OHLCV_COLS_WITH_ADJ if c in data.columns]
            if not keep or "Date" not in keep:
                raise ValueError("MISSING_COLUMNS")
            data = data[keep]

            data.to_csv(out_path, index=False, encoding="utf-8-sig")
            return ticker, True, f"OK ({len(data)} rows)"

        except Exception:
            time.sleep(2 * (attempt + 1))

    return ticker, False, "FAIL"


# ── 4) 청크 단위 병렬 다운로드 ─────────────────────────────
def download_all(tickers: list[str]) -> tuple[list, list]:
    os.makedirs(OUTDIR_RAW, exist_ok=True)

    chunks = [
        tickers[i : i + CHUNK_SIZE]
        for i in range(0, len(tickers), CHUNK_SIZE)
    ]
    log.info("총 %d개 chunk", len(chunks))

    all_ok: list[tuple[str, str]] = []
    all_fail: list[tuple[str, str]] = []

    for chunk_idx, chunk in enumerate(chunks):
        log.info(
            "=== Chunk %d/%d 시작 (%d개) ===",
            chunk_idx + 1, len(chunks), len(chunk),
        )
        t0 = time.time()
        ok: list[tuple[str, str]] = []
        fail: list[tuple[str, str]] = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(download_one, t): t for t in chunk}
            for future in tqdm(
                as_completed(futures),
                total=len(chunk),
                desc=f"Chunk {chunk_idx + 1}",
            ):
                ticker = futures[future]
                try:
                    code, success, msg = future.result()
                    if success:
                        ok.append((ticker, msg))
                    else:
                        fail.append((ticker, msg))
                except Exception:
                    fail.append((ticker, "EXCEPTION"))

        elapsed = time.time() - t0
        log.info(
            "Chunk %d 완료! 성공: %d / 실패: %d | %.1f초",
            chunk_idx + 1, len(ok), len(fail), elapsed,
        )
        all_ok.extend(ok)
        all_fail.extend(fail)

        if chunk_idx < len(chunks) - 1:
            time.sleep(CHUNK_DELAY)

    log.info("전체 완료! 성공: %d / 실패: %d", len(all_ok), len(all_fail))
    return all_ok, all_fail


# ── 5) 클린 함수 ──────────────────────────────────────────
UNNAMED_RE = re.compile(r"^\s*unnamed", re.I)
DUP_SUFFIX_RE = re.compile(r"^(.*)\.(\d+)$")  # Open.1 → Open
REQUIRED = ["Date", "Open", "High", "Low", "Close", "Volume"]


def normalize_columns(cols: list) -> tuple[list[int], list[str]]:
    keep_idx: list[int] = []
    clean_names: list[str] = []
    seen: set[str] = set()

    for i, c in enumerate(cols):
        if c is None:
            continue
        name = str(c).strip()
        if name == "" or UNNAMED_RE.match(name):
            continue

        m = DUP_SUFFIX_RE.match(name)
        base = m.group(1).strip() if m else name

        if base == "" or UNNAMED_RE.match(base):
            continue
        if base in seen:
            continue

        seen.add(base)
        keep_idx.append(i)
        clean_names.append(base)

    return keep_idx, clean_names


def clean_ohlcv_df(df: pd.DataFrame, ticker: str) -> pd.DataFrame | None:
    # 1) 컬럼 정리 (중복 제거)
    keep_idx, clean_names = normalize_columns(df.columns.tolist())
    df = df.iloc[:, keep_idx].copy()
    df.columns = clean_names

    # 2) Date 파싱 (정크행 제거)
    if "Date" not in df.columns:
        return None

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"])

    if df.empty:
        return None

    # 3) 필요한 컬럼 (Adj Close 포함)
    output_cols = REQUIRED.copy()
    if "Adj Close" in df.columns:
        output_cols.append("Adj Close")

    for c in output_cols:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[output_cols].copy()

    # 4) 숫자형 변환
    numeric_cols = [c for c in output_cols if c != "Date"]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # 5) ticker 컬럼 추가
    df["ticker"] = ticker

    # 6) 정렬 / 중복 제거
    df = (
        df.sort_values("Date")
        .drop_duplicates(subset=["Date"], keep="last")
        .reset_index(drop=True)
    )

    return df


# ── 6) 원본 CSV → 클린 CSV + INDEX ────────────────────────
def clean_all() -> int:
    os.makedirs(OUTDIR_CLEAN, exist_ok=True)

    raw_paths = sorted(glob.glob(os.path.join(OUTDIR_RAW, "*_ohlcv_3y.csv")))
    log.info("원본 CSV 수: %d", len(raw_paths))

    summary_rows = []
    clean_count = 0

    for path in tqdm(raw_paths, desc="Cleaning per-ticker"):
        ticker = os.path.basename(path).split("_")[0]

        try:
            df0 = pd.read_csv(path)
            df1 = clean_ohlcv_df(df0, ticker)
            if df1 is None or df1.empty:
                continue

            out_path = os.path.join(OUTDIR_CLEAN, f"{ticker}.csv")
            df1.to_csv(out_path, index=False, encoding="utf-8")

            summary_rows.append(
                {
                    "ticker": ticker,
                    "rows": len(df1),
                    "start_date": df1["Date"].min().date(),
                    "end_date": df1["Date"].max().date(),
                    "csv_file": f"{ticker}.csv",
                }
            )
            clean_count += 1

        except Exception:
            continue

    summary_df = (
        pd.DataFrame(summary_rows).sort_values("ticker").reset_index(drop=True)
    )
    index_path = os.path.join(OUTDIR_CLEAN, "_INDEX.csv")
    summary_df.to_csv(index_path, index=False, encoding="utf-8")
    log.info("클린 완료 티커 수: %d", clean_count)

    return clean_count


# ── 7) ZIP 생성 ────────────────────────────────────────────
def create_zip(zip_name: str = "sp500_by_ticker_csv_CLEAN.zip") -> str:
    with zipfile.ZipFile(zip_name, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for fn in sorted(os.listdir(OUTDIR_CLEAN)):
            if fn.lower().endswith(".csv"):
                z.write(os.path.join(OUTDIR_CLEAN, fn), arcname=fn)

    log.info("ZIP 생성 완료: %s", zip_name)
    return zip_name


# ── main ───────────────────────────────────────────────────
def main():
    tickers = load_sp500_tickers()
    download_all(tickers)
    clean_all()
    zip_path = create_zip()
    log.info("완료! ZIP 파일: %s", zip_path)


if __name__ == "__main__":
    main()
