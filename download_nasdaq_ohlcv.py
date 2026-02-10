"""
NASDAQ 3-year OHLCV 다운로드 + 클린 + 티커별 CSV + ZIP

사용법:
    pip install -r requirements.txt
    python download_nasdaq_ohlcv.py

수정사항:
- yf.download() MultiIndex 컬럼을 droplevel()로 평탄화
- auto_adjust=False 로 원본 OHLCV 보존 + Adj Close 별도 보관
- SKIP 캐시 검증: 깨진 CSV(Date만 있는 파일) 자동 삭제 후 재다운로드
- 시작 시 이전 raw/clean 폴더 자동 삭제 (깨진 캐시 방지)
"""

import os
import glob
import shutil
import time
import re
import zipfile
import logging

import pandas as pd
import yfinance as yf
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── 로깅 설정 ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── 1) NASDAQ 티커 리스트 로드 ─────────────────────────────
CSV_URL = (
    "https://raw.githubusercontent.com/datasets/nasdaq-listings/"
    "master/data/nasdaq-listed.csv"
)


def load_tickers() -> list[str]:
    df_list = pd.read_csv(CSV_URL)
    tickers = sorted(df_list["Symbol"].dropna().unique().tolist())
    tickers = [
        t for t in tickers
        if not t.startswith("^") and len(t) <= 5 and t.isalpha()
    ]
    log.info("NASDAQ 티커 수: %d개", len(tickers))
    return tickers


# ── 2) 다운로드 파라미터 ───────────────────────────────────
OUTDIR_RAW = "nasdaq_ohlcv_3y_raw"
OUTDIR_CLEAN = "nasdaq_ohlcv_3y_clean"
PERIOD = "3y"
CHUNK_SIZE = 1000
MAX_WORKERS = 12
RETRIES = 3
CHUNK_DELAY = 5

OHLCV_COLS_WITH_ADJ = [
    "Date", "Open", "High", "Low", "Close", "Adj Close", "Volume",
]
VALID_CHECK_COLS = {"Open", "High", "Low", "Close", "Volume"}


# ── 3) 단일 티커 다운로드 ─────────────────────────────────
def download_one(ticker: str) -> tuple[str, bool, str]:
    out_path = os.path.join(OUTDIR_RAW, f"{ticker}_ohlcv_3y.csv")

    # ★ SKIP 캐시 검증: OHLCV 컬럼이 실제로 있는지 확인
    if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        try:
            check = pd.read_csv(out_path, nrows=1)
            if VALID_CHECK_COLS.issubset(set(check.columns)):
                return ticker, True, "SKIP"
        except Exception:
            pass
        # 깨진 파일 → 삭제 후 재다운로드
        os.remove(out_path)

    for attempt in range(RETRIES):
        try:
            # ★ yf.Ticker().history() 사용 (스레드 안전)
            #   yf.download()는 멀티스레드에서 내부 공유 세션이 꼬여
            #   다른 티커의 데이터가 섞여 들어오는 치명적 버그가 있음
            t = yf.Ticker(ticker)
            data = t.history(period=PERIOD, interval="1d", auto_adjust=False)

            if data is None or data.empty:
                raise ValueError("EMPTY_DATA")

            data = data.reset_index()

            # 컬럼명 표준화
            col_map = {}
            for c in data.columns:
                low = str(c).strip().lower()
                if low == "date":          col_map[c] = "Date"
                elif low == "open":        col_map[c] = "Open"
                elif low == "high":        col_map[c] = "High"
                elif low == "low":         col_map[c] = "Low"
                elif low == "close":       col_map[c] = "Close"
                elif low in ("adj close", "adj_close", "adjclose"):
                    col_map[c] = "Adj Close"
                elif low == "volume":      col_map[c] = "Volume"
            data = data.rename(columns=col_map)

            # 필요한 컬럼 검증
            if not VALID_CHECK_COLS.issubset(set(data.columns)):
                raise ValueError(f"MISSING_COLUMNS: {data.columns.tolist()}")

            keep = [c for c in OHLCV_COLS_WITH_ADJ if c in data.columns]
            data = data[keep]

            # float64 → 소수점 2자리 반올림 (Yahoo Finance와 동일한 값)
            for c in ["Open", "High", "Low", "Close", "Adj Close"]:
                if c in data.columns:
                    data[c] = data[c].round(2)
            if "Volume" in data.columns:
                data["Volume"] = data["Volume"].astype(int)

            data.to_csv(out_path, index=False, encoding="utf-8-sig")
            return ticker, True, f"OK ({len(data)} rows)"

        except Exception:
            time.sleep(2 * (attempt + 1))

    return ticker, False, "FAIL"


# ── 4) 청크 단위 병렬 다운로드 ─────────────────────────────
def download_all(tickers: list[str]) -> tuple[list, list]:
    # ★ 이전 실행의 깨진 캐시 방지: raw/clean 폴더 초기화
    for d in (OUTDIR_RAW, OUTDIR_CLEAN):
        if os.path.exists(d):
            shutil.rmtree(d)
    os.makedirs(OUTDIR_RAW, exist_ok=True)
    os.makedirs(OUTDIR_CLEAN, exist_ok=True)

    chunks = [tickers[i : i + CHUNK_SIZE] for i in range(0, len(tickers), CHUNK_SIZE)]
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
DUP_SUFFIX_RE = re.compile(r"^(.*)\.(\d+)$")
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
    keep_idx, clean_names = normalize_columns(df.columns.tolist())
    df = df.iloc[:, keep_idx].copy()
    df.columns = clean_names

    if "Date" not in df.columns:
        return None

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"])

    if df.empty:
        return None

    output_cols = REQUIRED.copy()
    if "Adj Close" in df.columns:
        output_cols.append("Adj Close")

    for c in output_cols:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[output_cols].copy()

    numeric_cols = [c for c in output_cols if c != "Date"]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["ticker"] = ticker

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
def create_zip(zip_name: str = "nasdaq_by_ticker_csv_CLEAN.zip") -> str:
    with zipfile.ZipFile(zip_name, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for fn in sorted(os.listdir(OUTDIR_CLEAN)):
            if fn.lower().endswith(".csv"):
                z.write(os.path.join(OUTDIR_CLEAN, fn), arcname=fn)

    log.info("ZIP 생성 완료: %s", zip_name)
    return zip_name


# ── 8) 검증 ───────────────────────────────────────────────
def verify_sample(tickers_to_check=("TSLA", "NVDA", "AMZN", "AAPL", "MSFT")):
    log.info("=== 데이터 검증 ===")
    for t in tickers_to_check:
        path = os.path.join(OUTDIR_CLEAN, f"{t}.csv")
        if not os.path.exists(path):
            log.warning("%s: 파일 없음", t)
            continue
        df = pd.read_csv(path)
        latest = df.tail(1).iloc[0]
        log.info(
            "%s | 최신: %s | O=%.2f H=%.2f L=%.2f C=%.2f V=%d | rows=%d",
            t, latest["Date"],
            latest["Open"], latest["High"], latest["Low"], latest["Close"],
            int(latest["Volume"]), len(df),
        )


# ── main ───────────────────────────────────────────────────
def main():
    tickers = load_tickers()
    download_all(tickers)
    clean_all()
    zip_path = create_zip()
    verify_sample()
    log.info("완료! ZIP 파일: %s", zip_path)


if __name__ == "__main__":
    main()
