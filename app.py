import os
import re
import sqlite3
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from functools import wraps

import pandas as pd
from flask import (
    Flask, render_template, request, redirect, url_for,
    flash, abort, jsonify, send_file,
    session
)

from PIL import Image
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from werkzeug.middleware.proxy_fix import ProxyFix


# =========================
# ✅ .env 로드
# =========================
load_dotenv()

S3_BUCKET = os.environ.get("S3_BUCKET", "").strip()
AWS_REGION = os.environ.get("AWS_REGION", "").strip()
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "").strip()
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "").strip()

PRESIGNED_EXPIRES = int(os.environ.get("PRESIGNED_EXPIRES", "1800"))
PRESIGNED_REFRESH_MARGIN = int(os.environ.get("PRESIGNED_REFRESH_MARGIN", "120"))

FLASK_SECRET_KEY = os.environ.get("FLASK_SECRET_KEY", "change-this-to-a-random-secret")
AUTO_LOAD_MASTER = os.environ.get("AUTO_LOAD_MASTER", "1").strip()

MAX_CONTENT_LENGTH = int(os.environ.get("MAX_CONTENT_LENGTH", str(20 * 1024 * 1024)))

VIEW_PASSWORD = os.environ.get("VIEW_PASSWORD", "").strip()
EDIT_PASSWORD = os.environ.get("EDIT_PASSWORD", "").strip()
BANNER_IMAGE_URL = os.environ.get("BANNER_IMAGE_URL", "").strip()
BANNER_S3_KEY = os.environ.get("BANNER_S3_KEY", "").strip()

FLASK_DEBUG = os.environ.get("FLASK_DEBUG", "0").strip() == "1"

# ✅ 기존 COOKIE_SECURE + 신규 SAMG_COOKIE_SECURE 둘 다 지원
COOKIE_SECURE = (os.environ.get("SAMG_COOKIE_SECURE", os.environ.get("COOKIE_SECURE", "0")).strip() == "1")

if not all([S3_BUCKET, AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY]):
    raise SystemExit(
        "필수 환경변수 누락: S3_BUCKET, AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY\n"
        "서버 배포 시에도 환경변수로 반드시 설정하세요."
    )

if not VIEW_PASSWORD or not EDIT_PASSWORD:
    raise SystemExit(
        "필수 환경변수 누락: VIEW_PASSWORD, EDIT_PASSWORD\n"
        "조회/등록 화면 진입용 암호키를 환경변수로 설정하세요."
    )

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

# =========================
# 기본 설정
# =========================
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "products.db"
MASTER_XLSX_PATH = DATA_DIR / "master.xlsx"
TMP_MASTER_DIR = DATA_DIR / "tmp_master_upload"

ALLOWED_EXTENSIONS = {"png", "jpg", "jpeg", "gif", "webp"}

MAX_IMAGE_SIZE = (1280, 1280)
JPEG_QUALITY = 65

# =========================
# ✅ 사진 타입(3종)
# =========================
PHOTO_TYPE_ITEM = "ITEM"   # 상품사진
PHOTO_TYPE_BOX = "BOX"     # 외박스
PHOTO_TYPE_CASE = "CASE"   # RRP (= CASE)

PHOTO_TYPES = [PHOTO_TYPE_ITEM, PHOTO_TYPE_BOX, PHOTO_TYPE_CASE]
PHOTO_TYPE_LABELS = {
    PHOTO_TYPE_ITEM: "상품사진",
    PHOTO_TYPE_BOX: "외박스 사진",
    PHOTO_TYPE_CASE: "RRP(CASE) 사진",
}

app = Flask(__name__)
app.secret_key = FLASK_SECRET_KEY
app.config["MAX_CONTENT_LENGTH"] = MAX_CONTENT_LENGTH

app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)

app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=COOKIE_SECURE,   # HTTP면 0, HTTPS면 1
)


@app.context_processor
def inject_banner_image_url():
    banner_url = BANNER_IMAGE_URL
    if not banner_url and BANNER_S3_KEY:
        try:
            banner_url = presigned_get_url(BANNER_S3_KEY, expires_sec=PRESIGNED_EXPIRES)
        except Exception:
            banner_url = ""
    return {"banner_url": banner_url}

DATA_DIR.mkdir(parents=True, exist_ok=True)
TMP_MASTER_DIR.mkdir(parents=True, exist_ok=True)


# =========================
# 유틸
# =========================
def normalize_code(s: str) -> str:
    return (s or "").strip()

def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cur = conn.cursor()
    rows = cur.execute(f"PRAGMA table_info({table})").fetchall()
    cols = [r[1] for r in rows]
    return column in cols

def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def allowed_file(filename: str) -> bool:
    if not filename or "." not in filename:
        return False
    ext = filename.rsplit(".", 1)[-1].lower().strip()
    return ext in ALLOWED_EXTENSIONS

def safe_filename(filename: str) -> str:
    filename = (filename or "").strip().replace("\\", "_").replace("/", "_")
    filename = re.sub(r"[^0-9A-Za-z가-힣\.\-\_\(\)\[\]\s]+", "", filename)
    filename = re.sub(r"\s+", " ", filename).strip()
    if not filename:
        filename = "image.jpg"
    return filename

def to_int_or_none(v):
    if v is None:
        return None
    s = str(v).strip()
    if s == "" or s.lower() in ("nan", "none"):
        return None
    # 숫자만 남기기(엑셀에서 1.0 처럼 들어오는 경우 처리)
    try:
        f = float(s)
        i = int(f)
        return i
    except Exception:
        # "1,000" 같은 값 처리
        s2 = re.sub(r"[^\d\-]", "", s)
        if s2 == "":
            return None
        try:
            return int(s2)
        except Exception:
            return None


# =========================
# S3
# =========================
def s3_key_for(item_code: str, filename: str, photo_type: str | None = None) -> str:
    item_code = normalize_code(item_code)
    filename = (filename or "").strip().replace("\\", "_").replace("/", "_")

    # ✅ 신규: products/<item_code>/<photo_type>/<filename>
    if photo_type in PHOTO_TYPES:
        return f"products/{item_code}/{photo_type}/{filename}"

    # ✅ 구버전 호환: products/<item_code>/<filename>
    return f"products/{item_code}/{filename}"

def s3_put_bytes(key: str, data: bytes, content_type: str = "image/jpeg"):
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=data,
        ContentType=content_type,
    )

def s3_delete(key: str):
    try:
        s3.delete_object(Bucket=S3_BUCKET, Key=key)
    except Exception:
        pass

def presigned_get_url(key: str, expires_sec: int = 1800) -> str:
    try:
        return s3.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": S3_BUCKET, "Key": key},
            ExpiresIn=expires_sec,
        )
    except ClientError:
        return ""

def save_low_quality_jpeg_to_bytes(file_storage) -> bytes:
    img = Image.open(file_storage.stream).convert("RGB")
    img.thumbnail(MAX_IMAGE_SIZE)
    from io import BytesIO
    buf = BytesIO()
    img.save(buf, format="JPEG", quality=JPEG_QUALITY, optimize=True)
    return buf.getvalue()


# =========================
# 인증(조회/등록 모드)
# =========================
def require_view_auth(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if session.get("auth_view") is True or session.get("auth_edit") is True:
            return fn(*args, **kwargs)
        return redirect(url_for("auth_view", next=request.path))
    return wrapper

def require_edit_auth(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if session.get("auth_edit") is True:
            return fn(*args, **kwargs)
        return redirect(url_for("auth_edit", next=request.path))
    return wrapper


@app.route("/", methods=["GET"])
def gate_index():
    return render_template("gate_index.html")


@app.route("/auth/view", methods=["GET", "POST"])
def auth_view():
    next_url = request.args.get("next", "") or url_for("view_products")
    if request.method == "POST":
        pw = (request.form.get("password", "") or "").strip()
        if pw == VIEW_PASSWORD:
            session["auth_view"] = True
            session.pop("auth_edit", None)
            return redirect(next_url)
        flash("조회용 암호키가 올바르지 않습니다.", "danger")
    return render_template("auth.html", mode="view", next_url=next_url)


@app.route("/auth/edit", methods=["GET", "POST"])
def auth_edit():
    next_url = request.args.get("next", "") or url_for("register_home")
    if request.method == "POST":
        pw = (request.form.get("password", "") or "").strip()
        if pw == EDIT_PASSWORD:
            session["auth_edit"] = True
            session["auth_view"] = True
            return redirect(next_url)
        flash("등록용 암호키가 올바르지 않습니다.", "danger")
    return render_template("auth.html", mode="edit", next_url=next_url)


@app.route("/logout", methods=["GET"])
def logout():
    session.clear()
    flash("로그아웃 완료", "success")
    return redirect(url_for("gate_index"))


# =========================
# 디렉터리/DB
# =========================
def ensure_dirs():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    TMP_MASTER_DIR.mkdir(parents=True, exist_ok=True)

def init_db():
    conn = get_db()
    cur = conn.cursor()

    # ✅ products: SAMG 마스터 컬럼 확장
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS products (
            item_code TEXT PRIMARY KEY,
            item_name TEXT,
            scan_code TEXT
        )
        """
    )

    # remark
    if not _has_column(conn, "products", "remark"):
        cur.execute("ALTER TABLE products ADD COLUMN remark TEXT")

    # is_active
    if not _has_column(conn, "products", "is_active"):
        cur.execute("ALTER TABLE products ADD COLUMN is_active INTEGER DEFAULT 1")
    cur.execute("UPDATE products SET is_active=1 WHERE is_active IS NULL")

    # ✅ 추가 컬럼(마스터에서 읽을 컬럼)
    # 수량들은 INTEGER로 관리
    if not _has_column(conn, "products", "pallet_entry_quantity"):
        cur.execute("ALTER TABLE products ADD COLUMN pallet_entry_quantity INTEGER")
    if not _has_column(conn, "products", "box_entry_quantity"):
        cur.execute("ALTER TABLE products ADD COLUMN box_entry_quantity INTEGER")
    if not _has_column(conn, "products", "case_entry_quantity"):
        cur.execute("ALTER TABLE products ADD COLUMN case_entry_quantity INTEGER")

    # 코드/카테고리
    if not _has_column(conn, "products", "category_code"):
        cur.execute("ALTER TABLE products ADD COLUMN category_code TEXT")
    if not _has_column(conn, "products", "box_code"):
        cur.execute("ALTER TABLE products ADD COLUMN box_code TEXT")
    if not _has_column(conn, "products", "case_code"):
        cur.execute("ALTER TABLE products ADD COLUMN case_code TEXT")

    # ✅ photos 테이블 + photo_type
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS photos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_item_code TEXT NOT NULL,
            filename TEXT NOT NULL,
            uploaded_at TEXT NOT NULL,
            s3_key TEXT,
            photo_type TEXT DEFAULT 'ITEM',
            FOREIGN KEY(product_item_code) REFERENCES products(item_code)
        )
        """
    )
    if not _has_column(conn, "photos", "photo_type"):
        cur.execute("ALTER TABLE photos ADD COLUMN photo_type TEXT DEFAULT 'ITEM'")
        cur.execute("UPDATE photos SET photo_type='ITEM' WHERE photo_type IS NULL OR photo_type=''")

    conn.commit()
    conn.close()


# =========================
# 마스터 검증/임시 저장
# =========================
def _validate_master_df(df: pd.DataFrame):
    df.columns = df.columns.astype(str).str.strip()

    # ✅ SAMG 마스터에서 추가로 반드시 읽을 컬럼 포함
    required = [
        "ITEM_CODE", "ITEM_NAME", "SCAN_CODE",
        "PALLET_ENTRY_QUANTITY", "BOX_ENTRY_QUANTITY", "CASE_ENTRY_QUANTITY",
        "CATEGORY_CODE", "BOX_CODE", "CASE_CODE",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"필수 컬럼 누락: {missing} / 현재 컬럼: {list(df.columns)}")

    has_remark = "REMARK" in df.columns

    cols = required + (["REMARK"] if has_remark else [])
    df = df[cols].copy()

    # 문자열 컬럼 정리
    for col in ["ITEM_CODE", "ITEM_NAME", "SCAN_CODE", "CATEGORY_CODE", "BOX_CODE", "CASE_CODE"] + (["REMARK"] if has_remark else []):
        df[col] = df[col].fillna("").astype(str).str.strip()

    # 수량 컬럼 정리
    for col in ["PALLET_ENTRY_QUANTITY", "BOX_ENTRY_QUANTITY", "CASE_ENTRY_QUANTITY"]:
        df[col] = df[col].apply(to_int_or_none)

    df = df[df["ITEM_CODE"] != ""].copy()
    if len(df) == 0:
        raise ValueError("유효한 ITEM_CODE 행이 0건입니다.")

    dup_mask = df["ITEM_CODE"].duplicated(keep=False)
    dup_codes = sorted(set(df.loc[dup_mask, "ITEM_CODE"].tolist()))
    warnings = []
    if dup_codes:
        warnings.append(f"ITEM_CODE 중복 {len(dup_codes)}건 (샘플): {dup_codes[:20]}{'...' if len(dup_codes) > 20 else ''}")

    preview = df.head(30).to_dict(orient="records")

    meta = {
        "rows": int(len(df)),
        "has_remark": bool(has_remark),
        "columns": cols,
        "dup_count": int(len(dup_codes)),
        "dup_codes_sample": dup_codes[:50],
        "warnings": warnings,
    }
    return df, meta, preview

def _tmp_master_path(token: str) -> Path:
    return TMP_MASTER_DIR / f"{token}.json"

def _clear_pending_master_upload():
    payload = session.get("pending_master_upload")
    if payload and isinstance(payload, dict):
        token = (payload.get("token") or "").strip()
        if token:
            p = _tmp_master_path(token)
            try:
                if p.exists():
                    p.unlink()
            except Exception:
                pass
    session.pop("pending_master_upload", None)


# =========================
# ✅ 마스터 동기화: 삭제 대신 is_active 처리 + 컬럼 업서트
# =========================
def _apply_master_sync_is_active(df: pd.DataFrame) -> dict:
    df.columns = df.columns.astype(str).str.strip()
    has_remark = "REMARK" in df.columns

    need_cols = [
        "ITEM_CODE", "ITEM_NAME", "SCAN_CODE",
        "PALLET_ENTRY_QUANTITY", "BOX_ENTRY_QUANTITY", "CASE_ENTRY_QUANTITY",
        "CATEGORY_CODE", "BOX_CODE", "CASE_CODE",
    ] + (["REMARK"] if has_remark else [])
    df = df[need_cols].copy()

    for c in ["ITEM_CODE", "ITEM_NAME", "SCAN_CODE", "CATEGORY_CODE", "BOX_CODE", "CASE_CODE"] + (["REMARK"] if has_remark else []):
        df[c] = df[c].fillna("").astype(str).str.strip()

    for c in ["PALLET_ENTRY_QUANTITY", "BOX_ENTRY_QUANTITY", "CASE_ENTRY_QUANTITY"]:
        df[c] = df[c].apply(to_int_or_none)

    df = df[df["ITEM_CODE"] != ""].copy()
    keep_codes = set(df["ITEM_CODE"].tolist())

    conn = get_db()
    cur = conn.cursor()

    # 1) 마스터에 없는 상품 비활성화
    rows = cur.execute("SELECT item_code FROM products").fetchall()
    existing_codes = set([r[0] for r in rows if r and r[0]])
    to_deactivate = sorted(existing_codes - keep_codes)

    deactivated = 0
    if to_deactivate:
        CHUNK = 800
        for i in range(0, len(to_deactivate), CHUNK):
            chunk = to_deactivate[i:i + CHUNK]
            q = ",".join(["?"] * len(chunk))
            cur.execute(f"UPDATE products SET is_active=0 WHERE item_code IN ({q})", chunk)
            deactivated += len(chunk)

    # 2) 마스터에 있는 상품 upsert
    upserted = 0
    for _, row in df.iterrows():
        item_code = normalize_code(row["ITEM_CODE"])
        item_name = normalize_code(row["ITEM_NAME"])
        scan_code = normalize_code(row["SCAN_CODE"])

        pallet_q = to_int_or_none(row["PALLET_ENTRY_QUANTITY"])
        box_q = to_int_or_none(row["BOX_ENTRY_QUANTITY"])
        case_q = to_int_or_none(row["CASE_ENTRY_QUANTITY"])

        category_code = normalize_code(row["CATEGORY_CODE"])
        box_code = normalize_code(row["BOX_CODE"])
        case_code = normalize_code(row["CASE_CODE"])

        remark = normalize_code(row["REMARK"]) if has_remark else ""

        if has_remark:
            cur.execute(
                """
                INSERT INTO products (
                    item_code, item_name, scan_code, remark, is_active,
                    pallet_entry_quantity, box_entry_quantity, case_entry_quantity,
                    category_code, box_code, case_code
                )
                VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(item_code) DO UPDATE SET
                  item_name=excluded.item_name,
                  scan_code=excluded.scan_code,
                  remark=excluded.remark,
                  is_active=1,
                  pallet_entry_quantity=excluded.pallet_entry_quantity,
                  box_entry_quantity=excluded.box_entry_quantity,
                  case_entry_quantity=excluded.case_entry_quantity,
                  category_code=excluded.category_code,
                  box_code=excluded.box_code,
                  case_code=excluded.case_code
                """,
                (item_code, item_name, scan_code, remark,
                 pallet_q, box_q, case_q, category_code, box_code, case_code)
            )
        else:
            cur.execute(
                """
                INSERT INTO products (
                    item_code, item_name, scan_code, is_active,
                    pallet_entry_quantity, box_entry_quantity, case_entry_quantity,
                    category_code, box_code, case_code
                )
                VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(item_code) DO UPDATE SET
                  item_name=excluded.item_name,
                  scan_code=excluded.scan_code,
                  is_active=1,
                  pallet_entry_quantity=excluded.pallet_entry_quantity,
                  box_entry_quantity=excluded.box_entry_quantity,
                  case_entry_quantity=excluded.case_entry_quantity,
                  category_code=excluded.category_code,
                  box_code=excluded.box_code,
                  case_code=excluded.case_code
                """,
                (item_code, item_name, scan_code,
                 pallet_q, box_q, case_q, category_code, box_code, case_code)
            )

        upserted += 1

    conn.commit()
    conn.close()
    return {"upserted": upserted, "deactivated": deactivated}


# =========================
# ✅ S3 스캔 → photos 테이블 복구 (구/신 구조 모두 지원)
# =========================
def rebuild_photos_from_s3(prefix: str = "products/") -> dict:
    conn = get_db()
    cur = conn.cursor()

    existing = set()
    for r in cur.execute("SELECT s3_key FROM photos WHERE s3_key IS NOT NULL AND s3_key <> ''").fetchall():
        existing.add(r[0])

    paginator = s3.get_paginator("list_objects_v2")

    inserted = 0
    scanned = 0

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj.get("Key") or ""
            if not key.startswith(prefix):
                continue

            parts = key.split("/")
            # 구버전: products/<item_code>/<filename> => 3파트
            # 신버전: products/<item_code>/<photo_type>/<filename> => 4파트
            photo_type = PHOTO_TYPE_ITEM
            item_code = ""
            filename = ""

            if len(parts) == 3:
                _, item_code, filename = parts
            elif len(parts) >= 4:
                _, item_code, photo_type, filename = parts[0], parts[1], parts[2], "/".join(parts[3:])
                photo_type = (photo_type or "").strip().upper()
                if photo_type not in PHOTO_TYPES:
                    photo_type = PHOTO_TYPE_ITEM
            else:
                continue

            item_code = (item_code or "").strip()
            filename = (filename or "").strip()
            if not item_code or not filename:
                continue

            ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
            if ext not in {"jpg", "jpeg", "png", "webp", "gif"}:
                continue

            scanned += 1
            if key in existing:
                continue

            lm = obj.get("LastModified")
            if lm and hasattr(lm, "astimezone"):
                ts = lm.astimezone(timezone.utc).isoformat(timespec="seconds")
            else:
                ts = datetime.now(timezone.utc).isoformat(timespec="seconds")

            cur.execute(
                "INSERT INTO photos (product_item_code, filename, uploaded_at, s3_key, photo_type) VALUES (?, ?, ?, ?, ?)",
                (item_code, filename, ts, key, photo_type)
            )
            existing.add(key)
            inserted += 1

            if inserted % 1000 == 0:
                conn.commit()

    conn.commit()
    conn.close()
    return {"scanned": scanned, "inserted": inserted}


# =========================
# ✅ Presigned URL 새로고침 API (photo_type 포함)
# =========================
@app.get("/api/presign/photos")
@require_view_auth
def api_presign_photos():
    item_code = normalize_code(request.args.get("item_code", ""))
    if not item_code:
        return jsonify({"ok": False, "error": "missing_item_code"}), 400

    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, filename, s3_key, photo_type FROM photos WHERE product_item_code = ? ORDER BY id DESC",
        (item_code,)
    )
    rows = cur.fetchall()
    conn.close()

    out = []
    for r in rows:
        ptype = (r["photo_type"] or PHOTO_TYPE_ITEM).upper()
        if ptype not in PHOTO_TYPES:
            ptype = PHOTO_TYPE_ITEM
        key = r["s3_key"] or s3_key_for(item_code, r["filename"], ptype)
        out.append({
            "id": r["id"],
            "photo_type": ptype,
            "label": PHOTO_TYPE_LABELS.get(ptype, ptype),
            "url": presigned_get_url(key, expires_sec=PRESIGNED_EXPIRES),
        })

    return jsonify({
        "ok": True,
        "expires_in": PRESIGNED_EXPIRES,
        "refresh_margin": PRESIGNED_REFRESH_MARGIN,
        "photos": out
    })


# =========================
# ✅ 마스터 업로드
# =========================
@app.route("/admin/master_upload", methods=["GET", "POST"])
@require_edit_auth
def admin_master_upload():
    if request.method == "POST":
        f = request.files.get("file")
        if not f or not f.filename:
            flash("업로드할 엑셀 파일을 선택하세요.", "danger")
            return redirect(url_for("admin_master_upload"))

        filename = safe_filename(f.filename)
        ext = filename.rsplit(".", 1)[-1].lower()
        if ext not in {"xlsx", "xls"}:
            flash("엑셀 파일만 업로드 가능합니다. (.xlsx/.xls)", "danger")
            return redirect(url_for("admin_master_upload"))

        # 임시 저장
        token = os.urandom(16).hex()
        tmp_xlsx = TMP_MASTER_DIR / f"{token}.xlsx"
        f.save(tmp_xlsx)

        try:
            df = pd.read_excel(tmp_xlsx)
            df2, meta, preview = _validate_master_df(df)
        except Exception as e:
            try:
                tmp_xlsx.unlink()
            except Exception:
                pass
            flash(f"업로드/검증 실패: {e}", "danger")
            return redirect(url_for("admin_master_upload"))

        payload = {
            "token": token,
            "meta": meta,
            "preview": preview,
        }
        # df2는 json으로 저장(확정 반영용)
        _tmp_master_path(token).write_text(df2.to_json(orient="records", force_ascii=False), encoding="utf-8")
        session["pending_master_upload"] = payload

        try:
            tmp_xlsx.unlink()
        except Exception:
            pass

        return redirect(url_for("admin_master_preview"))

    return render_template("admin_master_upload.html")


@app.route("/admin/master_preview", methods=["GET", "POST"])
@require_edit_auth
def admin_master_preview():
    payload = session.get("pending_master_upload")
    if not payload or not isinstance(payload, dict):
        flash("미리보기 대상 마스터 업로드가 없습니다.", "warning")
        return redirect(url_for("admin_master_upload"))

    token = (payload.get("token") or "").strip()
    meta = payload.get("meta") or {}
    preview = payload.get("preview") or []

    if request.method == "POST":
        action = (request.form.get("_action") or "").strip()
        if action == "cancel":
            _clear_pending_master_upload()
            flash("마스터 반영이 취소되었습니다.", "info")
            return redirect(url_for("admin_master_upload"))

        p = _tmp_master_path(token)
        if not p.exists():
            flash("임시 마스터 데이터가 만료되었습니다. 다시 업로드하세요.", "danger")
            _clear_pending_master_upload()
            return redirect(url_for("admin_master_upload"))

        try:
            rows = json.loads(p.read_text(encoding="utf-8"))
            df = pd.DataFrame(rows)
            result = _apply_master_sync_is_active(df)
        except Exception as e:
            flash(f"마스터 반영 실패: {e}", "danger")
            return redirect(url_for("admin_master_preview"))

        _clear_pending_master_upload()

        flash(f"마스터 반영 완료: 업서트 {result['upserted']} / 비활성 {result['deactivated']}", "success")
        return redirect(url_for("admin_master_upload"))

    return render_template("admin_master_preview.html", meta=meta, preview=preview)


@app.route("/admin/master_upload_confirm", methods=["POST"])
@require_edit_auth
def admin_master_upload_confirm():
    return admin_master_preview()


# =========================
# ✅ 조회용
# =========================
@app.route("/view/products", methods=["GET"])
@require_view_auth
def view_products():
    q = normalize_code(request.args.get("q", ""))

    conn = get_db()
    cur = conn.cursor()

    base_sql = """
        SELECT
            p.item_code,
            p.item_name,
            p.scan_code,
            p.remark,
            p.box_entry_quantity,
            p.case_entry_quantity,
            SUM(CASE WHEN ph.photo_type='ITEM' THEN 1 ELSE 0 END) AS item_photo_count,
            SUM(CASE WHEN ph.photo_type='BOX' THEN 1 ELSE 0 END) AS box_photo_count,
            SUM(CASE WHEN ph.photo_type='CASE' THEN 1 ELSE 0 END) AS case_photo_count,
            COUNT(ph.id) AS photo_count,
            (
                SELECT
                  CASE
                    WHEN ph2.s3_key IS NOT NULL AND ph2.s3_key <> '' THEN ph2.s3_key
                    ELSE (
                      CASE
                        WHEN ph2.photo_type IN ('ITEM','BOX','CASE')
                          THEN ('products/' || p.item_code || '/' || ph2.photo_type || '/' || ph2.filename)
                        ELSE ('products/' || p.item_code || '/' || ph2.filename)
                      END
                    )
                  END
                FROM photos ph2
                WHERE ph2.product_item_code = p.item_code
                ORDER BY
                  CASE
                    WHEN ph2.photo_type='ITEM' THEN 0
                    WHEN ph2.photo_type='BOX' THEN 1
                    WHEN ph2.photo_type='CASE' THEN 2
                    ELSE 9
                  END,
                  ph2.uploaded_at ASC,
                  ph2.id ASC
                LIMIT 1
            ) AS thumb_key
        FROM products p
        LEFT JOIN photos ph
          ON ph.product_item_code = p.item_code
    """

    where = ["IFNULL(p.is_active,1) = 1"]
    params = []

    if q:
        like = f"%{q}%"
        where.append(
            "("
            "p.item_code LIKE ? OR "
            "p.scan_code LIKE ? OR "
            "p.box_code LIKE ? OR "
            "p.case_code LIKE ? OR "
            "p.item_name LIKE ? OR "
            "p.remark LIKE ?"
            ")"
        )
        params.extend([like, like, like, like, like, like])

    sql = base_sql + " WHERE " + " AND ".join(where) + """
        GROUP BY
            p.item_code,
            p.item_name,
            p.scan_code,
            p.remark,
            p.box_entry_quantity,
            p.case_entry_quantity
        ORDER BY photo_count DESC, p.item_code ASC
        LIMIT 2000
    """

    cur.execute(sql, params)
    products = cur.fetchall()
    conn.close()

    enriched = []
    for r in products:
        thumb = r["thumb_key"]
        thumb_url = presigned_get_url(thumb, expires_sec=PRESIGNED_EXPIRES) if thumb else ""
        enriched.append({**dict(r), "thumb_url": thumb_url})

    return render_template("products_with_photos.html", products=enriched, q=q)


@app.route("/view/product/<item_code>", methods=["GET"])
@require_view_auth
def view_product_detail(item_code):
    item_code = normalize_code(item_code)
    if not item_code:
        abort(404)

    conn = get_db()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT
          item_code, item_name, scan_code, remark,
          pallet_entry_quantity, box_entry_quantity, case_entry_quantity,
          category_code, box_code, case_code
        FROM products
        WHERE item_code = ? AND IFNULL(is_active,1)=1
        """,
        (item_code,)
    )
    product = cur.fetchone()
    if not product:
        conn.close()
        flash("해당 상품을 찾을 수 없습니다. (마스터에 없거나 비활성화된 상품)", "warning")
        return redirect(url_for("view_products"))

    cur.execute(
        "SELECT id, filename, uploaded_at, s3_key, photo_type FROM photos WHERE product_item_code=? ORDER BY id DESC",
        (item_code,)
    )
    photos_rows = cur.fetchall()
    conn.close()

    grouped = {t: [] for t in PHOTO_TYPES}
    for r in photos_rows:
        ptype = (r["photo_type"] or PHOTO_TYPE_ITEM).upper()
        if ptype not in PHOTO_TYPES:
            ptype = PHOTO_TYPE_ITEM
        key = r["s3_key"] or s3_key_for(item_code, r["filename"], ptype)
        grouped[ptype].append({
            "id": r["id"],
            "filename": r["filename"],
            "uploaded_at": r["uploaded_at"],
            "photo_type": ptype,
            "label": PHOTO_TYPE_LABELS.get(ptype, ptype),
            "url": presigned_get_url(key, expires_sec=PRESIGNED_EXPIRES),
        })

    return render_template(
        "view_product_detail.html",
        product=product,
        photos_grouped=grouped,
        photo_labels=PHOTO_TYPE_LABELS,
        item_code=item_code,
        presigned_expires=PRESIGNED_EXPIRES,
        presigned_refresh_margin=PRESIGNED_REFRESH_MARGIN,
    )


# =========================
# ✅ 등록용
# =========================
@app.route("/register", methods=["GET"])
@require_edit_auth
def register_home():
    q = normalize_code(request.args.get("q", ""))

    conn = get_db()
    cur = conn.cursor()

    base_sql = """
        SELECT
            p.item_code,
            p.item_name,
            p.scan_code,
            p.remark,
            SUM(CASE WHEN ph.photo_type='ITEM' THEN 1 ELSE 0 END) AS item_photo_count,
            SUM(CASE WHEN ph.photo_type='BOX' THEN 1 ELSE 0 END) AS box_photo_count,
            SUM(CASE WHEN ph.photo_type='CASE' THEN 1 ELSE 0 END) AS case_photo_count,
            COUNT(ph.id) AS photo_count
        FROM products p
        LEFT JOIN photos ph
          ON ph.product_item_code = p.item_code
        WHERE IFNULL(p.is_active,1)=1
    """

    where = []
    params = []

    if q:
        like = f"%{q}%"
        where.append(
            "("
            "p.item_code LIKE ? OR "
            "p.scan_code LIKE ? OR "
            "p.box_code LIKE ? OR "
            "p.case_code LIKE ? OR "
            "p.item_name LIKE ? OR "
            "p.remark LIKE ?"
            ")"
        )
        params.extend([like, like, like, like, like, like])

    sql = base_sql
    if where:
        sql += " AND " + " AND ".join(where)

    sql += """
        GROUP BY p.item_code, p.item_name, p.scan_code, p.remark
        ORDER BY photo_count DESC, p.item_code ASC
        LIMIT 2000
    """

    cur.execute(sql, params)
    products = cur.fetchall()
    conn.close()

    return render_template("home.html", products=products, q=q)


@app.route("/register/product/<item_code>", methods=["GET", "POST"])
@require_edit_auth
def register_product_detail(item_code):
    item_code = normalize_code(item_code)
    if not item_code:
        abort(404)

    conn = get_db()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT
          item_code, item_name, scan_code, remark,
          pallet_entry_quantity, box_entry_quantity, case_entry_quantity,
          category_code, box_code, case_code
        FROM products
        WHERE item_code = ? AND IFNULL(is_active,1)=1
        """,
        (item_code,)
    )
    product = cur.fetchone()
    if not product:
        conn.close()
        flash("해당 상품을 찾을 수 없습니다. (마스터에 없거나 비활성화된 상품)", "warning")
        return redirect(url_for("register_home"))

    action = (request.form.get("_action", "") or "").strip()

    # ✅ 비고 저장
    if request.method == "POST" and action == "save_remark":
        remark = (request.form.get("remark", "") or "").strip()
        cur.execute("UPDATE products SET remark=? WHERE item_code=?", (remark, item_code))
        conn.commit()
        conn.close()
        flash("비고 저장 완료", "success")
        return redirect(url_for("register_product_detail", item_code=item_code))

    # ✅ 사진 업로드(3종 분리)
    if request.method == "POST" and action == "upload_photos":
        fields = [
            ("photos_item", PHOTO_TYPE_ITEM),
            ("photos_box", PHOTO_TYPE_BOX),
            ("photos_case", PHOTO_TYPE_CASE),
        ]

        saved_count = 0
        fail_count = 0

        for field_name, ptype in fields:
            files = request.files.getlist(field_name) or []
            for f_ in files:
                if not f_ or not f_.filename:
                    continue
                if not allowed_file(f_.filename):
                    flash(f"{PHOTO_TYPE_LABELS[ptype]}: 허용되지 않는 확장자 → {f_.filename}", "warning")
                    fail_count += 1
                    continue

                original = safe_filename(f_.filename)
                stem, ext = (original.rsplit(".", 1) + ["jpg"])[:2]
                final_name = safe_filename(f"{stem}_{ptype}_{int(datetime.now().timestamp())}.jpg")

                key = s3_key_for(item_code, final_name, ptype)

                try:
                    data = save_low_quality_jpeg_to_bytes(f_)
                    s3_put_bytes(key, data, content_type="image/jpeg")
                except Exception as e:
                    flash(f"{PHOTO_TYPE_LABELS[ptype]} 업로드 실패: {f_.filename} → {e}", "danger")
                    fail_count += 1
                    continue

                cur.execute(
                    "INSERT INTO photos (product_item_code, filename, uploaded_at, s3_key, photo_type) VALUES (?, ?, ?, ?, ?)",
                    (item_code, final_name, datetime.now().isoformat(timespec="seconds"), key, ptype)
                )
                saved_count += 1

        conn.commit()
        conn.close()
        flash(f"사진 업로드: 성공 {saved_count} / 실패 {fail_count} (S3 저장)", "success" if saved_count else "warning")
        return redirect(url_for("register_product_detail", item_code=item_code))

    # GET
    cur.execute(
        "SELECT id, filename, uploaded_at, s3_key, photo_type FROM photos WHERE product_item_code=? ORDER BY id DESC",
        (item_code,)
    )
    photos_rows = cur.fetchall()
    conn.close()

    grouped = {t: [] for t in PHOTO_TYPES}
    for r in photos_rows:
        ptype = (r["photo_type"] or PHOTO_TYPE_ITEM).upper()
        if ptype not in PHOTO_TYPES:
            ptype = PHOTO_TYPE_ITEM
        key = r["s3_key"] or s3_key_for(item_code, r["filename"], ptype)
        grouped[ptype].append({
            "id": r["id"],
            "filename": r["filename"],
            "uploaded_at": r["uploaded_at"],
            "photo_type": ptype,
            "label": PHOTO_TYPE_LABELS.get(ptype, ptype),
            "url": presigned_get_url(key, expires_sec=PRESIGNED_EXPIRES),
        })

    return render_template(
        "product_detail.html",
        product=product,
        photos_grouped=grouped,
        photo_labels=PHOTO_TYPE_LABELS,
        item_code=item_code,
        presigned_expires=PRESIGNED_EXPIRES,
        presigned_refresh_margin=PRESIGNED_REFRESH_MARGIN,
    )


@app.route("/register/photo/<int:photo_id>/delete", methods=["POST"])
@require_edit_auth
def register_delete_photo(photo_id: int):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT id, product_item_code, filename, s3_key, photo_type FROM photos WHERE id = ?", (photo_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        abort(404)

    item_code = row["product_item_code"]
    filename = row["filename"]
    ptype = (row["photo_type"] or PHOTO_TYPE_ITEM).upper()
    if ptype not in PHOTO_TYPES:
        ptype = PHOTO_TYPE_ITEM

    key = row["s3_key"] or s3_key_for(item_code, filename, ptype)

    cur.execute("DELETE FROM photos WHERE id = ?", (photo_id,))
    conn.commit()
    conn.close()

    s3_delete(key)

    flash("사진 삭제 완료", "success")
    return redirect(url_for("register_product_detail", item_code=item_code))


# =========================
# healthz
# =========================
@app.get("/healthz")
def healthz():
    return jsonify({"ok": True})


# =========================
# 부트스트랩
# =========================
def bootstrap():
    ensure_dirs()
    init_db()

bootstrap()


def main_cli():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        nargs="?",
        default="run",
        choices=["run", "rebuild_photos_from_s3"]
    )
    args = parser.parse_args()

    if args.command == "rebuild_photos_from_s3":
        result = rebuild_photos_from_s3(prefix="products/")
        print(f"[OK] S3 scan done. scanned={result['scanned']} inserted={result['inserted']}")
        return

    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "5000")),
        debug=FLASK_DEBUG
    )


if __name__ == "__main__":
    main_cli()
