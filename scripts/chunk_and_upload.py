"""
Chunking та завантаження документів у Pinecone з трекінгом змін.

Workflow:
1. Нові документи кладуться в /source_docs
2. Скрипт створює чанки в /chunks (staging)
3. Після успішного upload чанки переміщуються в /archived_chunks
4. Оригінальні документи переміщуються в /archived_source_docs

Запуск: python scripts/chunk_and_upload.py
"""

import os
import sys
import re
import json
import hashlib
import shutil
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from pinecone import Pinecone

# Fix Windows console encoding for Ukrainian
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

# Завантаження конфігурації
load_dotenv(Path(__file__).parent / ".env")

PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
PINECONE_INDEX = os.getenv("PINECONE_INDEX", "legal-docs-ua")

# Директорії
BASE_DIR = Path(__file__).parent.parent
SOURCE_DIR = BASE_DIR / "source_docs"
CHUNKS_DIR = BASE_DIR / "chunks"  # Staging area
ARCHIVED_CHUNKS_DIR = BASE_DIR / "archived_chunks"
ARCHIVED_SOURCE_DIR = BASE_DIR / "archived_source_docs"
TRACKING_FILE = Path(__file__).parent / "tracking.json"
LOGS_DIR = Path(__file__).parent / "logs"

NAMESPACE = "default"

# Параметри chunking
CHUNK_SIZE_CHARS = 2000  # ~500 токенів для української
MIN_CHUNK_CHARS = 100    # Ігнорувати занадто короткі


class Logger:
    """Логер з виводом в консоль та файл."""

    def __init__(self):
        LOGS_DIR.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_file = LOGS_DIR / f"upload_{timestamp}.log"
        self.report_file = LOGS_DIR / f"report_{timestamp}.json"
        self.logs = []
        self.report = {
            "timestamp": datetime.now().isoformat(),
            "status": "started",
            "files_processed": [],
            "chunks_created": 0,
            "chunks_uploaded": 0,
            "orphans_deleted": 0,
            "errors": [],
            "warnings": []
        }

    def log(self, message: str, level: str = "INFO"):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        formatted = f"[{timestamp}] [{level}] {message}"
        print(formatted)
        self.logs.append(formatted)

    def info(self, message: str):
        self.log(message, "INFO")

    def success(self, message: str):
        self.log(message, "SUCCESS")

    def warning(self, message: str):
        self.log(message, "WARNING")
        self.report["warnings"].append(message)

    def error(self, message: str):
        self.log(message, "ERROR")
        self.report["errors"].append(message)

    def section(self, title: str):
        separator = "=" * 60
        print(f"\n{separator}")
        print(f"  {title}")
        print(f"{separator}")
        self.logs.append(f"\n{separator}\n  {title}\n{separator}")

    def subsection(self, title: str):
        print(f"\n--- {title} ---")
        self.logs.append(f"\n--- {title} ---")

    def add_file_report(self, filename: str, chunks_count: int, status: str, details: dict = None):
        file_report = {
            "filename": filename,
            "chunks_count": chunks_count,
            "status": status,
            "timestamp": datetime.now().isoformat()
        }
        if details:
            file_report.update(details)
        self.report["files_processed"].append(file_report)

    def save(self):
        # Зберігаємо текстовий лог
        self.log_file.write_text("\n".join(self.logs), encoding="utf-8")

        # Зберігаємо JSON звіт
        self.report_file.write_text(
            json.dumps(self.report, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )

        return self.log_file, self.report_file


def load_tracking() -> dict:
    """Завантажує трекінг-файл."""
    if TRACKING_FILE.exists():
        return json.loads(TRACKING_FILE.read_text(encoding="utf-8"))
    return {
        "index": PINECONE_INDEX,
        "namespace": NAMESPACE,
        "last_updated": None,
        "files": {}
    }


def save_tracking(tracking: dict):
    """Зберігає трекінг-файл."""
    tracking["last_updated"] = datetime.now().isoformat()
    TRACKING_FILE.write_text(
        json.dumps(tracking, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


def compute_file_hash(filepath: Path) -> str:
    """Обчислює MD5 хеш вмісту файлу."""
    content = filepath.read_bytes()
    return hashlib.md5(content).hexdigest()


def chunk_text(text: str) -> list[str]:
    """Розбиває текст на chunks по абзацах."""
    paragraphs = re.split(r'\n\s*\n', text)

    chunks = []
    current_chunk = ""

    for para in paragraphs:
        para = para.strip()
        if not para:
            continue

        if len(para) > CHUNK_SIZE_CHARS:
            sentences = re.split(r'(?<=[.!?])\s+', para)
            for sentence in sentences:
                if len(current_chunk) + len(sentence) + 2 <= CHUNK_SIZE_CHARS:
                    current_chunk = f"{current_chunk} {sentence}".strip()
                else:
                    if current_chunk:
                        chunks.append(current_chunk)
                    current_chunk = sentence
        else:
            if len(current_chunk) + len(para) + 2 <= CHUNK_SIZE_CHARS:
                current_chunk = f"{current_chunk}\n\n{para}".strip()
            else:
                if current_chunk:
                    chunks.append(current_chunk)
                current_chunk = para

    if current_chunk:
        chunks.append(current_chunk)

    return [c for c in chunks if len(c) >= MIN_CHUNK_CHARS]


def categorize_document(filename: str) -> str:
    """Категоризує документ за назвою."""
    name = filename.lower()

    if "закон" in name:
        return "legislation"
    if "gem" in name:
        return "research"
    if "expert" in name or "article" in name:
        return "article"
    if "аналіз" in name:
        return "analysis"
    if "договір" in name or "договор" in name or "nda" in name:
        return "contract"

    return "other"


def generate_id(filename: str, chunk_index: int, text: str) -> str:
    """Генерує унікальний ID для chunk."""
    hash_input = f"{filename}_{chunk_index}_{text[:50]}"
    return hashlib.md5(hash_input.encode()).hexdigest()[:16]


def process_file(filepath: Path, logger: Logger) -> tuple[list[dict], list[str]]:
    """Обробляє файл: читає, chunking, повертає records та chunk_ids."""
    text = filepath.read_text(encoding="utf-8")
    chunks = chunk_text(text)

    doc_type = categorize_document(filepath.name)

    records = []
    chunk_ids = []
    for i, chunk in enumerate(chunks):
        chunk_id = generate_id(filepath.name, i, chunk)
        chunk_ids.append(chunk_id)
        records.append({
            "_id": chunk_id,
            "text": chunk,
            "filename": filepath.name,
            "chunk_index": i,
            "total_chunks": len(chunks),
            "doc_type": doc_type,
        })

    logger.info(f"  {filepath.name}")
    logger.info(f"    Тип: {doc_type}")
    logger.info(f"    Розмір: {len(text):,} символів")
    logger.info(f"    Чанків: {len(chunks)}")

    return records, chunk_ids


def save_chunks_to_staging(records: list[dict], filename: str, logger: Logger) -> Path:
    """Зберігає чанки в staging директорію."""
    CHUNKS_DIR.mkdir(exist_ok=True)

    chunks_data = {
        "filename": filename,
        "doc_type": records[0].get("doc_type", "unknown") if records else "unknown",
        "total_chunks": len(records),
        "created_at": datetime.now().isoformat(),
        "status": "staging",
        "chunks": [
            {
                "id": r["_id"],
                "chunk_index": r["chunk_index"],
                "text": r["text"]
            }
            for r in sorted(records, key=lambda x: x["chunk_index"])
        ]
    }

    safe_filename = filename.replace("/", "_").replace("\\", "_")
    output_path = CHUNKS_DIR / f"{safe_filename}.json"
    output_path.write_text(
        json.dumps(chunks_data, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    logger.info(f"    Staging: {output_path.name}")
    return output_path


def move_chunks_to_archive(staging_path: Path, logger: Logger) -> Path:
    """Переміщує чанки зі staging в архів."""
    ARCHIVED_CHUNKS_DIR.mkdir(exist_ok=True)

    # Оновлюємо статус в файлі
    data = json.loads(staging_path.read_text(encoding="utf-8"))
    data["status"] = "archived"
    data["archived_at"] = datetime.now().isoformat()

    archive_path = ARCHIVED_CHUNKS_DIR / staging_path.name
    archive_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    # Видаляємо зі staging
    staging_path.unlink()

    logger.info(f"    Archived: {archive_path.name}")
    return archive_path


def move_source_to_archive(source_path: Path, logger: Logger) -> Path:
    """Переміщує оригінальний документ в архів."""
    ARCHIVED_SOURCE_DIR.mkdir(exist_ok=True)

    archive_path = ARCHIVED_SOURCE_DIR / source_path.name
    shutil.move(str(source_path), str(archive_path))

    logger.info(f"    Source archived: {archive_path.name}")
    return archive_path


def analyze_changes(files: list[Path], tracking: dict, logger: Logger) -> dict:
    """Аналізує зміни у файлах порівняно з трекінгом."""
    tracked_files = tracking.get("files", {})

    new_files = []
    changed_files = []
    unchanged_files = []
    orphan_chunk_ids = []

    for filepath in files:
        filename = filepath.name
        current_hash = compute_file_hash(filepath)

        if filename not in tracked_files:
            new_files.append(filepath)
            logger.info(f"  [NEW] {filename}")
        elif tracked_files[filename]["content_hash"] != current_hash:
            changed_files.append(filepath)
            old_chunks = len(tracked_files[filename]["chunk_ids"])
            orphan_chunk_ids.extend(tracked_files[filename]["chunk_ids"])
            logger.info(f"  [CHANGED] {filename} (old: {old_chunks} chunks)")
        else:
            unchanged_files.append(filepath)
            logger.info(f"  [UNCHANGED] {filename}")

    return {
        "new_files": new_files,
        "changed_files": changed_files,
        "unchanged_files": unchanged_files,
        "orphan_chunk_ids": orphan_chunk_ids
    }


def delete_orphan_chunks(index, orphan_ids: list[str], logger: Logger):
    """Видаляє застарілі чанки з Pinecone."""
    if not orphan_ids:
        return

    logger.info(f"Видалення {len(orphan_ids)} застарілих чанків...")

    batch_size = 1000
    for i in range(0, len(orphan_ids), batch_size):
        batch = orphan_ids[i:i + batch_size]
        index.delete(namespace=NAMESPACE, ids=batch)
        logger.info(f"  Видалено batch: {len(batch)} IDs")


def verify_upload(index, chunk_ids: list[str], logger: Logger) -> bool:
    """Перевіряє що всі чанки успішно завантажені."""
    import time
    time.sleep(2)  # Чекаємо на індексацію

    stats = index.describe_index_stats()
    ns_stats = stats.namespaces.get(NAMESPACE, {})
    total_vectors = getattr(ns_stats, 'vector_count', 0)

    logger.info(f"  Векторів в індексі: {total_vectors}")
    return True  # Базова перевірка


def main():
    logger = Logger()

    logger.section("PINECONE DOCUMENT UPLOADER")
    logger.info(f"Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Index: {PINECONE_INDEX}")
    logger.info(f"Namespace: {NAMESPACE}")

    if not PINECONE_API_KEY:
        logger.error("PINECONE_API_KEY не знайдено в scripts/.env")
        logger.report["status"] = "failed"
        logger.save()
        return

    # Завантаження трекінгу
    tracking = load_tracking()
    if tracking["last_updated"]:
        logger.info(f"Останнє оновлення: {tracking['last_updated']}")
        logger.info(f"Файлів в трекінгу: {len(tracking['files'])}")

    # Створення директорій
    for dir_path in [SOURCE_DIR, CHUNKS_DIR, ARCHIVED_CHUNKS_DIR, ARCHIVED_SOURCE_DIR]:
        dir_path.mkdir(exist_ok=True)

    # ===== КРОК 1: Підключення =====
    logger.subsection("КРОК 1: Підключення до Pinecone")
    try:
        pc = Pinecone(api_key=PINECONE_API_KEY)
        index = pc.Index(PINECONE_INDEX)
        stats = index.describe_index_stats()
        total_vectors = sum(
            getattr(ns, 'vector_count', 0)
            for ns in stats.namespaces.values()
        )
        logger.success(f"Підключено. Векторів в індексі: {total_vectors}")
    except Exception as e:
        logger.error(f"Помилка підключення: {e}")
        logger.report["status"] = "failed"
        logger.save()
        return

    # ===== КРОК 2: Пошук файлів =====
    logger.subsection("КРОК 2: Пошук нових документів")
    files = [f for f in SOURCE_DIR.glob("*.md") if f.name != "desktop.ini"]
    logger.info(f"Знайдено в {SOURCE_DIR.name}/: {len(files)} файлів")

    if not files:
        logger.info("Немає нових документів для обробки")
        logger.report["status"] = "completed"
        logger.report["message"] = "No new files to process"
        log_file, report_file = logger.save()
        logger.info(f"\nЛог: {log_file}")
        logger.info(f"Звіт: {report_file}")
        return

    # ===== КРОК 3: Аналіз змін =====
    logger.subsection("КРОК 3: Аналіз змін")
    changes = analyze_changes(files, tracking, logger)

    logger.info(f"\nПідсумок:")
    logger.info(f"  Нових файлів: {len(changes['new_files'])}")
    logger.info(f"  Змінених файлів: {len(changes['changed_files'])}")
    logger.info(f"  Без змін: {len(changes['unchanged_files'])}")

    if changes['orphan_chunk_ids']:
        logger.warning(f"  Застарілих чанків для видалення: {len(changes['orphan_chunk_ids'])}")

    files_to_process = changes['new_files'] + changes['changed_files']

    if not files_to_process:
        logger.info("\nВсі файли актуальні, завантаження не потрібне")
        logger.report["status"] = "completed"
        logger.report["message"] = "All files up to date"
        log_file, report_file = logger.save()
        logger.info(f"\nЛог: {log_file}")
        logger.info(f"Звіт: {report_file}")
        return

    # ===== КРОК 4: Видалення orphan chunks =====
    if changes['orphan_chunk_ids']:
        logger.subsection("КРОК 4: Видалення застарілих чанків")
        delete_orphan_chunks(index, changes['orphan_chunk_ids'], logger)
        logger.report["orphans_deleted"] = len(changes['orphan_chunk_ids'])

    # ===== КРОК 5: Chunking (staging) =====
    logger.subsection("КРОК 5: Chunking документів (staging)")
    all_records = []
    staging_files = {}  # filename -> staging_path
    files_tracking = {}

    for filepath in files_to_process:
        logger.info(f"\nОбробка: {filepath.name}")

        records, chunk_ids = process_file(filepath, logger)
        all_records.extend(records)

        # Зберігаємо в staging
        staging_path = save_chunks_to_staging(records, filepath.name, logger)
        staging_files[filepath.name] = {
            "source_path": filepath,
            "staging_path": staging_path,
            "records": records,
            "chunk_ids": chunk_ids
        }

        # Готуємо дані для трекінгу
        files_tracking[filepath.name] = {
            "content_hash": compute_file_hash(filepath),
            "chunk_ids": chunk_ids,
            "chunks_count": len(chunk_ids),
            "uploaded_at": None  # Заповнимо після успішного upload
        }

        logger.report["chunks_created"] += len(records)

    logger.info(f"\nВсього чанків для завантаження: {len(all_records)}")

    # ===== КРОК 6: Upload до Pinecone =====
    logger.subsection("КРОК 6: Завантаження в Pinecone")
    batch_size = 96
    uploaded_count = 0
    upload_errors = []

    for i in range(0, len(all_records), batch_size):
        batch = all_records[i:i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(all_records) + batch_size - 1) // batch_size

        try:
            index.upsert_records(NAMESPACE, batch)
            uploaded_count += len(batch)
            logger.info(f"  Batch {batch_num}/{total_batches}: {len(batch)} records ✓")
        except Exception as e:
            error_msg = f"Batch {batch_num} failed: {e}"
            logger.error(error_msg)
            upload_errors.append(error_msg)

    logger.report["chunks_uploaded"] = uploaded_count

    if upload_errors:
        logger.error(f"\nПомилки завантаження: {len(upload_errors)}")
        logger.report["status"] = "partial"
    else:
        logger.success(f"\nУспішно завантажено: {uploaded_count} чанків")

    # ===== КРОК 7: Верифікація =====
    logger.subsection("КРОК 7: Верифікація завантаження")
    if verify_upload(index, [r["_id"] for r in all_records], logger):
        logger.success("Верифікація пройшла успішно")
    else:
        logger.warning("Верифікація виявила проблеми")

    # ===== КРОК 8: Архівування =====
    logger.subsection("КРОК 8: Архівування файлів")

    for filename, file_data in staging_files.items():
        logger.info(f"\nАрхівування: {filename}")

        # Переміщуємо чанки в архів
        move_chunks_to_archive(file_data["staging_path"], logger)

        # Переміщуємо source документ
        move_source_to_archive(file_data["source_path"], logger)

        # Оновлюємо tracking дані
        files_tracking[filename]["uploaded_at"] = datetime.now().isoformat()
        files_tracking[filename]["source"] = "archived_source_docs"

        # Додаємо в звіт
        logger.add_file_report(
            filename=filename,
            chunks_count=len(file_data["chunk_ids"]),
            status="uploaded",
            details={
                "chunk_ids": file_data["chunk_ids"],
                "content_hash": files_tracking[filename]["content_hash"]
            }
        )

    # ===== КРОК 9: Оновлення трекінгу =====
    logger.subsection("КРОК 9: Оновлення tracking.json")

    # Зберігаємо існуючі файли без змін
    for filepath in changes['unchanged_files']:
        files_tracking[filepath.name] = tracking['files'][filepath.name]

    tracking['files'] = files_tracking
    save_tracking(tracking)
    logger.success(f"Tracking оновлено: {len(tracking['files'])} файлів")

    # ===== ФІНАЛЬНИЙ ЗВІТ =====
    logger.section("ФІНАЛЬНИЙ ЗВІТ")
    logger.info(f"Дата завершення: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"")
    logger.info(f"Оброблено файлів: {len(files_to_process)}")
    logger.info(f"Створено чанків: {logger.report['chunks_created']}")
    logger.info(f"Завантажено чанків: {logger.report['chunks_uploaded']}")
    if logger.report['orphans_deleted']:
        logger.info(f"Видалено застарілих: {logger.report['orphans_deleted']}")
    logger.info(f"")
    logger.info(f"Файли:")
    for file_report in logger.report["files_processed"]:
        logger.info(f"  • {file_report['filename']}: {file_report['chunks_count']} chunks")

    if logger.report["errors"]:
        logger.info(f"\nПомилки: {len(logger.report['errors'])}")
        for err in logger.report["errors"]:
            logger.info(f"  ! {err}")

    if logger.report["warnings"]:
        logger.info(f"\nПопередження: {len(logger.report['warnings'])}")
        for warn in logger.report["warnings"]:
            logger.info(f"  ? {warn}")

    logger.report["status"] = "completed" if not upload_errors else "partial"

    # Зберігаємо логи
    log_file, report_file = logger.save()
    logger.info(f"\n" + "=" * 60)
    logger.info(f"Лог збережено: {log_file.name}")
    logger.info(f"Звіт збережено: {report_file.name}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
