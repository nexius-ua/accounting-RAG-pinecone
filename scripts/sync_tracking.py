"""
Синхронізація tracking.json з локальними чанками.
Використовуйте для ініціалізації трекінгу з завантажених чанків.

Запуск: python scripts/sync_tracking.py
"""

import sys
import json
from pathlib import Path

# Fix Windows console encoding for Ukrainian
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

# Імпортуємо функції з основного скрипта
from chunk_and_upload import (
    load_tracking,
    save_tracking,
    compute_file_hash,
    PINECONE_INDEX,
    NAMESPACE
)

CHUNKS_DIR = Path(__file__).parent.parent / "archived_chunks"
ARCHIVED_SOURCE_DIR = Path(__file__).parent.parent / "archived_source_docs"


def sync_from_local_chunks():
    """Синхронізує трекінг з локальними чанками."""
    print("=" * 60)
    print("SYNC TRACKING FROM LOCAL CHUNKS")
    print("=" * 60)

    # Перевірка папки з чанками
    if not CHUNKS_DIR.exists():
        print(f"\nERROR: Папка {CHUNKS_DIR} не існує")
        print("Спочатку завантажте чанки: python scripts/download_chunks.py")
        return

    # Знаходимо всі JSON файли з чанками (крім _index.json)
    chunk_files = [f for f in CHUNKS_DIR.glob("*.json") if f.name != "_index.json"]
    print(f"\nЗнайдено файлів з чанками: {len(chunk_files)}")

    if not chunk_files:
        print("Немає файлів для синхронізації")
        return

    # Завантажуємо поточний трекінг
    tracking = load_tracking()
    existing_count = len(tracking.get("files", {}))
    print(f"Вже в трекінгу: {existing_count} файлів")

    # Обробка кожного файлу з чанками
    added = 0
    skipped = 0
    updated = 0

    for chunk_file in chunk_files:
        # Читаємо дані чанків
        chunk_data = json.loads(chunk_file.read_text(encoding="utf-8"))
        filename = chunk_data.get("filename", chunk_file.stem)

        # Отримуємо chunk IDs з локального файлу
        chunk_ids = [c["id"] for c in chunk_data.get("chunks", [])]
        chunks_count = len(chunk_ids)

        # Шукаємо оригінальний файл для обчислення хешу
        source_file = ARCHIVED_SOURCE_DIR / filename
        if source_file.exists():
            content_hash = compute_file_hash(source_file)
            source_location = "archived_source_docs"
        else:
            # Якщо оригіналу немає - використовуємо хеш з chunk_ids
            content_hash = f"chunks_only_{chunk_file.stem[:16]}"
            source_location = "chunks_only"

        # Перевіряємо чи вже є в трекінгу
        if filename in tracking.get("files", {}):
            existing = tracking["files"][filename]
            # Порівнюємо chunk_ids
            if set(existing.get("chunk_ids", [])) == set(chunk_ids):
                print(f"  ⏭️  {filename} - без змін")
                skipped += 1
                continue
            else:
                print(f"  🔄 {filename} - оновлено ({chunks_count} chunks)")
                updated += 1
        else:
            print(f"  ✓ {filename} - {chunks_count} chunks")
            added += 1

        # Оновлюємо/додаємо до трекінгу
        tracking["files"][filename] = {
            "content_hash": content_hash,
            "chunk_ids": chunk_ids,
            "chunks_count": chunks_count,
            "uploaded_at": chunk_data.get("uploaded_at", "2025-12-01T00:00:00"),
            "source": source_location
        }

    # Зберігаємо
    save_tracking(tracking)

    print("\n" + "=" * 60)
    print(f"ГОТОВО!")
    print(f"  Додано: {added} файлів")
    print(f"  Оновлено: {updated} файлів")
    print(f"  Без змін: {skipped} файлів")
    print(f"  Всього в трекінгу: {len(tracking['files'])} файлів")
    print("=" * 60)


if __name__ == "__main__":
    sync_from_local_chunks()
