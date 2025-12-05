"""
Завантаження існуючих чанків з Pinecone для локального бекапу.
Запуск: python scripts/download_chunks.py
"""

import os
import sys
import json
from pathlib import Path
from dotenv import load_dotenv
from pinecone import Pinecone

# Fix Windows console encoding for Ukrainian
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

# Завантаження конфігурації
load_dotenv(Path(__file__).parent / ".env")

PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
PINECONE_INDEX = os.getenv("PINECONE_INDEX", "accounting-policy")
CHUNKS_DIR = Path(__file__).parent.parent / "archived_chunks"
NAMESPACE = "default"


def download_all_chunks():
    """Завантажує всі чанки з Pinecone та зберігає локально."""
    print("=" * 60)
    print("PINECONE CHUNK DOWNLOADER")
    print("=" * 60)

    if not PINECONE_API_KEY:
        print("ERROR: PINECONE_API_KEY не знайдено в scripts/.env")
        return

    # Створення папки для чанків
    CHUNKS_DIR.mkdir(exist_ok=True)
    print(f"\nПапка для чанків: {CHUNKS_DIR}")

    # Підключення
    print(f"\n[1/4] Підключення до Pinecone...")
    pc = Pinecone(api_key=PINECONE_API_KEY)
    index = pc.Index(PINECONE_INDEX)
    print(f"      Index: {PINECONE_INDEX}")

    # Статистика індексу
    stats = index.describe_index_stats()
    ns_stats = stats.namespaces.get(NAMESPACE, {})
    total_vectors = getattr(ns_stats, 'vector_count', 0)
    print(f"      Namespace: {NAMESPACE}")
    print(f"      Vectors: {total_vectors}")

    if total_vectors == 0:
        print("\n      INFO: Індекс порожній, немає чанків для завантаження")
        return

    # Отримання всіх ID
    print(f"\n[2/4] Отримання списку ID...")
    all_ids = []

    for ids_batch in index.list(namespace=NAMESPACE):
        all_ids.extend(ids_batch)
        print(f"      Отримано: {len(all_ids)} IDs", end="\r")

    print(f"      Всього ID: {len(all_ids)}          ")

    # Завантаження записів (batch по 100)
    print(f"\n[3/4] Завантаження записів...")
    all_records = {}
    batch_size = 100

    for i in range(0, len(all_ids), batch_size):
        batch_ids = all_ids[i:i + batch_size]
        result = index.fetch(ids=batch_ids, namespace=NAMESPACE)

        for record_id, record in result.vectors.items():
            all_records[record_id] = {
                "id": record_id,
                "metadata": dict(record.metadata) if record.metadata else {}
            }

        downloaded = min(i + batch_size, len(all_ids))
        print(f"      Завантажено: {downloaded}/{len(all_ids)}", end="\r")

    print(f"      Завантажено: {len(all_records)} записів          ")

    # Групування по файлах
    print(f"\n[4/4] Збереження локально...")
    files_data = {}

    for record_id, record in all_records.items():
        metadata = record["metadata"]
        filename = metadata.get("filename", "unknown")

        if filename not in files_data:
            files_data[filename] = {
                "filename": filename,
                "doc_type": metadata.get("doc_type", "unknown"),
                "total_chunks": metadata.get("total_chunks", 0),
                "chunks": []
            }

        files_data[filename]["chunks"].append({
            "id": record_id,
            "chunk_index": metadata.get("chunk_index", 0),
            "text": metadata.get("text", "")  # Текст в metadata
        })

    # Сортування чанків по індексу
    for filename in files_data:
        files_data[filename]["chunks"].sort(key=lambda x: x["chunk_index"])

    # Збереження по файлах
    for filename, data in files_data.items():
        safe_filename = filename.replace("/", "_").replace("\\", "_")
        output_path = CHUNKS_DIR / f"{safe_filename}.json"

        output_path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )
        print(f"      {filename}: {len(data['chunks'])} chunks")

    # Зберігаємо також загальний індекс
    index_data = {
        "pinecone_index": PINECONE_INDEX,
        "namespace": NAMESPACE,
        "total_records": len(all_records),
        "files": {
            filename: {
                "chunks_count": len(data["chunks"]),
                "chunk_ids": [c["id"] for c in data["chunks"]]
            }
            for filename, data in files_data.items()
        }
    }

    index_path = CHUNKS_DIR / "_index.json"
    index_path.write_text(
        json.dumps(index_data, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    print("\n" + "=" * 60)
    print(f"ГОТОВО!")
    print(f"  Завантажено: {len(all_records)} chunks")
    print(f"  Файлів: {len(files_data)}")
    print(f"  Збережено в: {CHUNKS_DIR}")
    print("=" * 60)


if __name__ == "__main__":
    download_all_chunks()
