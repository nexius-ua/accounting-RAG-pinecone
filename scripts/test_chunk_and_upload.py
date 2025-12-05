"""
Тести для chunk_and_upload.py
Запуск: python -m pytest scripts/test_chunk_and_upload.py -v
"""

import pytest
from chunk_and_upload import (
    chunk_text,
    categorize_document,
    generate_id,
    CHUNK_SIZE_CHARS,
    MIN_CHUNK_CHARS,
)


class TestChunkText:
    """Тести для функції chunk_text."""

    def test_empty_text(self):
        """Порожній текст повертає порожній список."""
        assert chunk_text("") == []
        assert chunk_text("   \n\n   ") == []

    def test_short_text_single_chunk(self):
        """Текст >= MIN_CHUNK_CHARS залишається одним chunk."""
        # Текст має бути >= 100 символів (MIN_CHUNK_CHARS)
        text = "Це текст про NDA договір. " * 5  # ~130 символів
        chunks = chunk_text(text)
        assert len(chunks) == 1

    def test_paragraphs_split(self):
        """Абзаци розділяються на окремі chunks якщо перевищують ліміт."""
        para1 = "А" * 1500
        para2 = "Б" * 1500
        text = f"{para1}\n\n{para2}"
        chunks = chunk_text(text)
        assert len(chunks) == 2

    def test_small_paragraphs_combined(self):
        """Малі абзаци об'єднуються в один chunk."""
        # Кожен абзац ~50 символів, разом ~150 > MIN_CHUNK_CHARS
        para = "Це абзац тексту про договір NDA номер "
        text = f"{para}один.\n\n{para}два.\n\n{para}три."
        chunks = chunk_text(text)
        assert len(chunks) == 1
        assert "один" in chunks[0]
        assert "три" in chunks[0]

    def test_very_short_chunks_filtered(self):
        """Chunks менше MIN_CHUNK_CHARS відфільтровуються."""
        text = "Коротко.\n\n" + "Х" * 500
        chunks = chunk_text(text)
        # "Коротко." має < MIN_CHUNK_CHARS, тому відфільтрується
        assert all(len(c) >= MIN_CHUNK_CHARS for c in chunks)

    def test_long_paragraph_split_by_sentences(self):
        """Довгий абзац розбивається по реченнях."""
        # Створюємо абзац більший за CHUNK_SIZE_CHARS (2000)
        # "Це речення номер один. " = 24 символи, потрібно ~100 для 2400 символів
        sentences = ["Це речення номер один. "] * 100
        text = "".join(sentences)
        chunks = chunk_text(text)
        assert len(chunks) > 1
        assert all(len(c) <= CHUNK_SIZE_CHARS + 100 for c in chunks)

    def test_unicode_handling(self):
        """Коректна обробка українського тексту."""
        text = "Договір про нерозголошення конфіденційної інформації. " * 20
        chunks = chunk_text(text)
        assert len(chunks) >= 1
        assert "Договір" in chunks[0]


class TestCategorizeDocument:
    """Тести для функції categorize_document."""

    def test_legislation(self):
        """Файли із 'закон' категоризуються як legislation."""
        assert categorize_document("Закон про авторське право.md") == "legislation"
        assert categorize_document("закон_про_ІВ.md") == "legislation"

    def test_research_gems(self):
        """Файли Gem категоризуються як research."""
        assert categorize_document("Gem 1 Договори про ОІВ.md") == "research"
        assert categorize_document("gem_15_аналіз.md") == "research"

    def test_articles(self):
        """Expert articles категоризуються як article."""
        assert categorize_document("13 Expert article - NDA.md") == "article"
        assert categorize_document("article_про_nca.md") == "article"

    def test_contracts(self):
        """Договори категоризуються як contract."""
        assert categorize_document("договір_NDA.md") == "contract"
        assert categorize_document("NDA_template.md") == "contract"

    def test_analysis(self):
        """Аналізи категоризуються як analysis."""
        assert categorize_document("Аналіз_змін_документа.md") == "analysis"
        assert categorize_document("Gem 7 Аналіз змін NDA.md") == "research"  # Gem має пріоритет

    def test_other(self):
        """Невідомі файли категоризуються як other."""
        assert categorize_document("random_file.md") == "other"
        assert categorize_document("notes.md") == "other"


class TestGenerateId:
    """Тести для функції generate_id."""

    def test_consistent_id(self):
        """Однакові вхідні дані генерують однаковий ID."""
        id1 = generate_id("file.md", 0, "text content")
        id2 = generate_id("file.md", 0, "text content")
        assert id1 == id2

    def test_different_files_different_ids(self):
        """Різні файли генерують різні ID."""
        id1 = generate_id("file1.md", 0, "text")
        id2 = generate_id("file2.md", 0, "text")
        assert id1 != id2

    def test_different_chunks_different_ids(self):
        """Різні chunk_index генерують різні ID."""
        id1 = generate_id("file.md", 0, "text")
        id2 = generate_id("file.md", 1, "text")
        assert id1 != id2

    def test_id_length(self):
        """ID має бути 16 символів."""
        id1 = generate_id("file.md", 0, "text")
        assert len(id1) == 16

    def test_id_is_hex(self):
        """ID має бути hex-рядком."""
        id1 = generate_id("file.md", 0, "text")
        int(id1, 16)  # Має не викидати exception


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
