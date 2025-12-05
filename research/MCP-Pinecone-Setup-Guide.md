# Налаштування Pinecone MCP для Claude Code

## Що дає Pinecone MCP

| Інструмент | Призначення |
|------------|-------------|
| `search-docs` | Пошук у документації Pinecone |
| `search-records` | **Пошук у вашому індексі** |
| `list-indexes` | Список індексів |
| `describe-index` | Опис конфігурації індексу |
| `describe-index-stats` | Статистика індексу |
| `upsert-records` | Завантаження даних |
| `cascading-search` | Пошук по кількох індексах |
| `rerank-documents` | Переранжування результатів |

---

## Крок 1: Створити `.mcp.json` в корені проєкту

```json
{
  "mcpServers": {
    "pinecone-accounting": {
      "command": "npx",
      "args": ["-y", "@pinecone-database/mcp"],
      "env": {
        "PINECONE_API_KEY": "pcsk_ваш_ключ_тут"
      }
    }
  }
}
```

> ⚠️ **Важливо:** Вставте реальний API ключ. Синтаксис `${PINECONE_API_KEY}` НЕ підтримується.

---

## Крок 2: Активувати сервер

### Варіант А: Через CLI (рекомендовано)

```bash
# Скинути попередні вибори (якщо були)
claude mcp reset-project-choices

# Перезапустити Claude Code — з'явиться запит на дозвіл
```

### Варіант Б: Вручну через локальні налаштування проєкту

Якщо запит не з'явився, створіть файл `.claude/settings.local.json` в корені проєкту:

```json
{
  "enabledMcpjsonServers": ["pinecone-accounting"]
}
```

Структура:
```
ваш-проєкт/
├── .claude/
│   └── settings.local.json   # Локальні налаштування (не комітити)
├── .mcp.json                  # Конфігурація MCP серверів
└── ...
```

> Файл `settings.local.json` є специфічним для вашої машини і не має комітитися в git.

---

## Крок 3: Перевірити підключення

```bash
# В терміналі
claude mcp list

# Очікуваний результат:
# pinecone-accounting: npx -y @pinecone-database/mcp - ✓ Connected
```

Або в Claude Code:
```
/mcp
```

---

## Крок 4: Використання

Після підключення Claude Code автоматично отримає доступ до інструментів.

### Приклад пошуку в індексі:

```
Знайди в Pinecone документи про "облікова політика амортизація"
```

Claude використає `search-records` для пошуку у вашому індексі.

---

## Типові помилки

| Помилка | Причина | Рішення |
|---------|---------|---------|
| `Skipping database tools -- PINECONE_API_KEY not set` | Ключ не передається | Перевірте що ключ в `env`, а не в `args` |
| Тільки `search-docs` доступний | Немає API ключа | Додайте реальний ключ в `.mcp.json` |
| Сервер не з'являється в `/mcp` | Не активовано | Виконайте Крок 2 |
| `${PINECONE_API_KEY}` не працює | Змінні не підтримуються | Вставте реальний ключ |

---

## Структура файлів

```
ваш-проєкт/
├── .mcp.json          # Конфігурація MCP (НЕ комітити якщо є ключі!)
├── .gitignore         # Додати: .mcp.json
├── .env               # API ключі (опціонально, для скриптів)
└── ...
```

---

## Безпека

Додайте до `.gitignore`:
```
.mcp.json
.env
.claude/settings.local.json
```

Або використовуйте user-level конфігурацію (доступна в усіх проєктах):

```bash
claude mcp add-json pinecone-global '{"type":"stdio","command":"npx","args":["-y","@pinecone-database/mcp"],"env":{"PINECONE_API_KEY":"pcsk_..."}}' -s user
```
