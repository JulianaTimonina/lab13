# Лабораторная работа №13
## Тимонина Юлиана Александровна, группа 221131, вариант 19

### Задание

**Предметная область:** Финтех — кредитный скоринг (повышенная сложность)

**Реализованные требования:**

| № | Требование | Реализация в проекте |
|---|------------|----------------------|
| 1 | 3–5 агентов на Go, NATS | `data-collector`, `income-analyzer`, `risk-evaluator`, `decision-maker`, `orchestrator` |
| 2 | Цепочка задач (pipeline) | Оркестратор: сбор данных → анализ доходов и риска → решение → пояснение LLM |
| 3 | Трассировка Jaeger | OpenTelemetry во всех Go-сервисах и LLM-агенте, UI Jaeger |
| 4 | Агент с состоянием (Redis) | `risk-evaluator` хранит статистику в Redis, восстанавливает при старте |
| 5 | Динамическое масштабирование | Оркестратор через Docker API: scale up/down для `income-analyzer` и `risk-evaluator` |
| 6 | Аукционное распределение | Ставки `cost`, `skill`, `availability` → выбор победителя по минимальному score |
| 7 | LLM-агент (Python) | `llm-agent` — пояснение решения (Groq API) |
| 8 | Веб-интерфейс | Streamlit: результаты, ручной запуск заявки |

**Типы агентов по предметной области:**

- Сбор данных о клиенте — `data-collector`
- Анализ доходов — `income-analyzer`
- Оценка риска — `risk-evaluator`
- Формирование решения — `decision-maker`
- Пояснение решения (LLM) — `llm-agent`

---

### Структура репозитория

```
l13/
├── .gitignore
├── docker-compose.yml
├── README.md
│
├── common/                    # общие модели, NATS, Redis, OpenTelemetry
│   ├── models.go
│   ├── auction.go
│   ├── nats.go
│   ├── redis.go
│   └── telemetry.go
│
├── orchestrator/              # pipeline, аукцион, autoscaling, REST :8080
│   ├── main.go
│   ├── Dockerfile
│   └── go.mod
│
├── agents/
│   ├── data-collector/
│   ├── income-analyzer/
│   ├── risk-evaluator/        # состояние в Redis
│   ├── decision-maker/
│   └── llm-agent/             # Python + Groq
│       ├── main.py
│       └── requirements.txt
│
├── web-ui/                    # Streamlit :8501
│   ├── app.py
│   └── Dockerfile
│
└── tests/
    ├── test_scaling.py        # pytest: scale up/down, логи оркестратора
    └── run-pipeline.ps1       # сборка, up, smoke, pytest
```

---

### Схема pipeline

```
Клиент / Web UI / POST :8080/start
        │
        ▼
   Orchestrator
        │
        ├─► data-collector      (сбор ClientData)
        │
        ├─► auction.income ──► income-analyzer   (параллельно)
        └─► auction.risk   ──► risk-evaluator
        │
        ├─► decision-maker      (одобрение / сумма / ставка)
        └─► llm-agent           (текстовое пояснение)
        │
        ▼
   Redis (результаты) + ответ клиенту
```

---

### Запуск

**Требования:** Docker Desktop, Python 3 (для тестов), переменная `GROQ_API_KEY` для LLM-агента.

```powershell
# из корня проекта
cd l13_U

# ключ для llm-agent (PowerShell)
$env:GROQ_API_KEY = "ваш_ключ"

# сборка и запуск всей системы
docker compose -p lab13_3 up -d --build
```

**Сервисы после запуска:**

| Сервис | URL |
|--------|-----|
| Веб-панель (Streamlit) | http://localhost:8501 |
| REST оркестратора | http://localhost:8080/start |
| Jaeger UI | http://localhost:16686 |
| NATS monitoring | http://localhost:8222 |

**Ручной запуск заявки (REST):**

```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:8080/start" `
  -ContentType "application/json" -Body '{"client_id":"client-001"}'
```

**Просмотр аукциона в логах:**

```powershell
docker logs lab13_3-orchestrator-1 2>&1 | Select-String "Auction"
```

---

### Запуск тестов

```powershell
# полный пайплайн: build → up → smoke → pytest
.\tests\run-pipeline.ps1

# только pytest (если stack уже поднят)
.\tests\run-pipeline.ps1 -SkipBuild -SkipUp

# или вручную из корня
pip install pytest requests
pytest tests/test_scaling.py -v
```

Тесты проверяют динамическое масштабирование (`income-analyzer`, `risk-evaluator`) и сообщения `Scaling UP/DOWN` в логах оркестратора.

---

### Начальные данные (только для разработки и тестирования)

`data-collector` при запросе `data.collect` возвращает **синтетический профиль клиента** по `client_id` (фиксированные поля: возраст, доход, тип занятости, кредитная история).  
Это сделано **для удобства демонстрации pipeline**, без внешней БД клиентов.

В `income-analyzer` и `risk-evaluator` задана искусственная задержка `2 сек` на обработку — чтобы нагрузочные тесты успевали зафиксировать scale up.

В `web-ui` можно отправить заявку с произвольным `client_id` — результат сохраняется в Redis (`result:{id}`, список `recent_results`).
