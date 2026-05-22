# Prompt Log — Лабораторная работа №13, Вариант 19

## Задание: Финтех — кредитный скоринг

### Промпт 1

**Инструмент:** DeepSeek

**Промпт:**
```
Разработай подробный план архитектуры распределённой системы кредитного скоринга.
Требования:
- 4–5 микросервисов на Go + один агент на Python (LLM), обмен через NATS (JetStream).
- Pipeline: сбор данных о клиенте → анализ доходов → оценка риска → формирование решения → пояснение LLM.
- Отдельный orchestrator управляет цепочкой.
- Redis для состояния агента оценки риска (счётчики, кэш).
- OpenTelemetry + Jaeger в Docker.
- Динамическое масштабирование агентов income/risk через Docker API при высокой нагрузке.
- Аукцион: агенты подают ставки (cost, skill, availability), оркестратор выбирает лучшего.
- Веб-панель Streamlit: статус, результаты, ручной запуск заявки.
Опиши структуру папок, NATS-топики, модели данных и docker-compose сервисы.
```

**Результат:** Получен план со структурой `agents/`, `orchestrator/`, `common/`, топиками `data.collect`, `scoring.income.do`, `auction.*`, `decision.make`, `llm.explain.request`.

---

### Промпт 2

**Инструмент:** DeepSeek

**Промпт:**
```
Создай основу проекта кредитного скоринга на Go.
Реализуй:
- common/models.go: ClientData, IncomeAnalysis, RiskAssessment, Decision, ScoringResult, Bid, AuctionRequest.
- agents/data-collector: ответ на data.collect с синтетическим профилем клиента.
- agents/income-analyzer и risk-evaluator: QueueSubscribe на scoring.income.do / scoring.risk.do, задержка 2 сек для нагрузочных тестов.
- agents/decision-maker: decision.make по income + risk.
- orchestrator: подписка scoring.request, последовательный pipeline collect → income → risk → decision.
- docker-compose.yml: nats (-js), redis, jaeger, все сервисы в сети scoring_net.
```

**Результат:** Собран скелет системы, pipeline работает через NATS request-reply. Оркестратор обрабатывает заявки синхронно.

---

### Промпт 3

**Инструмент:** DeepSeek

**Промпт:**
```
Интегрируй OpenTelemetry во все Go-сервисы и настрой экспорт в Jaeger.
Добавь common/telemetry.go: InitTracerProvider, InjectTrace/ExtractTrace через заголовки NATS.
Прокидывай trace context в orchestrator и агентах на каждом шаге pipeline.
```

**Результат:** Трассировки видны в Jaeger UI (:16686), spans на collect, income, risk, decision.

---

### Промпт 4

**Инструмент:** DeepSeek

**Промпт:**
```
Реализуй агента risk-evaluator с сохранением состояния в Redis.
При старте загружай risk_evaluator_state (total_evaluated, last_evaluated, model_cache).
После каждой оценки обновляй счётчик и сохраняй в Redis.
Оркестратор пусть сохраняет готовые ScoringResult в result:{client_id} и recent_results.
```

**Результат:** Состояние risk-evaluator переживает перезапуск контейнера. Результаты доступны для web-ui.

---

### Промпт 5

**Инструмент:** DeepSeek

**Промпт:**
```
Добавь динамическое масштабирование в orchestrator через Docker HTTP API (unix socket).
Счётчики активных задач в Redis: scaler:active_income, scaler:active_risk.
При active > порога — scale up (до 5 реплик), при active == 0 и реплик > 1 — scale down.
Создавай контейнеры с меткой com.docker.compose.service, подключай к сети compose.
REST POST /start для нагрузочного тестирования (publish scoring.request без ожидания ответа).
Напиши test_scaling.py: 30 параллельных запросов, проверка роста/снижения числа контейнеров.
```

**Результат:** Scale up/down работает. Позже выяснилось, что нужна параллельная обработка scoring.request (goroutine) и корректные имена образов/сети compose.

---

### Промпт 6

**Инструмент:** DeepSeek

**Промпт:**
```
Скейлинг не работает: в логах pending=0, контейнеры не растут.
Исправь orchestrator: не смотри JetStream consumer, используй Redis active_*.
Обработку scoring.request делай в goroutine на каждое сообщение.
income и risk в pipeline запускай параллельно (оба используют clientData).
Образ и docker network определяй из уже запущенных контейнеров, не хардкодь lab13_*.
```

**Результат:** `test_scaling.py` проходит 4/4. В логах появились `Scaling UP/DOWN income-analyzer` и `risk-evaluator`.

---

### Промпт 7

**Инструмент:** DeepSeek

**Промпт:**
```
Добавь LLM-агента на Python в проект кредитного скоринга.
Подписка llm.explain.request, вход: decision + client, выход: JSON с полем explanation.
Используй Groq API (переменная GROQ_API_KEY), fallback-текст при ошибке API.
Dockerfile на Python 3.11, зависимости в requirements.txt.
Подключи в docker-compose и в pipeline оркестратора после decision.make.
```

**Результат:** `llm-agent` генерирует пояснение решения. Без ключа API возвращается заглушка.

---

### Промпт 8

**Инструмент:** DeepSeek

**Промпт:**
```
Сделай веб-интерфейс мониторинга на Streamlit (web-ui/app.py).
Показывай recent_results из Redis (до 20 заявок) с раскрывающимися JSON.
Форма: ввод client_id и кнопка Submit — запрос scoring.request через nats-py с таймаутом 15 сек.
Dockerfile, порт 8501, зависимости streamlit, redis, nats-py.
```

**Результат:** Панель на http://localhost:8501, ручной запуск заявок и просмотр результатов.

---

### Промпт 9

**Инструмент:** DeepSeek

**Промпт:**
```
Реализуй аукционное распределение задач вместо прямой отправки в очередь.
Агенты на auction.income.analyze и auction.risk.analyze отвечают Bid: cost, load, skill, availability.
Оркестратор собирает ставки 800 мс, выбирает минимальный score (common/auction.go).
Задачу отправляет победителю на scoring.income.agent.{hostname} / scoring.risk.agent.{hostname}.
Ставки учитывают загрузку агента и данные клиента (тип занятости, кредитная история).
```

**Результат:** В логах оркестратора: `Auction income: N bids, winner=...`. Аукцион интегрирован в основной pipeline.

---

### Промпт 10

**Инструмент:** DeepSeek

**Промпт:**
```
Перенеси test_scaling.py и run-pipeline.ps1 в папку tests/.
Почини run-pipeline.ps1: docker logs на Windows пишет в stderr и ломает скрипт при ErrorAction Stop.
Добавь автоочистку лишних scaled-контейнеров перед pytest (initial=5 ломает тесты).
Напиши README.md по образцу лабы 12: задание, таблица требований, структура, запуск, тесты.
```

**Результат:** `tests/run-pipeline.ps1`, `tests/test_scaling.py`, `README.md`. Пайплайн: `.\tests\run-pipeline.ps1`.

---

### Итого

| Показатель | Значение |
|------------|----------|
| Количество промптов | 10 |
| Инструменты | DeepSeek (план, LLM, UI, скейлинг, аукцион, тесты, README) |
| Что правили вручную | Имена compose-проекта (`lab13_3`), очистка «зависших» контейнеров после тестов, `GROQ_API_KEY`, stderr в PowerShell |
| Время (оценка) | ~4–5 часов |

---
