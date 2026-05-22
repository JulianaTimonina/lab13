# test_scaling.py
"""
Pytest-тесты динамического масштабирования агентов.
Требования:
- система поднята через docker compose up -d
- внутри income-analyzer/risk-evaluator есть искусственная задержка (time.Sleep(2*time.Second))
- доступен Docker (метки compose) и порт оркестратора 8080
"""
import pytest
import subprocess
import time
import requests
from concurrent.futures import ThreadPoolExecutor

BASE_URL = "http://localhost:8080"
NUM_REQUESTS = 30
CHECK_INTERVAL = 5
MAX_WAIT_SCALE_UP = 60
MAX_WAIT_SCALE_DOWN = 120
MAX_REPLICAS = 5  # как maxIncomeReplicas / maxRiskReplicas в orchestrator


def get_compose_project() -> str:
    """Имя compose-проекта (например lab13_3) по контейнеру orchestrator."""
    result = subprocess.run(
        [
            "docker", "ps", "-q",
            "--filter", "label=com.docker.compose.service=orchestrator",
        ],
        capture_output=True, text=True, check=True,
    )
    cid = result.stdout.strip().split()[0]
    if not cid:
        pytest.fail("Контейнер orchestrator не найден — сначала: docker compose up -d")
    result = subprocess.run(
        ["docker", "inspect", "-f", "{{index .Config.Labels \"com.docker.compose.project\"}}", cid],
        capture_output=True, text=True, check=True,
    )
    project = result.stdout.strip()
    if not project:
        pytest.fail("Не удалось определить compose project")
    return project


def list_running_service_containers(service_name: str) -> list[str]:
    result = subprocess.run(
        [
            "docker", "ps",
            "--filter", f"label=com.docker.compose.service={service_name}",
            "--format", "{{.Names}}",
        ],
        capture_output=True, text=True, check=True,
    )
    return [l.strip() for l in result.stdout.splitlines() if l.strip()]


def base_compose_container(service_name: str, names: list[str]) -> str | None:
    """Базовый контейнер compose: *-income-analyzer-1 (не income-analyzer-1779...)."""
    suffix = f"-{service_name}-1"
    matches = [n for n in names if n.endswith(suffix)]
    return matches[0] if matches else None


def prune_scaled_workers(service_name: str) -> int:
    """Останавливает лишние воркеры. Возвращает число остановленных."""
    names = list_running_service_containers(service_name)
    keep = base_compose_container(service_name, names)
    if not keep:
        return 0
    stopped = 0
    for name in names:
        if name != keep:
            subprocess.run(["docker", "stop", "-t", "3", name], capture_output=True, check=False)
            stopped += 1
    return stopped


def reset_scaler_redis_counters(project: str) -> None:
    redis = f"{project}-redis-1"
    subprocess.run(
        ["docker", "exec", redis, "redis-cli", "DEL", "scaler:active_income", "scaler:active_risk"],
        capture_output=True,
        check=False,
    )


def ensure_baseline_workers(project: str, timeout: int = 90) -> None:
    """Доводит до 1 базового воркера на сервис; иначе оркестратор снова scale-up."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        reset_scaler_redis_counters(project)
        prune_scaled_workers("income-analyzer")
        prune_scaled_workers("risk-evaluator")
        income = docker_ps_count("income-analyzer")
        risk = docker_ps_count("risk-evaluator")
        if income < MAX_REPLICAS and risk < MAX_REPLICAS:
            return
        time.sleep(3)
    pytest.fail(
        f"Не удалось сбросить воркеры за {timeout} с "
        f"(income={docker_ps_count('income-analyzer')}, risk={docker_ps_count('risk-evaluator')}). "
        "Остановите лишние контейнеры вручную или: docker compose -p lab13_3 down"
    )


def docker_ps_count(service_name: str) -> int:
    """Возвращает количество запущенных контейнеров сервиса по compose-метке."""
    try:
        result = subprocess.run(
            [
                "docker", "ps",
                "--filter", f"label=com.docker.compose.service={service_name}",
                "--format", "{{.Names}}"
            ],
            capture_output=True, text=True, check=True
        )
        lines = [l.strip() for l in result.stdout.split("\n") if l.strip()]
        return len(lines)
    except subprocess.CalledProcessError as e:
        pytest.fail(f"docker ps failed: {e}")


def send_request(client_id: str):
    """Отправляет POST /start и возвращает task_id или None."""
    try:
        resp = requests.post(f"{BASE_URL}/start", json={"client_id": client_id}, timeout=5)
        if resp.status_code == 200:
            return resp.json().get("task_id")
        else:
            return None
    except Exception:
        return None


def redis_active_count(project: str, key: str) -> int:
    redis = f"{project}-redis-1"
    result = subprocess.run(
        ["docker", "exec", redis, "redis-cli", "GET", key],
        capture_output=True, text=True, check=False,
    )
    val = result.stdout.strip()
    return int(val) if val.isdigit() else 0


def wait_for(predicate, timeout: int, description: str):
    """Утилита активного ожидания с assert-ом в случае таймаута."""
    start = time.time()
    while time.time() - start < timeout:
        if predicate():
            return True
        time.sleep(CHECK_INTERVAL)
    pytest.fail(f"{description} не выполнено за {timeout} секунд")


class TestDynamicScaling:
    """Группа тестов, проверяющих реакцию оркестратора на нагрузку."""

    @pytest.fixture(scope="class", autouse=True)
    def reset_workers_before_tests(self):
        """
        Убирает «зависшие» контейнеры от прошлых прогонов (income-analyzer-17...).
        Иначе initial=5 и scale-up тесты не могут пройти — лимит уже достигнут.
        """
        project = get_compose_project()
        ensure_baseline_workers(project)
        yield
        # после тестов тоже прибираем, чтобы не мешать следующему запуску
        ensure_baseline_workers(project, timeout=120)

    @pytest.fixture(autouse=True)
    def setup(self):
        """Сохраняет начальное количество воркеров перед каждым тестом."""
        self.initial_income = docker_ps_count("income-analyzer")
        self.initial_risk = docker_ps_count("risk-evaluator")
        assert self.initial_income > 0, "income-analyzer должен быть запущен"
        assert self.initial_risk > 0, "risk-evaluator должен быть запущен"
        if self.initial_income >= MAX_REPLICAS or self.initial_risk >= MAX_REPLICAS:
            project = get_compose_project()
            ensure_baseline_workers(project)
            self.initial_income = docker_ps_count("income-analyzer")
            self.initial_risk = docker_ps_count("risk-evaluator")
        if self.initial_income >= MAX_REPLICAS:
            pytest.fail(
                f"income-analyzer всё ещё {self.initial_income} шт. (макс. {MAX_REPLICAS}). "
                "Остановите лишние или перезапустите stack."
            )
        if self.initial_risk >= MAX_REPLICAS:
            pytest.fail(
                f"risk-evaluator всё ещё {self.initial_risk} шт. (макс. {MAX_REPLICAS}). "
                "Остановите лишние воркеры или перезапустите stack."
            )

    def test_scale_up_income_analyzer(self):
        """Отправка 30 параллельных запросов должна вызвать увеличение контейнеров income-analyzer."""
        with ThreadPoolExecutor(max_workers=NUM_REQUESTS) as executor:
            futures = [executor.submit(send_request, f"load-{i}") for i in range(NUM_REQUESTS)]
            results = [f.result() for f in futures]
        successful = [r for r in results if r]
        assert len(successful) == NUM_REQUESTS, (
            f"Не все запросы успешны: {len(successful)}/{NUM_REQUESTS}"
        )

        # Ждём масштабирования вверх
        def scaled_up():
            current = docker_ps_count("income-analyzer")
            return current > self.initial_income

        wait_for(scaled_up, MAX_WAIT_SCALE_UP,
                 f"income-analyzer не увеличил количество контейнеров (исходное={self.initial_income})")

    def test_scale_up_risk_evaluator(self):
        """После нагрузки должен увеличиться и risk-evaluator."""
        project = get_compose_project()
        reset_scaler_redis_counters(project)

        # Два залпа: сначала заполняем пайплайн, потом добиваем — чтобы вырос active_risk в Redis
        for batch in range(2):
            with ThreadPoolExecutor(max_workers=NUM_REQUESTS) as executor:
                futures = [
                    executor.submit(send_request, f"risk-{batch}-{i}")
                    for i in range(NUM_REQUESTS)
                ]
                [f.result() for f in futures]

            def scaled_up():
                return docker_ps_count("risk-evaluator") > self.initial_risk

            try:
                wait_for(
                    scaled_up, 45,
                    f"risk-evaluator (залп {batch + 1}, исходное={self.initial_risk})",
                )
                return
            except Exception:
                pass
            time.sleep(15)

        pytest.fail(
            f"risk-evaluator не увеличил количество контейнеров (исходное={self.initial_risk})"
        )

    def test_scale_down(self):
        """После обработки очереди контейнеры должны вернуться к исходному числу."""
        # Даём время на обработку (30 задач * 2 сек каждая + запас)
        time.sleep(70)

        def income_back():
            return docker_ps_count("income-analyzer") <= self.initial_income

        def risk_back():
            return docker_ps_count("risk-evaluator") <= self.initial_risk

        wait_for(income_back, MAX_WAIT_SCALE_DOWN,
                 f"income-analyzer не уменьшился до <= {self.initial_income}")
        wait_for(risk_back, MAX_WAIT_SCALE_DOWN,
                 f"risk-evaluator не уменьшился до <= {self.initial_risk}")

    def test_orchestrator_logs(self):
        """Проверяет наличие ключевых сообщений в логах оркестратора."""
        # Если предыдущие тесты не дали scale-up risk — короткая нагрузка
        project = get_compose_project()
        with ThreadPoolExecutor(max_workers=NUM_REQUESTS) as executor:
            futures = [
                executor.submit(send_request, f"logs-{i}") for i in range(NUM_REQUESTS)
            ]
            [f.result() for f in futures]
        time.sleep(25)

        container = f"{project}-orchestrator-1"
        try:
            result = subprocess.run(
                ["docker", "logs", container],
                capture_output=True, text=True, check=True
            )
        except subprocess.CalledProcessError:
            pytest.fail(f"Не удалось прочитать логи {container}")

        # На Windows docker logs часто идёт в stderr
        logs = result.stdout + result.stderr
        assert "Scaling UP income-analyzer" in logs, (
            "В логах оркестратора нет 'Scaling UP income-analyzer'"
        )
        assert "Scaling UP risk-evaluator" in logs, (
            "В логах оркестратора нет 'Scaling UP risk-evaluator'"
        )
        assert "Scaling DOWN income-analyzer" in logs, (
            "В логах оркестратора нет 'Scaling DOWN income-analyzer'"
        )
        assert "Scaling DOWN risk-evaluator" in logs, (
            "В логах оркестратора нет 'Scaling DOWN risk-evaluator'"
        )