#!/usr/bin/env python3
"""
Скрипт для тестирования динамического масштабирования агентов.
Перед запуском убедитесь, что система запущена (docker compose up -d) и
агенты замедлены (time.Sleep(2*time.Second) внутри income-analyzer/risk-evaluator).
"""
import subprocess
import time
import sys
import requests
from concurrent.futures import ThreadPoolExecutor

BASE_URL = "http://localhost:8080"
NUM_REQUESTS = 30          # Количество параллельных задач
CHECK_INTERVAL = 5         # Пауза между проверками состояния контейнеров
MAX_WAIT_SCALE_UP = 60     # Максимальное время ожидания масштабирования вверх (сек)
MAX_WAIT_SCALE_DOWN = 120  # Максимальное время ожидания масштабирования вниз (сек)

def docker_ps_count(service_name):
    """Возвращает количество работающих контейнеров с заданной меткой сервиса."""
    try:
        result = subprocess.run(
            ["docker", "ps", "--filter", f"label=com.docker.compose.service={service_name}",
             "--format", "{{.Names}}"],
            capture_output=True, text=True, check=True
        )
        lines = [line.strip() for line in result.stdout.split("\n") if line.strip()]
        return len(lines)
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] docker ps failed: {e}")
        return 0

def send_request(client_id):
    """Отправляет один POST-запрос к оркестратору."""
    try:
        resp = requests.post(f"{BASE_URL}/start", json={"client_id": client_id}, timeout=5)
        if resp.status_code == 200:
            return resp.json().get("task_id")
        else:
            print(f"[WARN] Request {client_id} returned {resp.status_code}")
            return None
    except Exception as e:
        print(f"[ERROR] Request {client_id} failed: {e}")
        return None

def wait_for_scale_up(service_name, initial_count, timeout=MAX_WAIT_SCALE_UP):
    """Ждёт увеличения количества контейнеров до > initial_count."""
    print(f"[INFO] Waiting for {service_name} to scale up (initial count: {initial_count})...")
    start = time.time()
    while time.time() - start < timeout:
        current = docker_ps_count(service_name)
        print(f"       {service_name}: current={current}, initial={initial_count}")
        if current > initial_count:
            print(f"[OK] {service_name} scaled up to {current} containers after {time.time()-start:.1f}s")
            return True
        time.sleep(CHECK_INTERVAL)
    print(f"[FAIL] {service_name} did NOT scale up within {timeout}s")
    return False

def wait_for_scale_down(service_name, initial_count, timeout=MAX_WAIT_SCALE_DOWN):
    """Ждёт возврата количества контейнеров к initial_count (или меньше)."""
    print(f"[INFO] Waiting for {service_name} to scale down (target: <= {initial_count})...")
    start = time.time()
    while time.time() - start < timeout:
        current = docker_ps_count(service_name)
        print(f"       {service_name}: current={current}, target<={initial_count}")
        if current <= initial_count:
            print(f"[OK] {service_name} scaled down to {current} containers after {time.time()-start:.1f}s")
            return True
        time.sleep(CHECK_INTERVAL)
    print(f"[FAIL] {service_name} did NOT scale down within {timeout}s")
    return False

def check_orchestrator_logs():
    """Проверяет наличие ключевых сообщений в логах оркестратора."""
    try:
        result = subprocess.run(
            ["docker", "logs", "orchestrator"],
            capture_output=True, text=True
        )
        logs = result.stdout
        if "Scaling UP income-analyzer" in logs and "Scaling UP risk-evaluator" in logs:
            print("[OK] Found 'Scaling UP' messages in orchestrator logs")
        else:
            print("[WARN] Missing 'Scaling UP' messages in orchestrator logs")
        if "Scaling DOWN income-analyzer" in logs and "Scaling DOWN risk-evaluator" in logs:
            print("[OK] Found 'Scaling DOWN' messages in orchestrator logs")
        else:
            print("[WARN] Missing 'Scaling DOWN' messages in orchestrator logs")
    except subprocess.CalledProcessError:
        print("[ERROR] Unable to read orchestrator logs")

def main():
    print("=== Dynamic Scaling Test ===")
    # 1. Определяем начальное количество контейнеров
    initial_income = docker_ps_count("income-analyzer")
    initial_risk = docker_ps_count("risk-evaluator")
    print(f"[INFO] Initial containers: income-analyzer={initial_income}, risk-evaluator={initial_risk}")

    # 2. Генерируем нагрузку – отправляем много параллельных запросов
    print(f"[INFO] Sending {NUM_REQUESTS} parallel requests...")
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=NUM_REQUESTS) as executor:
        futures = [executor.submit(send_request, f"test-load-{i}") for i in range(NUM_REQUESTS)]
        results = [f.result() for f in futures]
    success = [r for r in results if r]
    print(f"[INFO] Sent requests: {len(success)}/{NUM_REQUESTS} successful, took {time.time()-start_time:.1f}s")

    # 3. Ожидаем реакцию оркестратора: масштабирование вверх
    up_ok_income = wait_for_scale_up("income-analyzer", initial_income)
    up_ok_risk = wait_for_scale_up("risk-evaluator", initial_risk)

    # 4. Ждём, пока очередь обработается (задачи с задержкой 2 секунды * 30 запросов)
    print("[INFO] Waiting for all tasks to be processed (agents have 2s delay)...")
    time.sleep(70)   # 30 задач * 2 сек = 60 сек + запас

    # 5. Проверяем уменьшение масштаба
    down_ok_income = wait_for_scale_down("income-analyzer", initial_income)
    down_ok_risk = wait_for_scale_down("risk-evaluator", initial_risk)

    # 6. Анализируем логи оркестратора
    check_orchestrator_logs()

    # Итог
    if up_ok_income and up_ok_risk and down_ok_income and down_ok_risk:
        print("\n[SUCCESS] Dynamic scaling test PASSED.")
    else:
        print("\n[FAIL] Dynamic scaling test FAILED. Check if agents have artificial delay (time.Sleep) and docker socket is accessible.")
        print("Hint: For the test to work, agents must process tasks slower than the rate of incoming requests.")
        print("      Add 'time.Sleep(2 * time.Second)' in income-analyzer and risk-evaluator handlers, then rebuild images.")

if __name__ == "__main__":
    main()