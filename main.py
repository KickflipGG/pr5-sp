"""
    Разработайте программу на Python, которая отправляет асинхронные запросы к API 
    для получения информации о марке и характеристики автомобилей. 
    Используйте aiohttp для выполнения асинхронных запросов и обработайте ответы так, 
    чтобы отобразить результаты в консоли.
    Программа должна:
        1.Получать данные о марке и характеристики автомобиля из двух различных API одновременно.
        2.Обрабатывать ошибки запросов (например, неправильные данные, отсутствие подключения).
        3.Ограничить количество одновременных запросов до трёх, используя asyncio.Semaphore.
        4.Реализовать таймаут для каждого запроса (например, 5 секунд), чтобы избежать долгого ожидания ответа.
        5.Собрать и сохранить данные в формате JSON.
    Дополнительно: Реализуйте функцию для сбора данных с заданной частотой (например, раз в 10 минут) и сохраните результаты в файл с отметкой времени.
"""

import asyncio
import aiohttp
import json
import time
from datetime import datetime

MAKES_API_URL = "https://vpic.nhtsa.dot.gov/api/vehicles/getallmakes?format=json"
MODELS_API_URL = "https://vpic.nhtsa.dot.gov/api/vehicles/getmodelsformake/{make}?format=json"

TIMEOUT = 5
MAX_CONCURRENT_REQUESTS = 3
RETRIES = 3

async def fetch_data(url, session, semaphore, retries=RETRIES):
    for attempt in range(1, retries + 1):
        try:
            async with semaphore:
                async with session.get(url) as response:
                    if response.status == 200:
                        print(f"[OK] {url} (попытка {attempt})")
                        return await response.json()
                    if response.status >= 500 or response.status == 429:
                        print(f"[{response.status}] {url} — повтор (попытка {attempt}/{retries})")
                    else:
                        print(f"[Ошибка {response.status}] {url} — без повтора")
                        return None
        except asyncio.TimeoutError:
            print(f"[Таймаут] {url} (попытка {attempt}/{retries})")
        except aiohttp.ClientError as e:
            print(f"[Ошибка соединения] {url}: {e} (попытка {attempt}/{retries})")

        if attempt < retries:
            await asyncio.sleep(1) 

    print(f"[Провал] Не удалось получить данные: {url}")
    return None

async def get_car_data(make_name, session, semaphore):
    models_url = MODELS_API_URL.format(make=make_name)
    print(f"\n{'='*50}\nЗапуск запросов для марки: {make_name.upper()}\n{'='*50}")

    makes_task = fetch_data(MAKES_API_URL, session, semaphore)
    models_task = fetch_data(models_url, session, semaphore)
    makes_data, models_data = await asyncio.gather(makes_task, models_task)

    result = {
        "timestamp": datetime.now().isoformat(),
        "make_query": make_name,
        "makes_count": len(makes_data.get("Results", [])) if makes_data else 0,
        "models": [],
        "models_count": 0,
        "errors": [],
    }
    if models_data and "Results" in models_data:
        result["models"] = [
            {"make": item.get("Make_Name"), "model": item.get("Model_Name")}
            for item in models_data["Results"][:20]
        ]
        result["models_count"] = len(models_data["Results"])
    else:
        result["errors"].append(f"Не удалось получить модели для {make_name}")

    if not makes_data:
        result["errors"].append("Не удалось получить список марок")
    return result

def save_data(data, filename="cars_data.json"):
    try:
        with open(filename, "a", encoding="utf-8") as file:
            file.write(json.dumps(data, ensure_ascii=False) + "\n")
        print(f"[Сохранено] {filename}")
    except Exception as e:
        print(f"[Ошибка сохранения] {e}")

def save_json_pretty(data, filename="cars_data_pretty.json"):
    try:
        with open(filename, "w", encoding="utf-8") as file:
            json.dump(data, file, ensure_ascii=False, indent=2)
        print(f"[Сохранено] {filename}")
    except Exception as e:
        print(f"[Ошибка сохранения] {e}")

async def periodic_collection(interval_seconds=600, makes=None):
    if makes is None:
        makes = ["honda", "toyota", "ford"]

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    timeout = aiohttp.ClientTimeout(total=TIMEOUT)
    iteration = 0
    while True:
        iteration += 1
        print(f"\n{'#'*60}\n# ИТЕРАЦИЯ {iteration} — {datetime.now():%Y-%m-%d %H:%M:%S}\n{'#'*60}")
        async with aiohttp.ClientSession(timeout=timeout) as session:
            results = await asyncio.gather(*(get_car_data(make, session, semaphore) for make in makes))
        for result in results:
            save_data(result)
        print(f"\n[Ожидание] Следующий сбор через {interval_seconds} сек...")
        await asyncio.sleep(interval_seconds)

async def main():
    print("=" * 60)
    print("АСИНХРОННЫЙ СБОР ДАННЫХ ОБ АВТОМОБИЛЯХ")
    print("=" * 60)

    start_time = time.time()
    makes = ["honda", "toyota", "ford"]
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    timeout = aiohttp.ClientTimeout(total=TIMEOUT)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        results = await asyncio.gather(*(get_car_data(make, session, semaphore) for make in makes))

    print("\n" + "=" * 60 + "\nРЕЗУЛЬТАТЫ\n" + "=" * 60)
    for r in results:
        print(f"\nМарка: {r['make_query'].upper()}")
        print(f"  Всего марок в API: {r['makes_count']}")
        print(f"  Моделей найдено: {r['models_count']}")
        print(f"  Первые модели: {[m['model'] for m in r['models'][:5]]}")
        if r["errors"]:
            print(f"  Ошибки: {r['errors']}")

    save_json_pretty(results, "cars_data_all.json")
    for r in results:
        save_data(r)

    print(f"\n[Время выполнения] {time.time() - start_time:.2f} сек")
    print(f"[Всего запросов] {len(makes) * 2}")

if __name__ == "__main__":
    asyncio.run(main())