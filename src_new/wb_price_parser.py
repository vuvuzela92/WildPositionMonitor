import asyncio
import json
import os
import random
from curl_cffi.requests import AsyncSession

# Импортируем настройки из нашего конфигурационного файла
from src_new.config import (
    MAX_ARTICLES_TO_PROCESS,
    PRODUCT_LIST,
    WB_PRICE_URL,
    WB_RAW_COOKIES,
)


class WbPriceParser:
    # URL для получения информации о цене товара
    url = WB_PRICE_URL
    # Куки для прохождения проверок безопасности Wildberries
    cookies = WB_RAW_COOKIES

    @staticmethod
    async def get_wb_product_data(session, item_id):
        """Асинхронный запрос к внутреннему API Wildberries для одного товара."""
        params = {"appType": 1, "curr": "rub", "dest": "-1257786", "nm": item_id}

        headers = {
            "Accept": "*/*",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8",
            "Connection": "keep-alive",
            "Cookie": WbPriceParser.cookies,
            "Origin": "https://www.wildberries.ru",
            "Referer": f"https://www.wildberries.ru/catalog/{item_id}/detail.aspx",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/147.0.0.0 Safari/537.36"
            ),
        }

        try:
            response = await session.get(
                WbPriceParser.url, params=params, headers=headers, timeout=15
            )

            if response.status_code == 200:
                return item_id, response.json()

            return item_id, {
                "error": f"Сервер вернул код ошибки: {response.status_code}"
            }

        except Exception as e:
            return item_id, {"error": f"Ошибка сети при запросе: {str(e)}"}

    @staticmethod
    async def process_product(session, item_id, semaphore, index, total):
        """Обработка одного товара внутри батча с ограничением параллельных запросов."""
        async with semaphore:
            print(
                f"   [Товар {index}/{total}] Получение данных для артикула: {item_id}"
            )

            item_id, result = await WbPriceParser.get_wb_product_data(
                session, item_id
            )

            if "error" not in result:
                print(f"      ✅ Данные для {item_id} успешно получены!")
            else:
                print(f"      ❌ Ошибка для {item_id}: {result['error']}")

            # Случайная пауза между запросами от 0.5 до 1.5 секунд, чтобы имитировать действия человека
            await asyncio.sleep(random.uniform(0.5, 1.5))

            return item_id, result

    @staticmethod
    async def parse_batch(session, batch_products, max_concurrent=10):
        """Асинхронно обрабатывает ОДИН конкретный батч (порцию) товаров."""
        results = {}
        # Семафор ограничивает количество одновременно выполняемых запросов внутри батча
        semaphore = asyncio.Semaphore(max_concurrent)

        tasks = [
            WbPriceParser.process_product(
                session=session,
                item_id=item_id,
                semaphore=semaphore,
                index=index,
                total=len(batch_products),
            )
            for index, item_id in enumerate(batch_products, 1)
        ]

        # Запускаем все задачи текущего батча параллельно и ждем их выполнения
        responses = await asyncio.gather(*tasks)

        for item_id, data in responses:
            results[item_id] = data

        return results

    @staticmethod
    def save_results(new_data, filename="wb_products_data.json"):
        """Дозаписывает новые данные в JSON-файл, сохраняя старые."""
        existing_data = {}

        # Если файл уже существует, сначала считываем из него старые данные
        if os.path.exists(filename):
            try:
                with open(filename, "r", encoding="utf-8") as f:
                    existing_data = json.load(f)
            except Exception as e:
                print(
                    f"⚠️ Не удалось прочитать существующий файл {filename} ({e}), создаем новый."
                )

        # Объединяем старые данные с новыми результатами
        # Если артикул уже был, его данные обновятся на самые свежие
        existing_data.update(new_data)

        # Записываем объединенные данные обратно в файл
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(existing_data, f, indent=4, ensure_ascii=False)

        print(f"💾 Данные успешно сохранены в файл {filename}. Всего товаров в базе: {len(existing_data)}")


def get_batches(target_list, batch_size=50):
    """Вспомогательная функция (генератор), которая режет список на куски по batch_size."""
    for i in range(0, len(target_list), batch_size):
        yield target_list[i : i + batch_size]


async def main():
    print("🚀 Запуск парсера Wildberries...")

    # 1. Берем исходный список артикулов
    all_products = PRODUCT_LIST

    # 2. Проверяем ограничение на количество из config.py (для тестов)
    if MAX_ARTICLES_TO_PROCESS is not None:
        print(f"⚙️ Включено ограничение тестов: обрабатываем только первые {MAX_ARTICLES_TO_PROCESS} артикулов.")
        all_products = all_products[:MAX_ARTICLES_TO_PROCESS]

    total_products = len(all_products)
    batch_size = 50  # Размер одной порции
    
    print(f"📦 Всего к обработке: {total_products} артикулов. Размер батча: {batch_size}")

    # Создаем одну общую сессию для всех запросов (это экономит ресурсы компьютера)
    async with AsyncSession(impersonate="chrome120") as session:
        
        # Делим список на батчи и перебираем их по очереди
        # enumerate(..., 1) нужен, чтобы красиво выводить номера батчей (Батч 1, Батч 2 и т.д.)
        for batch_index, batch in enumerate(get_batches(all_products, batch_size), 1):
            print(f"\n🔹 Обработка пакета (батча) №{batch_index} | Товаров в пакете: {len(batch)}")
            
            # Запускаем парсинг текущего батча
            batch_results = await WbPriceParser.parse_all_products_by_batch_dummy(session, batch)
            
            # Как только батч завершился — сразу сохраняем его результаты в файл
            WbPriceParser.save_results(batch_results)
            
            # Небольшая пауза между батчами (3–5 секунд), чтобы дать отдохнуть API Wildberries
            if (batch_index * batch_size) < total_products:
                sleep_time = random.uniform(3.0, 5.0)
                print(f"⏳ Батч №{batch_index} завершен. Спим {sleep_time:.1f} сек. перед следующим батчем...")
                await asyncio.sleep(sleep_time)

    print("\n🎉 Программа успешно завершила работу! Все данные сохранены.")


# Метод-прослойка для совместимости, вызывает логику обработки одного батча
WbPriceParser.parse_all_products_by_batch_dummy = WbPriceParser.parse_batch

if __name__ == "__main__":
    # Запуск асинхронного главного метода
    asyncio.run(main())