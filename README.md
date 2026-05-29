# WildPositionMonitor

Проект мониторит цены и позиции товаров Wildberries. Основной сценарий запуска
находится в `src/main.py`.

## Логирование парсинга цен WB

Основной лог парсинга цен пишется в:

```text
logs/wb_price_parser.log
```

CSV с ошибками создаётся отдельно по дням:

```text
logs/wb_price_parser_errors_YYYY-MM-DD.csv
```

В основной лог попадают старт и завершение парсинга, успешные артикулы, HTTP
498, попытки обновления cookies, итоговые счётчики и время выполнения. CSV
ошибок содержит только неуспешные артикулы, чтобы файл было удобно открыть в
Excel или Google Sheets.

Типы ошибок:

- `request_error` - HTTP или сетевой сбой запроса.
- `timeout` - превышено время ожидания ответа.
- `empty_response` - ответ пустой или не содержит ожидаемый список товаров.
- `json_error` - ответ не удалось разобрать как JSON.
- `price_not_found` - товар найден, но поле цены отсутствует.
- `invalid_wbaas_token` - получен HTTP 498, вероятно устарели cookies.
- `unknown_error` - непредвиденная ошибка обработки.

Примеры строк лога:

```text
✅ Успешно | article_id=123456789 | price=599
⚠️ 498 Invalid token | article_id=123456789 | пробуем обновить cookies
❌ Ошибка | article_id=123456789 | error_type=invalid_wbaas_token | status=498
```

Пример CSV:

```csv
article_id,error_type,error_message,status_code,response_preview,created_at
123456789,invalid_wbaas_token,"Получен HTTP 498. Вероятно устарели cookies.",498,"...",2026-05-29 12:30:00
```

## Автообновление x_wbaas_token при HTTP 498

HTTP 498 в текущем сценарии WB обычно означает, что `x_wbaas_token` в cookies
устарел или не подходит к текущей сессии. `src/config.py` больше не
редактируется автоматически: это статическая конфигурация, а не хранилище
runtime-состояния.

При HTTP 498 парсер делает следующее:

1. Логирует 498 для конкретного `article_id`.
2. Через `SeleniumBase` открывает `https://www.wildberries.ru/` и пытается
   получить новый cookie `x_wbaas_token`.
3. Заменяет только значение `x_wbaas_token` в текущей raw cookie string в памяти
   процесса.
4. Повторяет запрос по тому же артикулу ограниченное число раз.
5. Если повтор успешен, артикул считается обработанным.
6. Если повтор снова неуспешен, артикул попадает в лог и daily CSV ошибок.

В первой версии обновляется только `x_wbaas_token`. Иногда WB может связывать
токен с другими cookies. Если в логах видно, что refresh был, а HTTP 498
остался, следующий шаг - обновлять всю cookie-сессию, а не один параметр.

Настройки:

```text
WB_TOKEN_AUTO_REFRESH_ENABLED=true
WB_TOKEN_REFRESH_URL=https://www.wildberries.ru/
WB_TOKEN_COOKIE_NAME=x_wbaas_token
WB_TOKEN_REFRESH_MAX_ATTEMPTS=3
WB_TOKEN_REFRESH_WAIT_SECONDS=5
WB_TOKEN_REFRESH_MAX_RETRIES_PER_ARTICLE=1
MAX_CONSECUTIVE_498_ERRORS=20
```

Диагностическая проверка получения токена без запуска парсера:

```powershell
python scripts/check_wb_token.py
```

Скрипт выводит только маскированный токен:

```text
✅ x_wbaas_token получен: abc123...z9x8
```

Нельзя коммитить cookies, токены и любые секреты в репозиторий.
Нельзя отправлять cookies в логи, скриншоты, задачи и публичные чаты.

## Очистка логов и CSV

Очистка старых служебных файлов запускается один раз на старт процесса и не
чаще одного раза в сутки. Она работает только внутри папки `logs` и удаляет
только файлы с расширениями:

- `.log`
- `.csv`

Секреты, `.env`, исходный код, конфиги и файлы вне `logs` не удаляются.
Служебный файл состояния:

```text
logs/.last_cleanup
```

Настройки хранения:

```text
LOG_CLEANUP_ENABLED=true
LOG_CLEANUP_INTERVAL_HOURS=24
LOG_RETENTION_DAYS=7
LOG_CLEANUP_STATE_FILE=logs/.last_cleanup
```

Чтобы отключить очистку, установите:

```text
LOG_CLEANUP_ENABLED=false
```

Основной price-log дополнительно ротируется через `loguru` раз в день с
удержанием архивов по `LOG_RETENTION_DAYS`. CSV ошибок создаётся по дням, чтобы
файлы не росли бесконечно и их было проще анализировать.

## Проверка

1. Создайте `secrets/wb_cookies.txt` и вставьте туда актуальную raw cookie
   string из браузера.
2. Запустите парсер на небольшом списке артикулов.
3. Проверьте `logs/wb_price_parser.log`.
4. Если были ошибки, откройте `logs/wb_price_parser_errors_YYYY-MM-DD.csv`.
5. Для проверки очистки можно временно снизить `LOG_RETENTION_DAYS` и удалить
   `logs/.last_cleanup`, затем запустить проект снова.
