# АвтоАналитик — Telegram бот

## Environment Variables (Railway)

```
TELEGRAM_TOKEN=8765532113:AAGXFDcDLW2_mLqIq60-1jBQkSmRjnPklj0 
MY_CHAT_ID=862359805
ANTHROPIC_KEY=<твой ключ Anthropic> 
MAYA_LEADS_SHEET=1ZM-rN0dlUdeQhgFltodIPW9KluvtdsIb2QkcjzZtvKI
CARCITY_LEADS_SHEET=1Y2X_nDDnyfQiadHO-B07IhaqZPCHVr7XU583_gaNlYM
SALES_SHEET=1iLzumAZSzCOXwsdso7XzAPhM_zKIZ4H4
CALENDAR_ID=6adb497d70d6f51fb1bfee8d5fda6661b9c61f79d88069ac4b0b843f2f9f4358@group.calendar.google.com
GOOGLE_SERVICE_ACCOUNT_JSON=<содержимое JSON файла сервисного аккаунта — одной строкой> 
```

## Как добавить GOOGLE_SERVICE_ACCOUNT_JSON

1. Открой скачанный JSON файл
2. Скопируй ВСЁ содержимое
3. В Railway Variables вставь как значение переменной GOOGLE_SERVICE_ACCOUNT_JSON

## Команды бота

- /start — приветствие
- /report — полный отчёт за 30 дней
- /dashboard — визуальный дашборд (PNG)
- /funnel — анализ воронки
- /sources — сравнение источников
- /meetings — анализ встреч
