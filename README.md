# Telegram Clock Bot

A Telegram bot for managing employee clock-in/clock-out, leave applications, and expense claims.

## Features

- 👨‍💼 Employee Management
  - Clock in/out tracking
  - Leave application
  - Expense claims with receipt upload
  - Working hours calculation

- 💰 Financial Management
  - Balance tracking
  - Expense claims processing
  - Salary management
  - Top-up functionality

- 📊 Reporting
  - PDF report generation
  - Daily attendance checking
  - Claims overview
  - Balance overview

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set environment variables in `.env` file or your deployment platform:
```bash
# Required environment variables:
DATABASE_URL=your_postgresql_database_url
TOKEN=your_telegram_bot_token
ADMIN_IDS=comma_separated_admin_ids

# See .env.example for all available options
```

3. Initialize database:
```bash
python init_db.py
```

4. Run the bot:
```bash
python clock_bot.py
```

## Commands

### User Commands
- `/start` - Start the bot
- `/clockin` - Clock in for work
- `/clockout` - Clock out from work
- `/offday` - Mark a day as leave
- `/claim` - Submit an expense claim

### Admin Commands
- `/balance` - Check all drivers' balances
- `/check` - Check today's attendance
- `/PDF` - Generate PDF reports
- `/topup` - Top up driver's balance
- `/viewclaims` - View recent claims
- `/salary` - Set driver's salary
- `/paid` - View driver's work summary and payment details

## Database Schema

The bot uses PostgreSQL with the following tables:
- `drivers` - Store driver information
- `clock_logs` - Store clock in/out records
- `claims` - Store expense claims
- `topups` - Store balance top-up records

## Security

- Admin access is controlled via ADMIN_IDS environment variable
- Database credentials are managed via environment variables
- Sensitive data is not stored in the code

## Contributing

Feel free to submit issues and enhancement requests!

## License

MIT License
