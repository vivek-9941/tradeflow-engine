# Contributing to TradeFlow

Thank you for your interest in contributing to TradeFlow! This is an open-source distributed trade orchestration and matching-engine system designed to explore the architecture, concurrency, reliability, and event-driven processing patterns used in electronic trading systems.

## Development Prerequisites

- Java 17+
- Maven 3.9+
- Docker Desktop (running)
- Node 18+ (for dashboard)

## Local Setup

### 1. Fork and Clone
```bash
git clone https://github.com/<your-username>/tradeflow-engine.git
cd tradeflow-engine
cp .env.example .env
```

### 2. Start Infrastructure
```bash
docker-compose up -d zookeeper kafka mysql
```
Wait about 20-30 seconds for Kafka to be ready.

### 3. Build the Project
```bash
mvn clean install -DskipTests
```

### 4. Run Tests
```bash
# Fast unit tests
mvn test -pl order-service,matching-engine,risk-service,margin-service,compliance-service

# Full integration tests
mvn test -pl integration-tests
```

### 5. Start Services
Services can be started locally via Maven or your IDE:
```bash
# Start gRPC services first
cd risk-service       && mvn spring-boot:run &
cd margin-service     && mvn spring-boot:run &
cd compliance-service && mvn spring-boot:run &

# Start core
cd matching-engine    && mvn spring-boot:run &
cd order-service      && mvn spring-boot:run &

# Start post-trade & data
cd ledger-service       && mvn spring-boot:run &
cd notification-service && mvn spring-boot:run &
cd analytics-service    && mvn spring-boot:run &
cd market-data-service  && mvn spring-boot:run &
```

## Repository Structure

- `common-module`: Shared domain models, enums, and protobuf generated classes.
- `order-service`: Order Management System (OMS), DAG executor orchestrating pre-trade checks.
- `risk-service`: Position limits, loss limits (gRPC).
- `margin-service`: Margin calculation and funds reservation (gRPC).
- `compliance-service`: Market hours and price band validation (gRPC).
- `matching-engine`: Core in-memory per-symbol order books and partial fills matcher.
- `ledger-service`: Kafka consumer for cash/holdings settlement.
- `notification-service`: Kafka consumer pushing WebSocket alerts.
- `analytics-service`: Kafka consumer monitoring trade stats and DLTs.
- `market-data-service`: Simulated exchange price tick generator and LTP/VWAP updater.
- `dashboard`: React + Vite frontend for observing the engine.
- `integration-tests`: End-to-end Testcontainers-based test suite.

## Development Workflow

1. **Fork** the repository and clone your fork.
2. **Create a branch** for your feature or bugfix (`git checkout -b feature/issue-123-description`).
3. **Make your changes**. Ensure you do not break existing business logic or tests.
4. **Run tests** (`mvn test` or via IDE).
5. **Commit** using standard conventions (see below).
6. **Push** to your fork.
7. **Open a Pull Request** against the `main` branch.

## Commit Conventions

We recommend the following prefix format for commits:

- `feat:` A new feature
- `fix:` A bug fix
- `docs:` Documentation only changes
- `test:` Adding missing tests or correcting existing tests
- `refactor:` Code changes that neither fix a bug nor add a feature
- `perf:` A code change that improves performance
- `chore:` Changes to the build process or auxiliary tools

*Example:* `feat: add distributed tracing across gRPC calls`

## Pull Request Requirements

When submitting a PR, please use the provided template and ensure:
- **Description:** Clearly state what was changed and why.
- **Motivation:** Link to the GitHub issue solving the problem.
- **Tests:** Add tests verifying your changes (if applicable).
- **Documentation:** Update `README.md` or other docs if architecture or APIs change.
- **Screenshots:** If you changed the `dashboard`, include screenshots.
