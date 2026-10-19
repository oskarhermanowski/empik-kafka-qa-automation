## Wymagania

- Docker + Docker Compose v2 (`docker compose`, nie `docker-compose`).
- Wolny port `9092` lokalnie (Kafka).
- Do pracy lokalnej nad testami (poza Dockerem): Node.js 20+.

## Uruchomienie środowiska

```bash
# Zbuduj obrazy i uruchom Kafkę + inicjalizację topiców + konsumenta
docker compose up --build -d

# Sprawdź logi konsumenta (powinien czekać na wiadomości)
docker compose logs -f consumer
```

## Ręczna demonstracja działania aplikacji (opcjonalnie, nie jest wymagane do zadania)

```bash
docker compose --profile demo run --rm producer
docker compose logs -f consumer
```

## Uruchomienie testów (tu pracujesz)

```bash
docker compose --profile test run --rm tests
```

### Uruchomienie testów lokalnie (bez Dockera dla samych testów)

Przydatne przy pisaniu/debugowaniu — szybszy cykl niż przebudowa obrazu Dockera:

```bash
cd tests
npm install
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 npm test
```

Sprawdzenie typów bez uruchamiania testów:

```bash
cd tests
npx tsc --noEmit
```

## Zatrzymanie i sprzątanie środowiska

```bash
docker compose down -v
```

(`-v` czyści też dane Kafki — przydatne, gdy chcesz zacząć testy od zera).


---

## Twoje notatki (uzupełnij przed oddaniem zadania)

### Uwagi do uruchomienia
Testy uruchamiane są w środowisku Docker Compose.

Na systemie Windows wymagane było ustawienie końców linii LF w pliku `kafka-init/init-topics.sh`, aby skrypt działał poprawnie w kontenerze Linux.

Po zmianach w kodzie testów należy przebudować obraz:

```bash
docker compose build tests
```

Uruchomienie testów:

```bash
docker compose --profile test run --rm tests
```

### Podejście do testowania
Zaimplementowałem testy integracyjne obejmujące poprawne przetwarzanie zamówień, obsługę niepoprawnych danych oraz przetwarzanie wielu wiadomości przez Apache Kafka.

Scenariusze pozytywne weryfikują zmianę statusu na PROCESSED i zachowanie danych zamówienia, natomiast scenariusze negatywne sprawdzają przekierowanie błędnych wiadomości do DLQ.

Do izolacji testów wykorzystałem unikalne identyfikatory zamówień oraz niezależne grupy konsumentów Kafka.

Zmodyfikowałem funkcje waitForMessage i collectMessages, aby rozdzielić odbieranie wiadomości od zamykania połączeń, poprawić obsługę błędów oraz zapewnić zwalnianie zasobów.

### Znane ograniczenia / co zrobiłbym/zrobiłabym inaczej mając więcej czasu
- Testy wymagają uruchomionego środowiska Docker i Kafka.
- Odczytywanie topiców od początku może wydłużać działanie testów przy dużej liczbie historycznych wiadomości.
- Warto rozszerzyć testy o dodatkowe przypadki brzegowe i scenariusze awarii brokera.
- Można rozbudować typowanie wiadomości i ograniczyć użycie typu `any` w helperach.
- Przy dalszym rozwoju projektu warto dodać automatyczne uruchamianie testów w CI/CD.
