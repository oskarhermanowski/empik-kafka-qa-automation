/**
 * TWOJE ZADANIE: zaimplementuj testy.
 *
 * Aplikacja (producer + consumer) jest już gotowa i działająca — Twoim
 * zadaniem jest napisanie testów E2E weryfikujących jej zachowanie.
 *
 * Masz do dyspozycji gotowe helpery z `src/kafkaHelpers.ts` (opisane
 * w komentarzach tego pliku) — możesz z nich korzystać, ale nie musisz:
 * jeśli uważasz, że lepiej napisać coś inaczej, zrób to i uzasadnij w README.
 *
 */
import { Producer } from "kafkajs";
import {
  ORDERS_DLQ_TOPIC,
  ORDERS_PROCESSED_TOPIC,
  ORDERS_TOPIC,
  Order,
  collectMessages,
  disconnectProducer,
  getProducer,
  makeTestOrder,
  sendOrder,
  sendRaw,
  waitForMessage,
} from "../src/kafkaHelpers";

describe("Przepływ przetwarzania zamówień przez Kafkę", () => {
  let producer: Producer;

  beforeAll(async () => {
    producer = await getProducer();
  });

  afterAll(async () => {
    await disconnectProducer();
  });

it("TC01 - poprawne zamówienie zostaje przetworzone", async () => {
  // 1. Przygotowanie przykładowego zamówienia
  const order: Order = makeTestOrder();

  // 2. Wysłanie zamówienia do Kafki
  await sendOrder(producer, order);

  // 3. Oczekiwanie na przetworzone zamówienie
  const result = await waitForMessage(
    ORDERS_PROCESSED_TOPIC,
    (message) => message?.orderId === order.orderId
  );

  // 4. Sprawdzenie otrzymanej odpowiedzi
  expect(result).toEqual({
    ...order,
    status: "PROCESSED",
  });
});

it("TC02 - brak pola customer kieruje zamówienie do DLQ", async () => {
  // 1. Przygotowanie zamówienia
  const invalidOrder: Partial<Order> = makeTestOrder();

  // 2. Usunięcie wymaganego pola
  delete invalidOrder.customer;

  // 3. Wysłanie niepoprawnego zamówienia
  await sendOrder(producer, invalidOrder);

  // 4. Oczekiwanie na wiadomość w DLQ
  const result = await waitForMessage(
    ORDERS_DLQ_TOPIC,
    (message) =>
      message?.originalMessage?.orderId === invalidOrder.orderId
  );

  // 5. Weryfikacja otrzymanej odpowiedzi
  expect(result).not.toBeNull();
  expect(result?.error).toContain("customer");
  expect(result?.originalMessage).toEqual(invalidOrder);
});

it("TC03 - niepoprawny JSON trafia do DLQ", async () => {
  // 1. Przygotowanie niepoprawnego JSON-a
  const orderId = makeTestOrder().orderId;
  const invalidJson = '{"orderId":"${orderId}","customer":';

  // 2. Wysłanie niepoprawnej wiadomości
  await sendRaw(producer, invalidJson, orderId);

  // 3. Oczekiwanie na wiadomość w DLQ
  const result = await waitForMessage(
    ORDERS_DLQ_TOPIC,
    (message) => message?.raw === invalidJson
  );

  // 4. Weryfikacja otrzymanej odpowiedzi
  expect(result).not.toBeNull();
  expect(result?.error).toContain("Niepoprawny JSON");
  expect(result?.raw).toBe(invalidJson);
});

it("TC04 - niepoprawny typ amount kieruje zamówienie do DLQ", async () => {
  // 1. Przygotowanie zamówienia
  const order = makeTestOrder();

  // 2. Zmiana amount z liczby na tekst
  const invalidOrder = {
    ...order,
    amount: "199.99",
  };

  // 3. Wysłanie niepoprawnego zamówienia
  await sendRaw(
    producer,
    JSON.stringify(invalidOrder),
    order.orderId
  );

  // 4. Oczekiwanie na wiadomość w DLQ
  const result = await waitForMessage(
    ORDERS_DLQ_TOPIC,
    (message) =>
      message?.originalMessage?.orderId === order.orderId
  );

  // 5. Weryfikacja otrzymanej odpowiedzi
  expect(result).not.toBeNull();
  expect(result?.error).toContain("amount");
  expect(result?.originalMessage).toEqual(invalidOrder);
});

it("TC05 - niepoprawny typ orderId kieruje zamówienie do DLQ", async () => {
  // 1. Przygotowanie zamówienia
  const order = makeTestOrder();

  // 2. Zmiana orderId z tekstu na liczbę
  const invalidOrder = {
    ...order,
    orderId: 12345,
  };

  // 3. Wysłanie niepoprawnego zamówienia
  await sendRaw(
    producer,
    JSON.stringify(invalidOrder),
    order.orderId
  );

  // 4. Oczekiwanie na wiadomość w DLQ
  const result = await waitForMessage(
    ORDERS_DLQ_TOPIC,
    (message) =>
      message?.originalMessage?.orderId === invalidOrder.orderId
  );

  // 5. Weryfikacja otrzymanej odpowiedzi
  expect(result).not.toBeNull();
  expect(result?.error).toContain("orderId");
  expect(result?.originalMessage).toEqual(invalidOrder);
});


it("TC06 - wiele zamówień zostaje poprawnie przetworzonych", async () => {
  // 1. Przygotowanie trzech zamówień
  const orders: Order[] = [
    makeTestOrder(),
    makeTestOrder(),
    makeTestOrder(),
  ];

  const orderIds = new Set(orders.map((order) => order.orderId));

  // 2. Wysłanie zamówień do Kafki
  await Promise.all(
    orders.map((order) => sendOrder(producer, order))
  );

  // 3. Oczekiwanie na trzy przetworzone zamówienia
  const results = await collectMessages(
    ORDERS_PROCESSED_TOPIC,
    (message) => orderIds.has(message?.orderId),
    orders.length
  );

  // 4. Weryfikacja liczby otrzymanych wiadomości
  expect(results).toHaveLength(orders.length);

  // 5. Weryfikacja każdego zamówienia
  for (const order of orders) {
    const result = results.find(
      (message) => message.orderId === order.orderId
    );

    expect(result).toEqual({
      ...order,
      status: "PROCESSED",
    });
  }
});

});
