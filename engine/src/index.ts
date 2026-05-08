import "dotenv/config";
import { createClient } from "redis";
import { env } from "./utils/env.js";
import {
  BALANCES,
  ORDERBOOKS,
  Fill,
  RestingOrder,
  Side,
  OrderType,
} from "./store/exchange-store.js";

export type EngineCommandType =
  | "create_order"
  | "get_depth"
  | "get_user_balance"
  | "get_order"
  | "cancel_order";

export interface EngineRequest {
  correlationId: string;
  responseQueue: string;
  type: EngineCommandType;
  payload: Record<string, unknown>;
}

export interface EngineResponse {
  correlationId: string;
  ok: boolean;
  data?: unknown;
  error?: string;
}

ORDERBOOKS.set("BTC", {
  bids: new Map([
    [98, [{ orderId: ""seed-bid-1", userId: "seed-seller", side: "buy", type: "limit", symbol: "BTC", price: 98, qty: 5, filledQty: 0, status: "open", createdAt: Date.now() }]],
    [97, [{ orderId: "seed-bid-2", userId: "seed-seller", side: "buy", type: "limit", symbol: "BTC", price: 97, qty: 3, filledQty: 0, status: "open", createdAt: Date.now() }]],
  ]),
  asks: new Map([
    [100, [{ orderId: "seed-ask-1", userId: "seed-buyer", side: "sell", type: "limit", symbol: "BTC", price: 100, qty: 5, filledQty: 0, status: "open", createdAt: Date.now() }]],
    [101, [{ orderId: "seed-ask-2", userId: "seed-buyer", side: "sell", type: "limit", symbol: "BTC", price: 101, qty: 3, filledQty: 0, status: "open", createdAt: Date.now() }]],
  ]),
});

function ensureUserBalance(userId: string): void {
  if (!BALANCES.has(userId)) {
    BALANCES.set(userId, {
      USD: { available: 100000, locked: 0 }, 
      BTC: { available: 10, locked: 0 },     
    });
  }
}

const brokerClient = createClient({ url: env.redisUrl }).on("error", (error) => {
  console.error("Redis broker client error", error);
});

const responseClient = createClient({ url: env.redisUrl }).on("error", (error) => {
  console.error("Redis response client error", error);
});

await Promise.all([brokerClient.connect(), responseClient.connect()]);

async function sendResponse(responseQueue: string, response: EngineResponse): Promise<void> {
  await responseClient.lPush(responseQueue, JSON.stringify(response));
}

function handleEngineRequest(message: EngineRequest): unknown {

  if (message.type === "create_order") {
    const { side, symbol, price, qty, type, userId } = message.payload as {
      side: Side;
      symbol: string;
      price: number;
      qty: number;
      type: OrderType;
      userId: string;
    };

    ensureUserBalance(userId);
    const userBalance = BALANCES.get(userId)!;

    const orderId = crypto.randomUUID();
    const fills: Fill[] = [];
    let filledQty = 0;

    if (type === "limit" && side === "buy") {
      // Check user has enough USD locked
      const costEstimate = price * qty;
      if (userBalance.USD.available < costEstimate) {
        return { ok: false, error: "Insufficient USD balance" };
      }

      // Lock the USD upfront
      userBalance.USD.available -= costEstimate;
      userBalance.USD.locked += costEstimate;

      const orderBook = ORDERBOOKS.get(symbol)!;
      let remainingQty = qty;

      for (const [askPrice, orders] of orderBook.asks) {
        if (remainingQty <= 0) break;
        if (askPrice > price) continue; 
        while (orders.length > 0 && remainingQty > 0) {
          const restingOrder = orders[0]!;
          const matchedQty = Math.min(restingOrder.qty, remainingQty);
          const fillCost = askPrice * matchedQty;

          userBalance.USD.locked -= fillCost;
          userBalance.BTC = userBalance.BTC ?? { available: 0, locked: 0 };
          userBalance.BTC.available += matchedQty;

          remainingQty -= matchedQty;
          filledQty += matchedQty;
          restingOrder.qty -= matchedQty;

          if (restingOrder.qty <= 0) {
            restingOrder.status = "filled";
            orders.shift();
          }

          fills.push({
            fillId: crypto.randomUUID(),
            symbol,
            price: askPrice,
            qty: matchedQty,
            buyOrderId: orderId,
            sellOrderId: restingOrder.orderId,
            createdAt: Date.now(),
          });
        }
      }

      if (remainingQty > 0) {
        const restingOrder: RestingOrder = {
          orderId,
          userId,
          type: "limit",
          side: "buy",
          symbol,
          price,
          qty: remainingQty,
          filledQty,
          status: filledQty > 0 ? "open" : "open",
          createdAt: Date.now(),
        };
        if (!orderBook.bids.has(price)) orderBook.bids.set(price, []);
        orderBook.bids.get(price)!.push(restingOrder);
      } else {
        if (userBalance.USD.locked < 0) userBalance.USD.locked = 0;
      }

      ORDERBOOKS.set(symbol, orderBook);

      return {
        orderId,
        status: remainingQty === 0 ? "filled" : filledQty > 0 ? "partially_filled" : "open",
        filledQty,
        remainingQty,
        averagePrice: fills.length > 0 ? fills.reduce((sum, f) => sum + f.price * f.qty, 0) / filledQty : price,
        fills,
        balance: { USD: userBalance.USD, BTC: userBalance.BTC },
      };
    }

    if (type === "limit" && side === "sell") {
      
      const btcBalance = userBalance.BTC ? { available: 0, locked: 0 };
      if (btcBalance.available < qty) {
        return { ok: false, error: "Insufficient BTC balance" };
      }

      // Lock BTC upfront
      btcBalance.available -= qty;
      btcBalance.locked += qty;
      userBalance.BTC = btcBalance;

      const orderBook = ORDERBOOKS.get(symbol)!;
      let remainingQty = qty;

      for (const [bidPrice, orders] of orderBook.bids) {
        if (remainingQty <= 0) break;
        if (bidPrice < price) continue; // limit: only match if bid >= ask price

        while (orders.length > 0 && remainingQty > 0) {
          const restingOrder = orders[0]!;
          const matchedQty = Math.min(restingOrder.qty, remainingQty);

          userBalance.USD = userBalance.USD ?? { available: 0, locked: 0 };
          userBalance.USD.available += bidPrice * matchedQty;
          btcBalance.locked -= matchedQty;

          remainingQty -= matchedQty;
          filledQty += matchedQty;
          restingOrder.qty -= matchedQty;

          if (restingOrder.qty <= 0) {
            restingOrder.status = "filled";
            orders.shift();
          }

          fills.push({
            fillId: crypto.randomUUID(),
            symbol,
            price: bidPrice,
            qty: matchedQty,
            buyOrderId: restingOrder.orderId,
            sellOrderId: orderId,
            createdAt: Date.now(),
          });
        }
      }

      if (remainingQty > 0) {
        const restingOrder: RestingOrder = {
          orderId,
          userId,
          type: "limit",
          side: "sell",
          symbol,
          price,
          qty: remainingQty,
          filledQty,
          status: filledQty > 0 ? "open" : "open",
          createdAt: Date.now(),
        };
        if (!orderBook.asks.has(price)) orderBook.asks.set(price, []);
        orderBook.asks.get(price)!.push(restingOrder);
      }

      ORDERBOOKS.set(symbol, orderBook);

      return {
        orderId,
        status: remainingQty === 0 ? "filled" : filledQty > 0 ? "partially_filled" : "open",
        filledQty,
        remainingQty,
        averagePrice: fills.length > 0 ? fills.reduce((sum, f) => sum + f.price * f.qty, 0) / filledQty : price,
        fills,
        balance: { USD: userBalance.USD, BTC: userBalance.BTC },
      };
    }

    if (type === "market" && side === "buy") {
      const orderBook = ORDERBOOKS.get(symbol)!;
      let remainingQty = qty;

      for (const [askPrice, orders] of orderBook.asks) {
        if (remainingQty <= 0) break;

        while (orders.length > 0 && remainingQty > 0) {
          const restingOrder = orders[0]!;
          const matchedQty = Math.min(restingOrder.qty, remainingQty);
          const fillCost = askPrice * matchedQty;

          userBalance.USD.available -= fillCost;
          userBalance.BTC = userBalance.BTC ?? { available: 0, locked: 0 };
          userBalance.BTC.available += matchedQty;

          remainingQty -= matchedQty;
          filledQty += matchedQty;
          restingOrder.qty -= matchedQty;

          if (restingOrder.qty <= 0) {
            restingOrder.status = "filled";
            orders.shift();
          }

          fills.push({
            fillId: crypto.randomUUID(),
            symbol,
            price: askPrice,
            qty: matchedQty,
            buyOrderId: orderId,
            sellOrderId: restingOrder.orderId,
            createdAt: Date.now(),
          });
        }
      }

      ORDERBOOKS.set(symbol, orderBook);

      return {
        orderId,
        status: filledQty === qty ? "filled" : filledQty > 0 ? "partially_filled" : "open",
        filledQty,
        remainingQty,
        averagePrice: fills.length > 0 ? fills.reduce((sum, f) => sum + f.price * f.qty, 0) / (filledQty || 1) : 0,
        fills,
        balance: { USD: userBalance.USD, BTC: userBalance.BTC },
      };
    }

    if (type === "market" && side === "sell") {
      const btcBalance = userBalance.BTC ?? { available: 0, locked: 0 };
      if (btcBalance.available < qty) {
        return { ok: false, error: "Insufficient BTC balance" };
      }

      btcBalance.available -= qty;
      userBalance.BTC = btcBalance;

      const orderBook = ORDERBOOKS.get(symbol)!;
      let remainingQty = qty;

      for (const [bidPrice, orders] of orderBook.bids) {
        if (remainingQty <= 0) break;

        while (orders.length > 0 && remainingQty > 0) {
          const restingOrder = orders[0]!;
          const matchedQty = Math.min(restingOrder.qty, remainingQty);

          userBalance.USD = userBalance.USD ?? { available: 0, locked: 0 };
          userBalance.USD.available += bidPrice * matchedQty;
          btcBalance.available += 0; // already deducted above

          remainingQty -= matchedQty;
          filledQty += matchedQty;
          restingOrder.qty -= matchedQty;

          if (restingOrder.qty <= 0) {
            restingOrder.status = "filled";
            orders.shift();
          }

          fills.push({
            fillId: crypto.randomUUID(),
            symbol,
            price: bidPrice,
            qty: matchedQty,
            buyOrderId: restingOrder.orderId,
            sellOrderId: orderId,
            createdAt: Date.now(),
          });
        }
      }

      if (remainingQty > 0) {
        btcBalance.available += remainingQty;
      }

      ORDERBOOKS.set(symbol, orderBook);

      
    }
  }

  // ── CANCEL ORDER ─────────────────────────────────────────────────────────
  if (message.type === "cancel_order") {
    const { orderId, symbol } = message.payload as { orderId: string; symbol: string };
    const orderBook = ORDERBOOKS.get(symbol)!;
    const { bids, asks } = orderBook;

    

    return { ok: false, error: `Order not found: ${orderId}` };
  }

  if (message.type === "get_depth") {
    
  }

  if (message.type === "get_order") {
  }

  if (message.type === "get_user_balance") {
   
}

console.log(`Engine listening on Redis queue: ${env.incomingQueue}`);

for (; ;) {
  const item = await brokerClient.brPop(env.incomingQueue, 0);
  if (!item) continue;

  let message: EngineRequest;

  try {
    message = JSON.parse(item.element) as EngineRequest;
  } catch {
    console.error("Skipping invalid broker message");
    continue;
  }

  try {
    const data = handleEngineRequest(message);
    await sendResponse(message.responseQueue, {
      correlationId: message.correlationId,
      ok: true,
      data,
    });
  } catch (error) {
    await sendResponse(message.responseQueue, {
      correlationId: message.correlationId,
      ok: false,
      error: error instanceof Error ? error.message : "engine_error",
    });
  }
}