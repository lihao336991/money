const expr = process.argv.slice(2).join(" ");

if (!expr) {
  console.error("Usage: node tools/jq_cdp_eval.mjs <javascript-expression>");
  process.exit(2);
}

const targets = await (await fetch("http://127.0.0.1:9222/json/list")).json();
const target = targets.find((t) => t.type === "page" && /joinquant\.com/.test(t.url));

if (!target) {
  console.error("No JoinQuant page found on Chrome debug port 9222");
  process.exit(1);
}

const ws = new WebSocket(target.webSocketDebuggerUrl);
let nextId = 1;
const pending = new Map();

function send(method, params = {}) {
  const id = nextId++;
  ws.send(JSON.stringify({ id, method, params }));
  return new Promise((resolve, reject) => {
    pending.set(id, { resolve, reject });
  });
}

ws.addEventListener("message", (event) => {
  const msg = JSON.parse(event.data);
  if (!msg.id || !pending.has(msg.id)) return;
  const { resolve, reject } = pending.get(msg.id);
  pending.delete(msg.id);
  if (msg.error) reject(new Error(JSON.stringify(msg.error)));
  else resolve(msg.result);
});

await new Promise((resolve, reject) => {
  ws.addEventListener("open", resolve, { once: true });
  ws.addEventListener("error", reject, { once: true });
});

const result = await send("Runtime.evaluate", {
  expression: expr,
  awaitPromise: true,
  returnByValue: true,
});

console.log(JSON.stringify(result.result?.value ?? result.result, null, 2));
ws.close();
