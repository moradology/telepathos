// Chaos proxy: listens on :8888, forwards to :8787 with heavy throttling and
// random connection murder. Forces the server's backpressure/retry code to run.
import net from "node:net";

const ARGS = process.argv.slice(2);
const KILL_AFTER_MS = Number(ARGS[0] ?? 8000);   // sever each conn after this long
const THROTTLE_MS = Number(ARGS[1] ?? 120);      // delay per forwarded chunk

let killed = 0;

const server = net.createServer((client) => {
  const upstream = net.connect(8787, "127.0.0.1");

  const pump = (from, to, label) => {
    from.on("data", (chunk) => {
      setTimeout(() => {
        try { to.write(chunk); } catch {}
      }, THROTTLE_MS);
    });
    from.on("error", () => {});
  };

  pump(client, upstream, "c->s");
  pump(upstream, client, "s->c");

  // abruptly murder BOTH sides mid-conversation
  setTimeout(() => {
    killed++;
    console.log(`chaos: severing connection #${killed}`);
    client.destroy();
    upstream.destroy();
  }, KILL_AFTER_MS + Math.random() * 1000);

  client.on("error", () => {});
  upstream.on("error", () => {});
});

server.listen(8888, () => console.log(`chaos proxy :8888 → :8787 (kill≈${KILL_AFTER_MS}ms, throttle=${THROTTLE_MS}ms)`));
