const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
export const results = [];
export const check = (name, ok, detail = "") =>
  results.push(`${ok ? "PASS" : "FAIL"}  ${name}${detail ? " — " + detail : ""}`);
