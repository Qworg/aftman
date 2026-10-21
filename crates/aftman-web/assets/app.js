// Live updates for the run detail page. Vanilla on purpose: the front end is server-rendered
// HTML with a little JS, so there is no bundler in the runtime path.
//
// Resumption is the browser's job. The server sends each event with `id: <seq>`, so an
// EventSource that reconnects automatically sends `Last-Event-ID` and the server replays from
// that sequence number onwards. That is why this file does not track a cursor or poll: doing
// so would be the second implementation of something the transport already guarantees.
(() => {
  "use strict";

  const timeline = document.querySelector("[data-run-id]");
  if (!timeline) return;

  const runId = timeline.dataset.runId;
  const tbody = document.getElementById("events");
  const state = document.getElementById("live-state");
  if (!tbody) return;

  let lastSeq = Number(timeline.dataset.lastSeq || 0);

  const setState = (text, cls) => {
    if (!state) return;
    state.textContent = text;
    state.className = "status " + (cls || "status-pending");
  };

  const cell = (text, cls) => {
    const td = document.createElement("td");
    if (cls) td.className = cls;
    td.textContent = text;
    return td;
  };

  const addRow = (kind, seq, at, detail) => {
    const tr = document.createElement("tr");
    tr.className = "event kind-" + kind.toLowerCase();
    tr.append(
      cell(String(seq), "mono num"),
      cell(at, "mono"),
      cell(kind, "mono"),
      cell(detail, ""),
    );
    // Newest first, matching the server-rendered order.
    tbody.prepend(tr);
  };

  const summarise = (kind, payload) => {
    const s = (key) => (payload && typeof payload[key] === "string" ? payload[key] : "");
    switch (kind) {
      case "DIRECTIVE_SELECTED":
      case "DIRECTIVE_PROPOSED":
        return s("directive") + (s("reason") ? " \u2014 " + s("reason") : "");
      case "WORKER_OUTPUT":
        return s("text").slice(0, 120);
      case "ESCALATION_RAISED":
        return s("ask");
      case "ESCALATION_ANSWERED":
        return "answered by " + s("surface") + ": " + s("decision");
      case "STATE_CHANGED":
        return s("from") + " \u2192 " + s("to");
      case "MODEL_ERROR":
      case "ERROR":
        return s("code") + ": " + s("message");
      case "BUDGET_EXCEEDED":
        return s("message");
      case "RUN_FINISHED":
        return s("status");
      default:
        return "";
    }
  };

  const source = new EventSource("/v1/runs/" + encodeURIComponent(runId) + "/events");

  source.onopen = () => setState("live", "status-live");

  source.onmessage = (event) => {
    let data;
    try {
      data = JSON.parse(event.data);
    } catch (_) {
      return;
    }
    // A duplicate can arrive on reconnect; the sequence number is the authority.
    if (lastSeq && data.seq <= lastSeq) return;
    lastSeq = data.seq;
    addRow(data.kind, data.seq, data.at, summarise(data.kind, data.payload));

    // A terminal event means there will be nothing more; close so the browser does not
    // reconnect to a finished stream forever.
    if (data.kind === "RUN_FINISHED") {
      setState("finished", "status-finished");
      source.close();
    }
  };

  source.onerror = () => {
    // The browser retries on its own, resuming from the last id it saw.
    setState("reconnecting\u2026", "status-stopped");
  };

  // The server sends `lagged` and then closes, so the browser reconnects and the daemon
  // replays the gap from the store. Surfacing it means a gap is never silent.
  source.addEventListener("lagged", (event) => {
    let data = {};
    try {
      data = JSON.parse(event.data);
    } catch (_) {}
    setState(
      "fell behind; resuming from " + (data.resume_from || "the start"),
      "status-stopped",
    );
  });
})();
