const chatWindow = document.getElementById("chatWindow");
const chatEmpty = document.getElementById("chatEmpty");
const sourcesDiv = document.getElementById("sources");
const sourcesMeta = document.getElementById("sourcesMeta");
const statusLine = document.getElementById("statusLine");

const questionInput = document.getElementById("question");
const sendBtn = document.getElementById("sendBtn");
const clearBtn = document.getElementById("clearBtn");
const topKSelect = document.getElementById("topK");

function timeNow() {
  const d = new Date();
  return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

function setStatus(text) {
  statusLine.textContent = text;
}

function showEmptyIfNeeded() {
  const hasMsgs = chatWindow.querySelectorAll(".msg").length > 0;
  chatEmpty.style.display = hasMsgs ? "none" : "grid";
}

function escapeHtml(s) {
  return (s ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function addMessage(role, text) {
  const wrap = document.createElement("div");
  wrap.className = `msg ${role}`;

  const avatar = document.createElement("div");
  avatar.className = "avatar";
  avatar.textContent = role === "user" ? "U" : "AI";

  const bubble = document.createElement("div");
  bubble.className = "bubble";
  bubble.innerHTML = `
    <div class="bubble-text">${escapeHtml(text).replaceAll("\n", "<br>")}</div>
    <div class="bubble-meta">${timeNow()}</div>
  `;

  wrap.appendChild(avatar);
  wrap.appendChild(bubble);
  chatWindow.appendChild(wrap);

  chatWindow.scrollTop = chatWindow.scrollHeight;
  showEmptyIfNeeded();
}

function addTyping() {
  const wrap = document.createElement("div");
  wrap.className = "msg bot";
  wrap.id = "typing";

  const avatar = document.createElement("div");
  avatar.className = "avatar";
  avatar.textContent = "AI";

  const bubble = document.createElement("div");
  bubble.className = "bubble";
  bubble.innerHTML = `
    <div class="typing">
      <span></span><span></span><span></span>
    </div>
    <div class="bubble-meta">en train d’écrire…</div>
  `;

  wrap.appendChild(avatar);
  wrap.appendChild(bubble);
  chatWindow.appendChild(wrap);
  chatWindow.scrollTop = chatWindow.scrollHeight;
  showEmptyIfNeeded();
}

function removeTyping() {
  const t = document.getElementById("typing");
  if (t) t.remove();
}

function renderSources(sources) {
  sourcesDiv.innerHTML = "";
  sourcesMeta.textContent = sources?.length ? `${sources.length} chunk(s)` : "—";

  if (!sources || sources.length === 0) {
    sourcesDiv.innerHTML = `<div class="sources-empty muted">Aucune source récupérée.</div>`;
    return;
  }

  sources.forEach((s, i) => {
    const card = document.createElement("div");
    card.className = "source-card";

    const title = escapeHtml(s.title || "Unknown Source");
    const page = escapeHtml(String(s.page ?? "N/A"));
    const score = escapeHtml(String(s.score ?? ""));
    const chunk = escapeHtml(String(s.chunk_id ?? ""));
    const snippet = escapeHtml(s.snippet || "");

    card.innerHTML = `
      <div class="source-top">
        <div class="source-idx">${i + 1}</div>
        <div class="source-main">
          <div class="source-title">${title}</div>
          <div class="source-meta">
            <span class="pill">page: ${page}</span>
            <span class="pill">score: ${score}</span>
            <span class="pill">chunk: ${chunk}</span>
          </div>
        </div>
        <button class="btn btn-small btn-ghost copyBtn" title="Copier l’extrait">Copier</button>
      </div>
      <div class="source-snippet">${snippet}</div>
    `;

    card.querySelector(".copyBtn").addEventListener("click", async () => {
      try {
        await navigator.clipboard.writeText(s.snippet || "");
        card.querySelector(".copyBtn").textContent = "Copié ✓";
        setTimeout(() => (card.querySelector(".copyBtn").textContent = "Copier"), 900);
      } catch {
        // ignore
      }
    });

    sourcesDiv.appendChild(card);
  });
}

async function sendQuestion() {
  const q = questionInput.value.trim();
  if (!q) return;

  const top_k = parseInt(topKSelect.value || "5", 10);

  addMessage("user", q);
  questionInput.value = "";
  renderSources([]);
  setStatus("Recherche + génération…");
  sendBtn.disabled = true;
  addTyping();

  try {
    const res = await fetch("/api/chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ question: q, top_k })
    });

    const data = await res.json();
    removeTyping();

    if (!res.ok || data.error) {
      addMessage("bot", "Erreur: " + (data.error || res.statusText));
      setStatus("Erreur.");
      renderSources([]);
      return;
    }

    addMessage("bot", data.answer || "");
    renderSources(data.sources || []);
    setStatus("Terminé.");
  } catch (e) {
    removeTyping();
    addMessage("bot", "Erreur réseau ou serveur.");
    setStatus("Erreur.");
  } finally {
    sendBtn.disabled = false;
  }
}

sendBtn.addEventListener("click", sendQuestion);

questionInput.addEventListener("keydown", (e) => {
  if (e.key === "Enter" && !e.shiftKey) {
    e.preventDefault();
    sendQuestion();
  }
});

clearBtn.addEventListener("click", () => {
  // Clear messages except empty state
  chatWindow.querySelectorAll(".msg").forEach(el => el.remove());
  renderSources([]);
  setStatus("Prêt.");
  showEmptyIfNeeded();
});

showEmptyIfNeeded();
