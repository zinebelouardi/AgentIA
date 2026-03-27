// web/static/dictionary_pro.js
const input = document.getElementById("dictSearch");
const tablesBox = document.getElementById("tablesBox");
const colsBox = document.getElementById("colsBox");

const tMeta = document.getElementById("tMeta");
const cMeta = document.getElementById("cMeta");

const dictError = document.getElementById("dictError");
const modePill = document.getElementById("modePill");

let lastTables = [];
let selectedTable = null;

function escapeHtml(s) {
  return (s ?? "")
    .toString()
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function showError(msg) {
  if (!dictError) return;
  if (!msg) {
    dictError.style.display = "none";
    dictError.textContent = "";
    return;
  }
  dictError.style.display = "block";
  dictError.textContent = msg;
}

function setMode(mode) {
  if (!modePill) return;
  modePill.textContent = mode || "—";
}

function setSelectedTable(name) {
  selectedTable = name;
  // highlight
  document.querySelectorAll("[data-dd-table]").forEach((el) => {
    const t = el.getAttribute("data-dd-table");
    if (t === name) el.classList.add("active-table");
    else el.classList.remove("active-table");
  });
}

function renderTables(tables) {
  const arr = tables || [];
  lastTables = arr;

  tMeta.textContent = `${arr.length} table(s)`;

  if (!arr.length) {
    tablesBox.innerHTML = `<div class="muted">Aucune table trouvée.</div>`;
    return;
  }

  tablesBox.innerHTML = arr
    .slice(0, 200)
    .map((t) => {
      const name = t.table_name || t.name || "";
      const desc = (t.description || "").toString();

      return `
        <div class="source-card dd-table-card" data-dd-table="${escapeHtml(name)}" style="margin-bottom:10px; cursor:pointer;">
          <div class="source-title">${escapeHtml(name)}</div>
          ${
            desc
              ? `<div class="muted" style="font-size:12px;margin-top:6px;white-space:pre-wrap;">${escapeHtml(desc)}</div>`
              : `<div class="muted" style="font-size:12px;margin-top:6px;">—</div>`
          }
        </div>
      `;
    })
    .join("");

  // click handlers
  document.querySelectorAll(".dd-table-card").forEach((el) => {
    el.addEventListener("click", async () => {
      const t = el.getAttribute("data-dd-table") || "";
      if (!t) return;
      setSelectedTable(t);
      await loadColumnsForTable(t);
    });
  });
}

function renderColumns(cols, title = "") {
  const arr = cols || [];
  cMeta.textContent = title ? `${arr.length} colonne(s) — ${title}` : `${arr.length} colonne(s)`;

  if (!arr.length) {
    colsBox.innerHTML = `<div class="muted">Aucune colonne trouvée.</div>`;
    return;
  }

  colsBox.innerHTML = arr
    .slice(0, 400)
    .map((c) => {
      const t = c.table_name || "";
      const col = c.column_name || "";
      const dt = c.data_type || "";
      const desc = (c.description || "").toString();

      return `
        <div class="source-card" style="margin-bottom:10px;">
          <div class="source-title">${escapeHtml(t)}.${escapeHtml(col)}</div>
          <div class="source-meta" style="margin-top:6px;">
            ${dt ? `<span class="pill">${escapeHtml(dt)}</span>` : ""}
          </div>
          ${
            desc
              ? `<div class="muted" style="font-size:12px;margin-top:8px;white-space:pre-wrap;">${escapeHtml(desc)}</div>`
              : `<div class="muted" style="font-size:12px;margin-top:8px;">—</div>`
          }
        </div>
      `;
    })
    .join("");
}

async function fetchDictionary(q = "") {
  const res = await fetch(`/api/dictionary/search?q=${encodeURIComponent(q)}`);
  let data = null;

  try {
    data = await res.json();
  } catch (e) {
    return { ok: false, error: "Réponse API non JSON. Vérifie Flask /api/dictionary/search.", data: null };
  }

  if (!res.ok) {
    return { ok: false, error: data?.error || `Erreur API (${res.status}).`, data };
  }
  return { ok: true, error: data?.error || "", data };
}

async function load(q = "") {
  showError("");
  setMode("—");

  const out = await fetchDictionary(q);
  if (!out.ok) {
    showError(out.error);
    renderTables([]);
    renderColumns([]);
    return;
  }

  const data = out.data || {};
  showError(data.error || "");
  setMode(data.mode || "—");

  renderTables(data.tables || []);

  // Cas 1: l’API a déjà renvoyé des colonnes (mode schema + q filtré)
  if ((data.columns || []).length > 0) {
    renderColumns(data.columns, q ? `filtre: "${q}"` : "");
    return;
  }

  // Cas 2: mode dd et q vide => columns=[]
  // => auto-select first table and load its columns
  const first = (data.tables || [])[0]?.table_name;
  if (first) {
    setSelectedTable(first);
    await loadColumnsForTable(first);
  } else {
    renderColumns([]);
  }
}

async function loadColumnsForTable(tableName) {
  // On force q=tableName pour récupérer les colonnes correspondantes
  const out = await fetchDictionary(tableName);
  if (!out.ok) {
    showError(out.error);
    renderColumns([]);
    return;
  }

  const data = out.data || {};
  showError(data.error || "");
  setMode(data.mode || "—");

  // On garde la liste de tables affichées (ne pas la remplacer)
  // Mais si jamais l'API renvoie tables vides, on ne casse pas.

  const cols = data.columns || [];
  renderColumns(cols, tableName);
}

function debounce(fn, ms) {
  let t = null;
  return (...args) => {
    clearTimeout(t);
    t = setTimeout(() => fn(...args), ms);
  };
}

const onInput = debounce(async () => {
  const q = (input.value || "").trim();
  // Si l'utilisateur tape, on fait une recherche globale (tables + colonnes)
  selectedTable = null;
  await load(q);
}, 250);

input.addEventListener("input", onInput);

// init
load("");
