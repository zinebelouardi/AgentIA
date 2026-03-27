// web/static/data_pro.js
const tableSelect = document.getElementById("tableSelect");
const searchInput = document.getElementById("searchInput");
const refreshBtn = document.getElementById("refreshBtn");
const tableWrap = document.getElementById("tableWrap");
const tableMeta = document.getElementById("tableMeta");
const metaLine = document.getElementById("metaLine");

const prevBtn = document.getElementById("prevBtn");
const nextBtn = document.getElementById("nextBtn");
const pageInfo = document.getElementById("pageInfo");

const langSel = document.getElementById("lang");
const pageSizeSel = document.getElementById("pageSize");

const suggestBox = document.getElementById("suggestBox");

let offset = 0;
let total = 0;

function escapeHtml(s) {
  return (s ?? "")
    .toString()
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function renderTable(columns, rows) {
  if (!columns || columns.length === 0) {
    tableWrap.innerHTML = `<div class="muted" style="padding:14px;">No data.</div>`;
    return;
  }
  let html = "<table><thead><tr>";
  columns.forEach((c) => (html += `<th>${escapeHtml(c)}</th>`));
  html += "</tr></thead><tbody>";

  rows.forEach((r) => {
    html += "<tr>";
    columns.forEach((c) => {
      const v = r[c];
      html += `<td>${escapeHtml(v === null || v === undefined ? "" : v)}</td>`;
    });
    html += "</tr>";
  });

  html += "</tbody></table>";
  tableWrap.innerHTML = html;
}

function renderSuggest(payload) {
  if (!suggestBox) return;

  const typed = payload?.typed || "";
  const best = payload?.best_match || null;
  const suggestions = payload?.suggestions || [];
  const similar = payload?.similar || [];

  if (!typed) {
    suggestBox.innerHTML = "";
    return;
  }

  let html = `<div class="muted" style="font-size:12px;">
    Vous avez tapé : <b>${escapeHtml(typed)}</b>
  </div>`;

  if (best) {
    html += `<div style="margin-top:8px;">
      Meilleure suggestion :
      <span class="suggest-item" data-val="${escapeHtml(best.text)}">${escapeHtml(best.text)} (${best.score})</span>
    </div>`;
  }

  if (suggestions.length) {
    html += `<div class="muted" style="font-size:12px; margin-top:10px;">Suggestions :</div><div>`;
    suggestions.forEach((s) => {
      html += `<span class="suggest-item" data-val="${escapeHtml(s.text)}">${escapeHtml(s.text)} (${s.score})</span>`;
    });
    html += `</div>`;
  }

  if (similar.length) {
    html += `<div class="muted" style="font-size:12px; margin-top:10px;">Similaires :</div><div>`;
    similar.forEach((s) => {
      html += `<span class="suggest-item" data-val="${escapeHtml(s.text)}">${escapeHtml(s.text)} (${s.score})</span>`;
    });
    html += `</div>`;
  }

  suggestBox.innerHTML = html;

  // clic => remplit la recherche et lance le search
  document.querySelectorAll(".suggest-item").forEach((el) => {
    el.addEventListener("click", () => {
      const v = el.getAttribute("data-val") || "";
      searchInput.value = v;
      offset = 0;
      // recharge suggestions + résultats
      loadSuggest();
      loadTablePage();
    });
  });
}

async function loadTables() {
  const res = await fetch("/api/data/tables");
  const data = await res.json();
  tableSelect.innerHTML = "";

  (data.tables || []).forEach((t) => {
    const opt = document.createElement("option");
    opt.value = t;
    opt.textContent = t;
    tableSelect.appendChild(opt);
  });

  metaLine.textContent = `${(data.tables || []).length} table(s) détectée(s)`;
}

async function loadSuggest() {
  const table = tableSelect.value;
  const q = (searchInput.value || "").trim();

  if (!q) {
    renderSuggest({ typed: "" });
    return;
  }

  const res = await fetch(
    `/api/data/suggest?table=${encodeURIComponent(table)}&q=${encodeURIComponent(q)}&top_k=8`
  );
  const data = await res.json();
  renderSuggest(data);
}

async function loadTablePage() {
  const table = tableSelect.value;
  const limit = parseInt(pageSizeSel.value || "50", 10);
  const lang = langSel.value;

  const q = (searchInput.value || "").trim();
  if (q.length > 0) {
    // search mode => /api/data/search renvoie aussi best_match/suggestions/similar
    const res = await fetch(
      `/api/data/search?table=${encodeURIComponent(table)}&q=${encodeURIComponent(q)}&limit=30&lang=${encodeURIComponent(lang)}`
    );
    const data = await res.json();

    // si backend renvoie error (404, etc.)
    if (data.error) {
      tableWrap.innerHTML = `<div class="muted" style="padding:14px;">${escapeHtml(data.error)}</div>`;
      tableMeta.textContent = `Search: "${q}"`;
      pageInfo.textContent = "—";
      prevBtn.disabled = true;
      nextBtn.disabled = true;
      return;
    }

    // suggestions
    renderSuggest({
      typed: data.typed || q,
      best_match: data.best_match,
      suggestions: data.suggestions,
      similar: data.similar,
    });

    renderTable(data.columns, data.rows);
    tableMeta.textContent = `Search: "${q}" | results: ${(data.rows || []).length} | cols used: ${(data.text_columns_used || []).join(", ")}`;
    pageInfo.textContent = "Mode recherche (pas de pagination)";
    prevBtn.disabled = true;
    nextBtn.disabled = true;
    return;
  }

  // normal pagination mode
  const res = await fetch(
    `/api/data/table/${encodeURIComponent(table)}?limit=${limit}&offset=${offset}&lang=${encodeURIComponent(lang)}`
  );
  const data = await res.json();

  if (data.error) {
    tableWrap.innerHTML = `<div class="muted" style="padding:14px;">${escapeHtml(data.error)}</div>`;
    tableMeta.textContent = `${table}`;
    pageInfo.textContent = "—";
    prevBtn.disabled = true;
    nextBtn.disabled = true;
    return;
  }

  total = data.total || 0;

  // clear suggest box in pagination mode (no search)
  renderSuggest({ typed: "" });

  renderTable(data.columns, data.rows);

  const start = total === 0 ? 0 : offset + 1;
  const end = Math.min(offset + limit, total);
  tableMeta.textContent = `${table} | ${start}-${end} / ${total}`;
  pageInfo.textContent = `Page: ${Math.floor(offset / limit) + 1}`;

  prevBtn.disabled = offset <= 0;
  nextBtn.disabled = offset + limit >= total;
}

function debounce(fn, ms) {
  let t = null;
  return (...args) => {
    clearTimeout(t);
    t = setTimeout(() => fn(...args), ms);
  };
}

const debouncedSearch = debounce(() => {
  offset = 0;
  loadSuggest();
  loadTablePage();
}, 300);

refreshBtn.addEventListener("click", () => {
  offset = 0;
  loadSuggest();
  loadTablePage();
});

searchInput.addEventListener("input", debouncedSearch);

tableSelect.addEventListener("change", () => {
  offset = 0;
  loadSuggest();
  loadTablePage();
});

langSel.addEventListener("change", () => {
  offset = 0;
  loadTablePage(); // traduction seulement (suggestions restent identiques)
});

pageSizeSel.addEventListener("change", () => {
  offset = 0;
  loadTablePage();
});

prevBtn.addEventListener("click", () => {
  const limit = parseInt(pageSizeSel.value || "50", 10);
  offset = Math.max(0, offset - limit);
  loadTablePage();
});

nextBtn.addEventListener("click", () => {
  const limit = parseInt(pageSizeSel.value || "50", 10);
  offset = offset + limit;
  loadTablePage();
});

(async function init() {
  await loadTables();
  await loadTablePage();
})();
