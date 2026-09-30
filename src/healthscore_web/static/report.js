(() => {
  const $ = (id) => document.getElementById(id);

  function esc(value) {
    return String(value ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;");
  }

  async function api(path, options = {}) {
    const response = await fetch(path, {
      credentials: "same-origin",
      cache: "no-store",
      ...options,
      headers: {
        ...(options.body ? { "Content-Type": "application/json" } : {}),
        ...(options.headers || {}),
      },
    });
    let payload = null;
    try {
      payload = await response.json();
    } catch (_) {
      payload = null;
    }
    if (!response.ok) {
      throw new Error((payload && payload.error) || `${response.status} ${response.statusText}`);
    }
    return payload;
  }

  function userInitials(me) {
    const name = String(me.name || "").trim();
    if (name) {
      const parts = name.split(/\s+/).filter(Boolean);
      if (parts.length >= 2) return (parts[0][0] + parts[parts.length - 1][0]).toUpperCase();
      return name.slice(0, 2).toUpperCase();
    }
    const local = String(me.email || "").split("@")[0];
    return (local.slice(0, 2) || "?").toUpperCase();
  }

  function setUserMenuOpen(open) {
    $("user-menu").classList.toggle("hidden", !open);
    $("user-badge").setAttribute("aria-expanded", open ? "true" : "false");
  }

  function renderUser(me) {
    const display = me.name || me.email || "Signed in";
    $("user-menu-name").textContent = display;
    $("user-menu-email").textContent = me.email || "";
    $("user-menu-email").hidden = !me.email || display === me.email;
    $("user-menu-role").textContent = me.is_catalog_admin
      ? "catalog admin (edit all)"
      : "lead (edit-own, read-all)";
    $("user-badge").title = display;
    $("user-badge").setAttribute("aria-label", `Account menu for ${display}`);
    const img = $("user-badge-img");
    const initials = $("user-badge-initials");
    if (me.picture) {
      img.src = me.picture;
      img.hidden = false;
      initials.hidden = true;
    } else {
      img.removeAttribute("src");
      img.hidden = true;
      initials.hidden = false;
      initials.textContent = userInitials(me);
    }
  }

  function showLogin(status, message) {
    $("session").classList.add("hidden");
    $("hs-report").classList.add("hidden");
    $("hs-login").classList.remove("hidden");
    const actions = $("hs-login-actions");
    actions.innerHTML = "";
    const next = encodeURIComponent("/healthscore/report");
    if (status.google_configured) {
      actions.innerHTML += `<a href="/auth/login?next=${next}"><button type="button">Sign in with Google</button></a>`;
    }
    if (status.dev_auth_enabled) {
      actions.innerHTML += `<a href="/auth/dev-login?next=${next}"><button type="button">Dev login</button></a>`;
    }
    if (message) {
      $("hs-login-error").hidden = false;
      $("hs-login-error").textContent = message;
    }
  }

  function scoreCell(row) {
    if (row.score_zeroed) return '<span class="hs-override-score">OVERRIDE</span>';
    if (row.shown_score == null) return '<span class="hs-band unscored">—</span>';
    return `<span class="hs-band ${esc(row.band)}">${esc(row.shown_score)}</span>`;
  }

  function isHttpUrl(value) {
    return /^https?:\/\//i.test(String(value || "").trim());
  }

  // A name with a Web Research source links to it; everything else is plain text.
  function detailItemHtml(item) {
    const text = esc(item.text);
    if (!isHttpUrl(item.url)) return text;
    return `<a href="${esc(item.url)}" target="_blank" rel="noopener noreferrer" class="hs-source-link">${text}</a>`;
  }

  function overrideLine(flag) {
    const items = Array.isArray(flag.items) ? flag.items.filter((item) => item && String(item.text || "").trim()) : [];
    if (!items.length) return esc(flag.detail || flag.name || "");
    return `${esc(flag.name)}: ${items.map(detailItemHtml).join(", ")}`;
  }

  function overrideCell(row) {
    if (!row.score_zeroed) return "";
    const lines = (row.overrides || []).map(overrideLine).filter(Boolean);
    const detail = lines.length
      ? `<div class="hs-override-detail">${lines.join("<br>")}</div>`
      : "";
    return `${detail}<button type="button" class="secondary hs-dismiss-btn" data-entity-id="${esc(row.id)}">Dismiss override</button>`;
  }

  let reportPayload = null;
  let sortKey = "score";

  function entityName(row) {
    return String(row.name || "").trim();
  }

  function compareEntity(a, b) {
    return entityName(a).localeCompare(entityName(b), undefined, { sensitivity: "base" });
  }

  function compareScoreThenEntity(a, b) {
    const aScore = a.shown_score;
    const bScore = b.shown_score;
    if (aScore == null && bScore == null) return compareEntity(a, b);
    if (aScore == null) return 1;
    if (bScore == null) return -1;
    if (aScore !== bScore) return bScore - aScore;
    return compareEntity(a, b);
  }

  function sortedEntities(rows) {
    const pinned = [];
    const rest = [];
    for (const row of rows) (row.score_zeroed ? pinned : rest).push(row);
    pinned.sort(compareEntity);
    rest.sort(sortKey === "entity" ? compareEntity : compareScoreThenEntity);
    return pinned.concat(rest);
  }

  function markSortHeaders() {
    $("hs-sort-score").setAttribute("aria-sort", sortKey === "score" ? "descending" : "none");
    $("hs-sort-entity").setAttribute("aria-sort", sortKey === "entity" ? "ascending" : "none");
  }

  function reportSparkline(row) {
    const points = (row.healthscore_history || [])
      .map((item) => ({ period: String(item.period_key || ""), value: Number(item.value) }))
      .filter((item) => item.period && Number.isFinite(item.value));
    if (points.length < 2) return '<span class="muted">—</span>';
    const width = 72;
    const height = 22;
    const pad = 2;
    const min = Math.min(...points.map((point) => point.value));
    const max = Math.max(...points.map((point) => point.value));
    const span = max - min;
    const coords = points
      .map((point, index) => {
        const x = pad + (index / (points.length - 1)) * (width - pad * 2);
        const y = span === 0 ? height / 2 : pad + (1 - (point.value - min) / span) * (height - pad * 2);
        return `${x.toFixed(1)},${y.toFixed(1)}`;
      })
      .join(" ");
    const tip = points.map((point) => `${point.period}: ${point.value}`).join(", ");
    return `<svg class="hs-spark" viewBox="0 0 ${width} ${height}" role="img" aria-label="Health score history for ${esc(row.name)}"><title>${esc(tip)}</title><polyline points="${coords}"></polyline></svg>`;
  }

  function renderReport(payload) {
    reportPayload = payload;
    const counts = payload.counts || {};
    $("hs-report-summary").textContent =
      `${payload.entity_count} active · ${counts.red || 0} red · ${counts.yellow || 0} yellow · ${counts.green || 0} green · ${counts.unscored || 0} unscored`;
    const rows = sortedEntities(payload.entities || []);
    markSortHeaders();
    $("hs-report-body").innerHTML = rows.length
      ? rows
          .map(
            (row) => `<tr class="${row.score_zeroed ? "override" : esc(row.band || "unscored")}">
              <td>${scoreCell(row)}</td>
              <td><a href="/healthscore?entity=${encodeURIComponent(row.id)}">${esc(row.name)}</a>${overrideCell(row)}</td>
              <td class="hs-col-center hs-spark-cell">${reportSparkline(row)}</td>
              <td class="hs-col-center"><button type="button" class="secondary hs-analysis-btn" data-entity-id="${esc(row.id)}" data-entity-name="${esc(row.name)}">Analysis</button></td>
            </tr>`,
          )
          .join("")
      : `<tr><td colspan="4" class="muted">No active Salesforce Customer Entities.</td></tr>`;
  }

  function analysisHtml(text) {
    const blocks = [];
    let bullets = [];
    const flush = () => {
      if (bullets.length) blocks.push(`<ul>${bullets.map((item) => `<li>${esc(item)}</li>`).join("")}</ul>`);
      bullets = [];
    };
    for (const raw of String(text || "").split("\n")) {
      const line = raw.trim();
      if (!line) continue;
      if (/^[-•*]\s+/.test(line)) {
        bullets.push(line.replace(/^[-•*]\s+/, ""));
      } else {
        flush();
        blocks.push(`<p>${esc(line)}</p>`);
      }
    }
    flush();
    return blocks.join("");
  }

  async function openAnalysis(entityId, entityName) {
    const dialog = $("hs-analysis-dialog");
    const row = ((reportPayload && reportPayload.entities) || []).find((item) => item.id === entityId);
    $("hs-analysis-title").textContent = entityName;
    const score = $("hs-analysis-score");
    score.className = `hs-band ${row && row.score_zeroed ? "override" : row && row.band ? row.band : "unscored"}`;
    score.textContent = row && row.score_zeroed ? "OVERRIDE" : row && row.shown_score != null ? row.shown_score : "—";
    $("hs-analysis-meta").textContent = "Asking Claude about this site's inputs and history…";
    $("hs-analysis-body").innerHTML = "";
    $("hs-analysis-error").hidden = true;
    $("hs-analysis-error").textContent = "";
    if (!dialog.open) dialog.showModal();
    try {
      const payload = await api(`/healthscore/api/entities/${encodeURIComponent(entityId)}/analysis`);
      if (!dialog.open || $("hs-analysis-title").textContent !== entityName) return;
      const gaps = payload.unscored_inputs || [];
      $("hs-analysis-meta").textContent =
        `${payload.scored_inputs} scored input${payload.scored_inputs === 1 ? "" : "s"} · ` +
        `${gaps.length} unscored · as of ${payload.as_of}`;
      $("hs-analysis-body").innerHTML = analysisHtml(payload.analysis);
    } catch (error) {
      if (!dialog.open) return;
      $("hs-analysis-meta").textContent = "";
      $("hs-analysis-error").hidden = false;
      $("hs-analysis-error").textContent = error.message;
    }
  }

  async function dismissOverride(entityId, button) {
    button.disabled = true;
    $("hs-report-error").hidden = true;
    try {
      await api(`/healthscore/api/entities/${encodeURIComponent(entityId)}/overrides/dismiss`, { method: "POST" });
      renderReport(await api("/healthscore/api/report"));
    } catch (error) {
      $("hs-report-error").hidden = false;
      $("hs-report-error").textContent = error.message;
      button.disabled = false;
    }
  }

  $("hs-report-body").addEventListener("click", (event) => {
    const dismiss = event.target.closest(".hs-dismiss-btn");
    if (dismiss) {
      dismissOverride(dismiss.dataset.entityId, dismiss);
      return;
    }
    const button = event.target.closest(".hs-analysis-btn");
    if (!button) return;
    openAnalysis(button.dataset.entityId, button.dataset.entityName);
  });

  async function boot() {
    let auth;
    try {
      auth = await api("/auth/status");
    } catch (error) {
      showLogin({ google_configured: false, dev_auth_enabled: false }, error.message);
      return;
    }
    if (!auth.authenticated) {
      showLogin(auth, auth.error);
      return;
    }
    $("hs-login").classList.add("hidden");
    $("hs-report").classList.remove("hidden");
    $("session").classList.remove("hidden");
    try {
      const me = await api("/api/me");
      renderUser(me);
      renderReport(await api("/healthscore/api/report"));
    } catch (error) {
      $("hs-report-error").hidden = false;
      $("hs-report-error").textContent = error.message;
    }
  }

  $("hs-sort-score").querySelector("button").addEventListener("click", () => {
    sortKey = "score";
    if (reportPayload) renderReport(reportPayload);
  });
  $("hs-sort-entity").querySelector("button").addEventListener("click", () => {
    sortKey = "entity";
    if (reportPayload) renderReport(reportPayload);
  });

  $("user-badge").addEventListener("click", (event) => {
    event.stopPropagation();
    setUserMenuOpen($("user-menu").classList.contains("hidden"));
  });
  $("user-menu").addEventListener("click", (event) => event.stopPropagation());
  document.addEventListener("click", () => setUserMenuOpen(false));
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") setUserMenuOpen(false);
  });

  boot();
})();
