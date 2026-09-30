(() => {
  const $ = (id) => document.getElementById(id);
  const state = {
    framework: null,
    entities: [],
    entity: null,
    score: null,
    selected: null,
    me: null,
  };
  const FLAGS_PILLAR = "Override conditions";
  const openPillars = new Set();
  let flagsTriggered = false;

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
      if (parts.length >= 2) {
        return (parts[0][0] + parts[parts.length - 1][0]).toUpperCase();
      }
      return name.slice(0, 2).toUpperCase();
    }
    const local = String(me.email || "").split("@")[0];
    const bits = local.split(/[._-]+/).filter(Boolean);
    if (bits.length >= 2) {
      return (bits[0][0] + bits[1][0]).toUpperCase();
    }
    return (local.slice(0, 2) || "?").toUpperCase();
  }

  function setUserMenuOpen(open) {
    const menu = $("user-menu");
    const badge = $("user-badge");
    if (!menu || !badge) return;
    menu.classList.toggle("hidden", !open);
    badge.setAttribute("aria-expanded", open ? "true" : "false");
  }

  function renderUser(me) {
    const role = me.is_catalog_admin
      ? "catalog admin (edit all)"
      : "lead (edit-own, read-all)";
    const display = me.name || me.email || "Signed in";
    $("user-menu-name").textContent = display;
    $("user-menu-email").textContent = me.email || "";
    $("user-menu-email").hidden = !me.email || display === me.email;
    $("user-menu-role").textContent = role;
    $("user-badge").title = display;
    $("user-badge").setAttribute("aria-label", `Account menu for ${display}`);
    const img = $("user-badge-img");
    const badgeInitials = $("user-badge-initials");
    if (me.picture) {
      img.src = me.picture;
      img.hidden = false;
      badgeInitials.hidden = true;
    } else {
      img.removeAttribute("src");
      img.hidden = true;
      badgeInitials.hidden = false;
      badgeInitials.textContent = userInitials(me);
    }
    setUserMenuOpen(false);
  }

  function showLogin(status, message) {
    setUserMenuOpen(false);
    $("session").classList.add("hidden");
    $("hs-app").classList.add("hidden");
    $("hs-login").classList.remove("hidden");
    const actions = $("hs-login-actions");
    actions.innerHTML = "";
    if (status.google_configured) {
      actions.innerHTML += '<a href="/auth/login?next=/healthscore"><button type="button">Sign in with Google</button></a>';
    }
    if (status.dev_auth_enabled) {
      actions.innerHTML += '<a href="/auth/dev-login?next=/healthscore"><button type="button">Dev login</button></a>';
    }
    if (message) {
      $("hs-login-error").hidden = false;
      $("hs-login-error").textContent = message;
    }
  }

  function showError(message) {
    const error = $("hs-global-error");
    error.hidden = !message;
    error.textContent = message || "";
  }

  const STATUS_LABEL = { automated: "Automated", manual: "Manual", blocked: "Blocked" };

  function sourcesOf(component) {
    const raw = component && component.data_source;
    const list = Array.isArray(raw) ? raw : raw ? [raw] : [];
    return list.map((item) => String(item).trim()).filter(Boolean);
  }

  function availableSourceSet() {
    const names = (state.framework && state.framework.available_data_sources) || [];
    return new Set(names.map((item) => String(item).trim().toLowerCase()).filter(Boolean));
  }

  function sourceChips(component) {
    const sources = sourcesOf(component);
    if (!sources.length) return "—";
    const known = availableSourceSet();
    const chips = sources.map((item) => {
      const available = known.has(item.toLowerCase());
      const label = item;
      const tone = available ? "available" : "unavailable";
      const hover = available ? "" : ` title="Nonexistent"`;
      return `<span class="hs-source-chip ${tone}"${hover}>${esc(label)}</span>`;
    });
    return `<span class="hs-sources">${chips.join("")}</span>`;
  }

  function statusBadge(component) {
    if (component && component.deactivated) {
      return `<span class="hs-source-badge deactivated" title="Deactivated. Reactivate to show the underlying status again."><i class="dot deactivated"></i>Deactivated</span>`;
    }
    const key = String((component && component.automation) || "").toLowerCase();
    const label = STATUS_LABEL[key] || "Unknown";
    return `<span class="hs-source-badge ${esc(key)}"><i class="dot ${esc(key)}"></i>${esc(label)}</span>`;
  }

  function generatorPill(component) {
    if (!component || typeof component !== "object") return "";
    if (String(component.automation || "").toLowerCase() === "manual") return "";
    const generator = component["metric-generator"];
    if (generator && String(generator).trim()) return "";
    return `<span class="hs-source-badge no-generator" title="This input has no metric generator."><i class="dot no-generator"></i>No generator</span>`;
  }

  function closeRowMenu() {
    const menu = $("hs-row-menu");
    if (!menu) return;
    menu.classList.add("hidden");
    menu.innerHTML = "";
    delete menu.dataset.component;
  }

  function hasGenerator(component) {
    const generator = component && component["metric-generator"];
    return Boolean(generator && String(generator).trim());
  }

  function statusChoices(component) {
    return hasGenerator(component) ? ["automated", "blocked"] : ["manual", "blocked"];
  }

  function openRowMenu(event, key) {
    if (!isAdmin()) return;
    const definition = componentDefinition(key);
    if (!definition) return;
    event.preventDefault();
    selectComponent(key);
    const menu = $("hs-row-menu");
    const current = String(definition.automation || "").toLowerCase();
    const statuses = statusChoices(definition).map((value) => {
      const selected = value === current;
      return `<button type="button" role="menuitem" data-action="status" data-status="${value}"${selected ? ' class="is-current" aria-checked="true"' : ""}>${esc(STATUS_LABEL[value])}</button>`;
    }).join("");
    const deactivated = Boolean(definition.deactivated);
    menu.innerHTML = `<div class="hs-row-menu-label">Status</div>${statuses}<div class="hs-row-menu-sep" role="separator"></div><button type="button" role="menuitem" data-action="deactivate">${deactivated ? "Reactivate" : "Deactivate"}</button>`;
    menu.dataset.component = key;
    menu.classList.remove("hidden");
    const pad = 8;
    const rect = menu.getBoundingClientRect();
    const left = Math.min(event.clientX, window.innerWidth - rect.width - pad);
    const top = Math.min(event.clientY, window.innerHeight - rect.height - pad);
    menu.style.left = `${Math.max(pad, left)}px`;
    menu.style.top = `${Math.max(pad, top)}px`;
  }

  async function setStatus(key, automation) {
    closeRowMenu();
    const definition = componentDefinition(key);
    if (definition && String(definition.automation || "").toLowerCase() === automation) return;
    showError("");
    try {
      const result = await api(`/healthscore/api/framework/components/${encodeURIComponent(key)}`, {
        method: "PUT",
        body: JSON.stringify({ automation }),
      });
      state.framework = result.framework;
      await loadScore();
    } catch (saveError) {
      showError(saveError.message);
    }
  }

  async function setDeactivated(key, deactivated) {
    closeRowMenu();
    showError("");
    try {
      const result = await api(`/healthscore/api/framework/components/${encodeURIComponent(key)}`, {
        method: "PUT",
        body: JSON.stringify({ deactivated }),
      });
      state.framework = result.framework;
      await loadScore();
    } catch (saveError) {
      showError(saveError.message);
    }
  }

  function renderEntities(source) {
    const select = $("hs-entity");
    select.innerHTML = '<option value="">Choose an active entity…</option>';
    for (const entity of state.entities) {
      const option = document.createElement("option");
      option.value = entity.id;
      option.textContent = entity.name;
      select.appendChild(option);
    }
    $("hs-entity-source").textContent = `${source} · ${state.entities.length} active`;
  }

  function influenceBar(contribution, weight) {
    const pct = weight ? Math.max(0, Math.min(100, (contribution / weight) * 100)) : 0;
    const shownWeight = Math.round(Number(weight) * 100) / 100;
    return `<div class="hs-influence">${contribution.toFixed(2)} / ${shownWeight}
      <div class="hs-influence-track"><span class="hs-influence-fill" style="width:${pct}%"></span></div>
    </div>`;
  }

  function influenceCell(component) {
    const contribution = component.contribution;
    const weight = component.weight;
    if (contribution == null || weight == null) return '<span class="muted">Unscored</span>';
    return influenceBar(contribution, weight);
  }

  function pillarInfluence(items) {
    const scored = items.filter((item) => item.contribution != null && item.weight != null);
    if (!scored.length) return '<span class="muted">Unscored</span>';
    const contribution = scored.reduce((sum, item) => sum + Number(item.contribution), 0);
    const weight = scored.reduce((sum, item) => sum + Number(item.weight), 0);
    return influenceBar(contribution, weight);
  }

  function formatWeight(value) {
    const rounded = Math.round(Number(value) * 100) / 100;
    return `${Number.isInteger(rounded) ? rounded : rounded}%`;
  }

  function pillarGroups(components) {
    const groups = new Map();
    for (const component of components) {
      const name = component.pillar || "Unassigned";
      if (!groups.has(name)) groups.set(name, []);
      groups.get(name).push(component);
    }
    return [...groups.entries()]
      .map(([name, items]) => {
        items.sort((a, b) => String(a.name || "").localeCompare(String(b.name || "")));
        const total = items.reduce((sum, item) => sum + (item.weight == null ? 0 : Number(item.weight)), 0);
        return { name, items, total };
      })
      .sort((a, b) => b.total - a.total || a.name.localeCompare(b.name));
  }

  function pillarHeader(name, weight, { toggle = false, flag = false, influence = "" } = {}) {
    const weightCell = weight == null ? "—" : formatWeight(weight);
    const open = openPillars.has(name);
    const cls = ["hs-pillar-row", toggle ? "hs-pillar-toggle-row" : "", flag ? "hs-flag-pillar" : "", open ? "open" : ""]
      .filter(Boolean).join(" ");
    const label = toggle
      ? `<button type="button" class="hs-pillar-toggle" aria-expanded="${open ? "true" : "false"}">
          <span class="hs-pillar-chevron" aria-hidden="true"></span>${esc(name)}
        </button>`
      : `<span class="hs-pillar-static"><span class="hs-pillar-bullet" aria-hidden="true"></span>${esc(name)}</span>`;
    return `<tr class="${cls}"${toggle ? ` data-pillar="${esc(name)}"` : ""}>
      <th scope="rowgroup">${label}</th>
      <td>${weightCell}</td>
      <td>${influence}</td>
      <td></td>
      <td></td>
      <td></td>
      <td colspan="2"></td>
    </tr>`;
  }

  function flagIsOn(component) {
    const value = component && component.latest ? component.latest.effective_value : null;
    return value != null && value !== "" && Number(value) !== 0;
  }

  function componentRows(component, collapsed, group) {
    const isFlag = !Object.prototype.hasOwnProperty.call(component, "pillar");
    const on = isFlag && flagIsOn(component);
    const rowClass = [
      isFlag ? "hs-flag-row" : "",
      collapsed ? "hs-collapsed" : "",
      component.key === state.selected ? "active" : "",
      component.deactivated ? "deactivated" : "",
    ].filter(Boolean).join(" ");
    const signal = isFlag
      ? (on ? '<span class="hs-status">On</span>' : "")
      : `<span class="hs-status">${esc(component.signal)}</span>`;
    const weight = isFlag
      ? '<span class="muted">—</span>'
      : (component.weight == null ? '<span class="hs-status">TBD</span>' : formatWeight(component.weight));
    const influence = isFlag ? '<span class="muted">—</span>' : influenceCell(component);
    return `<tr data-component="${esc(component.key)}" data-group="${esc(group)}" class="${rowClass}">
      <td><div class="hs-component-name">${esc(component.name)}</div>${signal}</td>
      <td>${weight}</td>
      <td>${influence}</td>
      <td class="hs-col-center">${currentValueCell(component)}</td>
      <td class="hs-col-center hs-as-of">${asOfCell(component)}</td>
      <td class="hs-col-center hs-spark-cell">${listSparkline(component)}</td>
      <td>${sourceChips(component)}</td>
      <td><span class="hs-status-pills">${statusBadge(component)}${generatorPill(component)}</span></td>
    </tr>`;
  }

  function renderHero() {
    const hero = $("hs-score-hero");
    if (!hero) return;
    const valueEl = $("hs-score-input-value");
    const coverageEl = $("hs-score-coverage");
    const healthEl = $("hs-score-health");
    const note = hero.querySelector(".hs-score-note");
    const raw = state.score ? Number(state.score.score) : NaN;
    if (!Number.isFinite(raw)) {
      hero.className = "hs-score-hero hs-score-empty";
      valueEl.textContent = "—";
      coverageEl.textContent = "—";
      healthEl.textContent = "—";
      note.textContent = state.entity ? "No scored inputs yet" : "Choose an entity";
      return;
    }
    const valueShown = Math.round(raw);
    const coverage = Number(state.score.coverage_pct);
    const coverageShown = Number.isFinite(coverage) ? coverage : 0;
    const health = Math.round(valueShown * coverageShown / 100);
    const band = health <= 33 ? "red" : health <= 67 ? "yellow" : "green";
    hero.className = `hs-score-hero hs-score-${band}`;
    valueEl.textContent = String(valueShown);
    coverageEl.textContent = `${coverageShown}%`;
    healthEl.textContent = String(health);
    if (state.score.score_zeroed) {
      const names = (state.score.zeroed_by || []).filter(Boolean);
      note.textContent = names.length ? `Set to 0 by ${names.join(", ")}` : "Set to 0 by an override condition";
    } else {
      note.textContent = "";
    }
  }

  function renderScore() {
    renderHero();
    const score = state.score;
    const components = score ? score.components : state.framework.inputs.map((row) => ({ ...row, latest: null, contribution: null }));
    const groups = pillarGroups(components);
    const assigned = groups.reduce((sum, group) => sum + group.total, 0);
    const unassigned = Math.round((100 - assigned) * 100) / 100;
    $("hs-unassigned-weight").textContent = `Unassigned weight: ${formatWeight(unassigned)}`;
    const overrides = score ? score.overrides : state.framework.overrides.map((row) => ({ ...row, latest: null }));
    const flags = [...overrides].sort((a, b) => String(a.name || "").localeCompare(String(b.name || "")));
    const anyOn = flags.some(flagIsOn);
    if (anyOn !== flagsTriggered) {
      if (anyOn) openPillars.add(FLAGS_PILLAR);
      else openPillars.delete(FLAGS_PILLAR);
      flagsTriggered = anyOn;
    }
    const rows = [];
    if (flags.length) {
      rows.push(pillarHeader(FLAGS_PILLAR, null, { toggle: true, flag: true }));
      for (const flag of flags) rows.push(componentRows(flag, !openPillars.has(FLAGS_PILLAR), FLAGS_PILLAR));
    }
    for (const group of groups) {
      rows.push(pillarHeader(group.name, group.total, { toggle: true, influence: pillarInfluence(group.items) }));
      for (const component of group.items) rows.push(componentRows(component, !openPillars.has(group.name), group.name));
    }
    if (unassigned > 0) rows.push(pillarHeader("Unassigned", unassigned));
    $("hs-components-body").innerHTML = rows.join("");
    for (const header of $("hs-components-body").querySelectorAll("tr[data-pillar]")) {
      header.addEventListener("click", () => togglePillar(header));
    }
    for (const row of $("hs-components-body").querySelectorAll("tr[data-component]")) {
      row.addEventListener("click", () => selectComponent(row.dataset.component));
      row.addEventListener("contextmenu", (event) => openRowMenu(event, row.dataset.component));
    }
    if (state.selected) selectComponent(state.selected);
    else showDetail(false);
  }

  function togglePillar(header) {
    // Flip the rows in place so the triangle can rotate instead of being rebuilt.
    const name = header.dataset.pillar;
    const open = !openPillars.has(name);
    if (open) openPillars.add(name);
    else openPillars.delete(name);
    header.classList.toggle("open", open);
    const button = header.querySelector(".hs-pillar-toggle");
    if (button) button.setAttribute("aria-expanded", open ? "true" : "false");
    for (const row of $("hs-components-body").querySelectorAll("tr[data-component]")) {
      if (row.dataset.group === name) row.classList.toggle("hs-collapsed", !open);
    }
  }

  function showDetail(visible) {
    const detail = $("hs-detail");
    const layout = document.querySelector(".hs-layout");
    if (detail) detail.classList.toggle("hidden", !visible);
    if (layout) layout.classList.toggle("hs-no-detail", !visible);
    if (!visible && detail) detail.innerHTML = "";
  }

  function componentDefinition(key) {
    return [...state.framework.inputs, ...state.framework.overrides].find((row) => row.key === key);
  }

  function componentLatest(key) {
    if (!state.score) return null;
    const rows = [...state.score.components, ...state.score.overrides];
    return rows.find((row) => row.key === key)?.latest || null;
  }

  function componentHistory(key) {
    const observations = state.score?.observations || [];
    return observations.filter((row) => row.metric_name === key);
  }

  function chartTick(value) {
    return String(Math.round(value * 100) / 100);
  }

  function unitSuffix(definition) {
    if (definition.unit === "percent") return "%";
    if (definition.unit === "multiple") return "x";
    if (definition.unit === "days") return " days";
    return "";
  }

  function targetText(definition) {
    if (definition.target == null) return null;
    const sign = definition.target_direction === "lower" ? "≤" : "≥";
    return `${sign} ${fmtNum(definition.target)}${unitSuffix(definition)}`;
  }

  function listSparkline(component) {
    const dated = componentHistory(component.key)
      .filter((row) => row.period_key)
      .slice()
      .sort((a, b) => String(a.period_key).localeCompare(String(b.period_key)));
    const points = [];
    for (const row of dated) {
      const value = Number(row.effective_value);
      if (Number.isFinite(value)) points.push({ period: String(row.period_key), value });
    }
    if (points.length < 2) return '<span class="muted">—</span>';
    const width = 72;
    const height = 22;
    const pad = 2;
    const min = Math.min(...points.map((point) => point.value));
    const max = Math.max(...points.map((point) => point.value));
    const span = max - min;
    const coords = points.map((point, index) => {
      const x = pad + (index / (points.length - 1)) * (width - pad * 2);
      const y = span === 0
        ? height / 2
        : pad + (1 - (point.value - min) / span) * (height - pad * 2);
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    }).join(" ");
    const tip = points
      .map((point) => `${point.period}: ${fmtNum(point.value, scoreDigits(component))}`)
      .join(", ");
    return `<svg class="hs-spark" viewBox="0 0 ${width} ${height}" role="img" aria-label="Value history for ${esc(component.name)}"><title>${esc(tip)}</title><polyline points="${coords}"></polyline></svg>`;
  }

  function sparkline(rows, definition) {
    const dated = (rows || [])
      .filter((row) => row.period_key)
      .slice()
      .sort((a, b) => String(a.period_key).localeCompare(String(b.period_key)));
    const dates = [...new Set(dated.map((row) => String(row.period_key)))];
    if (dates.length < 2) return "";
    const series = [
      { key: "effective_value", label: "Value", cls: "hs-chart-value" },
      { key: "effective_points", label: "Points", cls: "hs-chart-points" },
    ].map((spec) => {
      const byDate = new Map();
      for (const row of dated) {
        const value = Number(row[spec.key]);
        if (Number.isFinite(value)) byDate.set(String(row.period_key), value);
      }
      return { ...spec, byDate };
    }).filter((spec) => spec.byDate.size >= 2);
    if (!series.length) return "";

    const width = 280;
    const height = 112;
    const left = series.some((spec) => spec.key === "effective_value") ? 30 : 8;
    const right = series.some((spec) => spec.key === "effective_points") ? 30 : 8;
    const top = 8;
    const bottom = 18;
    const plotWidth = width - left - right;
    const plotHeight = height - top - bottom;

    const target = definition && definition.target != null ? Number(definition.target) : null;
    const maxPoints = definition && definition.max_points != null ? Number(definition.max_points) : null;
    const drawn = series.map((spec) => {
      const nums = [...spec.byDate.values()];
      let min = Math.min(...nums);
      let max = Math.max(...nums);
      if (spec.key === "effective_value" && Number.isFinite(target)) {
        min = Math.min(min, target);
        max = Math.max(max, target);
      }
      if (spec.key === "effective_points" && Number.isFinite(maxPoints) && maxPoints > 0) {
        min = Math.min(0, min);
        max = Math.max(maxPoints, max);
      }
      const span = max - min;
      const yAt = (value) => (span === 0
        ? top + plotHeight / 2
        : top + (1 - (value - min) / span) * plotHeight);
      const points = dates
        .map((date, index) => {
          if (!spec.byDate.has(date)) return null;
          const x = left + (index / (dates.length - 1)) * plotWidth;
          return `${x.toFixed(1)},${yAt(spec.byDate.get(date)).toFixed(1)}`;
        })
        .filter(Boolean)
        .join(" ");
      return { ...spec, points, min, max, yAt };
    });
    const valueSeries = drawn.find((spec) => spec.key === "effective_value");
    const targetLine = valueSeries && Number.isFinite(target)
      ? `<line class="hs-chart-target" x1="${left}" x2="${width - right}" y1="${valueSeries.yAt(target).toFixed(1)}" y2="${valueSeries.yAt(target).toFixed(1)}"><title>Target ${esc(targetText(definition))}</title></line>`
      : "";

    const axes = drawn.map((spec) => {
      const anchor = spec.key === "effective_points" ? "end" : "start";
      const x = spec.key === "effective_points" ? width - 2 : 2;
      if (spec.min === spec.max) {
        return `<text x="${x}" y="${top + plotHeight / 2 + 3}" text-anchor="${anchor}" class="chart-label">${chartTick(spec.min)}</text>`;
      }
      return `<text x="${x}" y="${top + 7}" text-anchor="${anchor}" class="chart-label">${chartTick(spec.max)}</text>
        <text x="${x}" y="${height - bottom}" text-anchor="${anchor}" class="chart-label">${chartTick(spec.min)}</text>`;
    }).join("");

    const legend = [
      ...drawn.map((spec) => `<span class="hs-chart-key ${spec.cls}">${spec.label}</span>`),
      targetLine ? `<span class="hs-chart-key hs-chart-target-key">Target ${esc(targetText(definition))}</span>` : "",
    ].join("");
    const lines = drawn
      .map((spec) => `<polyline class="hs-chart-line ${spec.cls}" points="${spec.points}"></polyline>`)
      .join("");
    return `<figure class="hs-chart-wrap">
      <figcaption class="hs-chart-legend">${legend}</figcaption>
      <svg class="hs-chart" viewBox="0 0 ${width} ${height}" role="img" aria-label="Value and points history">
        ${targetLine}
        ${lines}
        ${axes}
        <text x="${left}" y="${height - 3}" class="chart-label">${esc(dates[0])}</text>
        <text x="${width - right}" y="${height - 3}" text-anchor="end" class="chart-label">${esc(dates[dates.length - 1])}</text>
      </svg>
    </figure>`;
  }

  function selectComponent(key) {
    state.selected = key;
    const definition = componentDefinition(key);
    if (!definition) return;
    for (const row of $("hs-components-body").querySelectorAll("tr[data-component]")) {
      row.classList.toggle("active", row.dataset.component === key);
    }
    const latest = componentLatest(key);
    const history = componentHistory(key);
    const isOverride = !Object.prototype.hasOwnProperty.call(definition, "pillar");
    showDetail(true);
    $("hs-detail").innerHTML = `
      <div class="detail-head">
        ${nameHeading(definition)}
      </div>
      <div class="hs-meta-row">
        ${definition.signal ? `<span class="hs-status">${esc(definition.signal)}</span>` : '<span class="hs-status">override</span>'}
        ${statusBadge(definition)}
        ${generatorPill(definition)}
      </div>
      <dl class="hs-detail-grid">
        ${isOverride ? "" : `<dt>Pillar</dt><dd id="hs-pillar-cell">${pillarCell(definition)}</dd>`}
        ${isOverride ? "" : `<dt>Weight</dt><dd id="hs-weight-cell">${weightCell(definition)}</dd>`}
        <dt>Description</dt><dd id="hs-description-cell">${textCell(definition, TEXT_FIELDS.description)}</dd>
        ${definition.metric ? `<dt>Measurable metric</dt><dd>${esc(definition.metric)}</dd>` : ""}
        ${definition.scoring ? `<dt>Scoring rule</dt><dd>${esc(definition.scoring)}</dd>` : ""}
        ${targetText(definition) ? `<dt>Target</dt><dd>${esc(targetText(definition))}</dd>` : ""}
        <dt>Source</dt><dd>${sourceChips(definition)}</dd>
        <dt>Owner</dt><dd id="hs-owner-cell">${textCell(definition, TEXT_FIELDS.owner)}</dd>
        <dt>Grain</dt><dd id="hs-grain-cell">${grainCell(definition)}</dd>
        ${definition.grain && !isOverride ? `<dt>Current value${periodBadge(latest)}</dt><dd>${valueCell(definition, latest)}</dd>` : ""}
        ${definition.grain ? `<dt>${isOverride ? "Condition on" : "Points"}${periodBadge(latest)}</dt><dd id="hs-points-cell">${pointsCell(definition, latest)}</dd>` : ""}
      </dl>
      ${sparkline(history, definition)}
      ${history.length ? `<table class="hs-history"><thead><tr><th>Period</th><th>Value</th><th>Points</th><th>Mode</th></tr></thead><tbody>${history.map((row) => `<tr><td>${esc(row.period_key)}</td><td>${esc(fmtNum(row.effective_value, scoreDigits(definition)))}</td><td>${esc(fmtNum(row.effective_points, scoreDigits(definition)))}</td><td>${esc(row.source_mode)}</td></tr>`).join("")}</tbody></table>` : ""}
      ${definition.grain ? "" : `<p class="muted">No grain yet — the framework defines this as ${esc(definition.cadence_note || "an undefined cadence")}, so readings cannot be stored against a period.</p>`}
      <p id="hs-entry-error" class="error" hidden></p>`;
    wireNameHeading(definition);
    wireTextCell($("hs-description-cell"), definition, TEXT_FIELDS.description);
    wireTextCell($("hs-owner-cell"), definition, TEXT_FIELDS.owner);
    wireGrainCell(definition);
    if (!isOverride) {
      wirePillarCell(definition);
      wireWeightCell(definition);
    }
    if (definition.grain) wirePointsCell(definition, latest);
  }

  function periodBadge(latest) {
    return latest ? ` <span class="hs-period">${esc(latest.period_key)}</span>` : "";
  }

  // Save one or more framework fields for a component, then re-render.
  // On failure the caller's restore() puts the cell back and the error shows
  // in the detail panel — no silent fallback.
  async function saveFrameworkField(definition, payload, restore) {
    showEntryError("");
    try {
      const result = await api(`/healthscore/api/framework/components/${encodeURIComponent(definition.key)}`, {
        method: "PUT",
        body: JSON.stringify(payload),
      });
      state.framework = result.framework;
      await loadScore();
    } catch (saveError) {
      restore();
      showEntryError(saveError.message);
    }
  }

  // Shared click-to-edit text control. `spec` describes the field:
  //   field: framework key sent in the PUT body
  //   label: human label for errors / aria
  //   multiline: use a textarea (Shift+Enter inserts a newline)
  //   inputClass: extra class on the input for sizing
  function beginTextEdit(cell, definition, spec, render, rewire) {
    if (cell.querySelector("input, textarea")) return;
    const prior = String(definition[spec.field] || "");
    const control = spec.multiline
      ? `<textarea class="value-input hs-text-input hs-text-area" rows="4" aria-label="${esc(spec.label)}"></textarea>`
      : `<input class="value-input hs-text-input ${esc(spec.inputClass || "")}" type="text" aria-label="${esc(spec.label)}" />`;
    const hint = spec.multiline ? "Enter saves · Shift+Enter for a new line · Esc cancels" : "Enter saves · Esc cancels";
    cell.innerHTML = `${control}<span class="muted hs-points-hint">${hint}</span>`;
    const input = cell.querySelector(".hs-text-input");
    input.value = prior;
    input.focus();
    if (!spec.multiline) input.select();
    let done = false;
    const restore = () => {
      cell.innerHTML = render();
      rewire();
    };
    const finish = async (save) => {
      if (done) return;
      done = true;
      const typed = input.value.trim();
      if (!save || typed === prior) {
        restore();
        return;
      }
      if (!typed) {
        restore();
        showEntryError(`${spec.label} cannot be empty`);
        return;
      }
      input.disabled = true;
      await saveFrameworkField(definition, { [spec.field]: typed }, restore);
    };
    input.addEventListener("keydown", (event) => {
      if (event.key === "Enter" && !(spec.multiline && event.shiftKey)) {
        event.preventDefault();
        finish(true);
      } else if (event.key === "Escape") {
        event.preventDefault();
        finish(false);
      }
    });
    input.addEventListener("blur", () => finish(true));
  }

  const TEXT_FIELDS = {
    name: { field: "name", label: "Name", inputClass: "hs-name-input" },
    description: { field: "description", label: "Description", multiline: true },
    owner: { field: "owner", label: "Owner", inputClass: "hs-owner-input" },
  };

  function textCell(definition, spec) {
    const text = String(definition[spec.field] || "");
    if (!isAdmin()) return esc(text);
    return `<span class="hs-edit-value" title="Click to edit the ${esc(spec.label.toLowerCase())}">${esc(text)}</span>`;
  }

  function wireTextCell(cell, definition, spec) {
    if (!cell) return;
    const value = cell.querySelector(".hs-edit-value");
    if (!value) return;
    value.addEventListener("click", () =>
      beginTextEdit(cell, definition, spec, () => textCell(definition, spec), () => wireTextCell(cell, definition, spec)),
    );
  }

  function nameHeading(definition) {
    if (!isAdmin()) return `<h2>${esc(definition.name)}</h2>`;
    return `<h2><span class="hs-name-value hs-edit-value" title="Click to rename">${esc(definition.name)}</span></h2>`;
  }

  function wireNameHeading(definition) {
    const head = document.querySelector("#hs-detail .detail-head");
    const value = head && head.querySelector(".hs-name-value");
    if (!value) return;
    value.addEventListener("click", () =>
      beginTextEdit(head, definition, TEXT_FIELDS.name, () => nameHeading(definition), () => wireNameHeading(definition)),
    );
  }

  const GRAIN_OPTIONS = ["hourly", "daily", "weekly", "monthly", "quarterly"];

  function grainCell(definition) {
    const note = definition.cadence_note ? ` <span class="muted hs-grain-note">${esc(definition.cadence_note)}</span>` : "";
    if (!isAdmin()) return `${esc(definition.grain || "not defined")}${note}`;
    const current = String(definition.grain || "");
    const options = ["", ...GRAIN_OPTIONS]
      .map((name) => `<option value="${esc(name)}"${name === current ? " selected" : ""}>${esc(name || "not defined")}</option>`)
      .join("");
    return `<select id="hs-grain-select" class="hs-pillar-select" aria-label="Grain for ${esc(definition.name)}" title="Changing the grain saves immediately">${options}</select>${note}`;
  }

  function wireGrainCell(definition) {
    const select = $("hs-grain-select");
    if (!select) return;
    select.addEventListener("change", () => saveSelect(definition, select, "grain", String(definition.grain || "")));
  }

  // Auto-save a <select>-backed framework field; reverts the control on failure.
  async function saveSelect(definition, select, field, previous) {
    const next = String(select.value || "").trim();
    if (next === previous) return;
    select.disabled = true;
    await saveFrameworkField(definition, { [field]: next || null }, () => {
      select.disabled = false;
      select.value = previous;
    });
  }

  function currentValueCell(component) {
    const latest = component && component.latest;
    if (!state.entity || !latest) return '<span class="muted">—</span>';
    const value = latest.effective_value;
    if (value == null || value === "") {
      return latest.error
        ? `<span class="muted" title="${esc(latest.error)}">—</span>`
        : '<span class="muted">—</span>';
    }
    const overridden = latest.override_value != null;
    const tip = overridden
      ? `Manual value override — generated value: ${fmtNum(latest.value)}`
      : latest.generator
        ? `Generated by ${latest.generator}`
        : "Manual reading";
    return `<span class="hs-current-value${overridden ? " value-override" : ""}" title="${esc(tip)}">${esc(fmtNum(value, scoreDigits(component)))}</span>`;
  }

  function asOfCell(component) {
    const raw = component && component.latest ? component.latest.as_of : "";
    const day = String(raw || "").slice(0, 10);
    return day ? esc(day) : '<span class="muted">—</span>';
  }

  function valueCell(definition, latest) {
    if (!state.entity) return '<span class="muted">Choose an entity to see the latest reading</span>';
    if (!latest) return '<span class="muted">No reading yet</span>';
    const value = latest.effective_value;
    if (value == null) {
      return latest.error
        ? `<span class="muted" title="${esc(latest.error)}">— (error)</span>`
        : '<span class="muted">—</span>';
    }
    const overridden = latest.override_value != null;
    const tip = overridden
      ? `Manual value override — generated value: ${fmtNum(latest.value)}`
      : latest.generator
        ? `Generated by ${latest.generator}${latest.captured_at ? ` at ${String(latest.captured_at).slice(0, 16).replace("T", " ")}` : ""}`
        : "Manual reading";
    return `<span class="hs-current-value${overridden ? " value-override" : ""}" title="${esc(tip)}">${esc(fmtNum(value))}</span>`;
  }

  function weightText(definition) {
    return definition.weight == null ? "TBD" : formatWeight(definition.weight);
  }

  function weightCell(definition) {
    if (!isAdmin()) return weightText(definition);
    if (definition.deactivated) {
      const parked = definition.deactivated_weight;
      const tip = parked == null
        ? "Held at 0 while deactivated. Reactivate to edit the weight."
        : `Held at 0 while deactivated. Reactivate to restore ${formatWeight(parked)}.`;
      return `<span class="hs-weight-value muted" title="${esc(tip)}">${weightText(definition)}</span>`;
    }
    return `<span class="hs-weight-value hs-edit-value" title="Click to change the weight">${weightText(definition)}</span>`;
  }

  function wireWeightCell(definition) {
    const cell = $("hs-weight-cell");
    if (!cell) return;
    const value = cell.querySelector(".hs-weight-value.hs-edit-value");
    if (value) value.addEventListener("click", () => beginWeightEdit(cell, definition));
  }

  function beginWeightEdit(cell, definition) {
    if (cell.querySelector("input")) return;
    const prior = definition.weight == null ? "" : String(definition.weight);
    cell.innerHTML = `<input class="value-input hs-weight-input" type="text" inputmode="decimal" aria-label="Weight for ${esc(definition.name)}" title="Clear the box to mark the weight TBD" /><span class="muted hs-points-hint">% · Enter saves · Esc cancels</span>`;
    const input = cell.querySelector("input");
    input.value = prior;
    input.focus();
    input.select();
    let done = false;
    const restore = () => {
      cell.innerHTML = weightCell(definition);
      wireWeightCell(definition);
    };
    const finish = async (save) => {
      if (done) return;
      done = true;
      const typed = input.value.trim().replace(/%$/, "");
      if (!save || typed === prior) {
        restore();
        return;
      }
      let weight = null;
      if (typed) {
        weight = Number(typed);
        if (!Number.isFinite(weight) || weight < 0) {
          restore();
          showEntryError(`Invalid weight: ${typed}`);
          return;
        }
      }
      input.disabled = true;
      await saveFrameworkField(definition, { weight }, restore);
    };
    input.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        finish(true);
      } else if (event.key === "Escape") {
        event.preventDefault();
        finish(false);
      }
    });
    input.addEventListener("blur", () => finish(true));
  }

  function pillarCell(definition) {
    if (!isAdmin()) return esc(definition.pillar);
    const current = String(definition.pillar || "");
    const names = pillarNames();
    if (current && !names.includes(current)) names.unshift(current);
    const options = names
      .map((name) => `<option value="${esc(name)}"${name === current ? " selected" : ""}>${esc(name)}</option>`)
      .join("");
    return `<select id="hs-pillar-select" class="hs-pillar-select" aria-label="Pillar for ${esc(definition.name)}" title="Changing the pillar saves immediately">${options}</select>`;
  }

  function wirePillarCell(definition) {
    const select = $("hs-pillar-select");
    if (!select) return;
    select.addEventListener("change", () => saveSelect(definition, select, "pillar", String(definition.pillar || "")));
  }

  function isAdmin() {
    return Boolean(state.me && state.me.is_catalog_admin);
  }

  function isComponentOwner(definition) {
    const me = String((state.me && state.me.email) || "").trim().toLowerCase();
    const owner = String((definition && definition.owner) || "").trim().toLowerCase();
    return Boolean(me && owner && me === owner);
  }

  function overrideField(definition) {
    return Object.prototype.hasOwnProperty.call(definition, "pillar") ? "points" : "value";
  }

  function fmtNum(value, digits) {
    if (value == null || value === "") return "—";
    const n = Number(value);
    if (!Number.isFinite(n)) return String(value);
    return digits == null ? String(n) : n.toFixed(digits);
  }

  function scoreDigits(definition) {
    return definition && definition.key === "enhancement_engagement" ? 1 : null;
  }

  function overrideTip(definition, latest) {
    const field = overrideField(definition);
    const generated = latest[field];
    const generatedText = generated == null ? (latest.error ? `error: ${latest.error}` : "none") : fmtNum(generated);
    const who = latest.override_by ? ` by ${latest.override_by}` : "";
    const when = latest.override_at ? ` on ${String(latest.override_at).slice(0, 10)}` : "";
    const note = latest.override_note ? ` — ${latest.override_note}` : "";
    return `Manual override${who}${when} — generated ${field}: ${generatedText}${note}`;
  }

  function pointsCell(definition, latest) {
    const field = overrideField(definition);
    const max = field === "points" && definition.max_points != null ? ` / ${definition.max_points}` : "";
    if (!state.entity) return '<span class="muted">Choose an entity to record a reading</span>';
    const owner = isComponentOwner(definition);
    if (latest && latest.overridden) {
      const restore = owner
        ? `<button type="button" class="value-restore" title="Show the generated reading again">Restore</button>`
        : "";
      return `<span class="value-wrap"><span class="value-override hs-points-value" title="${esc(overrideTip(definition, latest))}">${esc(fmtNum(latest[`override_${field}`], scoreDigits(definition)))}${max}</span>${restore}</span>`;
    }
    const current = latest ? latest[field] : null;
    const shown = current == null ? "—" : `${fmtNum(current, scoreDigits(definition))}${max}`;
    const tip = owner
      ? (latest && latest.generator ? `Generated by ${latest.generator} · click to override` : "Click to record a manual reading")
      : `Only ${definition.owner || "the KPI owner"} can change this score`;
    return `<span class="value-wrap"><span class="value-ok hs-points-value" title="${esc(tip)}">${esc(shown)}</span></span>`;
  }

  function wirePointsCell(definition, latest) {
    const cell = $("hs-points-cell");
    if (!cell || !state.entity || !isComponentOwner(definition)) return;
    const value = cell.querySelector(".hs-points-value");
    if (value) value.addEventListener("click", () => beginPointsEdit(cell, definition, latest));
    const restore = cell.querySelector(".value-restore");
    if (restore) restore.addEventListener("click", () => savePoints(definition, latest, null));
  }

  function beginPointsEdit(cell, definition, latest) {
    if (!isComponentOwner(definition) || cell.querySelector("input")) return;
    const field = overrideField(definition);
    const prior = latest && latest.overridden ? fmtNum(latest[`override_${field}`]) : latest && latest[field] != null ? fmtNum(latest[field]) : "";
    const max = field === "points" && definition.max_points != null ? definition.max_points : field === "value" ? 1 : null;
    const min = field === "points" && definition.min_points != null ? definition.min_points : max != null ? 0 : null;
    const range = max == null ? "" : `${min}–${max} · `;
    cell.innerHTML = `<input class="value-input" type="text" inputmode="decimal" aria-label="Override ${field} for ${esc(definition.name)}" title="Clear the box to drop the override and show the generated reading" /><span class="muted hs-points-hint">${range}Enter saves · Esc cancels</span>`;
    const input = cell.querySelector("input");
    input.value = prior === "—" ? "" : prior;
    input.focus();
    input.select();
    let done = false;
    const finish = async (save) => {
      if (done) return;
      done = true;
      const typed = input.value.trim();
      if (!save || typed === (prior === "—" ? "" : prior)) {
        cell.innerHTML = pointsCell(definition, latest);
        wirePointsCell(definition, latest);
        return;
      }
      let n = null;
      if (typed) {
        n = Number(typed);
        if (!Number.isFinite(n)) {
          cell.innerHTML = pointsCell(definition, latest);
          wirePointsCell(definition, latest);
          showEntryError(`Invalid number: ${typed}`);
          return;
        }
      }
      await savePoints(definition, latest, n);
    };
    input.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        finish(true);
      } else if (event.key === "Escape") {
        event.preventDefault();
        finish(false);
      }
    });
    input.addEventListener("blur", () => finish(true));
  }

  function showEntryError(message) {
    const error = $("hs-entry-error");
    if (!error) return;
    error.hidden = !message;
    error.textContent = message || "";
  }

  async function savePoints(definition, latest, number) {
    if (!state.entity) return;
    const field = overrideField(definition);
    const body = { note: null };
    if (latest && latest.period_key) body.period_key = latest.period_key;
    else body.as_of = new Date().toISOString().slice(0, 10);
    body[field] = number;
    showEntryError("");
    try {
      await api(`/healthscore/api/entities/${encodeURIComponent(state.entity.id)}/components/${encodeURIComponent(definition.key)}`, {
        method: "PUT",
        body: JSON.stringify(body),
      });
      await loadScore();
    } catch (saveError) {
      const cell = $("hs-points-cell");
      if (cell) {
        cell.innerHTML = pointsCell(definition, latest);
        wirePointsCell(definition, latest);
      }
      showEntryError(saveError.message);
    }
  }

  function pillarNames() {
    const names = [...new Set(state.framework.inputs.map((row) => String(row.pillar || "").trim()).filter(Boolean))];
    return names.sort((a, b) => a.localeCompare(b));
  }

  async function loadScore() {
    if (!state.entity) {
      state.score = null;
      renderScore();
      return;
    }
    showError("");
    try {
      const payload = await api(`/healthscore/api/entities/${encodeURIComponent(state.entity.id)}/score`);
      state.score = payload;
      renderScore();
    } catch (error) {
      showError(error.message);
    }
  }

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
    $("hs-app").classList.remove("hidden");
    $("session").classList.remove("hidden");
    try {
      state.me = await api("/api/me");
      renderUser(state.me);
      const [frameworkPayload, entitiesPayload] = await Promise.all([
        api("/healthscore/api/framework"),
        api("/healthscore/api/entities"),
      ]);
      state.framework = frameworkPayload.framework;
      state.entities = entitiesPayload.entities;
      renderEntities(entitiesPayload.source);
      restoreEntitySelection();
      await loadScore();
    } catch (error) {
      showError(error.message);
    }
  }

  const ENTITY_STORAGE_KEY = "cortex.healthscore.entity";

  function rememberEntity(entityId) {
    const url = new URL(window.location.href);
    if (entityId) {
      url.searchParams.set("entity", entityId);
      try {
        window.localStorage.setItem(ENTITY_STORAGE_KEY, entityId);
      } catch (_) {
        /* storage unavailable */
      }
    } else {
      url.searchParams.delete("entity");
      try {
        window.localStorage.removeItem(ENTITY_STORAGE_KEY);
      } catch (_) {
        /* storage unavailable */
      }
    }
    window.history.replaceState(null, "", url);
  }

  function restoreEntitySelection() {
    let wanted = new URL(window.location.href).searchParams.get("entity");
    if (!wanted) {
      try {
        wanted = window.localStorage.getItem(ENTITY_STORAGE_KEY);
      } catch (_) {
        wanted = null;
      }
    }
    const entity = wanted ? state.entities.find((row) => row.id === wanted) : null;
    state.entity = entity || null;
    $("hs-entity").value = entity ? entity.id : "";
    rememberEntity(entity ? entity.id : null);
  }

  $("hs-entity").addEventListener("change", async (event) => {
    state.entity = state.entities.find((row) => row.id === event.target.value) || null;
    state.selected = null;
    rememberEntity(state.entity ? state.entity.id : null);
    await loadScore();
  });
  function openAvailableSources() {
    setUserMenuOpen(false);
    const labels = (state.framework && state.framework.available_source_labels) || [];
    const list = $("hs-sources-list");
    list.innerHTML = labels.length
      ? labels.map((name) => `<li><span class="hs-source-chip available">${esc(name)}</span></li>`).join("")
      : `<li class="muted">No connected sources are configured.</li>`;
    $("hs-sources-dialog").showModal();
  }

  $("hs-row-menu").addEventListener("click", (event) => {
    const button = event.target.closest("button");
    const key = $("hs-row-menu").dataset.component;
    const definition = key && componentDefinition(key);
    if (!button || !definition) return;
    event.stopPropagation();
    if (button.dataset.action === "status") setStatus(key, button.dataset.status);
    else if (button.dataset.action === "deactivate") setDeactivated(key, !definition.deactivated);
  });
  $("btn-available-sources").addEventListener("click", openAvailableSources);
  $("user-badge").addEventListener("click", (event) => {
    event.stopPropagation();
    setUserMenuOpen($("user-menu").classList.contains("hidden"));
  });
  $("user-menu").addEventListener("click", (event) => event.stopPropagation());
  document.addEventListener("click", () => {
    setUserMenuOpen(false);
    closeRowMenu();
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      setUserMenuOpen(false);
      closeRowMenu();
    }
  });
  document.addEventListener("scroll", closeRowMenu, true);

  boot();
})();
