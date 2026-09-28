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
    if (row.shown_score == null) return '<span class="hs-band unscored">—</span>';
    return `<span class="hs-band ${esc(row.band)}">${esc(row.shown_score)}</span>`;
  }

  function renderReport(payload) {
    const counts = payload.counts || {};
    $("hs-report-summary").textContent =
      `${payload.entity_count} active · ${counts.red || 0} red · ${counts.yellow || 0} yellow · ${counts.green || 0} green · ${counts.unscored || 0} unscored`;
    const rows = payload.entities || [];
    $("hs-report-body").innerHTML = rows.length
      ? rows
          .map(
            (row) => `<tr class="${esc(row.band || "unscored")}">
              <td>${scoreCell(row)}</td>
              <td><a href="/healthscore?entity=${encodeURIComponent(row.id)}">${esc(row.name)}</a></td>
              <td>${row.coverage_pct == null ? "—" : `${row.coverage_pct}%`}</td>
            </tr>`,
          )
          .join("")
      : `<tr><td colspan="3" class="muted">No active Salesforce Customer Entities.</td></tr>`;
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
