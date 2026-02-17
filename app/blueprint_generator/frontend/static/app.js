// ─────────────────────────────────────────────
// Agent Blueprint Generator — Frontend
// ─────────────────────────────────────────────

let nuggets = [];
let selectedIds = new Set();

// ── Init ──────────────────────────────────────

document.addEventListener("DOMContentLoaded", loadNuggets);

async function loadNuggets() {
  try {
    const res = await fetch("/api/nuggets");
    nuggets = await res.json();
    renderNuggets();
    selectAll(); // start with all selected
  } catch (err) {
    console.error("Failed to load nuggets:", err);
  }
}

// ── Render Nuggets ────────────────────────────

function renderNuggets() {
  const container = document.getElementById("nuggetList");
  container.innerHTML = nuggets
    .map((n) => {
      const isSelected = selectedIds.has(n.id);
      const typeClass = n.type;
      const categoryTag =
        n.category
          ? `<span class="tag tag-category">${n.category.replace("_", " ")}</span>`
          : "";

      return `
      <div class="nugget-card ${isSelected ? "selected" : ""}"
           data-id="${n.id}"
           onclick="toggleNugget('${n.id}')">
        <div class="nugget-top">
          <div class="nugget-check">${isSelected ? "✓" : ""}</div>
          <div class="nugget-info">
            <div class="nugget-name">${n.name}</div>
            <div class="nugget-desc">${n.description}</div>
          </div>
        </div>
        <div class="nugget-tags">
          <span class="tag tag-type ${typeClass}">${n.type}</span>
          ${categoryTag}
          ${n.domain_tags.map((d) => `<span class="tag">${d}</span>`).join("")}
        </div>
      </div>`;
    })
    .join("");
}

// ── Selection ─────────────────────────────────

function toggleNugget(id) {
  if (selectedIds.has(id)) {
    selectedIds.delete(id);
  } else {
    selectedIds.add(id);
  }
  renderNuggets();
}

function selectAll() {
  selectedIds = new Set(nuggets.map((n) => n.id));
  renderNuggets();
}

function selectNone() {
  selectedIds.clear();
  renderNuggets();
  // Reset output
  document.getElementById("results").style.display = "none";
  document.getElementById("noResults").style.display = "none";
  document.getElementById("emptyState").style.display = "flex";
  document.getElementById("metadata").textContent = "";
}

// ── Generate ──────────────────────────────────

async function generateBlueprints() {
  const btn = document.getElementById("generateBtn");
  const ids = Array.from(selectedIds);

  if (ids.length === 0) {
    return;
  }

  // Loading state
  btn.classList.add("loading");
  btn.innerHTML = '<span class="spinner"></span> Generating...';

  try {
    const res = await fetch("/api/blueprints", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ nugget_ids: ids }),
    });

    const data = await res.json();
    renderResults(data);
  } catch (err) {
    console.error("Failed to generate blueprints:", err);
  } finally {
    btn.classList.remove("loading");
    btn.textContent = "Generate Blueprints";
  }
}

// ── Render Results ────────────────────────────

function renderResults(data) {
  const emptyState = document.getElementById("emptyState");
  const results = document.getElementById("results");
  const noResults = document.getElementById("noResults");
  const metadataEl = document.getElementById("metadata");

  const svBlueprints = data.semantic_view_blueprints || [];
  const agentBlueprints = data.agent_blueprints || [];
  const meta = data.metadata || {};

  // Metadata
  metadataEl.textContent = `${meta.nuggets_analyzed} nuggets → ${svBlueprints.length} views, ${agentBlueprints.length} agents`;

  if (svBlueprints.length === 0 && agentBlueprints.length === 0) {
    emptyState.style.display = "none";
    results.style.display = "none";
    noResults.style.display = "flex";
    return;
  }

  emptyState.style.display = "none";
  noResults.style.display = "none";
  results.style.display = "block";

  // Semantic View cards
  document.getElementById("svCards").innerHTML = svBlueprints
    .map(
      (sv) => `
    <div class="blueprint-card sv-card">
      <h4>${sv.name}</h4>
      <div class="card-id">${sv.id}</div>
      <div class="card-purpose">${sv.purpose}</div>
      <div class="card-label">Domains</div>
      <div class="nugget-ref-list">
        ${sv.domains.map((d) => `<span class="nugget-ref">${d}</span>`).join("")}
      </div>
      <div class="card-label">Skill Nuggets</div>
      <div class="nugget-ref-list">
        ${sv.skill_nugget_ids.map((id) => `<span class="nugget-ref">${id}</span>`).join("")}
      </div>
      ${
        sv.evaluation_nugget_ids.length > 0
          ? `<div class="card-label">Evaluation Nuggets</div>
             <div class="nugget-ref-list">
               ${sv.evaluation_nugget_ids.map((id) => `<span class="nugget-ref">${id}</span>`).join("")}
             </div>`
          : ""
      }
    </div>`
    )
    .join("");

  // Agent cards
  document.getElementById("agentCards").innerHTML = agentBlueprints
    .map(
      (ab) => `
    <div class="blueprint-card agent-card">
      <h4>${ab.name}</h4>
      <div class="card-id">${ab.id}</div>
      <span class="agent-type-badge">${ab.agent_type}</span>
      <div class="card-purpose">${ab.description}</div>
      <div class="card-label">Linked Semantic View</div>
      <div class="nugget-ref-list">
        <span class="nugget-ref">${ab.semantic_view_id}</span>
      </div>
      <div class="card-label">Expected Capabilities</div>
      <ul class="capability-list">
        ${ab.expected_capabilities.map((c) => `<li>${c}</li>`).join("")}
      </ul>
      ${
        ab.evaluation_nugget_ids.length > 0
          ? `<div class="card-label">Evaluation Packs</div>
             <div class="nugget-ref-list">
               ${ab.evaluation_nugget_ids.map((id) => `<span class="nugget-ref">${id}</span>`).join("")}
             </div>`
          : ""
      }
    </div>`
    )
    .join("");
}
