"use strict";
const $ = (id) => document.getElementById(id);
const state = { run: null, version: null, latencies: [], sessionEvents: [], sessionBusy: false, sessionAvailable: false };
const format = (value) => typeof value === "number" && Number.isFinite(value) ? new Intl.NumberFormat("vi-VN").format(value) : "—";
const percent = (value) => typeof value === "number" && Number.isFinite(value) ? `${(value * 100).toFixed(value !== 0 && Math.abs(value) < 0.0001 ? 4 : 2)}%` : "—";
const el = (name, cls, text) => { const node = document.createElement(name); if (cls) node.className = cls; if (text !== undefined) node.textContent = text; return node; };
function notice(message) { $("notice").hidden = !message; $("notice").textContent = message || ""; }
function clearModelView() {
  state.run = null; state.version = null; state.latencies = [];
  for (const id of ["kpi-events", "kpi-users", "kpi-items", "kpi-ratio", "model-version", "trained-at", "metric-model-version", "exported-users"]) $(id).textContent = "—";
  $("event-bars").replaceChildren(el("p", "muted", "Chưa có KPI của run đang chọn."));
  $("model-metrics").replaceChildren(el("p", "muted", "Chưa có model đánh giá của run đang chọn."));
  $("sample-users").replaceChildren(el("option", "", "Chưa có user của run đang chọn")); $("sample-users").firstChild.value = "";
  $("recommend-results").className = "results empty";
  $("recommend-results").replaceChildren(el("p", "muted", "Model chưa sẵn sàng. Không hiển thị kết quả của run trước."));
  $("recommend-meta").textContent = "Đợi model hợp lệ cho run đã chọn.";
  $("updated-at").textContent = "Chưa có dữ liệu của run này"; $("quality-metrics").replaceChildren(el("p", "muted", "Chưa có chỉ số của run này.")); $("quality-scope").textContent = "Chưa có báo cáo"; $("quality-protocol").textContent = "Đợi mô hình hợp lệ.";
  state.sessionAvailable = false; state.sessionEvents = []; shop.cart.clear(); renderCart();
  for (const button of document.querySelectorAll(".session-action, #session-recommend, #session-add")) button.disabled = true;
  $("session-results").replaceChildren(el("p", "muted", "Model chưa sẵn sàng."));
  $("session-metrics").replaceChildren(el("p", "muted", "Chưa có đánh giá theo phiên của run này."));
  renderSessionContext(); chart();
}
async function get(path) {
  const response = await fetch(path, { cache: "no-store" });
  const body = await response.json();
  if (!response.ok) { const error = new Error(typeof body.detail === "string" ? body.detail : "Run hiện tại chưa có model bundle hợp lệ. Hãy chạy pipeline và thử cập nhật lại."); error.status = response.status; throw error; }
  return body;
}
function first(source, keys) { for (const key of keys) { if (typeof source?.[key] === "number") return source[key]; } return null; }
function showHealth(health) {
  const ready = health.model?.ready === true;
  $("health-dot").className = `status-dot ${ready ? "ok" : "bad"}`;
  $("health-text").textContent = !ready ? "Chưa sẵn sàng" : health.status === "degraded" ? "Sẵn sàng · có giới hạn" : "Sẵn sàng phục vụ";
  $("dependencies").replaceChildren();
  const model = el("span", `dependency ${ready ? "" : "bad"}`, `Model ${ready ? "sẵn sàng" : "chưa có"}`);
  $("dependencies").append(model);
  for (const [name, value] of Object.entries(health.dependencies || {})) {
    const label = !value.enabled ? "không bật" : value.status === "ok" ? "kết nối" : "tạm mất kết nối";
    $("dependencies").append(el("span", `dependency ${!value.enabled ? "off" : value.status === "ok" ? "" : "bad"}`, `${name === "mongodb" ? "MongoDB" : "Redis"} · ${label}`));
  }
  if (health.model?.stale) notice("Bản cập nhật model cùng run chưa hợp lệ. API đang giữ bản đã xác thực trước đó; xem phiên bản đang phục vụ bên dưới.");
  else if (!ready) notice("Chưa có model bundle dùng được cho run đã chọn. Liveness vẫn hoạt động; hãy chạy pipeline Phase 1–7 để tạo model và KPI.");
  else if (health.status === "degraded") notice("Một dịch vụ online đang không khả dụng. API vẫn dùng model bundle và lịch sử cục bộ của đúng run; lịch sử mới trong dịch vụ bị gián đoạn có thể chưa được đồng bộ.");
  else notice(null);
}
function showKpis(data) {
  const kpis = data.kpis || {};
  const counts = kpis.event_counts || kpis.events || kpis.counts_by_event || {};
  const values = { view: first(counts, ["view", "page_view"]), addtocart: first(counts, ["addtocart", "add_to_cart"]), transaction: first(counts, ["transaction", "purchase"]), review: first(counts, ["review"]) };
  $("kpi-events").textContent = format(first(kpis, ["silver_events", "total_events", "event_count", "clean_events", "clean_event_count", "total_rows", "silver_rows"]));
  $("kpi-users").textContent = format(first(kpis, ["visitors", "unique_visitors", "distinct_visitors", "unique_users", "visitor_count"]));
  $("kpi-items").textContent = format(first(kpis, ["items", "unique_items", "distinct_items", "item_count"]));
  const ratio = first(kpis, ["purchase_per_view", "event_purchase_per_view"]) ?? (values.view > 0 && values.transaction !== null ? values.transaction / values.view : null);
  $("kpi-ratio").textContent = percent(ratio); $("data-note").textContent = data.kpis ? "Nguồn: events.csv · đã làm sạch và khử trùng. Tên, hình minh họa không tham gia huấn luyện hay chấm điểm." : "Chưa đọc được KPI hợp lệ của run hiện tại. Dashboard không tự điền số đếm.";
  $("event-bars").replaceChildren();
  if (Object.values(values).every((value) => value === null)) { $("event-bars").append(el("p", "muted", "KPI của run chưa được xuất. Dashboard không điền số liệu giả.")); return; }
  const max = Math.max(...Object.values(values).map((value) => value || 0), 1);
  const labels = [["view", "Xem sản phẩm", "#377f65"], ["addtocart", "Thêm giỏ hàng", "#8eaf72"], ["transaction", "Mua hàng", "#c5dc91"], ["review", "Đánh giá", "#c5cec5"]];
  for (const [key, label, color] of labels) {
    const value = values[key]; if (value === null) continue;
    const row = el("div", "event-row"); const name = el("div", "event-name"); const dot = el("span"); dot.style.background = color; name.append(dot, document.createTextNode(label));
    const track = el("div", "bar-track"); const fill = el("div", "bar-fill"); fill.style.background = color; fill.style.width = `${value / max * 100}%`; track.append(fill); row.append(name, track, el("div", "event-number", format(value))); $("event-bars").append(row);
  }
  const visitorRatio = first(kpis, ["visitor_purchase_conversion"]);
  if (visitorRatio !== null) $("event-bars").append(el("p", "panel-note", `Visitor có mua / tổng visitor quan sát: ${percent(visitorRatio)} · đếm visitor phân biệt.`));
}
function showModel(data) {
  const policy = data.model_metrics?.selected_policy;
  $("model-algorithm").textContent = data.algorithm === "als_popularity_hybrid" && policy?.als_weight === 1 ? `ALS + dự phòng ${policy.popularity_window_days || "toàn lịch sử"}${policy.popularity_window_days ? " ngày" : ""}` : ({graph_session:"Graph · Item–Item + Session Transition",als_popularity_hybrid:"ALS + độ phổ biến", popularity_recent:"Độ phổ biến theo thời gian", item_knn_hybrid:"Item-item + dự phòng"})[data.algorithm] || "Chưa xác định";
  $("model-version").textContent = data.model_version;
  $("trained-at").textContent = data.trained_at_utc;
  $("metric-model-version").textContent = data.metric_model_version || "Chưa công bố";
  $("exported-users").textContent = data.algorithm === "graph_session" ? "Graph theo ngữ cảnh · tính khi có yêu cầu" : format(data.exported_recommendation_users);
  const metrics = data.model_metrics?.heldout_test || data.model_metrics?.test || data.model_metrics;
  $("model-metrics").replaceChildren();
  for (const [keys, label] of [[["precision_at_k", "precision@k"], "Precision@K"], [["recall_at_k", "recall@k"], "Recall@K"], [["ndcg_at_k", "ndcg@k"], "NDCG@K"], [["hit_rate_at_k"], "HitRate@K"]]) {
    const value = first(metrics, keys); const node = el("div", "metric-small"); node.append(el("span", "", label.replace("@K", `@${metrics?.k || data.model_metrics?.top_k || 10}`)), el("strong", "", percent(value))); $("model-metrics").append(node);
  }
  if (metrics?.population_eligible_users && metrics.population_eligible_users > metrics.eligible_users) {
    const scope = el("p", "muted", `Đánh giá ${format(metrics.eligible_users)} / ${format(metrics.population_eligible_users)} user test đủ điều kiện, chọn bằng hash cố định; giữ cả cold-start. ${data.algorithm === "item_knn_hybrid" ? "Đọc full lịch sử; đồ thị giữ tối đa 20 sản phẩm gần nhất/user." : "Huấn luyện dùng đầy đủ dữ liệu trong mỗi khoảng thời gian."}`);
    scope.style.gridColumn = "1 / -1"; $("model-metrics").append(scope);
  }
  const comparison = data.model_metrics?.quality_comparison;
  if (comparison?.popularity_baseline) {
    const baseline = comparison.popularity_baseline;
    const old = comparison.previous_reported_als;
    const note = el("p", "muted", `Cùng cohort/K: baseline phổ biến P ${percent(baseline.precision_at_k)} · R ${percent(baseline.recall_at_k)} · NDCG ${percent(baseline.ndcg_at_k)}. ALS trước: P ${percent(old?.precision_at_k)} · R ${percent(old?.recall_at_k)} · NDCG ${percent(old?.ndcg_at_k)}.`);
    note.style.gridColumn = "1 / -1"; $("model-metrics").append(note);
  }
  const secondary = data.model_metrics?.secondary_test;
  if (secondary?.selected && secondary?.popularity_baseline) {
    const model = secondary.selected, base = secondary.popularity_baseline;
    const note = el("p", "muted", `Kiểm tra phụ ${format(model.eligible_users)} user không trùng cohort chính: P ${percent(model.precision_at_k)} / baseline ${percent(base.precision_at_k)}; R ${percent(model.recall_at_k)} / ${percent(base.recall_at_k)}; NDCG ${percent(model.ndcg_at_k)} / ${percent(base.ndcg_at_k)}. Cùng giai đoạn lịch sử; chưa phải test ở tương lai mới.`);
    note.style.gridColumn = "1 / -1"; $("model-metrics").append(note);
    const keys = ["precision_at_k", "recall_at_k", "ndcg_at_k"];
    const primaryCertain = keys.every(key => (data.model_metrics?.paired_primary_bootstrap_95?.[key]?.percentile_95_interval?.[0] ?? -Infinity) > 0);
    const secondaryCertain = keys.every(key => (secondary.paired_user_bootstrap_95?.[key]?.percentile_95_interval?.[0] ?? -Infinity) > 0);
    const warning = el("p", "muted", `${primaryCertain ? "Cohort chính: khoảng tin cậy mức tăng dương cho cả ba metric." : "Cohort chính ít hit; khoảng tin cậy của mức tăng còn chứa 0."} ${secondaryCertain ? "Cohort phụ có khoảng tin cậy dương cho cả ba metric." : "Cohort phụ chưa xác nhận mức tăng chắc chắn cho cả ba metric."}`);
    warning.style.gridColumn = "1 / -1"; $("model-metrics").append(warning);
  }
  const staticExperiment = data.model_metrics?.static_upgrade_experiment;
  if (staticExperiment?.promoted === false && staticExperiment.selected) {
    const candidate = staticExperiment.selected;
    const note = el("p", "muted", `Giữ mô hình static hiện tại: ứng viên item-item có trọng số chưa đạt điều kiện thay thế. Kết quả ứng viên (không phải model đang phục vụ): P ${percent(candidate.precision_at_k)} · R ${percent(candidate.recall_at_k)} · NDCG ${percent(candidate.ndcg_at_k)}.`);
    note.style.gridColumn = "1 / -1"; $("model-metrics").append(note);
  }
  if (data.metric_model_version && data.metric_model_version !== data.model_version) $("model-metrics").title = `Metric của model đánh giá ${data.metric_model_version}; model phục vụ được refit sau đánh giá.`;
}
async function refresh() {
  $("refresh").disabled = true;
  try {
    const healthResponse = await fetch("/health/ready", { cache: "no-store" }); const health = await healthResponse.json(); showHealth(health);
    if (!health.model?.ready) { clearModelView(); $("run-id").textContent = health.model?.run_id || "chưa chọn"; return; }
    const data = await get("/api/metrics");
    if (state.run !== data.run_id || state.version !== data.model_version) { state.latencies = []; chart(); $("recommend-results").replaceChildren(el("p", "muted", "Model đã cập nhật. Chọn user để lấy gợi ý của đúng phiên bản này.")); $("recommend-meta").textContent = "Mỗi kết quả luôn ghi rõ run và model version."; }
    const changed = state.run !== data.run_id || state.version !== data.model_version;
    state.run = data.run_id; state.version = data.model_version;
    if (changed) { state.sessionEvents = []; shop.cart.clear(); renderCart(); renderSessionContext(); $("session-results").replaceChildren(el("p", "muted", "Phiên bản model đã cập nhật. Hãy lấy gợi ý mới.")); }
    $("run-id").textContent = data.run_id; showKpis(data); showModel(data); showSessionMetrics(data);
    $("updated-at").textContent = `Cập nhật ${new Date().toLocaleTimeString("vi-VN")}`;
    const users = await get("/api/users?limit=100");
    if (users.run_id !== state.run || users.model_version !== state.version) { notice("Run vừa thay đổi trong lúc tải. Bấm cập nhật để xem một phiên bản thống nhất."); return; }
    $("sample-users").replaceChildren(el("option", "", "Chọn một người dùng…")); $("sample-users").firstChild.value = "";
    for (const id of users.user_ids) { const option = el("option", "", `Visitor ${id}`); option.value = String(id); $("sample-users").append(option); }
    if (!$("visitor-id").value && users.user_ids.length) { $("visitor-id").value = String(users.user_ids[0]); $("sample-users").value = String(users.user_ids[0]); }
  } catch (error) { notice(`Không tải được dữ liệu: ${error.message}`); $("health-dot").className = "status-dot bad"; $("health-text").textContent = "Mất kết nối · xem thời điểm tải"; } finally { $("refresh").disabled = false; }
}
function chart() {
  const canvas = $("latency-chart"); const size = canvas.getBoundingClientRect(); const dpr = window.devicePixelRatio || 1; canvas.width = Math.max(1, size.width * dpr); canvas.height = 140 * dpr; const ctx = canvas.getContext("2d"); ctx.scale(dpr, dpr);
  const values = state.latencies; $("chart-empty").hidden = !!values.length; if (!values.length) { $("latency-current").replaceChildren(document.createTextNode("—"), el("small", "", " ms")); return; }
  $("latency-current").replaceChildren(document.createTextNode(values.at(-1).toFixed(1)), el("small", "", " ms"));
  const width = size.width; const left = 37; const right = width - 8; const top = 12; const bottom = 113; const peak = Math.max(...values, 1) * 1.16;
  ctx.font = "9px system-ui"; ctx.strokeStyle = "#eaf0eb"; ctx.fillStyle = "#99aaa1"; ctx.lineWidth = 1;
  for (let i = 0; i < 3; i++) { const y = top + (bottom - top) * i / 2; ctx.beginPath(); ctx.moveTo(left, y); ctx.lineTo(right, y); ctx.stroke(); ctx.fillText((peak * (1 - i / 2)).toFixed(0), 0, y + 3); }
  const point = (v, i) => [values.length === 1 ? (left + right) / 2 : left + (right - left) * i / (values.length - 1), bottom - v / peak * (bottom - top)];
  ctx.beginPath(); values.forEach((v, i) => { const [x, y] = point(v, i); if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y); }); ctx.strokeStyle = "#2b805a"; ctx.lineWidth = 2; ctx.stroke();
  values.forEach((v, i) => { const [x, y] = point(v, i); ctx.beginPath(); ctx.arc(x, y, 3, 0, Math.PI * 2); ctx.fillStyle = "#fff"; ctx.fill(); ctx.strokeStyle = "#2b805a"; ctx.stroke(); });
  ctx.fillStyle = "#a1afa7"; ctx.fillText(`Truy vấn 1`, left, 136); if (values.length > 1) ctx.fillText(`Truy vấn ${values.length}`, Math.max(left + 65, right - 56), 136);
}
async function recommend(event) {
  event.preventDefault(); $("recommend-error").hidden = true; const input = $("visitor-id").value.trim();
  if (!/^[0-9]+$/.test(input) || input.length > 10 || Number(input) > 2147483647) { $("recommend-error").textContent = "Visitor ID phải là số nguyên từ 0 đến 2.147.483.647."; $("recommend-error").hidden = false; return; }
  $("recommend-button").disabled = true; const started = performance.now();
  try {
    const data = await get(`/api/recommendations/${encodeURIComponent(input)}?k=${$("top-k").value}`); const elapsed = performance.now() - started;
    if (data.run_id !== state.run || data.model_version !== state.version) {
      await refresh();
      if (data.run_id !== state.run || data.model_version !== state.version) throw new Error("Run thay đổi trong lúc truy vấn. Hãy gửi lại để lấy đúng model đang hiển thị.");
    }
    state.latencies.push(elapsed); state.latencies = state.latencies.slice(-20); chart();
    const source = data.source === "popular_fallback" ? "Baseline phổ biến · user chưa có gợi ý xuất sẵn" : data.source === "neighborhood_bundle" ? "Sản phẩm liên quan · có dự phòng phổ biến và lọc lịch sử" : data.source === "popularity_bundle" ? "Độ phổ biến đã chọn bằng validation · có lọc lịch sử" : (data.source === "hybrid_bundle" ? "ALS với dự phòng phổ biến · có lọc lịch sử" : "ALS cá nhân hóa · có thể bổ sung baseline khi thiếu ứng viên");
    $("recommend-meta").textContent = `${source} · ${data.returned} gợi ý · đã lọc ${format(data.seen_items_excluded)} sản phẩm từng tương tác.`; $("recommend-meta").title = `${data.run_id} / ${data.model_version} · API ${data.request_ms.toFixed(1)} ms`;
    if (data.warnings?.length || data.model_stale) { $("recommend-error").textContent = data.model_stale ? "Đang dùng bản model hợp lệ trước đó của cùng run vì bản cập nhật chưa hợp lệ." : "Dịch vụ lịch sử online tạm gián đoạn; đã dùng lịch sử còn khả dụng của cùng run."; $("recommend-error").hidden = false; }
    $("recommend-results").className = data.items.length ? "results" : "results empty"; $("recommend-results").replaceChildren();
    if (!data.items.length) $("recommend-results").append(el("strong", "", "Không còn ứng viên chưa tương tác trong tập sản phẩm đã xuất."));
    data.items.forEach((item, index) => $("recommend-results").append(productCard(item, index)));
  } catch (error) { $("recommend-error").textContent = error.message; $("recommend-error").hidden = false; } finally { $("recommend-button").disabled = false; }
}
function productCard(item, index, options = {}) {
  const product = describeItem(item.itemid), discovery = options.discovery === true;
  const card=el("article","result-card");card.dataset.itemid=String(item.itemid);
  const visual=el("div","product-visual"),open=el("button","product-open");open.type="button";
  open.setAttribute("aria-label",`Xem chi tiết Item ${item.itemid} · ${product.name}`);open.disabled=state.sessionBusy;
  const image=el("img");image.src=product.image;image.alt=`${product.name} · hình minh họa`;image.width=240;image.height=180;image.loading=index<5?"eager":"lazy";image.decoding="async";
  open.append(image);open.addEventListener("click",()=>openProduct(item,index,discovery));visual.append(open);
  if(!discovery)visual.append(el("span","result-rank",`#${index+1}`));
  visual.append(el("span","illustration-pill","Minh họa"));
  const body=el("div","product-body");body.append(el("p","product-category",product.group));
  const heading=el("h3"),name=el("button","product-name-button",product.name);name.type="button";name.disabled=state.sessionBusy;name.addEventListener("click",()=>openProduct(item,index,discovery));heading.append(name);body.append(heading);
  const price=el("div","price-row");price.append(el("strong","product-price",money(product.priceVnd)),el("span","price-label","Giá minh họa"));body.append(price,el("p","product-id",`Item ID · ${item.itemid}`));
  if(Number.isFinite(item.score)){const score=el("details","score-details");score.append(el("summary","","Điểm xếp hạng"),el("p","",`Score ${Number(item.score).toPrecision(4)} · không phải xác suất mua.`));body.append(score);}
  const actions=el("div","session-actions");
  const view=el("button","product-view","Xem");view.type="button";view.disabled=state.sessionBusy;view.setAttribute("aria-label",`Xem Item ${item.itemid}`);view.addEventListener("click",()=>openProduct(item,index,discovery));
  const add=el("button","session-action","+ Giỏ");add.type="button";add.disabled=!state.sessionAvailable||state.sessionBusy;add.setAttribute("aria-label",`+ Giỏ Item ${item.itemid}`);add.addEventListener("click",()=>addToCart(item.itemid));
  const buy=el("button","session-action product-buy","Mua ngay");buy.type="button";buy.disabled=!state.sessionAvailable||state.sessionBusy;
  buy.setAttribute("aria-label",`Mua ngay Item ${item.itemid} (mô phỏng)`);buy.title="Ghi nhận hành vi mua mô phỏng và cập nhật gợi ý; không tạo đơn hàng hay thanh toán.";
  buy.addEventListener("click",()=>simulatePurchase(item.itemid));
  actions.append(view,add,buy,el("small","purchase-note","Mua mô phỏng · cập nhật gợi ý"));card.append(visual,body,actions);return card;
}
function renderSessionContext() {
  const target = $("session-context"); target.replaceChildren();
  const names = {view: "Xem", addtocart: "+ Giỏ", transaction: "Mua thử"};
  const count = state.sessionEvents.length;
  if (!count) { target.append(el("span", "", "Phiên mới · chưa có tương tác. Bắt đầu bằng các sản phẩm phổ biến.")); return; }
  target.append(el("span", "context-label", `PHIÊN NÀY · ${count} TƯƠNG TÁC`));
  state.sessionEvents.slice(-5).forEach((row, index) => {
    if (index) target.append(el("span", "context-arrow", "→"));
    const product = window.ProductCatalog?.forItem(row.itemid);
    const chip = el("span", "context-chip", `${names[row.event]} · ${product?.name || "Sản phẩm"}`);
    chip.append(el("small", "", `ID ${row.itemid}`)); target.append(chip);
  });
  if (count > 5) target.append(el("small", "", "Hiện 5 tương tác gần nhất"));
}
function showSessionMetrics(data) {
  const session = data.session_model;
  state.sessionAvailable = session?.available === true;
  $("session-recommend").disabled = !state.sessionAvailable;
  $("session-add").disabled = !state.sessionAvailable;
  document.querySelectorAll(".session-action").forEach(button => { button.disabled = !state.sessionAvailable; });
  const target = $("session-metrics"), quality = $("quality-metrics"); target.replaceChildren(); quality.replaceChildren();
  if (!session) { const message = "Chưa có kết quả đánh giá theo phiên của run này."; target.append(el("p", "muted", message)); quality.append(el("p", "muted", message)); $("quality-scope").textContent = "Chưa có báo cáo"; $("quality-protocol").textContent = "Chưa có chỉ số hợp lệ của mô hình đang chọn."; return; }
  if (!session.available) target.append(el("p", "inline-error", "Mô hình theo phiên chưa khả dụng."));
  const metrics = session.metrics || {};
  const nested = value => value?.metrics || value;
  const selected = nested(metrics.selected);
  const served = nested(metrics.served || (metrics.selected_model_promoted === true ? metrics.selected : metrics.current_item_item));
  const baseline = nested(metrics.popularity || metrics.popularity_baseline);
  if (!selected || !served) { target.append(el("p", "muted", "Chưa công bố chỉ số cho mô hình đang phục vụ.")); quality.append(el("p", "muted", "Chưa có chỉ số đã xác thực.")); $("quality-scope").textContent = "Chưa có báo cáo"; $("quality-protocol").textContent = "Chưa có chỉ số hợp lệ của mô hình đang chọn."; return; }
  const remaining = metrics.protocol_kind === "remaining_session", k = served.k || metrics.k || 10;
  const queries = served.eligible_queries ?? served.queries ?? served.eligible_users ?? metrics.queries;
  $("quality-scope").textContent = `${format(queries)} lượt dự đoán · Top ${k}`;
  $("quality-protocol").textContent = remaining
    ? `Chấm các món mới còn lại trong cùng phiên. Số đánh giá cố định ở Top ${k}; chọn 5 hoặc 20 sản phẩm bên dưới không đổi báo cáo.`
    : `Chấm món mới tiếp theo trong phiên. Chỉ số đã lưu ở Top ${k}, không được tính từ các lần bấm thử.`;
  const definitions = [
    ["precision_at_k", `Precision@${k}`, `Trong ${k} gợi ý, có bao nhiêu món trùng đáp án.`, ""],
    ["recall_at_k", `Recall@${k}`, "Tìm được bao nhiêu phần trong các món đúng cần gợi ý.", "featured"],
    ["ndcg_at_k", `NDCG@${k}`, "Món đúng càng nằm gần đầu danh sách, điểm càng cao.", ""],
    ["hit_rate_at_k", `HitRate@${k}`, "Tỷ lệ lượt dự đoán có ít nhất một món đúng.", ""]
  ];
  definitions.forEach(([key, label, description, cls], index) => {
    const card = el("article", `quality-card ${cls}`), heading = el("div", "quality-label", label);
    heading.append(el("span", "quality-index", `0${index + 1}`));
    card.append(heading, el("strong", "quality-value", percent(served[key])), el("p", "", description));
    if (baseline) card.append(el("div", "quality-baseline", `Mốc so sánh phổ biến: ${percent(baseline[key])}`));
    quality.append(card);
  });
  const rows = data.algorithm === "graph_session"
    ? [["Graph đang phục vụ", served], ["Phổ biến", baseline], ["ALS đối chứng (cùng cohort)", metrics.als_comparison?.metrics]]
    : metrics.selected_model_promoted === true
    ? [["Mô hình đang phục vụ", served], ["Bản trước", nested(metrics.current_item_item)], ["Phổ biến", baseline]]
    : [["Mô hình đang phục vụ", served], ["Ứng viên chưa được chọn", selected], ["Phổ biến", baseline]];
  function appendTable(parent, rows) {
    const table = el("table", "session-metric-table"), caption = el("caption", "sr-only", `So sánh các mô hình ở Top ${k}`), head = el("thead"), header = el("tr");
    table.append(caption);
    for (const label of ["Mô hình", `Precision@${k}`, `Recall@${k}`, `NDCG@${k}`, `HitRate@${k}`]) { const cell = el("th", "", label); cell.scope = "col"; header.append(cell); }
    head.append(header); table.append(head); const body = el("tbody");
    rows.forEach(([label, values]) => { if (!values) return; const row = el("tr"), name = el("th", "", label); name.scope = "row"; row.append(name); for (const key of ["precision_at_k", "recall_at_k", "ndcg_at_k", "hit_rate_at_k"]) row.append(el("td", "", percent(values[key]))); body.append(row); });
    table.append(body); const wrap = el("div", "table-scroll"); wrap.append(table); parent.append(wrap);
  }
  target.append(el("h3", "", remaining ? "Phép chấm chính · các món mới còn lại trong phiên" : "Phép chấm · món mới tiếp theo"));
  target.append(el("p", "panel-note", `${format(queries)} lượt dự đoán. Các mô hình dùng cùng ngữ cảnh, cùng đáp án và cùng Top ${k}. Đồ thị chỉ học từ dữ liệu trước mốc kiểm tra.`));
  appendTable(target, rows);
  target.append(el("p", "method-note", remaining
    ? `Ví dụ: sau khi xem A, người dùng xem tiếp B và C. Nếu Top ${k} có B thì tìm được 1/2 đáp án: Recall 50%, Precision ${(100/k).toFixed(0)}%. NDCG còn xét vị trí của B. Bảng trên là trung bình trên toàn bộ lượt dự đoán.`
    : `Mỗi lượt có một đáp án là món mới tiếp theo; Recall là tỷ lệ lượt có đáp án trong danh sách, Precision@${k} tối đa ${(100/k).toFixed(0)}%.`));
  const intervals = metrics.paired_visitor_cluster_bootstrap_95?.metrics;
  if (intervals) {
    const keys = [["precision_at_k", "Precision"], ["recall_at_k", "Recall"], ["ndcg_at_k", "NDCG"]];
    const confirmed = keys.filter(([key]) => (intervals[key]?.percentile_95_interval?.[0] ?? -Infinity) > 0).map(([, label]) => label);
    target.append(el("p", "panel-note", confirmed.length === 3 ? "Khoảng tin cậy 95% của mức tăng so với bản trước dương ở cả ba chỉ số." : `So với bản trước, ${confirmed.length ? `chỉ ${confirmed.join(", ")}` : "chưa có chỉ số nào"} có khoảng tin cậy 95% của mức tăng hoàn toàn dương. Chưa thể khẳng định cải thiện chắc chắn ở cả ba chỉ số.`));
  }
  if (data.algorithm === "graph_session") target.append(el("p", "panel-note", `Phiên bản Graph: ${data.model_version}. Phiên bản chấm điểm: ${data.metric_model_version}. ALS chỉ là đối chứng khi có số liệu; không tham gia phục vụ.`));
  if (metrics.test_reuse_disclosure) target.append(el("p", "panel-note", "Giới hạn: tập kiểm tra lịch sử đã được xem trong quá trình cải tiến. Cần thêm dữ liệu ở giai đoạn tương lai để xác nhận khả năng tổng quát hóa."));
  const detail = el("details", "method-details"); detail.append(el("summary", "", "Phép chấm bổ sung & thông tin đối chiếu"));
  if (remaining && metrics.next_item?.served) {
    detail.append(el("h3", "", "Chỉ dự đoán một món mới tiếp theo"));
    appendTable(detail, [["Mô hình đang phục vụ", metrics.next_item.served]]);
    detail.append(el("p", "panel-note", "Đây là một mục tiêu khác với phép chấm chính. Không so trực tiếp hai mức Recall hoặc Precision của hai phép chấm."));
  }
  const labels = metrics.label_profile || {};
  if (remaining) detail.append(el("p", "panel-note", `${format(labels.queries_with_multiple_labels || 0)} / ${format(labels.queries || queries)} lượt có nhiều hơn một món đúng. Sự kiện mô phỏng trong dashboard không tham gia tập kiểm tra.`));
  detail.append(el("p", "panel-note mono", `Model đánh giá: ${session.metric_model_version || session.model_version}. Cấu hình: ${metrics.served_policy || metrics.selected_configuration?.name || "item-item"}.`));
  if (metrics.limitations) detail.append(el("p", "panel-note", Array.isArray(metrics.limitations) ? metrics.limitations.join(" ") : String(metrics.limitations)));
  target.append(detail);
}
async function post(path, payload) {
  const response = await fetch(path, {method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify(payload), cache: "no-store"});
  const body = await response.json();
  if (!response.ok) throw new Error(typeof body.detail === "string" ? body.detail : "Yêu cầu phiên không hợp lệ hoặc model theo phiên chưa sẵn sàng.");
  return body;
}
async function recommendSession(event) {
  if (event) event.preventDefault();
  if (state.sessionBusy) return false;
  $("session-error").hidden = true;
  const visitor = $("session-visitor").value.trim();
  if (visitor && (!/^[0-9]+$/.test(visitor) || visitor.length > 10 || Number(visitor) > 2147483647)) { $("session-error").textContent = "Visitor ID phải là số nguyên từ 0 đến 2.147.483.647 hoặc để trống."; $("session-error").hidden = false; return false; }
  state.sessionBusy = true; setShopBusy(); $("session-results").setAttribute("aria-busy","true");
  for (const button of document.querySelectorAll(".session-action, #session-recommend, #session-add, #session-clear")) button.disabled = true;
  const started = performance.now();
  try {
    const payload = {events: state.sessionEvents.slice(), k: Number($("session-k").value)};
    if (visitor) payload.visitorid = Number(visitor);
    const data = await post("/api/session/recommendations", payload);
    if (data.run_id !== state.run || data.model_version !== state.version) { await refresh(); throw new Error("Model vừa cập nhật; phiên đã được xóa để tải lại đúng phiên bản."); }
    state.latencies.push(performance.now() - started); state.latencies = state.latencies.slice(-20); chart();
    const sessionSources = {
      session_item_item: "Theo ngữ cảnh phiên",
      session_item_item_transition: "Theo phiên · item-item + chuyển tiếp",
      session_strength_fusion: "Theo phiên · kết hợp độ mạnh item-item và chuyển tiếp"
    };
    const source = sessionSources[data.source] || (data.source === "popular_fallback" ? "Dự phòng phổ biến · chưa có seed phù hợp" : "Gợi ý theo phiên");
    $("session-meta").textContent = `${data.source === "popular_fallback" ? "Gợi ý phổ biến · chưa có ngữ cảnh phù hợp" : "Gợi ý theo hành vi trong phiên"} · ${data.returned} sản phẩm · đã lọc ${format(data.seen_items_excluded)} món từng tương tác.`;
    $("session-debug").textContent = `${source} · ${data.context_events} sự kiện / ${data.context_items} ID ngữ cảnh · ${data.matched_seeds} nhánh tín hiệu · API ${data.request_ms.toFixed(1)} ms · ${data.run_id} / ${data.session_model_version}`;
    $("session-results").className = data.items.length ? "results" : "results empty"; $("session-results").replaceChildren();
    data.items.forEach((item, index) => $("session-results").append(productCard(item, index)));
    if (!data.items.length) $("session-results").append(el("p", "muted", "Không còn ứng viên mới sau khi lọc lịch sử."));
    if (data.warnings?.length || data.model_stale) { $("session-error").textContent = data.model_stale ? "Model đang dùng bản hợp lệ trước đó của cùng run." : "Lịch sử online có thể chưa đầy đủ do một dịch vụ gián đoạn."; $("session-error").hidden = false; }
    return true;
  } catch (error) { $("session-error").textContent = error.message; $("session-error").hidden = false; return false; }
  finally { $("session-results").setAttribute("aria-busy","false"); state.sessionBusy = false; document.querySelectorAll(".product-open, .product-name-button, .product-view").forEach(button => {button.disabled = false;}); for (const button of document.querySelectorAll(".session-action, #session-recommend, #session-add")) button.disabled = !state.sessionAvailable; $("session-clear").disabled = false; }
}
async function recordSessionEvent(item, event) {
  if (state.sessionBusy || !state.sessionAvailable) return false;
  const now = Date.now();
  if (state.sessionEvents.length && now - state.sessionEvents.at(-1).timestamp > 30 * 60 * 1000) state.sessionEvents = [];
  state.sessionEvents.push({itemid: item, event, timestamp: now}); state.sessionEvents = state.sessionEvents.slice(-100);
  renderSessionContext(); return await recommendSession();
}
function clearSession() {
  if (state.sessionBusy) return;
  state.sessionEvents = []; shop.cart.clear(); renderCart(); $("session-visitor").value = ""; renderSessionContext();
  $("session-results").className = "results empty"; $("session-results").replaceChildren(el("p", "muted", "Đã xóa ngữ cảnh. Lấy gợi ý để bắt đầu một phiên mới."));
  $("session-meta").textContent = "Phiên ẩn danh mới · không kế thừa tương tác của phiên trước."; $("session-debug").textContent = "Chưa có yêu cầu của phiên này."; $("session-error").hidden = true; recommendSession();
}
$("session-form").addEventListener("submit", recommendSession);
$("session-event-form").addEventListener("submit", async event => { event.preventDefault(); const value = $("session-item").value.trim(); if (!/^[0-9]+$/.test(value) || value.length > 10 || Number(value) > 2147483647) { $("session-error").textContent = "Item ID phải là số nguyên từ 0 đến 2.147.483.647."; $("session-error").hidden = false; return; } await recordSessionEvent(Number(value), $("session-event").value); });
$("session-clear").addEventListener("click", clearSession);
$("session-visitor").addEventListener("change", () => { state.sessionEvents = []; shop.cart.clear(); renderCart(); renderSessionContext(); $("session-results").replaceChildren(el("p", "muted", "Đã đổi visitor. Lấy gợi ý để bắt đầu ngữ cảnh mới.")); });
$("sample-users").addEventListener("change", (event) => { if (event.target.value) $("visitor-id").value = event.target.value; });
$("refresh").addEventListener("click", async () => { await refresh(); if (state.sessionAvailable) await recommendSession(); }); $("recommend-form").addEventListener("submit", recommend); window.addEventListener("resize", chart);
$("model-section").addEventListener("toggle", () => { if ($("model-section").open) chart(); });
$("open-evaluation").addEventListener("click", () => { $("session-evaluation").open = true; });
// Storefront state belongs only to this tab; it does not create orders.
const shop = {query:"", category:"Tất cả", limit:12, cart:new Map(), detail:null, toastTimer:null};
const money = value => new Intl.NumberFormat("vi-VN", {style:"currency",currency:"VND",maximumFractionDigits:0}).format(value);
const describeItem = id => window.ShopCatalog.describe(id);
const normalizeSearch = value => String(value).normalize("NFD").replace(/[\u0300-\u036f]/g, "").replace(/đ/g, "d").replace(/Đ/g, "D").toLowerCase().trim();
function toast(message) {
  $("shop-toast").textContent = message; $("shop-toast").hidden = false;
  clearTimeout(shop.toastTimer); shop.toastTimer = setTimeout(() => { $("shop-toast").hidden = true; }, 4000);
}
function renderShop() {
  const categories = $("shop-categories"); categories.replaceChildren();
  for (const category of window.ShopCatalog.groups) {
    const button = el("button", `category-chip${shop.category === category ? " active" : ""}`, category);
    button.type = "button"; button.dataset.category = category; button.setAttribute("aria-pressed", String(shop.category === category));
    button.addEventListener("click", () => { shop.category = category; shop.limit = 12; renderShop(); }); categories.append(button);
  }
  const query = normalizeSearch(shop.query);
  const filtered = window.ShopCatalog.items.filter(id => {
    const item = describeItem(id);
    return (shop.category === "Tất cả" || item.group === shop.category) && (!query || normalizeSearch(`${item.name} ${item.category} ${item.group} ${id}`).includes(query));
  });
  const results = $("shop-results"); results.replaceChildren();
  results.className = filtered.length ? "results shop-grid" : "results empty";
  for (const [index,id] of filtered.slice(0,shop.limit).entries()) results.append(productCard({itemid:id},index,{discovery:true}));
  if (!filtered.length) results.append(el("div","cart-empty","Chưa tìm thấy sản phẩm minh họa phù hợp. Thử tên khác, Item ID hoặc xóa bộ lọc."));
  $("shop-filter-note").textContent = `${Math.min(filtered.length,shop.limit)} / ${filtered.length} sản phẩm${shop.query ? ` cho “${shop.query}”` : ""} · Lọc theo tên và danh mục minh họa; không đổi danh sách Dành cho bạn.`;
  $("shop-more").hidden = filtered.length <= shop.limit;
  $("shop-clear-filters").hidden = !shop.query && shop.category === "Tất cả";
}
function modalOpen(id) {
  const dialog = $(id); if (!dialog.open) dialog.showModal();
}
function modalClose(id) {
  const dialog = $(id); dialog.close();
  requestAnimationFrame(() => { if (!document.activeElement || document.activeElement === document.body) $(id === "cart-dialog" ? "cart-open" : "session-recommend").focus(); });
}
function setShopBusy() {
  document.querySelectorAll(".session-action, #session-recommend, #session-add, #session-clear").forEach(button => { button.disabled = state.sessionBusy || !state.sessionAvailable; });
  document.querySelectorAll(".product-open, .product-name-button, .product-view").forEach(button => { button.disabled = state.sessionBusy; });
}
async function simulatePurchase(id) {
  if (state.sessionBusy || !state.sessionAvailable) { toast("Đợi mô hình sẵn sàng để mua mô phỏng."); return; }
  const updated = await recordSessionEvent(id,"transaction");
  toast(updated
    ? `Đã mua mô phỏng Item ${id}. Gợi ý đã cập nhật; không phát sinh thanh toán.`
    : `Chưa cập nhật được gợi ý cho Item ${id}. Xem thông báo lỗi ở mục Dành cho bạn và thử lại.`);
}
async function addToCart(id) {
  if (state.sessionBusy || !state.sessionAvailable) { toast("Đợi mô hình sẵn sàng để ghi nhận thao tác."); return; }
  const old = shop.cart.get(id) || {itemid:id,quantity:0};
  shop.cart.set(id, {...old,quantity:old.quantity+1}); renderCart();
  toast(`Đã thêm ${describeItem(id).name} vào giỏ mô phỏng.`);
  await recordSessionEvent(id,"addtocart");
}
function renderCart() {
  let count = 0, total = 0; const target = $("cart-items"); target.replaceChildren();
  for (const [id,row] of shop.cart) {
    const product = describeItem(id); count += row.quantity; total += product.priceVnd * row.quantity;
    const line = el("article","cart-row"); line.dataset.itemid = String(id);
    const image = el("img"); image.src = product.image; image.alt = `Minh họa ${product.name}`; image.width = 80; image.height = 80;
    const info = el("div","cart-row-info"); info.append(el("strong","cart-row-name",product.name),el("span","cart-row-id",`Item ID ${id}`),el("span","cart-row-price",`${money(product.priceVnd)} · giá minh họa`));
    const quantity = el("div","cart-quantity");
    const minus = el("button","","−"); minus.type="button"; minus.setAttribute("aria-label",`Giảm số lượng Item ${id}`); minus.disabled=row.quantity<=1;
    minus.addEventListener("click",()=>{ shop.cart.set(id,{...row,quantity:row.quantity-1}); renderCart(); });
    const plus = el("button","","+"); plus.type="button"; plus.setAttribute("aria-label",`Tăng số lượng Item ${id}`);
    plus.addEventListener("click",()=>{ shop.cart.set(id,{...row,quantity:row.quantity+1}); renderCart(); });
    quantity.append(minus,el("span","",String(row.quantity)),plus);
    const remove = el("button","cart-remove","Xóa"); remove.type="button"; remove.setAttribute("aria-label",`Xóa Item ${id} khỏi giỏ`);
    remove.addEventListener("click",()=>{shop.cart.delete(id);renderCart();});
    line.append(image,info,quantity,remove); target.append(line);
  }
  if (!shop.cart.size) target.append(el("div","cart-empty","Giỏ đang trống. Thêm một món để thử trải nghiệm mua sắm và cập nhật gợi ý."));
  $("cart-count").textContent = String(count); $("cart-total").textContent = money(total);
  $("cart-open").setAttribute("aria-label",`Mở giỏ mô phỏng, ${count} sản phẩm`);
  $("cart-reset").disabled = !shop.cart.size;
}
function openProduct(item, index, discovery=false) {
  if (state.sessionBusy) { toast("Đợi cập nhật gợi ý xong rồi mở sản phẩm nhé."); return; }
  const product = describeItem(item.itemid); shop.detail = {...item};
  const target = $("product-detail"); target.replaceChildren();
  const layout=el("div","detail-layout"), visual=el("div","detail-visual"),image=el("img");
  image.src=product.image; image.alt=`Hình minh họa ${product.name}`; image.width=480;image.height=360; visual.append(image,el("span","illustration-pill","Hình minh họa"));
  const info=el("div","detail-info");
  info.append(el("p","product-category",`${product.group} · danh mục minh họa`));
  const title=el("h2","",product.name); title.id="product-detail-name";
  info.append(title,el("p","product-id",`Item ID gốc · ${item.itemid}`));
  const price=el("div","price-row");price.append(el("strong","product-price",money(product.priceVnd)),el("span","price-label","Giá minh họa")); info.append(price);
  info.append(el("p","detail-description","Một sản phẩm trong bộ sưu tập trình bày của dự án. Tên, ảnh, danh mục và giá được gán để minh họa; dữ liệu RetailRocket không cung cấp các thông tin này."));
  const evidence=el("div","detail-evidence");
  evidence.append(el("strong","",discovery?"Khám phá từ ID trong dữ liệu":"Được mô hình gợi ý"));
  evidence.append(el("p","",discovery?"Đây là danh mục khám phá riêng, không phải thứ hạng gợi ý theo phiên.":`Vị trí ${index+1} trong danh sách API trả về. Model xếp hạng bằng các tương tác gắn với ID, không dùng tên hoặc giá minh họa.`));
  if (Number.isFinite(item.score)) evidence.append(el("p","",`Điểm xếp hạng ${Number(item.score).toPrecision(4)} · không phải xác suất mua.`));
  info.append(evidence);
  const actions=el("div","detail-actions");
  const add=el("button","primary session-action","+ Thêm vào giỏ mô phỏng");add.type="button";add.setAttribute("aria-label",`Thêm Item ${item.itemid} vào giỏ mô phỏng`);add.addEventListener("click",()=>addToCart(item.itemid));
  const buy=el("button","secondary session-action","Mua ngay · mô phỏng");buy.type="button";buy.setAttribute("aria-label",`Mua ngay Item ${item.itemid} (mô phỏng)`);buy.addEventListener("click",()=>simulatePurchase(item.itemid));
  actions.append(add,buy);info.append(actions,el("p","panel-note",state.sessionAvailable ? "Mở sản phẩm ghi nhận một lượt xem mô phỏng. Các thao tác chỉ dùng trong tab này, không phát sinh thanh toán." : "Mô hình chưa sẵn sàng nên lượt xem này chưa được ghi nhận. Tên, ảnh và giá đang hiển thị chỉ là minh họa."));
  layout.append(visual,info);target.append(layout);modalOpen("product-dialog");
  if (state.sessionAvailable && !state.sessionBusy) recordSessionEvent(item.itemid,"view");
  setShopBusy();
}
function bindStorefront() {
  $("shop-search-form").addEventListener("submit",event=>{
    event.preventDefault();shop.query=$("shop-search").value.trim();shop.limit=12;renderShop();
    $("shop-section").scrollIntoView({behavior:"instant",block:"start"});
  });
  $("shop-clear-filters").addEventListener("click",()=>{shop.query="";shop.category="Tất cả";shop.limit=12;$("shop-search").value="";renderShop();});
  $("shop-more").addEventListener("click",()=>{shop.limit+=12;renderShop();});
  $("cart-open").addEventListener("click",()=>{renderCart();modalOpen("cart-dialog");});
  $("cart-close").addEventListener("click",()=>modalClose("cart-dialog"));
  $("product-close").addEventListener("click",()=>modalClose("product-dialog"));
  $("cart-reset").addEventListener("click",()=>{shop.cart.clear();renderCart();toast("Đã xóa giỏ mô phỏng. Lịch sử tương tác trong phiên vẫn giữ nguyên.");});
  for (const id of ["cart-dialog","product-dialog"]) {
    $(id).addEventListener("click",event=>{if(event.target!==$(id))return;const rect=$(id).getBoundingClientRect();if(event.clientX<rect.left||event.clientX>rect.right||event.clientY<rect.top||event.clientY>rect.bottom)modalClose(id);});
  }
  renderShop(); renderCart();
}

async function start() { bindStorefront(); await refresh(); if (state.sessionAvailable) await recommendSession(); }
start();
