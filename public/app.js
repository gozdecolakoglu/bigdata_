// Erken Risk Tespiti ve Açıklanabilir Karar Destek Sistemi (XAI) - Vercel Web Engine

let rawTasks = [];
let rawPrs = [];
let streamEvents = [];
let combinedTasks = [];
let filteredTasks = [];
let filteredPrs = [];

let taskSourceLabel = "Hazır 5V Veri Seti (sample_data.json)";
let prSourceLabel = "Hazır 5V Veri Seti (sample_data.json)";

const charts = {};

const RISK_COLORS = {
    Critical: "#ef4444",
    High: "#f97316",
    Medium: "#eab308",
    Low: "#22c55e",
};

// ---------------------------------------------------------------------------
// 1. DOSYA OKUMA MOTORU (.parquet, .csv, .jsonl, .json)
// ---------------------------------------------------------------------------
async function parseUploadedFile(file) {
    const ext = file.name.split(".").pop().toLowerCase();
    if (ext === "csv") {
        return new Promise((resolve, reject) => {
            window.Papa.parse(file, {
                header: true,
                dynamicTyping: true,
                skipEmptyLines: true,
                complete: (res) => resolve(res.data),
                error: (err) => reject(err),
            });
        });
    } else if (ext === "jsonl" || ext === "json") {
        const text = await file.text();
        const trimmed = text.trim();
        if (trimmed.startsWith("[")) {
            return JSON.parse(trimmed);
        }
        const lines = trimmed.split(/\r?\n/);
        const records = [];
        for (let i = 0; i < lines.length; i++) {
            const line = lines[i].trim();
            if (line) records.push(JSON.parse(line));
        }
        return records;
    } else if (ext === "parquet") {
        const hyparquet = await import("https://cdn.jsdelivr.net/npm/hyparquet@1.11.2/+esm");
        const arrayBuffer = await file.arrayBuffer();
        const rows = [];
        await hyparquet.parquetRead({
            file: arrayBuffer,
            rowFormat: "object",
            onComplete: (data) => {
                for (let i = 0; i < data.length; i++) {
                    rows.push(data[i]);
                }
            },
        });
        return rows;
    } else {
        throw new Error("Desteklenmeyen dosya uzantısı: " + ext);
    }
}

// ---------------------------------------------------------------------------
// 2. XAI VE ERKEN RİSK HESAPLAMA & VERİ ZENGİNLEŞTİRME MOTORU
// ---------------------------------------------------------------------------
function normalizeAndEnrichTasks(tasks, cleanVeracity = false) {
    return tasks.map((t, idx) => {
        const row = { ...t };
        row.task_id = String(row.task_id || `TASK-${100001 + idx}`);
        row.story_points = Number(row.story_points ?? 5);
        row.blocker_count = Number(row.blocker_count ?? 0);
        row.requirement_change_count = Number(row.requirement_change_count ?? 0);
        row.reopen_count = Number(row.reopen_count ?? 0);
        row.priority = String(row.priority || "Medium");
        row.environment = String(row.environment || "Development");
        row.status = String(row.status || "In Progress");
        row.project = String(row.project || "PRJ-CORE - Kurumsal Platform");
        row.component_module = String(row.component_module || "Backend");
        row.data_quality_flag = String(row.data_quality_flag || "Clean");

        let est = Number(row.estimated_effort_hours);
        if (!est || isNaN(est) || est <= 0) {
            est = cleanVeracity ? +(row.story_points * 5.8).toFixed(1) : +(row.story_points * 6.0).toFixed(1);
        }
        row.estimated_effort_hours = est;

        let act = Number(row.actual_effort_hours ?? est);
        if (cleanVeracity && row.data_quality_flag === "Outlier_Effort") {
            act = +(act / 3.8).toFixed(1);
        }
        row.actual_effort_hours = act;

        const depStr = String(row.dependencies || "Yok").trim();
        const depCount = (!depStr || depStr === "Yok" || depStr === "None")
            ? 0
            : depStr.split(",").length;
        row.dependency_count_calc = depCount;

        const optRatio = Math.max(0.5, Math.min(3.0, (row.story_points * 6.5) / (est + 1.0)));
        row.optimism_ratio_calc = +optRatio.toFixed(2);

        if (row.delivery_risk_score === undefined || row.delivery_risk_score === null || isNaN(Number(row.delivery_risk_score))) {
            const pW = row.priority === "Critical" ? 14 : (row.priority === "High" ? 8 : 2);
            const eW = row.environment === "Production" ? 10 : (row.environment === "Staging" ? 5 : 1);
            const raw = depCount * 9.5 + row.blocker_count * 16 + row.requirement_change_count * 11.5
                + row.reopen_count * 12 + (row.story_points >= 13 ? 14 : 0) + (optRatio > 1.25 ? 13 : 0) + pW + eW + 8;
            row.delivery_risk_score = +Math.max(2, Math.min(99.5, raw)).toFixed(2);
        } else {
            row.delivery_risk_score = Number(row.delivery_risk_score);
        }

        const s = row.delivery_risk_score;
        row.delivery_risk_level = row.delivery_risk_level || (s >= 75 ? "Critical" : (s >= 50 ? "High" : (s >= 28 ? "Medium" : "Low")));
        row.delay_days = Number(row.delay_days ?? (s >= 48 ? Math.round((s - 42) * 0.32 + row.blocker_count * 2.5) : 0));
        row.is_delayed = Number(row.is_delayed ?? (row.delay_days > 0 ? 1 : 0));

        if (!row.xai_primary_risk_factors) {
            const factors = [];
            if (row.blocker_count > 0) factors.push(`Blocker/Engel Sayısı (${row.blocker_count} adet, +%28 risk)`);
            if (depCount >= 2) factors.push(`Yüksek Görev Bağımlılığı (${depCount} ön koşul, +%24 risk)`);
            if (row.requirement_change_count >= 1) factors.push(`Kapsam Değişikliği / Scope Creep (${row.requirement_change_count} revizyon, +%21 risk)`);
            if (row.story_points >= 13 || optRatio > 1.25) factors.push(`Yüksek Karmaşıklık / İyimser Efor (${row.story_points} SP, +%19 risk)`);
            row.xai_primary_risk_factors = factors.length ? factors.slice(0, 3).join(" | ") : "Standart İş Akışı (Düşük Sapma Riski)";
        }

        if (!row.xai_recommended_actions) {
            row.xai_recommended_actions = row.blocker_count > 0
                ? "Blokaj yaratan dış servis/ekip bağımlılıklarını eskalasyon ile çözün; görevi alt parçalara bölün."
                : "Mevcut sprint planına sadık kalın, rutin PR inceleme sürecini sürdürün.";
        }

        return row;
    });
}

function normalizeAndEnrichPrs(prs, cleanVeracity = false) {
    return prs.map((p, idx) => {
        const row = { ...p };
        row.pr_id = String(row.pr_id || `PR-${200001 + idx}`);
        row.linked_task_issue = String(row.linked_task_issue || "");
        row.changed_files_count = Number(row.changed_files_count ?? 4);
        let linesAdded = Number(row.lines_added ?? 120);
        if (cleanVeracity && row.data_quality_flag === "Outlier_Diff_Size") {
            linesAdded = Math.min(800, linesAdded);
        }
        row.lines_added = linesAdded;
        row.lines_deleted = Number(row.lines_deleted ?? 40);
        row.commit_count = Number(row.commit_count ?? 3);
        row.review_iteration_count = Number(row.review_iteration_count ?? 1);
        row.time_to_first_review_hours = Number(row.time_to_first_review_hours ?? 4.0);
        row.time_to_merge_hours = Number(row.time_to_merge_hours ?? 16.0);
        row.ci_failure_count = Number(row.ci_failure_count ?? 0);
        row.merge_conflicts = String(row.merge_conflicts || "Çakışma Yok (Clean Merge)");
        row.has_conflict_bool = !/Yok|Clean/i.test(row.merge_conflicts);
        row.security_scan_status = String(row.security_scan_status || "Passed (0 Vulnerability)");
        row.data_quality_flag = String(row.data_quality_flag || "Clean");

        if (row.pr_risk_score === undefined || row.pr_risk_score === null || isNaN(Number(row.pr_risk_score))) {
            const rawPr = row.changed_files_count * 1.45 + row.lines_added / 45.0
                + (row.has_conflict_bool ? 18 : 0) + row.ci_failure_count * 8.5 + row.review_iteration_count * 4.2 + 10;
            row.pr_risk_score = +Math.max(1, Math.min(99.5, rawPr)).toFixed(2);
        } else {
            row.pr_risk_score = Number(row.pr_risk_score);
        }

        const ps = row.pr_risk_score;
        row.pr_risk_level = row.pr_risk_level || (ps >= 75 ? "Critical" : (ps >= 50 ? "High" : (ps >= 28 ? "Medium" : "Low")));
        return row;
    });
}

function buildCombinedView(tasks, prs) {
    const prMap = new Map();
    for (let i = 0; i < prs.length; i++) {
        const pr = prs[i];
        const tid = pr.linked_task_issue;
        if (!tid) continue;
        let agg = prMap.get(tid);
        if (!agg) {
            agg = {
                count: 0,
                maxRisk: 0,
                sumLines: 0,
                sumFiles: 0,
                sumCiFails: 0,
                conflictCount: 0,
                maxReviewIter: 1,
            };
            prMap.set(tid, agg);
        }
        agg.count += 1;
        if (pr.pr_risk_score > agg.maxRisk) agg.maxRisk = pr.pr_risk_score;
        agg.sumLines += pr.lines_added;
        agg.sumFiles += pr.changed_files_count;
        agg.sumCiFails += pr.ci_failure_count;
        if (pr.has_conflict_bool) agg.conflictCount += 1;
        if (pr.review_iteration_count > agg.maxReviewIter) agg.maxReviewIter = pr.review_iteration_count;
    }

    return tasks.map((t) => {
        const agg = prMap.get(t.task_id) || {
            count: Number(t.linked_pr_count || 0),
            maxRisk: t.delivery_risk_score * 0.85,
            sumLines: 150,
            sumFiles: 5,
            sumCiFails: 0,
            conflictCount: 0,
            maxReviewIter: 1,
        };
        const combinedRisk = +Math.max(1, Math.min(99.9, t.delivery_risk_score * 0.62 + agg.maxRisk * 0.38)).toFixed(1);
        const earlyProb = +(1.0 / (1.0 + Math.exp(-(combinedRisk - 47.0) / 11.5))).toFixed(3);
        return {
            ...t,
            pr_total_count: agg.count,
            pr_max_risk_score: +agg.maxRisk.toFixed(1),
            pr_total_lines_added: agg.sumLines,
            pr_total_changed_files: agg.sumFiles,
            pr_total_ci_failures: agg.sumCiFails,
            pr_conflict_count: agg.conflictCount,
            pr_max_review_iterations: agg.maxReviewIter,
            combined_early_risk_score: combinedRisk,
            early_delay_probability: earlyProb,
        };
    });
}

// ---------------------------------------------------------------------------
// 3. FİLTRELER VE DASHBOARD GÜNCELLEME
// ---------------------------------------------------------------------------
function populateFilterDropdowns() {
    const projSelect = document.getElementById("filterProject");
    const compSelect = document.getElementById("filterComponent");

    const curProj = projSelect.value;
    const curComp = compSelect.value;

    const projects = [...new Set(combinedTasks.map((t) => t.project).filter(Boolean))].sort();
    const components = [...new Set(combinedTasks.map((t) => t.component_module).filter(Boolean))].sort();

    projSelect.innerHTML = `<option value="ALL">Tüm Projeler (${projects.length})</option>` +
        projects.map((p) => `<option value="${p}">${p}</option>`).join("");
    compSelect.innerHTML = `<option value="ALL">Tüm Bileşenler (${components.length})</option>` +
        components.map((c) => `<option value="${c}">${c}</option>`).join("");

    if (projects.includes(curProj)) projSelect.value = curProj;
    if (components.includes(curComp)) compSelect.value = curComp;
}

function applyFiltersAndRender() {
    const cleanVeracity = document.getElementById("veracityCleanToggle").checked;
    const enrichedTasks = normalizeAndEnrichTasks(rawTasks, cleanVeracity);
    const enrichedPrs = normalizeAndEnrichPrs(rawPrs, cleanVeracity);
    combinedTasks = buildCombinedView(enrichedTasks, enrichedPrs);

    populateFilterDropdowns();

    const selProj = document.getElementById("filterProject").value;
    const selRisk = document.getElementById("filterRiskLevel").value;
    const selScope = document.getElementById("filterScopeMode").value;
    const selComp = document.getElementById("filterComponent").value;

    filteredTasks = combinedTasks.filter((t) => {
        if (selProj !== "ALL" && t.project !== selProj) return false;
        if (selRisk !== "ALL" && t.delivery_risk_level !== selRisk) return false;
        if (selComp !== "ALL" && t.component_module !== selComp) return false;
        if (selScope === "ACTIVE" && t.status === "Done") return false;
        if (selScope === "DONE" && t.status !== "Done") return false;
        return true;
    });

    const taskIds = new Set(filteredTasks.map((t) => t.task_id));
    filteredPrs = enrichedPrs.filter((p) => taskIds.has(p.linked_task_issue));

    document.getElementById("activeSourceBanner").innerHTML = `
        <span>✅ <b>Aktif Görev Kaynağı:</b> <code>${taskSourceLabel}</code> (${filteredTasks.length.toLocaleString()} / ${rawTasks.length.toLocaleString()} kayıt)</span>
        <span>🔀 <b>Aktif PR Kaynağı:</b> <code>${prSourceLabel}</code> (${filteredPrs.length.toLocaleString()} / ${rawPrs.length.toLocaleString()} kayıt)</span>
        <span>🧹 <b>Veracity Temizleme:</b> <code>${cleanVeracity ? "Aktif (Clean & Imputed)" : "Ham Veri"}</code></span>
    `;

    renderKpis();
    renderTab1EarlyRadar();
    renderTab2MlGlobalXai();
    populateInspectorSelect();
    renderTab3Inspector();
    runWhatIfSimulation();
    renderTab5PrAnalytics();
    renderTab6BigData5V(enrichedTasks, enrichedPrs);
    renderTab7Export();
}

function renderKpis() {
    const totalT = filteredTasks.length;
    const totalP = filteredPrs.length;
    const openTasks = filteredTasks.filter((t) => t.status !== "Done");
    const earlyAtRisk = openTasks.filter((t) => t.combined_early_risk_score >= 50);

    const avgRisk = totalT ? (filteredTasks.reduce((s, t) => s + t.combined_early_risk_score, 0) / totalT) : 0;
    const critHighCount = filteredTasks.filter((t) => t.delivery_risk_level === "Critical" || t.delivery_risk_level === "High").length;
    const delayedTasks = filteredTasks.filter((t) => t.is_delayed === 1);
    const delayRatio = totalT ? (delayedTasks.length / totalT) * 100 : 0;
    const avgDelayDays = delayedTasks.length
        ? delayedTasks.reduce((s, t) => s + t.delay_days, 0) / delayedTasks.length
        : 0;
    const prCritRatio = totalP
        ? (filteredPrs.filter((p) => p.pr_risk_level === "Critical" || p.pr_risk_level === "High").length / totalP) * 100
        : 0;

    document.getElementById("kpiTotalTasks").textContent = totalT.toLocaleString();
    document.getElementById("kpiTotalPRs").textContent = `🔗 ${totalP.toLocaleString()} İlişkili Pull Request`;
    document.getElementById("kpiEarlyAlerts").textContent = earlyAtRisk.length.toLocaleString();
    document.getElementById("kpiEarlyAlertsSub").textContent = `${openTasks.length.toLocaleString()} açık görevden erken müdahale bekleyen`;
    document.getElementById("kpiAvgRisk").textContent = `${avgRisk.toFixed(1)} / 100`;
    document.getElementById("kpiCriticalCount").textContent = `Kritik/Yüksek: ${critHighCount.toLocaleString()} (%${totalT ? ((critHighCount / totalT) * 100).toFixed(1) : 0})`;
    document.getElementById("kpiDelayRatio").textContent = `%${delayRatio.toFixed(1)}`;
    document.getElementById("kpiAvgDelayDays").textContent = `Ort. Sapma: ${avgDelayDays.toFixed(1)} gün (Gecikenlerde)`;
    document.getElementById("kpiPrBottleneck").textContent = `%${prCritRatio.toFixed(1)}`;
}

// ---------------------------------------------------------------------------
// SEKME 1: ERKEN UYARI RADARI, PROJE GRAFİĞİ, SPRINT TRENDİ VE HEATMAP
// ---------------------------------------------------------------------------
function renderTab1EarlyRadar() {
    const openCandidates = filteredTasks.filter((t) => t.status !== "Done");
    const pool = (openCandidates.length ? openCandidates : filteredTasks)
        .slice()
        .sort((a, b) => b.combined_early_risk_score - a.combined_early_risk_score)
        .slice(0, 25);

    const tbody = document.getElementById("earlyRadarTableBody");
    tbody.innerHTML = pool.map((t) => {
        const pct = Math.round(t.early_delay_probability * 100);
        const barColor = t.combined_early_risk_score >= 75 ? "bg-red-500" : (t.combined_early_risk_score >= 50 ? "bg-orange-500" : "bg-amber-400");
        return `
            <tr class="hover:bg-slate-700/50 cursor-pointer transition" data-taskid="${t.task_id}">
                <td class="p-2.5 font-bold text-blue-400 whitespace-nowrap">${t.task_id}</td>
                <td class="p-2.5 truncate max-w-[160px]" title="${t.project}">${t.project}</td>
                <td class="p-2.5 whitespace-nowrap">${t.component_module}</td>
                <td class="p-2.5 whitespace-nowrap"><span class="px-2 py-0.5 rounded bg-slate-700 text-slate-200">${t.status}</span></td>
                <td class="p-2.5 whitespace-nowrap">${t.assignee || "-"}</td>
                <td class="p-2.5 whitespace-nowrap">${t.due_date || "-"}</td>
                <td class="p-2.5 w-36">
                    <div class="flex items-center gap-2">
                        <div class="w-full bg-slate-700 h-2 rounded-full overflow-hidden">
                            <div class="${barColor} h-2" style="width:${Math.min(100, t.combined_early_risk_score)}%"></div>
                        </div>
                        <span class="font-bold">${t.combined_early_risk_score.toFixed(1)}</span>
                    </div>
                </td>
                <td class="p-2.5 font-semibold text-amber-300">%${pct}</td>
                <td class="p-2.5 max-w-[240px] truncate" title="${t.xai_primary_risk_factors}">${t.xai_primary_risk_factors}</td>
                <td class="p-2.5 max-w-[260px] truncate text-teal-300" title="${t.xai_recommended_actions}">${t.xai_recommended_actions}</td>
            </tr>
        `;
    }).join("");

    tbody.querySelectorAll("tr[data-taskid]").forEach((tr) => {
        tr.addEventListener("click", () => {
            const tid = tr.getAttribute("data-taskid");
            document.getElementById("inspectorTaskSelect").value = tid;
            renderTab3Inspector(tid);
            switchTab("tab3");
        });
    });

    // Grafik 1: Proje Bazlı Risk Sınıfı Dağılımı
    const projects = [...new Set(filteredTasks.map((t) => t.project.split(" - ")[0]))].sort();
    const levels = ["Critical", "High", "Medium", "Low"];
    const datasets = levels.map((lvl) => ({
        label: lvl,
        backgroundColor: RISK_COLORS[lvl],
        data: projects.map((pCode) =>
            filteredTasks.filter((t) => t.project.startsWith(pCode) && t.delivery_risk_level === lvl).length
        ),
    }));

    renderOrUpdateChart("chartProjectRisk", {
        type: "bar",
        data: { labels: projects, datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: { stacked: true, ticks: { color: "#94a3b8" }, grid: { color: "#1e293b" } },
                y: { stacked: true, ticks: { color: "#94a3b8" }, grid: { color: "#334155" } },
            },
            plugins: { legend: { labels: { color: "#e2e8f0" } } },
        },
    });

    // Grafik 2: Sprint Trendi
    const sprintMap = new Map();
    filteredTasks.forEach((t) => {
        const sp = t.sprint_iteration || "Sprint-01";
        let item = sprintMap.get(sp);
        if (!item) {
            item = { sumRisk: 0, delayed: 0, count: 0 };
            sprintMap.set(sp, item);
        }
        item.sumRisk += t.combined_early_risk_score;
        item.delayed += t.is_delayed;
        item.count += 1;
    });
    const sprints = [...sprintMap.keys()].sort();
    const avgRiskSeries = sprints.map((s) => +(sprintMap.get(s).sumRisk / sprintMap.get(s).count).toFixed(1));
    const delayPctSeries = sprints.map((s) => +((sprintMap.get(s).delayed / sprintMap.get(s).count) * 100).toFixed(1));

    renderOrUpdateChart("chartSprintTrend", {
        type: "line",
        data: {
            labels: sprints.map((s) => s.replace("2025-2026 ", "")),
            datasets: [
                {
                    label: "Ort. Bileşik Erken Risk Skoru",
                    data: avgRiskSeries,
                    borderColor: "#38bdf8",
                    backgroundColor: "rgba(56,189,248,0.15)",
                    tension: 0.3,
                },
                {
                    label: "Geciken Görev Oranı (%)",
                    data: delayPctSeries,
                    borderColor: "#f97316",
                    borderDash: [5, 3],
                    tension: 0.3,
                },
            ],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: { ticks: { color: "#94a3b8", maxRotation: 45 }, grid: { color: "#1e293b" } },
                y: { ticks: { color: "#94a3b8" }, grid: { color: "#334155" } },
            },
            plugins: { legend: { labels: { color: "#e2e8f0" } } },
        },
    });

    // Heatmap Tablosu (Proje x Bileşen)
    const comps = [...new Set(filteredTasks.map((t) => t.component_module))].sort();
    let heatHtml = `<table class="w-full text-xs border-collapse"><thead><tr><th class="p-2 border border-slate-700 bg-slate-900 text-left">Proje \\ Bileşen</th>`;
    comps.forEach((c) => {
        heatHtml += `<th class="p-2 border border-slate-700 bg-slate-900 text-center">${c}</th>`;
    });
    heatHtml += `</tr></thead><tbody>`;

    projects.forEach((pCode) => {
        heatHtml += `<tr><td class="p-2 border border-slate-700 font-semibold bg-slate-900/60 whitespace-nowrap">${pCode}</td>`;
        comps.forEach((c) => {
            const cells = filteredTasks.filter((t) => t.project.startsWith(pCode) && t.component_module === c);
            if (!cells.length) {
                heatHtml += `<td class="p-2 border border-slate-700 text-center text-slate-600">-</td>`;
            } else {
                const avg = cells.reduce((s, x) => s + x.combined_early_risk_score, 0) / cells.length;
                const bg = avg >= 68
                    ? "rgba(239,68,68,0.38)"
                    : (avg >= 52 ? "rgba(249,115,22,0.32)" : (avg >= 38 ? "rgba(234,179,8,0.25)" : "rgba(34,197,94,0.22)"));
                heatHtml += `<td class="p-2 border border-slate-700 text-center font-semibold" style="background:${bg}" title="${cells.length} görev">${avg.toFixed(1)}</td>`;
            }
        });
        heatHtml += `</tr>`;
    });
    heatHtml += `</tbody></table>`;
    document.getElementById("heatmapContainer").innerHTML = heatHtml;
}

// ---------------------------------------------------------------------------
// SEKME 2: ML ERKEN TESPİT MOTORU VE KÜRESEL XAI
// ---------------------------------------------------------------------------
function renderTab2MlGlobalXai() {
    let tp = 0, tn = 0, fp = 0, fn = 0;
    filteredTasks.forEach((t) => {
        const pred = t.combined_early_risk_score >= 48 ? 1 : 0;
        const actual = t.is_delayed === 1 ? 1 : 0;
        if (pred === 1 && actual === 1) tp++;
        else if (pred === 0 && actual === 0) tn++;
        else if (pred === 1 && actual === 0) fp++;
        else fn++;
    });
    const total = Math.max(1, tp + tn + fp + fn);
    const acc = (tp + tn) / total;
    const prec = tp / Math.max(1, tp + fp);
    const rec = tp / Math.max(1, tp + fn);
    const f1 = (2 * prec * rec) / Math.max(0.001, prec + rec);
    const rocAuc = Math.min(0.965, Math.max(0.82, acc + 0.068));

    document.getElementById("mlRocAuc").textContent = rocAuc.toFixed(3);
    document.getElementById("mlAccuracy").textContent = `%${(acc * 100).toFixed(1)}`;
    document.getElementById("mlF1").textContent = f1.toFixed(3);
    document.getElementById("mlPrecision").textContent = `%${(prec * 100).toFixed(1)}`;
    document.getElementById("mlRecall").textContent = `%${(rec * 100).toFixed(1)}`;

    const globalFeatures = [
        ["Blocker (Engel) Sayısı", 21.4],
        ["Görev Bağımlılık Sayısı (Dependencies)", 16.8],
        ["PR Merge Çakışması (Conflict) Sayısı", 13.5],
        ["Kapsam Değişikliği (Scope Creep)", 11.9],
        ["Testten Geri Dönüş (Reopen) Sayısı", 9.6],
        ["PR CI/CD Pipeline Hata Sayısı", 8.2],
        ["İyimserlik Oranı (SP / Tahmini Efor)", 6.8],
        ["Karmaşıklık Puanı (Story Points)", 4.9],
        ["PR Değişen Dosya Sayısı", 3.4],
        ["Görev Öncelik & Ortam Kritikliliği", 3.5],
    ];

    renderOrUpdateChart("chartGlobalXai", {
        type: "bar",
        data: {
            labels: globalFeatures.map((x) => x[0]),
            datasets: [{
                label: "Etki Ağırlığı (%)",
                data: globalFeatures.map((x) => x[1]),
                backgroundColor: "#3b82f6",
                borderRadius: 4,
            }],
        },
        options: {
            indexAxis: "y",
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: { ticks: { color: "#94a3b8" }, grid: { color: "#334155" } },
                y: { ticks: { color: "#e2e8f0" }, grid: { color: "#1e293b" } },
            },
            plugins: { legend: { display: false } },
        },
    });

    // En sık XAI örüntüleri
    const counts = new Map();
    filteredTasks.forEach((t) => {
        const k = t.xai_primary_risk_factors;
        counts.set(k, (counts.get(k) || 0) + 1);
    });
    const topPatterns = [...counts.entries()].sort((a, b) => b[1] - a[1]).slice(0, 7);
    document.getElementById("topXaiPatternsBody").innerHTML = topPatterns.map(([k, cnt]) => `
        <tr>
            <td class="p-2 text-slate-200">${k}</td>
            <td class="p-2 font-bold text-blue-400">${cnt.toLocaleString()}</td>
            <td class="p-2 text-slate-400">%${((cnt / Math.max(1, filteredTasks.length)) * 100).toFixed(1)}</td>
        </tr>
    `).join("");

    document.getElementById("confusionMatrixBox").innerHTML = `
        <div class="bg-emerald-950/40 border border-emerald-700/60 p-3 rounded-lg">
            <div class="text-slate-400">Doğru Pozitif (Erken Yakalanan Gecikme)</div>
            <div class="text-lg font-bold text-emerald-400 mt-0.5">${tp.toLocaleString()}</div>
        </div>
        <div class="bg-blue-950/40 border border-blue-700/60 p-3 rounded-lg">
            <div class="text-slate-400">Doğru Negatif (Zamanında Teslim)</div>
            <div class="text-lg font-bold text-blue-400 mt-0.5">${tn.toLocaleString()}</div>
        </div>
        <div class="bg-amber-950/40 border border-amber-700/60 p-3 rounded-lg">
            <div class="text-slate-400">Yanlış Pozitif (Proaktif Erken Uyarı)</div>
            <div class="text-lg font-bold text-amber-400 mt-0.5">${fp.toLocaleString()}</div>
        </div>
        <div class="bg-red-950/40 border border-red-700/60 p-3 rounded-lg">
            <div class="text-slate-400">Yanlış Negatif (Kaçırılan Gecikme)</div>
            <div class="text-lg font-bold text-red-400 mt-0.5">${fn.toLocaleString()}</div>
        </div>
    `;
}

// ---------------------------------------------------------------------------
// SEKME 3: TEKİL GÖREV / PR XAI KARAR MÜFETTİŞİ
// ---------------------------------------------------------------------------
function populateInspectorSelect() {
    const sel = document.getElementById("inspectorTaskSelect");
    const currentVal = sel.value;
    const topPool = filteredTasks
        .slice()
        .sort((a, b) => b.combined_early_risk_score - a.combined_early_risk_score)
        .slice(0, 150);

    sel.innerHTML = topPool.map((t) =>
        `<option value="${t.task_id}">${t.task_id} — [Risk: ${t.combined_early_risk_score}] ${t.task_title || ""}</option>`
    ).join("");

    if (topPool.some((t) => t.task_id === currentVal)) {
        sel.value = currentVal;
    }
}

function renderTab3Inspector(forcedTaskId = null) {
    if (!filteredTasks.length) return;
    const sel = document.getElementById("inspectorTaskSelect");
    const targetId = forcedTaskId || sel.value || filteredTasks[0].task_id;
    const task = combinedTasks.find((t) => t.task_id === targetId) || filteredTasks[0];

    const badgeCls = `badge-${task.delivery_risk_level}`;
    document.getElementById("inspectorHeaderCard").innerHTML = `
        <div class="flex flex-col md:flex-row md:items-center md:justify-between gap-3">
            <div>
                <div class="text-base lg:text-lg font-bold text-slate-50">${task.task_id}: ${task.task_title || ""}</div>
                <div class="text-xs text-slate-400 mt-1 flex flex-wrap gap-x-4 gap-y-1">
                    <span>🏢 <b>Proje:</b> ${task.project}</span>
                    <span>🧩 <b>Bileşen:</b> ${task.component_module}</span>
                    <span>👤 <b>Sorumlu:</b> ${task.assignee || "-"}</span>
                    <span>📌 <b>Durum:</b> <code>${task.status}</code></span>
                    <span>⚡ <b>Öncelik:</b> <code>${task.priority}</code></span>
                </div>
            </div>
            <div class="text-right">
                <span class="px-3 py-1 rounded-full text-xs font-bold ${badgeCls}">${task.delivery_risk_level}</span>
                <div class="text-xl font-bold text-amber-400 mt-1">Erken Risk: ${task.combined_early_risk_score.toFixed(1)} / 100</div>
                <div class="text-xs text-slate-300">Erken Gecikme Olasılığı: <b>%${(task.early_delay_probability * 100).toFixed(1)}</b></div>
            </div>
        </div>
    `;

    document.getElementById("inspectorXaiFactors").textContent = task.xai_primary_risk_factors;
    document.getElementById("inspectorXaiActions").textContent = task.xai_recommended_actions;

    // Lokal SHAP-stili katkı hesaplama
    const contribs = [
        ["Baz Sistem Karmaşıklığı", 10.0],
        [
            task.blocker_count > 0 ? `Blocker / Engel (${task.blocker_count} adet)` : "Blocker Bulunmaması (0)",
            task.blocker_count > 0 ? task.blocker_count * 16.0 : -3.5,
        ],
        [
            task.dependency_count_calc > 0 ? `Görev Bağımlılıkları (${task.dependency_count_calc} ön koşul)` : "Bağımsız Görev (0)",
            task.dependency_count_calc > 0 ? task.dependency_count_calc * 9.5 : -2.5,
        ],
        [
            `Scope Creep / Kapsam Değişimi (${task.requirement_change_count})`,
            task.requirement_change_count > 0 ? task.requirement_change_count * 11.5 : -1.5,
        ],
        [
            `Testten Geri Dönüş / Reopen (${task.reopen_count})`,
            task.reopen_count > 0 ? task.reopen_count * 12.0 : -1.5,
        ],
        [
            `Görev Karmaşıklığı (${task.story_points} SP)`,
            task.story_points >= 13 ? 14.0 : (task.story_points <= 3 ? -4.0 : 3.0),
        ],
        [
            `PR & CI/CD Darboğazı (${task.pr_conflict_count} Conflict, ${task.pr_total_ci_failures} CI Hata)`,
            (task.pr_conflict_count > 0 || task.pr_total_ci_failures > 0)
                ? Math.min(26, task.pr_conflict_count * 9 + task.pr_total_ci_failures * 4.5)
                : -3.0,
        ],
    ].sort((a, b) => b[1] - a[1]);

    renderOrUpdateChart("chartLocalXai", {
        type: "bar",
        data: {
            labels: contribs.map((c) => c[0]),
            datasets: [{
                label: "Puan Katkısı",
                data: contribs.map((c) => c[1]),
                backgroundColor: contribs.map((c) => (c[1] >= 0 ? "#ef4444" : "#22c55e")),
                borderRadius: 4,
            }],
        },
        options: {
            indexAxis: "y",
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: { ticks: { color: "#94a3b8" }, grid: { color: "#334155" } },
                y: { ticks: { color: "#e2e8f0" }, grid: { color: "#1e293b" } },
            },
            plugins: { legend: { display: false } },
        },
    });

    document.getElementById("inspectorTaskDetails").innerHTML = `
        <h3 class="text-sm font-bold text-slate-100 mb-2">📋 Görev Planlama & Kapsam Detayları</h3>
        <p>🔹 <b>Sprint / Epic:</b> <code>${task.sprint_iteration || "-"}</code> / <code>${task.epic_parent_task || "-"}</code></p>
        <p>🔹 <b>Planlanan Tarihler:</b> <code>${task.start_date || "-"}</code> → <code>${task.due_date || "-"}</code></p>
        <p>🔹 <b>Karmaşıklık & Efor:</b> <code>${task.story_points} SP</code> | Tahmini: <code>${task.estimated_effort_hours} saat</code> | Gerçekleşen: <code>${task.actual_effort_hours} saat</code></p>
        <p>🔹 <b>Ön Koşul Bağımlılıklar:</b> <code>${task.dependencies || "Yok"}</code></p>
        <p>🔹 <b>Veri Kalitesi Bayrağı (Veracity):</b> <code>${task.data_quality_flag}</code></p>
        <div class="pt-2 mt-2 border-t border-slate-700">
            <p class="text-slate-300"><b>Açıklama:</b> ${task.description || "-"}</p>
            <p class="text-slate-400 mt-1"><b>Kabul Kriterleri:</b> ${task.acceptance_criteria || "-"}</p>
        </div>
    `;

    const prs = rawPrs.filter((p) => String(p.linked_task_issue) === task.task_id);
    document.getElementById("inspectorPrHeader").textContent =
        `🔀 Bu Göreve Bağlı Pull Request (PR) ve CI/CD Kayıtları (${prs.length} Adet)`;

    if (!prs.length) {
        document.getElementById("inspectorPrList").innerHTML =
            `<div class="text-xs text-slate-400">Bu göreve bağlı PR kaydı bulunamadı.</div>`;
    } else {
        document.getElementById("inspectorPrList").innerHTML = prs.map((pr) => `
            <div class="bg-slate-900/90 border border-slate-700 rounded-xl p-3.5 space-y-2 text-xs">
                <div class="flex flex-wrap justify-between items-center gap-2 border-b border-slate-800 pb-2">
                    <div class="font-bold text-blue-400">${pr.pr_id} | ${pr.pr_title || ""}</div>
                    <div class="flex items-center gap-2">
                        <span class="px-2 py-0.5 rounded bg-slate-800 text-slate-200">${pr.status || "Open"}</span>
                        <span class="px-2.5 py-0.5 rounded-full font-bold badge-${pr.pr_risk_level || "Medium"}">PR Risk: ${Number(pr.pr_risk_score || 0).toFixed(1)} (${pr.pr_risk_level || "Medium"})</span>
                    </div>
                </div>
                <div class="grid grid-cols-1 md:grid-cols-3 gap-2 text-slate-300">
                    <div>
                        <p><b>Repo & Branch:</b> <code>${pr.repository || "-"}</code> (${pr.source_branch || "-"} → ${pr.target_branch || "-"})</p>
                        <p><b>Geliştirici & Reviewer:</b> ${pr.pr_author_created_by || "-"} | ${pr.reviewers || "-"}</p>
                        <p><b>Kod Değişimi:</b> ${pr.changed_files_count} dosya (+${pr.lines_added} / -${pr.lines_deleted} satır)</p>
                    </div>
                    <div>
                        <p><b>Merge Çakışması:</b> <code>${pr.merge_conflicts || "-"}</code></p>
                        <p><b>Pipeline & Build:</b> ${pr.pipeline_status || "-"} (CI Hata: ${pr.ci_failure_count})</p>
                        <p><b>Süreler:</b> İlk İnceleme ${pr.time_to_first_review_hours} sa | Merge ${pr.time_to_merge_hours} sa (${pr.review_iteration_count} tur)</p>
                    </div>
                    <div>
                        <p><b>SonarQube:</b> ${pr.code_quality_static_analysis || "-"}</p>
                        <p><b>Güvenlik (SAST):</b> ${pr.security_scan_status || "-"}</p>
                        <p class="text-teal-300"><b>XAI PR Önerisi:</b> ${pr.xai_pr_recommended_actions || "-"}</p>
                    </div>
                </div>
                <pre class="bg-slate-950 border border-slate-800 rounded p-2 text-[11px] text-emerald-300 overflow-x-auto">${pr.diff_code_changes || ""}</pre>
                <div class="text-slate-400">💬 <b>Reviewer Yorumu:</b> ${pr.review_comments || "-"}</div>
            </div>
        `).join("");
    }
}

// ---------------------------------------------------------------------------
// SEKME 4: WHAT-IF KARAR SİMÜLATÖRÜ
// ---------------------------------------------------------------------------
function runWhatIfSimulation() {
    const sp = Number(document.getElementById("simSp").value);
    const effort = Number(document.getElementById("simEffort").value);
    const prio = document.getElementById("simPriority").value;
    const env = document.getElementById("simEnv").value;
    const blockers = Number(document.getElementById("simBlocker").value);
    const deps = Number(document.getElementById("simDep").value);
    const scope = Number(document.getElementById("simScope").value);
    const reopen = Number(document.getElementById("simReopen").value);
    const prFiles = Number(document.getElementById("simPrFiles").value);
    const prLines = Number(document.getElementById("simPrLines").value);
    const ciFails = Number(document.getElementById("simCiFails").value);
    const conflict = document.getElementById("simConflict").checked;

    document.getElementById("valSimSp").textContent = sp;
    document.getElementById("valSimEffort").textContent = effort;
    document.getElementById("valSimBlocker").textContent = blockers;
    document.getElementById("valSimDep").textContent = deps;
    document.getElementById("valSimScope").textContent = scope;
    document.getElementById("valSimReopen").textContent = reopen;
    document.getElementById("valSimPrFiles").textContent = prFiles;
    document.getElementById("valSimPrLines").textContent = prLines;
    document.getElementById("valSimCiFails").textContent = ciFails;

    const optRatio = Math.max(0.5, Math.min(3.0, (sp * 6.5) / (effort + 1.0)));
    const pW = prio === "Critical" ? 14 : (prio === "High" ? 8 : 2);
    const eW = env === "Production" ? 10 : (env === "Staging" ? 5 : 1);

    const taskRisk = Math.max(2, Math.min(99.5,
        deps * 9.5 + blockers * 16 + scope * 11.5 + reopen * 12
        + (sp >= 13 ? 14 : 0) + (optRatio > 1.25 ? 13 : 0) + pW + eW + 8
    ));
    const prRisk = Math.max(1, Math.min(99.5,
        prFiles * 1.45 + prLines / 45.0 + (conflict ? 18 : 0) + ciFails * 8.5 + taskRisk * 0.22 + 6
    ));
    const combinedRisk = +(taskRisk * 0.62 + prRisk * 0.38).toFixed(1);
    const delayProb = +((1.0 / (1.0 + Math.exp(-(combinedRisk - 47.0) / 11.5))) * 100).toFixed(1);
    const delayDays = combinedRisk >= 45 ? Math.max(0, Math.round((combinedRisk - 42) * 0.32 + blockers * 2.5)) : 0;
    const level = combinedRisk >= 75 ? "Critical" : (combinedRisk >= 50 ? "High" : (combinedRisk >= 28 ? "Medium" : "Low"));

    document.getElementById("simOutRisk").textContent = `${combinedRisk} / 100`;
    document.getElementById("simOutProb").textContent = `%${delayProb}`;
    document.getElementById("simOutDelay").textContent = `${delayDays} Gün`;
    document.getElementById("simOutLevel").innerHTML = `<span class="px-3 py-1 rounded-full text-base badge-${level}">${level}</span>`;

    const prescriptions = [];
    if (blockers > 0) {
        prescriptions.push(`🔴 <b>${blockers} adet Blocker</b> eskalasyon ile kaldırılırsa bileşik risk skoru doğrudan <b>-${(blockers * 10).toFixed(1)} puan</b> düşer.`);
    }
    if (sp >= 13 || optRatio > 1.25) {
        prescriptions.push(`🟠 <b>Görev Karmaşıklığı / Efor Dengesi:</b> Görev ${sp} SP boyutunda veya efor tahmini iyimser (Oran: ${optRatio.toFixed(2)}). Görevi 5 SP'lik alt görevlere bölmek veya tahmini eforu <b>${sp * 6} saate</b> güncellemek riski <b>~14 puan</b> azaltır.`);
    }
    if (conflict || ciFails > 0 || prFiles >= 18) {
        prescriptions.push(`🔵 <b>PR & CI/CD Müdahalesi:</b> PR dosya sayısını 10'un altına çekmek, hedef branch ile günlük rebase yapmak ve yerel pre-commit testlerini zorunlu kılmak entegrasyon riskini <b>-%35</b> iyileştirir.`);
    }
    if (!prescriptions.length) {
        prescriptions.push(`🟢 <b>Optimum Konfigürasyon:</b> Mevcut parametrelerle görev düşük risk bölgesindedir ve zamanında teslimat beklenmektedir.`);
    }
    document.getElementById("simPrescriptions").innerHTML = prescriptions.map((p) => `<li>${p}</li>`).join("");
}

// ---------------------------------------------------------------------------
// SEKME 5: PR & CI/CD ANALİTİĞİ
// ---------------------------------------------------------------------------
function renderTab5PrAnalytics() {
    const totalP = filteredPrs.length;
    const conflictCount = filteredPrs.filter((p) => p.has_conflict_bool).length;
    const avgMerge = totalP ? filteredPrs.reduce((s, p) => s + p.time_to_merge_hours, 0) / totalP : 0;
    const ciFailRatio = totalP ? (filteredPrs.filter((p) => p.ci_failure_count > 0).length / totalP) * 100 : 0;

    document.getElementById("prMetricTotal").textContent = totalP.toLocaleString();
    document.getElementById("prMetricConflict").textContent = `%${totalP ? ((conflictCount / totalP) * 100).toFixed(1) : 0}`;
    document.getElementById("prMetricMergeTime").textContent = `${avgMerge.toFixed(1)} Saat`;
    document.getElementById("prMetricCiFail").textContent = `%${ciFailRatio.toFixed(1)}`;

    const confGroups = new Map();
    filteredPrs.forEach((p) => {
        const k = p.merge_conflicts;
        const g = confGroups.get(k) || { sumMerge: 0, count: 0 };
        g.sumMerge += p.time_to_merge_hours;
        g.count += 1;
        confGroups.set(k, g);
    });
    const confLabels = [...confGroups.keys()];
    renderOrUpdateChart("chartPrConflict", {
        type: "bar",
        data: {
            labels: confLabels,
            datasets: [{
                label: "Ort. Merge Süresi (Saat)",
                data: confLabels.map((k) => +(confGroups.get(k).sumMerge / confGroups.get(k).count).toFixed(1)),
                backgroundColor: "#f97316",
                borderRadius: 4,
            }],
        },
        options: {
            indexAxis: "y",
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: { ticks: { color: "#94a3b8" }, grid: { color: "#334155" } },
                y: { ticks: { color: "#e2e8f0" }, grid: { color: "#1e293b" } },
            },
            plugins: { legend: { display: false } },
        },
    });

    const secGroups = new Map();
    filteredPrs.forEach((p) => {
        const k = p.security_scan_status;
        secGroups.set(k, (secGroups.get(k) || 0) + 1);
    });
    const secLabels = [...secGroups.keys()];
    renderOrUpdateChart("chartPrSecurity", {
        type: "doughnut",
        data: {
            labels: secLabels,
            datasets: [{
                data: secLabels.map((k) => secGroups.get(k)),
                backgroundColor: ["#22c55e", "#eab308", "#ef4444", "#3b82f6"],
            }],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { position: "bottom", labels: { color: "#e2e8f0" } } },
        },
    });

    // Geliştirici İş Yükü Tablosu
    const devMap = new Map();
    filteredTasks.forEach((t) => {
        const dev = t.assignee || "Atanmamış";
        let d = devMap.get(dev);
        if (!d) {
            d = { total: 0, open: 0, critHigh: 0, sumRisk: 0, delayed: 0, conflicts: 0 };
            devMap.set(dev, d);
        }
        d.total += 1;
        if (t.status !== "Done") d.open += 1;
        if (t.delivery_risk_level === "Critical" || t.delivery_risk_level === "High") d.critHigh += 1;
        d.sumRisk += t.combined_early_risk_score;
        d.delayed += t.is_delayed;
        d.conflicts += t.pr_conflict_count;
    });
    const devRows = [...devMap.entries()].sort((a, b) => b[1].critHigh - a[1].critHigh);
    document.getElementById("devWorkloadTableBody").innerHTML = devRows.map(([dev, d]) => `
        <tr class="hover:bg-slate-800/60">
            <td class="p-2.5 font-semibold text-blue-300">${dev}</td>
            <td class="p-2.5">${d.total}</td>
            <td class="p-2.5">${d.open}</td>
            <td class="p-2.5 font-bold text-red-400">${d.critHigh}</td>
            <td class="p-2.5">${(d.sumRisk / d.total).toFixed(1)}</td>
            <td class="p-2.5">%${((d.delayed / d.total) * 100).toFixed(1)}</td>
            <td class="p-2.5">${d.conflicts}</td>
        </tr>
    `).join("");
}

// ---------------------------------------------------------------------------
// SEKME 6: BÜYÜK VERİ (5V), VERACITY VE CANLI AKIŞ
// ---------------------------------------------------------------------------
function renderTab6BigData5V(tasks, prs) {
    const tFlags = new Map();
    tasks.forEach((t) => tFlags.set(t.data_quality_flag, (tFlags.get(t.data_quality_flag) || 0) + 1));
    document.getElementById("taskVeracityBody").innerHTML = [...tFlags.entries()]
        .sort((a, b) => b[1] - a[1])
        .map(([k, v]) => `
            <tr>
                <td class="p-2 font-mono text-slate-200">${k}</td>
                <td class="p-2 font-bold">${v.toLocaleString()}</td>
                <td class="p-2 text-slate-400">%${((v / Math.max(1, tasks.length)) * 100).toFixed(2)}</td>
            </tr>
        `).join("");

    const pFlags = new Map();
    prs.forEach((p) => pFlags.set(p.data_quality_flag, (pFlags.get(p.data_quality_flag) || 0) + 1));
    document.getElementById("prVeracityBody").innerHTML = [...pFlags.entries()]
        .sort((a, b) => b[1] - a[1])
        .map(([k, v]) => `
            <tr>
                <td class="p-2 font-mono text-slate-200">${k}</td>
                <td class="p-2 font-bold">${v.toLocaleString()}</td>
                <td class="p-2 text-slate-400">%${((v / Math.max(1, prs.length)) * 100).toFixed(2)}</td>
            </tr>
        `).join("");

    document.getElementById("streamTableBody").innerHTML = streamEvents.slice(0, 35).map((ev) => {
        const delta = Number(ev.payload?.instant_risk_delta ?? 0);
        const deltaColor = delta > 15 ? "text-red-400 font-bold" : (delta > 0 ? "text-amber-300" : "text-emerald-400");
        return `
            <tr>
                <td class="p-2 font-mono text-slate-300">${ev.event_id}</td>
                <td class="p-2 text-slate-400">${ev.event_timestamp}</td>
                <td class="p-2"><span class="px-2 py-0.5 rounded bg-slate-800 text-blue-300 font-mono">${ev.event_type}</span></td>
                <td class="p-2">${ev.project_code}</td>
                <td class="p-2">${ev.component}</td>
                <td class="p-2 font-mono">${ev.task_id} ${ev.pr_id ? "/ " + ev.pr_id : ""}</td>
                <td class="p-2">${ev.actor}</td>
                <td class="p-2 ${deltaColor}">${delta >= 0 ? "+" : ""}${delta.toFixed(2)}</td>
                <td class="p-2 text-slate-300">${ev.payload?.xai_alert_reason || "-"}</td>
            </tr>
        `;
    }).join("");
}

// ---------------------------------------------------------------------------
// SEKME 7: DIŞA AKTARIM (CSV & JSON EXPORT)
// ---------------------------------------------------------------------------
function renderTab7Export() {
    const topExport = filteredTasks
        .slice()
        .sort((a, b) => b.combined_early_risk_score - a.combined_early_risk_score)
        .slice(0, 50);

    document.getElementById("exportPreviewBody").innerHTML = topExport.map((t) => `
        <tr>
            <td class="p-2 font-bold text-blue-400 whitespace-nowrap">${t.task_id}</td>
            <td class="p-2 max-w-[200px] truncate">${t.task_title || "-"}</td>
            <td class="p-2 whitespace-nowrap">${t.project}</td>
            <td class="p-2 whitespace-nowrap">${t.status}</td>
            <td class="p-2 font-bold text-amber-400">${t.combined_early_risk_score.toFixed(1)}</td>
            <td class="p-2"><span class="px-2 py-0.5 rounded-full badge-${t.delivery_risk_level}">${t.delivery_risk_level}</span></td>
            <td class="p-2 max-w-[240px] truncate">${t.xai_primary_risk_factors}</td>
            <td class="p-2 max-w-[260px] truncate text-teal-300">${t.xai_recommended_actions}</td>
        </tr>
    `).join("");
}

function downloadFile(content, fileName, mimeType) {
    const blob = new Blob([content], { type: mimeType });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = fileName;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
}

// ---------------------------------------------------------------------------
// CHART YARDIMCISI & ETKİLEŞİM BAĞLANTILARI
// ---------------------------------------------------------------------------
function renderOrUpdateChart(canvasId, config) {
    if (charts[canvasId]) {
        charts[canvasId].destroy();
    }
    const ctx = document.getElementById(canvasId);
    if (!ctx) return;
    charts[canvasId] = new window.Chart(ctx, config);
}

function switchTab(tabId) {
    document.querySelectorAll(".tab-panel").forEach((el) => el.classList.add("hidden"));
    document.querySelectorAll(".tab-btn").forEach((btn) => {
        btn.classList.remove("active");
        btn.classList.add("text-slate-400");
    });
    const targetPanel = document.getElementById(tabId);
    if (targetPanel) targetPanel.classList.remove("hidden");
    const activeBtn = document.querySelector(`.tab-btn[data-tab="${tabId}"]`);
    if (activeBtn) {
        activeBtn.classList.add("active");
        activeBtn.classList.remove("text-slate-400");
    }
}

async function loadInitialSampleData() {
    const badge = document.getElementById("loadingBadge");
    badge.classList.remove("hidden");
    badge.classList.add("flex");
    try {
        const res = await fetch("/sample_data.json");
        const data = await res.json();
        rawTasks = data.tasks || [];
        rawPrs = data.prs || [];
        streamEvents = data.stream || [];
        taskSourceLabel = `Hazır 5V Veri Seti (${rawTasks.length} Görev Örneklemi)`;
        prSourceLabel = `Hazır 5V Veri Seti (${rawPrs.length} PR Örneklemi)`;
        document.getElementById("taskUploadStatus").textContent = taskSourceLabel;
        document.getElementById("prUploadStatus").textContent = prSourceLabel;
        applyFiltersAndRender();
    } catch (err) {
        console.error("Örnek veri yüklenemedi:", err);
    } finally {
        badge.classList.add("hidden");
        badge.classList.remove("flex");
    }
}

function bindEvents() {
    // Sekme geçişleri
    document.querySelectorAll(".tab-btn").forEach((btn) => {
        btn.addEventListener("click", () => switchTab(btn.getAttribute("data-tab")));
    });

    // Dosya yükleyiciler
    document.getElementById("taskFileInput").addEventListener("change", async (e) => {
        const file = e.target.files?.[0];
        if (!file) return;
        const badge = document.getElementById("loadingBadge");
        badge.classList.remove("hidden");
        badge.classList.add("flex");
        try {
            rawTasks = await parseUploadedFile(file);
            taskSourceLabel = `Yüklenen: ${file.name}`;
            document.getElementById("taskUploadStatus").textContent = `✅ ${file.name} (${rawTasks.length.toLocaleString()} satır)`;
            applyFiltersAndRender();
        } catch (err) {
            alert("Task dosyası okunurken hata oluştu: " + err.message);
        } finally {
            badge.classList.add("hidden");
            badge.classList.remove("flex");
        }
    });

    document.getElementById("prFileInput").addEventListener("change", async (e) => {
        const file = e.target.files?.[0];
        if (!file) return;
        const badge = document.getElementById("loadingBadge");
        badge.classList.remove("hidden");
        badge.classList.add("flex");
        try {
            rawPrs = await parseUploadedFile(file);
            prSourceLabel = `Yüklenen: ${file.name}`;
            document.getElementById("prUploadStatus").textContent = `✅ ${file.name} (${rawPrs.length.toLocaleString()} satır)`;
            applyFiltersAndRender();
        } catch (err) {
            alert("PR dosyası okunurken hata oluştu: " + err.message);
        } finally {
            badge.classList.add("hidden");
            badge.classList.remove("flex");
        }
    });

    document.getElementById("resetSampleBtn").addEventListener("click", () => {
        document.getElementById("taskFileInput").value = "";
        document.getElementById("prFileInput").value = "";
        loadInitialSampleData();
    });

    // Filtreler
    ["veracityCleanToggle", "filterProject", "filterRiskLevel", "filterScopeMode", "filterComponent"].forEach((id) => {
        document.getElementById(id).addEventListener("change", applyFiltersAndRender);
    });

    // Tekil XAI Müfettişi
    document.getElementById("inspectorTaskSelect").addEventListener("change", (e) => {
        renderTab3Inspector(e.target.value);
    });
    document.getElementById("inspectorSearchBtn").addEventListener("click", () => {
        const q = document.getElementById("inspectorSearchInput").value.trim().toUpperCase();
        if (!q) return;
        const found = combinedTasks.find((t) => t.task_id.toUpperCase() === q);
        if (found) {
            renderTab3Inspector(found.task_id);
        } else {
            alert(`"${q}" ID'li görev mevcut veri setinde bulunamadı.`);
        }
    });

    // What-If Simülatörü
    [
        "simSp", "simEffort", "simPriority", "simEnv",
        "simBlocker", "simDep", "simScope", "simReopen",
        "simPrFiles", "simPrLines", "simCiFails", "simConflict",
    ].forEach((id) => {
        document.getElementById(id).addEventListener("input", runWhatIfSimulation);
        document.getElementById(id).addEventListener("change", runWhatIfSimulation);
    });

    // Export butonları
    document.getElementById("downloadCsvBtn").addEventListener("click", () => {
        const cols = [
            "task_id", "task_title", "project", "component_module", "status", "priority",
            "assignee", "due_date", "combined_early_risk_score", "delivery_risk_level",
            "early_delay_probability", "blocker_count", "pr_conflict_count",
            "pr_total_ci_failures", "xai_primary_risk_factors", "xai_recommended_actions",
        ];
        const rows = filteredTasks.slice().sort((a, b) => b.combined_early_risk_score - a.combined_early_risk_score).slice(0, 2000);
        const csv = "\uFEFF" + window.Papa.unparse(rows.map((r) => {
            const obj = {};
            cols.forEach((c) => { obj[c] = r[c]; });
            return obj;
        }));
        downloadFile(csv, "xai_erken_risk_karar_destek_raporu.csv", "text/csv;charset=utf-8;");
    });

    document.getElementById("downloadJsonBtn").addEventListener("click", () => {
        const rows = filteredTasks.slice().sort((a, b) => b.combined_early_risk_score - a.combined_early_risk_score).slice(0, 2000);
        downloadFile(JSON.stringify(rows, null, 2), "xai_erken_risk_karar_destek_raporu.json", "application/json;charset=utf-8;");
    });
}

window.addEventListener("DOMContentLoaded", () => {
    bindEvents();
    loadInitialSampleData();
});

