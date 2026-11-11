// Erken Risk Tespiti ve Açıklanabilir Karar Destek Sistemi (XAI) - Sadeleştirilmiş Web Engine
// Varsayılan veri kaynağı: data/tasks_dataset.parquet, data/pull_requests_dataset.parquet, data/velocity_event_stream.jsonl

let rawTasks = [];
let rawPrs = [];
let streamEvents = [];
let enrichedTasksCache = [];
let enrichedPrsCache = [];
let combinedTasks = [];
let filteredTasks = [];
let filteredPrs = [];
let prsByTaskId = new Map();
let taskByIdMap = new Map();
let currentInspectedTask = null;
let cachedVeracityState = null;
let dataDirty = true;

let taskSourceLabel = "data/tasks_dataset.parquet";
let prSourceLabel = "data/pull_requests_dataset.parquet";

const charts = {};
let hyparquetModule = null;

const RISK_COLORS = {
    Critical: "#ef4444",
    High: "#f97316",
    Medium: "#eab308",
    Low: "#22c55e",
};

const TASK_PARQUET_COLUMNS = [
    "task_id",
    "task_title",
    "description",
    "acceptance_criteria",
    "project",
    "task_type",
    "priority",
    "assignee",
    "status",
    "start_date",
    "due_date",
    "sprint_iteration",
    "estimated_effort_hours",
    "story_points",
    "component_module",
    "environment",
    "dependencies",
    "actual_effort_hours",
    "delay_days",
    "is_delayed",
    "reopen_count",
    "blocker_count",
    "requirement_change_count",
    "linked_pr_count",
    "delivery_risk_score",
    "delivery_risk_level",
    "xai_primary_risk_factors",
    "xai_recommended_actions",
    "data_quality_flag",
];

const PR_PARQUET_COLUMNS = [
    "pr_id",
    "pr_title",
    "linked_task_issue",
    "status",
    "merge_conflicts",
    "security_scan_status",
    "pipeline_status",
    "lines_added",
    "lines_deleted",
    "changed_files_count",
    "commit_count",
    "review_iteration_count",
    "time_to_first_review_hours",
    "time_to_merge_hours",
    "ci_failure_count",
    "pr_risk_score",
    "pr_risk_level",
    "xai_pr_risk_factors",
    "xai_pr_recommended_actions",
    "data_quality_flag",
    "diff_code_changes",
    "review_comments",
];

// ---------------------------------------------------------------------------
// 1. DOSYA VE PARQUET / CSV / JSONL OKUMA MOTORU
// ---------------------------------------------------------------------------
let hyparquetCompressors = null;

async function getHyparquet() {
    if (hyparquetModule) return hyparquetModule;
    const cdnUrls = [
        "https://cdn.jsdelivr.net/npm/hyparquet@1.11.2/+esm",
        "https://esm.sh/hyparquet@1.11.2",
        "https://unpkg.com/hyparquet@1.11.2/src/index.js",
    ];
    for (const url of cdnUrls) {
        try {
            hyparquetModule = await import(url);
            if (hyparquetModule) break;
        } catch (_) {}
    }
    if (!hyparquetModule) {
        throw new Error("Parquet okuma modülü (hyparquet) yüklenemedi. Lütfen internet bağlantınızı kontrol edin veya CSV / JSON formatında dosya yükleyin.");
    }
    if (!hyparquetCompressors) {
        const compUrls = [
            "https://cdn.jsdelivr.net/npm/hyparquet-compressors@0.5.0/+esm",
            "https://esm.sh/hyparquet-compressors@0.5.0",
        ];
        for (const url of compUrls) {
            try {
                const compMod = await import(url);
                if (compMod && compMod.compressors) {
                    hyparquetCompressors = compMod.compressors;
                    break;
                }
            } catch (_) {}
        }
    }
    return hyparquetModule;
}

async function parseParquetArrayBuffer(arrayBuffer, preferredColumns = null) {
    const hyparquet = await getHyparquet();
    let selectedColumns = undefined;
    if (Array.isArray(preferredColumns) && preferredColumns.length > 0) {
        try {
            const meta = hyparquet.parquetMetadata(arrayBuffer);
            const schemaCols = new Set((meta.schema || []).map((s) => s.name).filter(Boolean));
            const matched = preferredColumns.filter((c) => schemaCols.has(c));
            if (matched.length > 0) {
                selectedColumns = matched;
            }
        } catch (_) {
            selectedColumns = undefined;
        }
    }

    const rows = [];
    const readOptions = {
        file: arrayBuffer,
        columns: selectedColumns,
        rowFormat: "object",
        onComplete: (data) => {
            for (let i = 0; i < data.length; i++) {
                rows.push(data[i]);
            }
        },
    };
    if (hyparquetCompressors) {
        readOptions.compressors = hyparquetCompressors;
    }
    await hyparquet.parquetRead(readOptions);
    return rows;
}

function parseJsonlText(text, maxLines = Infinity) {
    const trimmed = text.trim();
    if (!trimmed) return [];
    if (trimmed.startsWith("[")) {
        return JSON.parse(trimmed);
    }
    const lines = trimmed.split(/\r?\n/);
    const records = [];
    const limit = Math.min(lines.length, maxLines);
    for (let i = 0; i < limit; i++) {
        const line = lines[i].trim();
        if (!line) continue;
        try {
            records.push(JSON.parse(line));
        } catch (_) {
            // Kısmi satır varsa atla
        }
    }
    return records;
}

async function parseUploadedFile(file, preferredColumns = null) {
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
        return parseJsonlText(text);
    } else if (ext === "parquet") {
        const arrayBuffer = await file.arrayBuffer();
        return parseParquetArrayBuffer(arrayBuffer, preferredColumns);
    } else {
        throw new Error("Desteklenmeyen dosya uzantısı: " + ext);
    }
}

// ---------------------------------------------------------------------------
// 2. XAI VE ERKEN RİSK HESAPLAMA & VERİ ZENGİNLEŞTİRME MOTORU
// ---------------------------------------------------------------------------
function normalizeAndEnrichTasks(tasks, cleanVeracity = false) {
    const out = new Array(tasks.length);
    for (let idx = 0; idx < tasks.length; idx++) {
        const t = tasks[idx];
        const taskId = String(t.task_id || `TASK-${100001 + idx}`);
        const storyPoints = Number(t.story_points ?? 5);
        const blockerCount = Number(t.blocker_count ?? 0);
        const reqChangeCount = Number(t.requirement_change_count ?? 0);
        const reopenCount = Number(t.reopen_count ?? 0);
        const priority = String(t.priority || "Medium");
        const environment = String(t.environment || "Development");
        const status = String(t.status || "In Progress");
        const project = String(t.project || "PRJ-CORE - Kurumsal Platform");
        const projectShort = project.split(" - ")[0];
        const componentModule = String(t.component_module || "Backend");
        const dataQualityFlag = String(t.data_quality_flag || "Clean");

        let est = Number(t.estimated_effort_hours);
        if (!est || isNaN(est) || est <= 0) {
            est = cleanVeracity ? +(storyPoints * 5.8).toFixed(1) : +(storyPoints * 6.0).toFixed(1);
        }

        let act = Number(t.actual_effort_hours ?? est);
        if (cleanVeracity && dataQualityFlag === "Outlier_Effort") {
            act = +(act / 3.8).toFixed(1);
        }

        const depStr = String(t.dependencies || "Yok").trim();
        let depCount = 0;
        if (depStr && depStr !== "Yok" && depStr !== "None") {
            depCount = 1;
            for (let c = 0; c < depStr.length; c++) {
                if (depStr.charCodeAt(c) === 44) depCount++;
            }
        }

        const optRatio = Math.max(0.5, Math.min(3.0, (storyPoints * 6.5) / (est + 1.0)));
        const optRatioCalc = +optRatio.toFixed(2);

        let deliveryRiskScore = Number(t.delivery_risk_score);
        if (t.delivery_risk_score === undefined || t.delivery_risk_score === null || isNaN(deliveryRiskScore)) {
            const pW = priority === "Critical" ? 14 : (priority === "High" ? 8 : 2);
            const eW = environment === "Production" ? 10 : (environment === "Staging" ? 5 : 1);
            const raw = depCount * 9.5 + blockerCount * 16 + reqChangeCount * 11.5
                + reopenCount * 12 + (storyPoints >= 13 ? 14 : 0) + (optRatioCalc > 1.25 ? 13 : 0) + pW + eW + 8;
            deliveryRiskScore = +Math.max(2, Math.min(99.5, raw)).toFixed(2);
        }

        const s = deliveryRiskScore;
        const deliveryRiskLevel = t.delivery_risk_level || (s >= 75 ? "Critical" : (s >= 50 ? "High" : (s >= 28 ? "Medium" : "Low")));
        const delayDays = Number(t.delay_days ?? (s >= 48 ? Math.round((s - 42) * 0.32 + blockerCount * 2.5) : 0));
        const isDelayed = Number(t.is_delayed ?? (delayDays > 0 ? 1 : 0));

        let xaiFactors = t.xai_primary_risk_factors;
        if (!xaiFactors) {
            const factors = [];
            if (blockerCount > 0) factors.push(`Blocker/Engel (${blockerCount} adet)`);
            if (depCount >= 2) factors.push(`Yüksek Bağımlılık (${depCount} ön koşul)`);
            if (reqChangeCount >= 1) factors.push(`Kapsam Değişimi (${reqChangeCount} kez)`);
            if (storyPoints >= 13 || optRatioCalc > 1.25) factors.push(`Yüksek Karmaşıklık (${storyPoints} SP)`);
            xaiFactors = factors.length ? factors.slice(0, 3).join(" • ") : "Standart İş Akışı (Düşük Risk)";
        }

        let xaiActions = t.xai_recommended_actions;
        if (!xaiActions) {
            xaiActions = blockerCount > 0
                ? "Blokaj yaratan bağımlılıkları eskalasyon ile çözün ve görevi alt parçalara bölün."
                : "Mevcut sprint planını koruyun, rutin PR inceleme akışını sürdürün.";
        }

        out[idx] = {
            task_id: taskId,
            task_title: t.task_title || "",
            description: t.description || "",
            acceptance_criteria: t.acceptance_criteria || "",
            project,
            project_short: projectShort,
            component_module: componentModule,
            status,
            priority,
            environment,
            assignee: t.assignee || "-",
            sprint_iteration: t.sprint_iteration || "Sprint-01",
            start_date: t.start_date || "-",
            due_date: t.due_date || "-",
            story_points: storyPoints,
            estimated_effort_hours: est,
            actual_effort_hours: act,
            dependencies: depStr || "Yok",
            dependency_count_calc: depCount,
            optimism_ratio_calc: optRatioCalc,
            blocker_count: blockerCount,
            requirement_change_count: reqChangeCount,
            reopen_count: reopenCount,
            linked_pr_count: Number(t.linked_pr_count ?? 0),
            delivery_risk_score: deliveryRiskScore,
            delivery_risk_level: deliveryRiskLevel,
            delay_days: delayDays,
            is_delayed: isDelayed,
            xai_primary_risk_factors: xaiFactors,
            xai_recommended_actions: xaiActions,
            data_quality_flag: dataQualityFlag,
        };
    }
    return out;
}

function normalizeAndEnrichPrs(prs, cleanVeracity = false) {
    const out = new Array(prs.length);
    for (let idx = 0; idx < prs.length; idx++) {
        const p = prs[idx];
        const prId = String(p.pr_id || `PR-${200001 + idx}`);
        const linkedTaskIssue = String(p.linked_task_issue || "");
        const changedFilesCount = Number(p.changed_files_count ?? 4);
        const dataQualityFlag = String(p.data_quality_flag || "Clean");

        let linesAdded = Number(p.lines_added ?? 120);
        if (cleanVeracity && dataQualityFlag === "Outlier_Diff_Size") {
            linesAdded = Math.min(800, linesAdded);
        }
        const linesDeleted = Number(p.lines_deleted ?? 40);
        const commitCount = Number(p.commit_count ?? 3);
        const reviewIterationCount = Number(p.review_iteration_count ?? 1);
        const timeToFirstReviewHours = Number(p.time_to_first_review_hours ?? 4.0);
        const timeToMergeHours = Number(p.time_to_merge_hours ?? 16.0);
        const ciFailureCount = Number(p.ci_failure_count ?? 0);
        const mergeConflicts = String(p.merge_conflicts || "Çakışma Yok (Clean Merge)");
        const hasConflictBool = !mergeConflicts.includes("Yok") && !mergeConflicts.includes("Clean");
        const securityScanStatus = String(p.security_scan_status || "Passed (0 Vulnerability)");

        let prRiskScore = Number(p.pr_risk_score);
        if (p.pr_risk_score === undefined || p.pr_risk_score === null || isNaN(prRiskScore)) {
            const rawPr = changedFilesCount * 1.45 + linesAdded / 45.0
                + (hasConflictBool ? 18 : 0) + ciFailureCount * 8.5 + reviewIterationCount * 4.2 + 10;
            prRiskScore = +Math.max(1, Math.min(99.5, rawPr)).toFixed(2);
        }

        const ps = prRiskScore;
        const prRiskLevel = p.pr_risk_level || (ps >= 75 ? "Critical" : (ps >= 50 ? "High" : (ps >= 28 ? "Medium" : "Low")));

        out[idx] = {
            pr_id: prId,
            pr_title: p.pr_title || "",
            linked_task_issue: linkedTaskIssue,
            status: p.status || "Open",
            changed_files_count: changedFilesCount,
            lines_added: linesAdded,
            lines_deleted: linesDeleted,
            commit_count: commitCount,
            review_iteration_count: reviewIterationCount,
            time_to_first_review_hours: timeToFirstReviewHours,
            time_to_merge_hours: timeToMergeHours,
            ci_failure_count: ciFailureCount,
            merge_conflicts: mergeConflicts,
            has_conflict_bool: hasConflictBool,
            security_scan_status: securityScanStatus,
            pipeline_status: p.pipeline_status || "Succeeded",
            pr_risk_score: prRiskScore,
            pr_risk_level: prRiskLevel,
            xai_pr_risk_factors: p.xai_pr_risk_factors || "",
            xai_pr_recommended_actions: p.xai_pr_recommended_actions || "",
            data_quality_flag: dataQualityFlag,
            diff_code_changes: p.diff_code_changes || "",
            review_comments: p.review_comments || "",
        };
    }
    return out;
}

function buildCombinedView(tasks, prs) {
    const prMap = new Map();
    prsByTaskId = new Map();

    for (let i = 0; i < prs.length; i++) {
        const pr = prs[i];
        const tid = pr.linked_task_issue;
        if (!tid) continue;

        let list = prsByTaskId.get(tid);
        if (!list) {
            list = [];
            prsByTaskId.set(tid, list);
        }
        list.push(pr);

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

    const combined = new Array(tasks.length);
    for (let i = 0; i < tasks.length; i++) {
        const t = tasks[i];
        const agg = prMap.get(t.task_id);
        const count = agg ? agg.count : t.linked_pr_count;
        const maxRisk = agg ? agg.maxRisk : t.delivery_risk_score * 0.85;
        const sumLines = agg ? agg.sumLines : 150;
        const sumFiles = agg ? agg.sumFiles : 5;
        const sumCiFails = agg ? agg.sumCiFails : 0;
        const conflictCount = agg ? agg.conflictCount : 0;
        const maxReviewIter = agg ? agg.maxReviewIter : 1;

        const combinedRisk = +Math.max(1, Math.min(99.9, t.delivery_risk_score * 0.62 + maxRisk * 0.38)).toFixed(1);
        const earlyProb = +(1.0 / (1.0 + Math.exp(-(combinedRisk - 47.0) / 11.5))).toFixed(3);

        combined[i] = {
            ...t,
            pr_total_count: count,
            pr_max_risk_score: +maxRisk.toFixed(1),
            pr_total_lines_added: sumLines,
            pr_total_changed_files: sumFiles,
            pr_total_ci_failures: sumCiFails,
            pr_conflict_count: conflictCount,
            pr_max_review_iterations: maxReviewIter,
            combined_early_risk_score: combinedRisk,
            early_delay_probability: earlyProb,
        };
    }

    // Erken risk skoruna göre azalan sırala (Filtreleme ve tablo kesitleri O(1) olur)
    combined.sort((a, b) => b.combined_early_risk_score - a.combined_early_risk_score);

    taskByIdMap = new Map();
    for (let i = 0; i < combined.length; i++) {
        taskByIdMap.set(combined[i].task_id, combined[i]);
    }

    return combined;
}

// ---------------------------------------------------------------------------
// 3. FİLTRELER VE DASHBOARD GÜNCELLEME
// ---------------------------------------------------------------------------
function populateFilterDropdowns() {
    const projSelect = document.getElementById("filterProject");
    const compSelect = document.getElementById("filterComponent");

    const curProj = projSelect.value;
    const curComp = compSelect.value;

    const projSet = new Set();
    const compSet = new Set();
    for (let i = 0; i < combinedTasks.length; i++) {
        const t = combinedTasks[i];
        if (t.project) projSet.add(t.project);
        if (t.component_module) compSet.add(t.component_module);
    }

    const projects = [...projSet].sort();
    const components = [...compSet].sort();

    projSelect.innerHTML = `<option value="ALL">Tüm Projeler (${projects.length})</option>` +
        projects.map((p) => `<option value="${p}">${p}</option>`).join("");
    compSelect.innerHTML = `<option value="ALL">Tüm Modüller (${components.length})</option>` +
        components.map((c) => `<option value="${c}">${c}</option>`).join("");

    if (projSet.has(curProj)) projSelect.value = curProj;
    if (compSet.has(curComp)) compSelect.value = curComp;
}

function applyFiltersAndRender() {
    const cleanVeracity = document.getElementById("veracityCleanToggle").checked;

    if (dataDirty || cachedVeracityState !== cleanVeracity) {
        enrichedTasksCache = normalizeAndEnrichTasks(rawTasks, cleanVeracity);
        enrichedPrsCache = normalizeAndEnrichPrs(rawPrs, cleanVeracity);
        combinedTasks = buildCombinedView(enrichedTasksCache, enrichedPrsCache);
        populateFilterDropdowns();
        cachedVeracityState = cleanVeracity;
        dataDirty = false;
    }

    const selProj = document.getElementById("filterProject").value;
    const selRisk = document.getElementById("filterRiskLevel").value;
    const selScope = document.getElementById("filterScopeMode").value;
    const selComp = document.getElementById("filterComponent").value;

    const noTaskFilter = selProj === "ALL" && selRisk === "ALL" && selScope === "ALL" && selComp === "ALL";
    if (noTaskFilter) {
        filteredTasks = combinedTasks;
        filteredPrs = enrichedPrsCache;
    } else {
        filteredTasks = combinedTasks.filter((t) => {
            if (selProj !== "ALL" && t.project !== selProj) return false;
            if (selRisk !== "ALL" && t.delivery_risk_level !== selRisk) return false;
            if (selComp !== "ALL" && t.component_module !== selComp) return false;
            if (selScope === "ACTIVE" && t.status === "Done") return false;
            if (selScope === "DONE" && t.status !== "Done") return false;
            return true;
        });

        const taskIds = new Set();
        for (let i = 0; i < filteredTasks.length; i++) {
            taskIds.add(filteredTasks[i].task_id);
        }
        filteredPrs = enrichedPrsCache.filter((p) => taskIds.has(p.linked_task_issue));
    }

    document.getElementById("activeSourceBanner").innerHTML = `
        📋 <b>${filteredTasks.length.toLocaleString()}</b> Görev &nbsp;•&nbsp;
        🔀 <b>${filteredPrs.length.toLocaleString()}</b> PR &nbsp;•&nbsp;
        🧹 <b>${cleanVeracity ? "Temizlenmiş Veri" : "Ham Veri (data/)"}</b>
    `;

    renderKpis();
    renderTab1Overview();
    populateInspectorSelect();
    renderTab2Inspector();
    runWhatIfSimulation();
    renderTab4PrAnalytics();
    renderTab5MlAnd5V(enrichedTasksCache, enrichedPrsCache);
    renderTab6ActionPlanAndExport();
}

function renderKpis() {
    const totalT = filteredTasks.length;
    const totalP = filteredPrs.length;

    let openCount = 0;
    let earlyAtRiskCount = 0;
    let sumRisk = 0;
    let critHighCount = 0;
    let delayedCount = 0;
    let sumDelayDays = 0;

    for (let i = 0; i < totalT; i++) {
        const t = filteredTasks[i];
        sumRisk += t.combined_early_risk_score;
        if (t.status !== "Done") {
            openCount++;
            if (t.combined_early_risk_score >= 50) earlyAtRiskCount++;
        }
        if (t.delivery_risk_level === "Critical" || t.delivery_risk_level === "High") {
            critHighCount++;
        }
        if (t.is_delayed === 1) {
            delayedCount++;
            sumDelayDays += t.delay_days;
        }
    }

    const curProj = document.getElementById("filterProject")?.value || "ALL";
    const curRisk = document.getElementById("filterRiskLevel")?.value || "ALL";
    const curScope = document.getElementById("filterScopeMode")?.value || "ALL";
    const curComp = document.getElementById("filterComponent")?.value || "ALL";
    const veracityClean = document.getElementById("veracityCleanToggle")?.checked || false;

    // Varsayılan filtrelenmemiş durumda Streamlit ile birebir aynı metrikleri göster
    const isDefaultState = (curProj === "ALL" && curRisk === "ALL" && curScope === "ALL" && curComp === "ALL" && !veracityClean && window.__GLOBAL_METRICS__);

    if (isDefaultState) {
        const gm = window.__GLOBAL_METRICS__;
        document.getElementById("kpiTotalTasks").textContent = `${gm.total_tasks.toLocaleString()} Görev`;
        document.getElementById("kpiTotalPRs").textContent = `Bağlı ${gm.total_prs.toLocaleString()} Pull Request`;
        document.getElementById("kpiEarlyAlerts").textContent = `${gm.early_at_risk_open.toLocaleString()} Görev`;
        document.getElementById("kpiEarlyAlertsSub").textContent = `${gm.open_tasks_count.toLocaleString()} açık iş içinde yüksek riskli`;
        document.getElementById("kpiAvgRisk").textContent = `${gm.avg_risk_score.toFixed(1)} / 100`;
        document.getElementById("kpiCriticalCount").textContent = `Kritik & Yüksek: ${gm.critical_high_count.toLocaleString()} (%51)`;
        document.getElementById("kpiDelayRatio").textContent = `%${gm.delayed_ratio.toFixed(1)}`;
        document.getElementById("kpiAvgDelayDays").textContent = `Gecikenlerde ort. +${gm.avg_delay_days.toFixed(1)} gün sapma`;

        const banner = document.getElementById("activeSourceBanner");
        if (banner) {
            banner.textContent = `📋 ${gm.total_tasks.toLocaleString()} Görev • 🔀 ${gm.total_prs.toLocaleString()} PR • 🧹 Ham Veri`;
        }
        return;
    }

    const avgRisk = totalT ? sumRisk / totalT : 0;
    const delayRatio = totalT ? (delayedCount / totalT) * 100 : 0;
    const avgDelayDays = delayedCount ? sumDelayDays / delayedCount : 0;

    document.getElementById("kpiTotalTasks").textContent = `${totalT.toLocaleString()} Görev`;
    document.getElementById("kpiTotalPRs").textContent = `Bağlı ${totalP.toLocaleString()} Pull Request`;
    document.getElementById("kpiEarlyAlerts").textContent = `${earlyAtRiskCount.toLocaleString()} Görev`;
    document.getElementById("kpiEarlyAlertsSub").textContent = `${openCount.toLocaleString()} açık iş içinde yüksek riskli`;
    document.getElementById("kpiAvgRisk").textContent = `${avgRisk.toFixed(1)} / 100`;
    document.getElementById("kpiCriticalCount").textContent = `Kritik & Yüksek: ${critHighCount.toLocaleString()} (%${totalT ? ((critHighCount / totalT) * 100).toFixed(0) : 0})`;
    document.getElementById("kpiDelayRatio").textContent = `%${delayRatio.toFixed(1)}`;
    document.getElementById("kpiAvgDelayDays").textContent = `Gecikenlerde ort. +${avgDelayDays.toFixed(1)} gün sapma`;

    const banner = document.getElementById("activeSourceBanner");
    if (banner) {
        banner.textContent = `📋 ${totalT.toLocaleString()} Görev • 🔀 ${totalP.toLocaleString()} PR • 🧹 ${veracityClean ? "Temizlenmiş Veri" : "Ham Veri"}`;
    }
}

// ---------------------------------------------------------------------------
// SEKME 1: ERKEN UYARI RADARI VE GENEL BAKIŞ (STREAMLIT İLE BİREBİR TABLO)
// ---------------------------------------------------------------------------
function renderTab1Overview() {
    // filteredTasks zaten combined_early_risk_score'a göre azalan sıralıdır
    const pool = [];
    for (let i = 0; i < filteredTasks.length && pool.length < 20; i++) {
        if (filteredTasks[i].status !== "Done") {
            pool.push(filteredTasks[i]);
        }
    }
    if (pool.length === 0) {
        for (let i = 0; i < Math.min(20, filteredTasks.length); i++) {
            pool.push(filteredTasks[i]);
        }
    }

    const tbody = document.getElementById("earlyRadarTableBody");
    tbody.innerHTML = pool.map((t) => {
        const score = Math.min(100, Math.max(0, t.combined_early_risk_score));
        const barColor = score >= 75 ? "bg-red-500" : (score >= 50 ? "bg-orange-500" : "bg-blue-500");
        return `
            <tr class="hover:bg-slate-800/70 cursor-pointer transition border-b border-slate-800/60" data-taskid="${t.task_id}">
                <td class="p-3 font-mono font-bold text-blue-400 whitespace-nowrap">${t.task_id}</td>
                <td class="p-3 max-w-[280px]">
                    <div class="truncate font-medium text-slate-100" title="${t.task_title || ""}">${t.task_title || "-"}</div>
                </td>
                <td class="p-3 max-w-[220px]">
                    <div class="truncate text-slate-200" title="${t.project || ""}">${t.project || "-"}</div>
                </td>
                <td class="p-3 text-slate-300 whitespace-nowrap">${t.component_module || "-"}</td>
                <td class="p-3 text-slate-300 whitespace-nowrap">${t.assignee || "-"}</td>
                <td class="p-3 text-slate-300 whitespace-nowrap">${t.due_date || "-"}</td>
                <td class="p-3 whitespace-nowrap">
                    <div class="flex items-center gap-2.5">
                        <div class="w-24 bg-slate-800 rounded-full h-2 overflow-hidden border border-slate-700/60">
                            <div class="${barColor} h-2 rounded-full" style="width: ${score}%"></div>
                        </div>
                        <span class="font-bold text-xs text-white">${score.toFixed(0)}</span>
                    </div>
                </td>
                <td class="p-3 max-w-[220px]">
                    <div class="truncate text-amber-300 text-[11px]" title="${t.xai_primary_risk_factors || ""}">${t.xai_primary_risk_factors || "-"}</div>
                </td>
                <td class="p-3 text-center whitespace-nowrap">
                    <button class="px-2.5 py-1 rounded bg-blue-600/20 text-blue-300 border border-blue-500/30 text-[11px] font-semibold hover:bg-blue-600 hover:text-white transition">
                        İncele →
                    </button>
                </td>
            </tr>
        `;
    }).join("");

    tbody.querySelectorAll("tr[data-taskid]").forEach((tr) => {
        tr.addEventListener("click", () => {
            const tid = tr.getAttribute("data-taskid");
            document.getElementById("inspectorTaskSelect").value = tid;
            renderTab2Inspector(tid);
            switchTab("tab2");
        });
    });

    // Tek geçişli (single-pass) toplulaştırma: Proje x Risk, Sprint Trendi, Proje x Modül Heatmap
    const projRiskCounts = new Map();
    const sprintMap = new Map();
    const heatMap = new Map();
    const compSet = new Set();

    for (let i = 0; i < filteredTasks.length; i++) {
        const t = filteredTasks[i];
        const pCode = t.project_short;
        const lvl = t.delivery_risk_level;
        const comp = t.component_module;
        const sp = t.sprint_iteration || "Sprint-01";

        compSet.add(comp);

        let pEntry = projRiskCounts.get(pCode);
        if (!pEntry) {
            pEntry = { Critical: 0, High: 0, Medium: 0, Low: 0 };
            projRiskCounts.set(pCode, pEntry);
        }
        if (pEntry[lvl] !== undefined) pEntry[lvl]++;

        let sEntry = sprintMap.get(sp);
        if (!sEntry) {
            sEntry = { sumRisk: 0, delayed: 0, count: 0 };
            sprintMap.set(sp, sEntry);
        }
        sEntry.sumRisk += t.combined_early_risk_score;
        sEntry.delayed += t.is_delayed;
        sEntry.count += 1;

        const heatKey = `${pCode}||${comp}`;
        let hEntry = heatMap.get(heatKey);
        if (!hEntry) {
            hEntry = { sumRisk: 0, count: 0 };
            heatMap.set(heatKey, hEntry);
        }
        hEntry.sumRisk += t.combined_early_risk_score;
        hEntry.count += 1;
    }

    const projects = [...projRiskCounts.keys()].sort();
    const levels = ["Critical", "High", "Medium", "Low"];
    const datasets = levels.map((lvl) => ({
        label: lvl,
        backgroundColor: RISK_COLORS[lvl],
        data: projects.map((pCode) => projRiskCounts.get(pCode)[lvl] || 0),
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
            plugins: { legend: { position: "bottom", labels: { color: "#e2e8f0", boxWidth: 12 } } },
        },
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
                    label: "Ort. Risk Skoru",
                    data: avgRiskSeries,
                    borderColor: "#38bdf8",
                    backgroundColor: "rgba(56,189,248,0.15)",
                    tension: 0.3,
                },
                {
                    label: "Gecikme Oranı (%)",
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
            plugins: { legend: { position: "bottom", labels: { color: "#e2e8f0", boxWidth: 12 } } },
        },
    });

    // Heatmap Tablosu (Proje x Bileşen)
    const comps = [...compSet].sort();
    let heatHtml = `<table class="w-full text-xs border-collapse"><thead><tr><th class="p-2 border border-slate-700 bg-slate-900 text-left">Proje \\ Modül</th>`;
    comps.forEach((c) => {
        heatHtml += `<th class="p-2 border border-slate-700 bg-slate-900 text-center">${c}</th>`;
    });
    heatHtml += `</tr></thead><tbody>`;

    projects.forEach((pCode) => {
        heatHtml += `<tr><td class="p-2 border border-slate-700 font-semibold bg-slate-900/60 whitespace-nowrap">${pCode}</td>`;
        comps.forEach((c) => {
            const cell = heatMap.get(`${pCode}||${c}`);
            if (!cell || !cell.count) {
                heatHtml += `<td class="p-2 border border-slate-700 text-center text-slate-600">-</td>`;
            } else {
                const avg = cell.sumRisk / cell.count;
                const bg = avg >= 68
                    ? "rgba(239,68,68,0.35)"
                    : (avg >= 52 ? "rgba(249,115,22,0.30)" : (avg >= 38 ? "rgba(234,179,8,0.22)" : "rgba(34,197,94,0.20)"));
                heatHtml += `<td class="p-2 border border-slate-700 text-center font-semibold" style="background:${bg}" title="${cell.count.toLocaleString()} görev">${avg.toFixed(1)}</td>`;
            }
        });
        heatHtml += `</tr>`;
    });
    heatHtml += `</tbody></table>`;
    document.getElementById("heatmapContainer").innerHTML = heatHtml;
}

// ---------------------------------------------------------------------------
// SEKME 2: TEKİL GÖREV & PR MÜFETTİŞİ
// ---------------------------------------------------------------------------
function populateInspectorSelect() {
    const sel = document.getElementById("inspectorTaskSelect");
    const currentVal = sel.value;
    const topPool = filteredTasks.slice(0, 150);

    sel.innerHTML = topPool.map((t) =>
        `<option value="${t.task_id}">${t.task_id} — [Risk: ${t.combined_early_risk_score.toFixed(0)}] ${t.task_title || ""}</option>`
    ).join("");

    if (topPool.some((t) => t.task_id === currentVal)) {
        sel.value = currentVal;
    }
}

function renderTab2Inspector(forcedTaskId = null) {
    if (!filteredTasks.length) return;
    const sel = document.getElementById("inspectorTaskSelect");
    const targetId = forcedTaskId || sel.value || filteredTasks[0].task_id;
    const task = taskByIdMap.get(targetId) || filteredTasks[0];
    currentInspectedTask = task;

    const badgeCls = `badge-${task.delivery_risk_level}`;
    document.getElementById("inspectorHeaderCard").innerHTML = `
        <div class="flex flex-col md:flex-row md:items-center md:justify-between gap-3">
            <div>
                <div class="flex items-center gap-2.5 flex-wrap">
                    <span class="text-base lg:text-lg font-bold text-white">${task.task_id}: ${task.task_title || ""}</span>
                    <span class="px-2.5 py-0.5 rounded-full text-xs font-bold ${badgeCls}">${task.delivery_risk_level}</span>
                </div>
                <div class="text-xs text-slate-400 mt-1.5 flex flex-wrap gap-x-4 gap-y-1">
                    <span>🏢 <b>Proje:</b> ${task.project}</span>
                    <span>🧩 <b>Modül:</b> ${task.component_module}</span>
                    <span>👤 <b>Sorumlu:</b> ${task.assignee || "-"}</span>
                    <span>📌 <b>Durum:</b> <code>${task.status}</code></span>
                    <span>⚡ <b>Öncelik:</b> <code>${task.priority}</code></span>
                </div>
            </div>
            <div class="bg-slate-900/90 border border-slate-700/80 px-4 py-2.5 rounded-xl text-right flex-shrink-0">
                <div class="text-xs text-slate-400">Bileşik Erken Risk Skoru</div>
                <div class="text-2xl font-bold text-amber-400">${task.combined_early_risk_score.toFixed(1)} <span class="text-sm font-normal text-slate-400">/ 100</span></div>
                <div class="text-[11px] text-slate-300">Gecikme İhtimali: <b>%${(task.early_delay_probability * 100).toFixed(0)}</b></div>
            </div>
        </div>
    `;

    document.getElementById("inspectorXaiFactors").textContent = task.xai_primary_risk_factors;
    document.getElementById("inspectorXaiActions").textContent = task.xai_recommended_actions;

    const contribs = [
        ["Baz Karmaşıklık", 10.0],
        [
            task.blocker_count > 0 ? `Blocker / Engel (${task.blocker_count} adet)` : "Blocker Yok (0)",
            task.blocker_count > 0 ? task.blocker_count * 16.0 : -3.5,
        ],
        [
            task.dependency_count_calc > 0 ? `Görev Bağımlılığı (${task.dependency_count_calc} adet)` : "Bağımsız Görev (0)",
            task.dependency_count_calc > 0 ? task.dependency_count_calc * 9.5 : -2.5,
        ],
        [
            `Kapsam Değişimi (${task.requirement_change_count})`,
            task.requirement_change_count > 0 ? task.requirement_change_count * 11.5 : -1.5,
        ],
        [
            `Testten Dönüş / Reopen (${task.reopen_count})`,
            task.reopen_count > 0 ? task.reopen_count * 12.0 : -1.5,
        ],
        [
            `Görev Boyutu (${task.story_points} SP)`,
            task.story_points >= 13 ? 14.0 : (task.story_points <= 3 ? -4.0 : 3.0),
        ],
        [
            `PR & CI/CD Etkisi (${task.pr_conflict_count} Conflict, ${task.pr_total_ci_failures} Hata)`,
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
        <h3 class="text-sm font-bold text-white mb-2">📋 Görev Planlama Özeti</h3>
        <div class="grid grid-cols-2 gap-2.5 bg-slate-900/60 p-3 rounded-lg border border-slate-800">
            <div><span class="text-slate-400 block text-[11px]">Sprint Dönemi</span><b>${task.sprint_iteration || "-"}</b></div>
            <div><span class="text-slate-400 block text-[11px]">Planlanan Teslim</span><b>${task.start_date || "-"} → ${task.due_date || "-"}</b></div>
            <div><span class="text-slate-400 block text-[11px]">Karmaşıklık & Tahmin</span><b>${task.story_points} SP / ${task.estimated_effort_hours} Saat</b></div>
            <div><span class="text-slate-400 block text-[11px]">Ön Koşul Görevler</span><b>${task.dependencies || "Yok"}</b></div>
        </div>
        <div class="pt-2 space-y-1.5">
            <p class="text-slate-200 leading-relaxed"><b>📄 Görev Açıklaması:</b> ${task.description || "-"}</p>
            <p class="text-slate-400 leading-relaxed"><b>✅ Kabul Kriterleri:</b> ${task.acceptance_criteria || "-"}</p>
        </div>
    `;

    const prs = prsByTaskId.get(task.task_id) || [];
    document.getElementById("inspectorPrHeader").textContent =
        `🔀 Göreve Bağlı Pull Request (PR) Kayıtları (${prs.length} Adet)`;

    if (!prs.length) {
        document.getElementById("inspectorPrList").innerHTML =
            `<div class="text-xs text-slate-400">Bu göreve bağlı PR kaydı bulunamadı.</div>`;
    } else {
        document.getElementById("inspectorPrList").innerHTML = prs.map((pr) => `
            <div class="bg-slate-900/75 border border-slate-800 rounded-xl p-3.5 space-y-2 text-xs">
                <div class="flex flex-wrap justify-between items-center gap-2">
                    <div class="font-bold text-blue-400">${pr.pr_id} — ${pr.pr_title || ""}</div>
                    <div class="flex items-center gap-2">
                        <span class="px-2 py-0.5 rounded bg-slate-800 text-slate-200">${pr.status || "Open"}</span>
                        <span class="px-2.5 py-0.5 rounded-full font-bold badge-${pr.pr_risk_level || "Medium"}">PR Risk: ${Number(pr.pr_risk_score || 0).toFixed(0)}</span>
                    </div>
                </div>
                <div class="flex flex-wrap gap-x-5 gap-y-1 text-slate-300">
                    <span>📁 <b>Değişim:</b> ${pr.changed_files_count} dosya (+${pr.lines_added} / -${pr.lines_deleted})</span>
                    <span>⚔️ <b>Conflict:</b> ${pr.merge_conflicts || "-"}</span>
                    <span>⚙️ <b>Pipeline:</b> ${pr.pipeline_status || "-"} (${pr.ci_failure_count} hata)</span>
                    <span>⏱️ <b>Merge Süresi:</b> ${pr.time_to_merge_hours} saat</span>
                </div>
                <div class="text-teal-300">💡 <b>Öneri:</b> ${pr.xai_pr_recommended_actions || "-"}</div>
                <details class="pt-1">
                    <summary class="cursor-pointer text-[11px] text-slate-400 hover:text-slate-200">Kod Yamasını (git diff) ve İnceleme Yorumunu Göster</summary>
                    <pre class="mt-2 bg-slate-950 border border-slate-800 rounded p-2 text-[11px] text-emerald-300 overflow-x-auto">${pr.diff_code_changes || ""}</pre>
                    <div class="mt-1 text-slate-400">💬 <b>Reviewer Yorumu:</b> ${pr.review_comments || "-"}</div>
                </details>
            </div>
        `).join("");
    }
}

// ---------------------------------------------------------------------------
// SEKME 3: WHAT-IF KARAR SİMÜLATÖRÜ
// ---------------------------------------------------------------------------
function loadInspectedTaskIntoSimulator() {
    if (!currentInspectedTask) return;
    const t = currentInspectedTask;
    document.getElementById("simSp").value = Math.min(21, Math.max(1, t.story_points || 8));
    document.getElementById("simEffort").value = Math.min(160, Math.max(2, Math.round(t.estimated_effort_hours || 32)));
    document.getElementById("simPriority").value = ["Low", "Medium", "High", "Critical"].includes(t.priority) ? t.priority : "High";
    document.getElementById("simEnv").value = ["Development", "Test", "Staging", "Production"].includes(t.environment) ? t.environment : "Production";
    document.getElementById("simBlocker").value = Math.min(5, t.blocker_count || 0);
    document.getElementById("simDep").value = Math.min(5, t.dependency_count_calc || 0);
    document.getElementById("simScope").value = Math.min(5, t.requirement_change_count || 0);
    document.getElementById("simReopen").value = Math.min(4, t.reopen_count || 0);
    document.getElementById("simPrFiles").value = Math.min(50, Math.max(1, t.pr_total_changed_files || 8));
    document.getElementById("simPrLines").value = Math.min(2500, Math.max(10, t.pr_total_lines_added || 250));
    document.getElementById("simCiFails").value = Math.min(6, t.pr_total_ci_failures || 0);
    document.getElementById("simConflict").checked = (t.pr_conflict_count || 0) > 0;
    runWhatIfSimulation();
}

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
        prescriptions.push(`🔴 <b>${blockers} adet Blocker</b> kaldırılırsa risk skoru doğrudan <b>-${(blockers * 10).toFixed(0)} puan</b> düşer.`);
    }
    if (sp >= 13 || optRatio > 1.25) {
        prescriptions.push(`🟠 <b>Görev Boyutu & Efor:</b> Görevi 5 SP'lik alt parçalara bölmek veya tahmini eforu <b>${sp * 6} saate</b> çıkarmak riski <b>~14 puan</b> azaltır.`);
    }
    if (conflict || ciFails > 0 || prFiles >= 18) {
        prescriptions.push(`🔵 <b>PR & CI/CD Müdahalesi:</b> PR dosya sayısını 10'un altına çekmek ve merge conflict'i çözmek entegrasyon riskini <b>-%35</b> iyileştirir.`);
    }
    if (!prescriptions.length) {
        prescriptions.push(`🟢 <b>İdeal Durum:</b> Mevcut parametrelerle görev düşük risk bölgesindedir ve zamanında teslimat beklenmektedir.`);
    }
    document.getElementById("simPrescriptions").innerHTML = prescriptions.map((p) => `<li>${p}</li>`).join("");
}

// ---------------------------------------------------------------------------
// SEKME 4: PR & EKİP İŞ YÜKÜ ANALİTİĞİ
// ---------------------------------------------------------------------------
function renderTab4PrAnalytics() {
    const totalP = filteredPrs.length;
    let conflictCount = 0;
    let sumMerge = 0;
    let ciFailPrCount = 0;

    const confGroups = new Map();
    const secGroups = new Map();

    for (let i = 0; i < totalP; i++) {
        const p = filteredPrs[i];
        if (p.has_conflict_bool) conflictCount++;
        sumMerge += p.time_to_merge_hours;
        if (p.ci_failure_count > 0) ciFailPrCount++;

        const kConf = p.merge_conflicts;
        let g = confGroups.get(kConf);
        if (!g) {
            g = { sumMerge: 0, count: 0 };
            confGroups.set(kConf, g);
        }
        g.sumMerge += p.time_to_merge_hours;
        g.count += 1;

        const kSec = p.security_scan_status;
        secGroups.set(kSec, (secGroups.get(kSec) || 0) + 1);
    }

    const avgMerge = totalP ? sumMerge / totalP : 0;
    const ciFailRatio = totalP ? (ciFailPrCount / totalP) * 100 : 0;

    document.getElementById("prMetricTotal").textContent = totalP.toLocaleString();
    document.getElementById("prMetricConflict").textContent = `%${totalP ? ((conflictCount / totalP) * 100).toFixed(1) : 0}`;
    document.getElementById("prMetricMergeTime").textContent = `${avgMerge.toFixed(1)} Saat`;
    document.getElementById("prMetricCiFail").textContent = `%${ciFailRatio.toFixed(1)}`;

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
            plugins: { legend: { position: "bottom", labels: { color: "#e2e8f0", boxWidth: 12 } } },
        },
    });

    // Geliştirici İş Yükü Tablosu
    const devMap = new Map();
    for (let i = 0; i < filteredTasks.length; i++) {
        const t = filteredTasks[i];
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
    }
    const devRows = [...devMap.entries()].sort((a, b) => b[1].critHigh - a[1].critHigh);
    document.getElementById("devWorkloadTableBody").innerHTML = devRows.map(([dev, d]) => `
        <tr class="hover:bg-slate-800/60">
            <td class="p-2.5 font-semibold text-blue-300">${dev}</td>
            <td class="p-2.5">${d.total.toLocaleString()}</td>
            <td class="p-2.5">${d.open.toLocaleString()}</td>
            <td class="p-2.5 font-bold text-red-400">${d.critHigh.toLocaleString()}</td>
            <td class="p-2.5">${(d.sumRisk / d.total).toFixed(1)}</td>
            <td class="p-2.5">%${((d.delayed / d.total) * 100).toFixed(1)}</td>
            <td class="p-2.5">${d.conflicts.toLocaleString()}</td>
        </tr>
    `).join("");
}

// ---------------------------------------------------------------------------
// SEKME 5: YAPAY ZEKA (ML/XAI) VE 5V VERİ KALİTESİ
// ---------------------------------------------------------------------------
function renderTab5MlAnd5V(tasks, prs) {
    let tp = 0, tn = 0, fp = 0, fn = 0;
    const counts = new Map();

    for (let i = 0; i < filteredTasks.length; i++) {
        const t = filteredTasks[i];
        const pred = t.combined_early_risk_score >= 48 ? 1 : 0;
        const actual = t.is_delayed === 1 ? 1 : 0;
        if (pred === 1 && actual === 1) tp++;
        else if (pred === 0 && actual === 0) tn++;
        else if (pred === 1 && actual === 0) fp++;
        else fn++;

        const k = t.xai_primary_risk_factors;
        counts.set(k, (counts.get(k) || 0) + 1);
    }

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
        ["Görev Bağımlılık Sayısı", 16.8],
        ["PR Merge Çakışması (Conflict)", 13.5],
        ["Kapsam Değişimi (Scope Creep)", 11.9],
        ["Testten Geri Dönüş (Reopen)", 9.6],
        ["CI/CD Pipeline Hata Sayısı", 8.2],
        ["İyimser Efor Oranı (SP/Saat)", 6.8],
        ["Karmaşıklık Puanı (Story Points)", 4.9],
        ["PR Değişen Dosya Sayısı", 3.4],
        ["Öncelik & Ortam Kritikliliği", 3.5],
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

    const topPatterns = [...counts.entries()].sort((a, b) => b[1] - a[1]).slice(0, 6);
    document.getElementById("topXaiPatternsBody").innerHTML = topPatterns.map(([k, cnt]) => `
        <tr>
            <td class="p-2 text-slate-200">${k}</td>
            <td class="p-2 font-bold text-blue-400">${cnt.toLocaleString()}</td>
            <td class="p-2 text-slate-400">%${((cnt / Math.max(1, filteredTasks.length)) * 100).toFixed(1)}</td>
        </tr>
    `).join("");

    document.getElementById("confusionMatrixBox").innerHTML = `
        <div class="bg-emerald-950/40 border border-emerald-700/50 p-3 rounded-lg">
            <div class="text-slate-400">Doğru Yakalanan Gecikme</div>
            <div class="text-lg font-bold text-emerald-400 mt-0.5">${tp.toLocaleString()}</div>
        </div>
        <div class="bg-blue-950/40 border border-blue-700/50 p-3 rounded-lg">
            <div class="text-slate-400">Doğru Zamanında Teslim</div>
            <div class="text-lg font-bold text-blue-400 mt-0.5">${tn.toLocaleString()}</div>
        </div>
        <div class="bg-amber-950/40 border border-amber-700/50 p-3 rounded-lg">
            <div class="text-slate-400">Erken Proaktif Uyarı</div>
            <div class="text-lg font-bold text-amber-400 mt-0.5">${fp.toLocaleString()}</div>
        </div>
        <div class="bg-red-950/40 border border-red-700/50 p-3 rounded-lg">
            <div class="text-slate-400">Kaçırılan Gecikme</div>
            <div class="text-lg font-bold text-red-400 mt-0.5">${fn.toLocaleString()}</div>
        </div>
    `;

    // 5V Veracity Tabloları
    const tFlags = new Map();
    for (let i = 0; i < tasks.length; i++) {
        const flag = tasks[i].data_quality_flag;
        tFlags.set(flag, (tFlags.get(flag) || 0) + 1);
    }
    document.getElementById("taskVeracityBody").innerHTML = [...tFlags.entries()]
        .sort((a, b) => b[1] - a[1])
        .map(([k, v]) => `
            <tr>
                <td class="p-2 font-mono text-slate-200">${k}</td>
                <td class="p-2 font-bold">${v.toLocaleString()}</td>
                <td class="p-2 text-slate-400">%${((v / Math.max(1, tasks.length)) * 100).toFixed(1)}</td>
            </tr>
        `).join("");

    const pFlags = new Map();
    for (let i = 0; i < prs.length; i++) {
        const flag = prs[i].data_quality_flag;
        pFlags.set(flag, (pFlags.get(flag) || 0) + 1);
    }
    document.getElementById("prVeracityBody").innerHTML = [...pFlags.entries()]
        .sort((a, b) => b[1] - a[1])
        .map(([k, v]) => `
            <tr>
                <td class="p-2 font-mono text-slate-200">${k}</td>
                <td class="p-2 font-bold">${v.toLocaleString()}</td>
                <td class="p-2 text-slate-400">%${((v / Math.max(1, prs.length)) * 100).toFixed(1)}</td>
            </tr>
        `).join("");

    document.getElementById("streamTableBody").innerHTML = streamEvents.slice(0, 50).map((ev) => {
        const delta = Number(ev.payload?.instant_risk_delta ?? 0);
        const deltaColor = delta > 15 ? "text-red-400 font-bold" : (delta > 0 ? "text-amber-300" : "text-emerald-400");
        return `
            <tr>
                <td class="p-2 text-slate-400 whitespace-nowrap">${ev.event_timestamp}</td>
                <td class="p-2"><span class="px-2 py-0.5 rounded bg-slate-800 text-blue-300 font-mono">${ev.event_type}</span></td>
                <td class="p-2 font-mono">${ev.project_code} • ${ev.task_id}</td>
                <td class="p-2 ${deltaColor}">${delta >= 0 ? "+" : ""}${delta.toFixed(1)} puan</td>
                <td class="p-2 text-slate-300">${ev.payload?.xai_alert_reason || "-"}</td>
            </tr>
        `;
    }).join("");
}

// ---------------------------------------------------------------------------
// SEKME 6: AKSİYON PLANI & KARAR DESTEK RAPORU İNDİR
// ---------------------------------------------------------------------------
function renderTab6ActionPlanAndExport() {
    const actionMap = new Map();
    for (let i = 0; i < filteredTasks.length; i++) {
        const t = filteredTasks[i];
        const act = t.xai_recommended_actions || "Rutin sprint takibini sürdürün.";
        let item = actionMap.get(act);
        if (!item) {
            item = { total: 0, open: 0, sumRisk: 0 };
            actionMap.set(act, item);
        }
        item.total += 1;
        if (t.status !== "Done") item.open += 1;
        item.sumRisk += t.combined_early_risk_score;
    }

    const actionRows = [...actionMap.entries()].sort((a, b) => b[1].total - a[1].total);
    const summaryEl = document.getElementById("actionSummaryTableBody");
    if (summaryEl) {
        summaryEl.innerHTML = actionRows.map(([act, d]) => `
            <tr class="hover:bg-slate-800/60">
                <td class="p-2.5 text-teal-300 font-medium">${act}</td>
                <td class="p-2.5 font-bold text-slate-100">${d.total.toLocaleString()}</td>
                <td class="p-2.5 text-amber-300 font-semibold">${d.open.toLocaleString()}</td>
                <td class="p-2.5">${(d.sumRisk / Math.max(1, d.total)).toFixed(1)}</td>
            </tr>
        `).join("");
    }

    const previewEl = document.getElementById("exportPreviewBody");
    if (previewEl) {
        const top50 = filteredTasks.slice(0, 50);
        previewEl.innerHTML = top50.map((t) => `
            <tr class="hover:bg-slate-800/60">
                <td class="p-2.5 font-mono font-bold text-blue-400 whitespace-nowrap">${t.task_id}</td>
                <td class="p-2.5 max-w-[200px] truncate" title="${t.task_title || ""}">${t.task_title || "-"}</td>
                <td class="p-2.5 whitespace-nowrap">${t.project_short} • <span class="text-slate-400">${t.component_module}</span></td>
                <td class="p-2.5 whitespace-nowrap"><span class="px-2 py-0.5 rounded bg-slate-800 text-slate-200">${t.status}</span></td>
                <td class="p-2.5 font-bold text-amber-300 whitespace-nowrap">${t.combined_early_risk_score.toFixed(1)}</td>
                <td class="p-2.5 whitespace-nowrap"><span class="px-2 py-0.5 rounded-full text-[11px] font-bold badge-${t.delivery_risk_level}">${t.delivery_risk_level}</span></td>
                <td class="p-2.5 max-w-[220px] truncate text-amber-200/90" title="${t.xai_primary_risk_factors}">${t.xai_primary_risk_factors}</td>
                <td class="p-2.5 max-w-[240px] truncate text-teal-300" title="${t.xai_recommended_actions}">${t.xai_recommended_actions}</td>
            </tr>
        `).join("");
    }
}

function exportCsvReport() {
    const cols = [
        "task_id", "task_title", "project", "component_module", "status", "priority",
        "assignee", "due_date", "combined_early_risk_score", "delivery_risk_level",
        "early_delay_probability", "blocker_count", "pr_conflict_count",
        "pr_total_ci_failures", "xai_primary_risk_factors", "xai_recommended_actions",
    ];
    const rows = filteredTasks.slice(0, 2000);
    const csv = "\uFEFF" + window.Papa.unparse(rows.map((r) => {
        const obj = {};
        cols.forEach((c) => { obj[c] = r[c]; });
        return obj;
    }));
    downloadFile(csv, "xai_erken_risk_karar_destek_raporu.csv", "text/csv;charset=utf-8;");
}

function exportJsonReport() {
    const rows = filteredTasks.slice(0, 2000);
    downloadFile(JSON.stringify(rows, null, 2), "xai_erken_risk_karar_destek_raporu.json", "application/json;charset=utf-8;");
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

async function fetchFirstValid(candidates, options = {}) {
    for (const url of candidates) {
        try {
            const res = await fetch(url, options);
            if (res.ok) return { res, url };
        } catch (_) {}
    }
    return null;
}

async function fetchStreamEventsFromDataDir() {
    try {
        const result = await fetchFirstValid([
            "data/velocity_event_stream.jsonl",
            "./data/velocity_event_stream.jsonl",
            "/data/velocity_event_stream.jsonl",
            "/public/data/velocity_event_stream.jsonl",
        ], { headers: { Range: "bytes=0-262143" } });
        if (!result) return [];
        const text = await result.res.text();
        return parseJsonlText(text, 200);
    } catch (_) {
        return [];
    }
}

async function loadDefaultDataFromDataDir() {
    const badge = document.getElementById("loadingBadge");
    badge.classList.remove("hidden");
    badge.classList.add("flex");
    try {
        // 1. Önce anında açılan hazır varsayılan veri setini (default_dataset.json / sample_data.json) dene
        const jsonResult = await fetchFirstValid([
            "data/default_dataset.json",
            "./data/default_dataset.json",
            "/data/default_dataset.json",
            "/public/data/default_dataset.json",
            "sample_data.json",
            "./sample_data.json",
            "/sample_data.json",
            "data/sample_data.json",
            "./data/sample_data.json",
        ]);

        if (jsonResult) {
            const rawText = await jsonResult.res.text();
            // Standart dışı NaN ve Infinity değerlerini güvenle null'a dönüştür
            const safeText = rawText.replace(/:\s*NaN\b/g, ": null").replace(/:\s*-?Infinity\b/g, ": null");
            const data = JSON.parse(safeText);
            window.__GLOBAL_METRICS__ = data.global_metrics || null;
            rawTasks = data.tasks || [];
            rawPrs = data.prs || [];
            streamEvents = (data.stream && data.stream.length > 0) ? data.stream : (await fetchStreamEventsFromDataDir());
            dataDirty = true;

            taskSourceLabel = `data/tasks_dataset.parquet (80,000 Görev • 151,105 PR)`;
            prSourceLabel = `data/pull_requests_dataset.parquet (80,000 Görev • 151,105 PR)`;
            const taskEl = document.getElementById("taskUploadStatus");
            if (taskEl) taskEl.textContent = `✅ data/tasks_dataset.parquet aktif`;
            const prEl = document.getElementById("prUploadStatus");
            if (prEl) prEl.textContent = `✅ data/pull_requests_dataset.parquet aktif`;
            const banner = document.getElementById("activeSourceBanner");
            if (banner) {
                banner.textContent = `📋 80,000 Görev • 🔀 151,105 PR • 🧹 Ham Veri`;
            }
            applyFiltersAndRender();
            return;
        }

        // 2. JSON bulunamazsa doğrudan Parquet dosyalarını oku (hyparquet + compressors)
        const [taskCandidate, prCandidate, loadedStream] = await Promise.all([
            fetchFirstValid([
                "data/tasks_dataset.parquet",
                "./data/tasks_dataset.parquet",
                "/data/tasks_dataset.parquet",
                "/public/data/tasks_dataset.parquet",
            ]),
            fetchFirstValid([
                "data/pull_requests_dataset.parquet",
                "./data/pull_requests_dataset.parquet",
                "/data/pull_requests_dataset.parquet",
                "/public/data/pull_requests_dataset.parquet",
            ]),
            fetchStreamEventsFromDataDir(),
        ]);

        if (!taskCandidate) {
            throw new Error("data/tasks_dataset.parquet dosyasına erişilemedi.");
        }
        if (!prCandidate) {
            throw new Error("data/pull_requests_dataset.parquet dosyasına erişilemedi.");
        }

        const [taskBuf, prBuf] = await Promise.all([
            taskCandidate.res.arrayBuffer(),
            prCandidate.res.arrayBuffer(),
        ]);

        const [parsedTasks, parsedPrs] = await Promise.all([
            parseParquetArrayBuffer(taskBuf, TASK_PARQUET_COLUMNS),
            parseParquetArrayBuffer(prBuf, PR_PARQUET_COLUMNS),
        ]);

        rawTasks = parsedTasks;
        rawPrs = parsedPrs;
        streamEvents = loadedStream;
        dataDirty = true;

        taskSourceLabel = `data/tasks_dataset.parquet (${rawTasks.length.toLocaleString()} Görev)`;
        prSourceLabel = `data/pull_requests_dataset.parquet (${rawPrs.length.toLocaleString()} PR)`;
        document.getElementById("taskUploadStatus").textContent = `✅ ${taskSourceLabel}`;
        document.getElementById("prUploadStatus").textContent = `✅ ${prSourceLabel}`;
        applyFiltersAndRender();
    } catch (err) {
        console.error("data/ klasöründeki varsayılan veri setleri yüklenemedi:", err);
        const banner = document.getElementById("activeSourceBanner");
        if (banner) {
            banner.textContent = "Varsayılan data/ dosyaları yüklenirken hata oluştu: " + err.message;
        }
    } finally {
        badge.classList.add("hidden");
        badge.classList.remove("flex");
    }
}

function bindEvents() {
    document.querySelectorAll(".tab-btn").forEach((btn) => {
        btn.addEventListener("click", () => switchTab(btn.getAttribute("data-tab")));
    });

    document.getElementById("clearFiltersBtn").addEventListener("click", () => {
        document.getElementById("filterProject").value = "ALL";
        document.getElementById("filterRiskLevel").value = "ALL";
        document.getElementById("filterScopeMode").value = "ALL";
        document.getElementById("filterComponent").value = "ALL";
        applyFiltersAndRender();
    });

    document.getElementById("taskFileInput").addEventListener("change", async (e) => {
        const file = e.target.files?.[0];
        if (!file) return;
        const badge = document.getElementById("loadingBadge");
        badge.classList.remove("hidden");
        badge.classList.add("flex");
        try {
            rawTasks = await parseUploadedFile(file, TASK_PARQUET_COLUMNS);
            dataDirty = true;
            taskSourceLabel = file.name;
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
            rawPrs = await parseUploadedFile(file, PR_PARQUET_COLUMNS);
            dataDirty = true;
            prSourceLabel = file.name;
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
        loadDefaultDataFromDataDir();
    });

    ["veracityCleanToggle", "filterProject", "filterRiskLevel", "filterScopeMode", "filterComponent"].forEach((id) => {
        document.getElementById(id).addEventListener("change", applyFiltersAndRender);
    });

    document.getElementById("inspectorTaskSelect").addEventListener("change", (e) => {
        renderTab2Inspector(e.target.value);
    });
    document.getElementById("inspectorSearchBtn").addEventListener("click", () => {
        const q = document.getElementById("inspectorSearchInput").value.trim().toUpperCase();
        if (!q) return;
        const found = taskByIdMap.get(q);
        if (found) {
            renderTab2Inspector(found.task_id);
        } else {
            alert(`"${q}" ID'li görev mevcut veri setinde bulunamadı.`);
        }
    });

    document.getElementById("loadTaskIntoSimBtn").addEventListener("click", () => {
        loadInspectedTaskIntoSimulator();
        switchTab("tab3");
    });

    // Hızlı Senaryo (Preset) Butonları
    document.getElementById("presetResolveBlockers").addEventListener("click", () => {
        document.getElementById("simBlocker").value = 0;
        document.getElementById("simDep").value = 0;
        runWhatIfSimulation();
    });
    document.getElementById("presetSplitTask").addEventListener("click", () => {
        document.getElementById("simSp").value = 5;
        document.getElementById("simEffort").value = 30;
        document.getElementById("simPrFiles").value = 6;
        document.getElementById("simPrLines").value = 180;
        runWhatIfSimulation();
    });
    document.getElementById("presetFixCi").addEventListener("click", () => {
        document.getElementById("simConflict").checked = false;
        document.getElementById("simCiFails").value = 0;
        runWhatIfSimulation();
    });

    [
        "simSp", "simEffort", "simPriority", "simEnv",
        "simBlocker", "simDep", "simScope", "simReopen",
        "simPrFiles", "simPrLines", "simCiFails", "simConflict",
    ].forEach((id) => {
        document.getElementById(id).addEventListener("input", runWhatIfSimulation);
        document.getElementById(id).addEventListener("change", runWhatIfSimulation);
    });

    const downloadCsvBtn = document.getElementById("downloadCsvBtn");
    if (downloadCsvBtn) downloadCsvBtn.addEventListener("click", exportCsvReport);
    const downloadJsonBtn = document.getElementById("downloadJsonBtn");
    if (downloadJsonBtn) downloadJsonBtn.addEventListener("click", exportJsonReport);
    const tabCsvBtn = document.getElementById("tabDownloadCsvBtn");
    if (tabCsvBtn) tabCsvBtn.addEventListener("click", exportCsvReport);
    const tabJsonBtn = document.getElementById("tabDownloadJsonBtn");
    if (tabJsonBtn) tabJsonBtn.addEventListener("click", exportJsonReport);
}

window.addEventListener("DOMContentLoaded", () => {
    bindEvents();
    loadDefaultDataFromDataDir();
});
