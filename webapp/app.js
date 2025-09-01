/**
 * AstroEdge Mini-App JavaScript
 * Enhanced with comprehensive error handling and stable event listeners
 */

// Configuration
const BACKEND = window.BACKEND_BASE_URL || window.location.origin;
console.log('[AstroEdge] BACKEND base =', BACKEND);
const API_TIMEOUT = 10000; // 10 second timeout

// Global state
let currentTab = "dashboard";
let dashboardData = {};
let isLoading = false;

// Toast notification system
function showToast(message, type = "info") {
    // Remove existing toast
    const existingToast = document.getElementById("toast");
    if (existingToast) {
        existingToast.remove();
    }

    // Create toast element
    const toast = document.createElement("div");
    toast.id = "toast";
    toast.className = `toast toast-${type}`;
    toast.textContent = message;

    // Add styles
    toast.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        background: ${type === "error" ? "#d32f2f" : type === "success" ? "#2e7d32" : "#1976d2"};
        color: white;
        padding: 12px 20px;
        border-radius: 8px;
        z-index: 10000;
        font-size: 14px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.3);
        transform: translateX(100%);
        transition: transform 0.3s ease;
        max-width: 300px;
        word-wrap: break-word;
    `;

    document.body.appendChild(toast);

    // Animate in
    setTimeout(() => {
        toast.style.transform = "translateX(0)";
    }, 100);

    // Remove after 5 seconds
    setTimeout(() => {
        if (toast.parentNode) {
            toast.style.transform = "translateX(100%)";
            setTimeout(() => {
                if (toast.parentNode) {
                    toast.remove();
                }
            }, 300);
        }
    }, 5000);
}

// Safe fetch wrapper with comprehensive error handling
async function safeFetch(url, options = {}) {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), API_TIMEOUT);

    try {
        const response = await fetch(url, {
            ...options,
            signal: controller.signal,
            headers: {
                "Content-Type": "application/json",
                ...options.headers
            }
        });

        clearTimeout(timeoutId);

        if (!response.ok) {
            const errorText = await response.text().catch(() => "Unknown error");
            throw new Error(`HTTP ${response.status}: ${errorText}`);
        }

        return await response.json();
    } catch (error) {
        clearTimeout(timeoutId);
        
        if (error.name === "AbortError") {
            throw new Error("Request timed out");
        } else if (error.name === "TypeError" && error.message.includes("fetch")) {
            throw new Error("Network error - check connection");
        }
        
        throw error;
    }
}

// Centralized API helper (path relative to BACKEND)
async function api(path, opts = {}) {
    const url = `${BACKEND}${path}`;
    return safeFetch(url, {
        method: opts.method || 'GET',
        body: opts.body,
        headers: { 'Content-Type': 'application/json', ...(opts.headers || {}) },
    });
}

// Normalize quarter values (handles "2024 Q3" -> "2024-Q3")
function normQuarter(q) {
    return (q || '').trim().replace(/\s+/g, '-').toUpperCase();
}

// Loading indicator management
function showLoading() {
    isLoading = true;
    const indicator = document.getElementById("loadingIndicator");
    if (indicator) {
        indicator.classList.remove("hidden");
    }
}

function hideLoading() {
    isLoading = false;
    const indicator = document.getElementById("loadingIndicator");
    if (indicator) {
        indicator.classList.add("hidden");
    }
}

// Tab management with stable IDs
function showTab(tabName) {
    if (isLoading) {
        showToast("Please wait for current operation to complete", "info");
        return;
    }

    // Hide all sections
    document.querySelectorAll(".tab-content").forEach(section => {
        section.classList.add("hidden");
    });

    // Remove active class from all buttons
    document.querySelectorAll(".tab-btn").forEach(btn => {
        btn.classList.remove("active");
    });

    // Show selected section
    const section = document.getElementById(`${tabName}Sec`);
    if (section) {
        section.classList.remove("hidden");
    }

    // Mark button as active
    const button = document.getElementById(`btn${tabName.charAt(0).toUpperCase() + tabName.slice(1)}`);
    if (button) {
        button.classList.add("active");
    }

    currentTab = tabName;

    // Load tab-specific data
    loadTabData(tabName);
}

// Load tab-specific data
async function loadTabData(tabName) {
    try {
        showLoading();

        switch (tabName) {
            case "dashboard":
                await loadDashboardData();
                break;
            case "aspects":
                await loadAspectsData();
                break;
            case "opps":
                await loadOpportunitiesData();
                break;
            case "backtest":
                await loadBacktestData();
                break;
            case "upcoming":
                await onUpcoming();
                break;
            case "categories":
                await onCategories();
                break;
        }
    } catch (error) {
        console.error(`Error loading ${tabName} data:`, error);
        showToast(`Failed to load ${tabName} data: ${error.message}`, "error");
    } finally {
        hideLoading();
    }
}

// Dashboard data loading
async function loadDashboardData() {
    try {
        const [portfolioKpis, positions, dailyPnl, scatterData] = await Promise.all([
            safeFetch(`${BACKEND}/kpis`).catch(() => ({})),
            safeFetch(`${BACKEND}/positions`).catch(() => []),
            safeFetch(`${BACKEND}/pnl/daily?days=30`).catch(() => []),
            safeFetch(`${BACKEND}/trades/scatter`).catch(() => [])
        ]);

        dashboardData = { kpis: portfolioKpis, positions, dailyPnl, scatterData };
        renderDashboard();
    } catch (error) {
        throw new Error(`Dashboard loading failed: ${error.message}`);
    }
}

// Render dashboard components
function renderDashboard() {
    try {
        renderPortfolioSummary();
        renderPerformanceKpis();
        renderPnlChart();
        renderScatterChart();
        renderPositions();
    } catch (error) {
        console.error("Dashboard rendering error:", error);
        showToast("Dashboard display error", "error");
    }
}

function renderPortfolioSummary() {
    const container = document.getElementById("portfolioSummary");
    if (!container) return;

    const { kpis = {} } = dashboardData;
    
    container.innerHTML = `
        <div class="kpi-item">
            <div class="kpi-value">${formatCurrency(kpis.equity_usdc || 0)}</div>
            <div class="kpi-label">Total Equity</div>
        </div>
        <div class="kpi-item">
            <div class="kpi-value ${(kpis.unrealized_pnl || 0) >= 0 ? 'kpi-positive' : 'kpi-negative'}">
                ${formatCurrency(kpis.unrealized_pnl || 0)}
            </div>
            <div class="kpi-label">Unrealized P&L</div>
        </div>
        <div class="kpi-item">
            <div class="kpi-value ${(kpis.realized_pnl || 0) >= 0 ? 'kpi-positive' : 'kpi-negative'}">
                ${formatCurrency(kpis.realized_pnl || 0)}
            </div>
            <div class="kpi-label">Realized P&L</div>
        </div>
        <div class="kpi-item">
            <div class="kpi-value">${formatCurrency(kpis.fees_usdc || 0)}</div>
            <div class="kpi-label">Total Fees</div>
        </div>
    `;
}

function renderPerformanceKpis() {
    const container = document.getElementById("performanceKpis");
    if (!container) return;

    const { kpis = {} } = dashboardData;
    
    container.innerHTML = `
        <div class="kpi-item">
            <div class="kpi-value">${formatPercent(kpis.total_return || 0)}</div>
            <div class="kpi-label">Total Return</div>
        </div>
        <div class="kpi-item">
            <div class="kpi-value">${formatDecimal(kpis.sharpe_ratio || 0, 2)}</div>
            <div class="kpi-label">Sharpe Ratio</div>
        </div>
        <div class="kpi-item">
            <div class="kpi-value">${formatPercent(kpis.win_rate || 0)}</div>
            <div class="kpi-label">Win Rate</div>
        </div>
        <div class="kpi-item">
            <div class="kpi-value">${formatPercent(kpis.max_drawdown || 0)}</div>
            <div class="kpi-label">Max Drawdown</div>
        </div>
    `;
}

function renderPnlChart() {
    const container = document.getElementById("pnlChart");
    if (!container || !dashboardData.dailyPnl?.length) {
        if (container) {
            container.innerHTML = '<div class="chart-placeholder">No P&L data available</div>';
        }
        return;
    }

    const data = dashboardData.dailyPnl.slice(-30); // Last 30 days
    const maxAbs = Math.max(...data.map(d => Math.abs(d.pnl || 0)));
    
    const chartHtml = `
        <div class="simple-chart">
            ${data.map(d => {
                const pnl = d.pnl || 0;
                const height = maxAbs > 0 ? Math.max(4, Math.abs(pnl) / maxAbs * 100) : 4;
                const colorClass = pnl >= 0 ? 'chart-positive' : 'chart-negative';
                return `<div class="chart-bar ${colorClass}" style="height: ${height}%" title="${formatCurrency(pnl)} on ${d.date}"></div>`;
            }).join('')}
        </div>
    `;
    
    container.innerHTML = chartHtml;
}

function renderScatterChart() {
    const container = document.getElementById("scatterChart");
    if (!container || !dashboardData.scatterData?.length) {
        if (container) {
            container.innerHTML = '<div class="chart-placeholder">No trade data available</div>';
        }
        return;
    }

    const data = dashboardData.scatterData.slice(0, 100); // Limit to 100 points
    const maxHoldTime = Math.max(...data.map(d => d.hold_hours || 0));
    const maxPnl = Math.max(...data.map(d => Math.abs(d.pnl || 0)));
    
    const pointsHtml = data.map(d => {
        const x = maxHoldTime > 0 ? (d.hold_hours || 0) / maxHoldTime * 90 : 45;
        const y = maxPnl > 0 ? 50 - ((d.pnl || 0) / maxPnl * 40) : 50;
        const colorClass = (d.pnl || 0) >= 0 ? 'chart-positive' : 'chart-negative';
        return `<div class="scatter-point ${colorClass}" style="left: ${x}%; bottom: ${y}%" title="Hold: ${d.hold_hours}h, P&L: ${formatCurrency(d.pnl)}"></div>`;
    }).join('');
    
    container.innerHTML = `<div class="scatter-chart">${pointsHtml}</div>`;
}

function renderPositions() {
    const container = document.getElementById("positionsList");
    if (!container) return;

    const positions = dashboardData.positions || [];
    
    if (positions.length === 0) {
        container.innerHTML = '<div class="no-data">No active positions</div>';
        return;
    }

    const positionsHtml = positions.map(pos => `
        <div class="position-item">
            <div class="position-info">
                <h4>${pos.market_name || pos.market_id}</h4>
                <div class="position-details">
                    ${pos.side} • ${formatNumber(pos.qty)} @ ${formatPrice(pos.vwap)}
                </div>
            </div>
            <div class="position-pnl ${pos.unrealized_pnl >= 0 ? 'kpi-positive' : 'kpi-negative'}">
                ${formatCurrency(pos.unrealized_pnl)}
            </div>
        </div>
    `).join('');

    container.innerHTML = positionsHtml;
}

// Aspects data loading (placeholder)
async function loadAspectsData() {
    const quarterSel = document.getElementById("quarterSel");
    if (quarterSel && quarterSel.value && quarterSel.value !== "Loading...") {
        try {
            const q = normQuarter(quarterSel.value);
            const resp = await api(`/astrology/aspects?quarter=${encodeURIComponent(q)}`);
            const aspects = resp?.aspects || [];
            renderAspectsTable(aspects);
        } catch (error) {
            showToast(`Failed to load aspects: ${error.message}`, "error");
        }
    }
}

function renderAspectsTable(aspects) {
    const tbody = document.querySelector("#aspectsTbl tbody");
    if (!tbody) return;

    if (!aspects || aspects.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" class="no-data">No aspects found for this quarter</td></tr>';
        return;
    }

    const rowsHtml = (aspects || []).map(aspect => `
        <tr>
            <td>${formatDateTime(aspect.peak_utc)}</td>
            <td>${aspect.planet1}-${aspect.planet2}</td>
            <td>${aspect.aspect}</td>
            <td>${formatDecimal(aspect.orb_deg ?? aspect.orb ?? 0, 2)}°</td>
            <td>${aspect.severity}</td>
            <td>${aspect.is_eclipse ? 'Yes' : 'No'}</td>
        </tr>
    `).join('');

    tbody.innerHTML = rowsHtml;
}

// Opportunities data loading (placeholder)
async function loadOpportunitiesData() {
    const container = document.getElementById("oppsList");
    if (!container) return;

    try {
        const quarterSel = document.getElementById("quarterSel");
        const quarter = normQuarter(quarterSel?.value || getCurrentQuarter());
        const opportunities = await api(`/opportunities/quarter?quarter=${encodeURIComponent(quarter)}`);
        renderOpportunities(opportunities);
    } catch (error) {
        container.innerHTML = '<div class="no-data">Failed to load opportunities</div>';
        showToast(`Failed to load opportunities: ${error.message}`, "error");
    }
}

function renderOpportunities(opportunities) {
    const container = document.getElementById("oppsList");
    if (!container) return;

    if (!opportunities || opportunities.length === 0) {
        container.innerHTML = '<div class="no-data">No opportunities available</div>';
        return;
    }

    const oppsHtml = opportunities.map(opp => `
        <div class="card">
            <div class="card-title">${opp.market_name || opp.market_id}</div>
            <div class="card-metrics">
                <div class="metric"><strong>Side:</strong> ${opp.side}</div>
                <div class="metric"><strong>Price:</strong> ${formatPrice(opp.price)}</div>
                <div class="metric"><strong>Edge:</strong> ${formatPercent(opp.edge)}</div>
            </div>
            <div class="decision ${opp.action?.toLowerCase() || 'hold'}">${opp.action || 'HOLD'}</div>
        </div>
    `).join('');

    container.innerHTML = oppsHtml;
}

// Upcoming + Categories
async function onUpcoming(category = null) {
    const q = normQuarter(document.getElementById('quarterSel')?.value || getCurrentQuarter());
    const qs = category ? `&category=${encodeURIComponent(category)}` : '';
    try {
        const res = await api(`/markets/upcoming?quarter=${encodeURIComponent(q)}${qs}`);
        renderUpcoming(res || []);
    } catch (err) {
        showToast(`Failed to load upcoming: ${err.message}`, 'error');
        const el = document.getElementById('upcomingList');
        if (el) el.innerHTML = '<div class="no-data">Failed to load upcoming markets</div>';
    }
}

async function onCategories() {
    const q = normQuarter(document.getElementById('quarterSel')?.value || getCurrentQuarter());
    try {
        const res = await api(`/markets/categories?quarter=${encodeURIComponent(q)}`);
        renderCategories(res || {});
    } catch (err) {
        showToast(`Failed to load categories: ${err.message}`, 'error');
        const el = document.getElementById('categoriesGrid');
        if (el) el.innerHTML = '<div class="no-data">Failed to load categories</div>';
    }
}

async function onAnalyzeMarket(marketId) {
    const q = normQuarter(document.getElementById('quarterSel')?.value || getCurrentQuarter());
    const payload = {
        quarter: q,
        market_ids: [marketId],
        params: { lambda_gain:0.10, threshold:0.04, lambda_days:5,
                  orb_limits:{square:8,opposition:8,conjunction:6}, K_cap:5,
                  fees_bps:60, slippage:0.005, size_cap:0.05 }
    };
    try {
        const out = await api(`/markets/analyze`, { method: 'POST', body: JSON.stringify(payload) });
        renderAnalysis(out || []);
    } catch (err) {
        showToast(`Analyze failed: ${err.message}`, 'error');
    }
}

function renderUpcoming(list) {
    const el = document.getElementById('upcomingList');
    if (!el) return;
    if (!list || list.length === 0) {
        el.innerHTML = '<div class="no-data">No upcoming markets for this quarter</div>';
        return;
    }
    el.innerHTML = list.map(m => `
        <div class="card">
          <div class="card-title">${m.title}</div>
          <div class="card-metrics">
            <div class="metric"><strong>Deadline:</strong> ${formatDateTime(m.deadline_utc)}</div>
            <div class="metric"><strong>p0:</strong> ${formatPrice(m.price_yes_mid)}</div>
            <div class="metric"><strong>Liquidity:</strong> ${formatDecimal(m.liquidity_score ?? 0, 2)}</div>
          </div>
          <div>
            ${(m.tags||[]).map(t => `<span class="chip">${t}</span>`).join(' ')}
          </div>
          <div style="margin-top:8px"><button class="btn-primary" onclick="onAnalyzeMarket('${m.id}')">Analyze</button></div>
        </div>
    `).join('');
}

function renderCategories(map) {
    const el = document.getElementById('categoriesGrid');
    if (!el) return;
    const entries = Object.entries(map || {});
    if (entries.length === 0) {
        el.innerHTML = '<div class="no-data">No categories in this quarter</div>';
        return;
    }
    el.innerHTML = entries.map(([k, v]) => `
        <div class="kpi-item chip" style="cursor:pointer" onclick="AstroEdge.onCategoryClick('${k}')">
          <div class="kpi-value">${v}</div>
          <div class="kpi-label">${k}</div>
        </div>
    `).join('');
}

function renderAnalysis(out) {
    if (!out || out.length === 0) {
        showToast('No analysis produced', 'info');
        return;
    }
    const r = out[0];
    const pct = (x)=> `${(x*100).toFixed(1)}%`;
    showToast(`p0 ${pct(r.p0)} → p_astro ${pct(r.p_astro)} | edge ${pct(r.edge_net)} | ${r.decision}`,'success');
}

// Backtest data loading
async function loadBacktestData() {
    try {
        const backtests = await safeFetch(`${BACKEND}/backtest/runs`);
        renderRecentBacktests(backtests);
    } catch (error) {
        const container = document.getElementById("recentBacktests");
        if (container) {
            container.innerHTML = '<div class="no-data">Failed to load backtests</div>';
        }
        showToast(`Failed to load backtests: ${error.message}`, "error");
    }
}

function renderRecentBacktests(backtests) {
    const container = document.getElementById("recentBacktests");
    if (!container) return;

    if (!backtests || backtests.length === 0) {
        container.innerHTML = '<div class="no-data">No backtests found</div>';
        return;
    }

    const backteststHtml = backtests.slice(0, 10).map(bt => `
        <div class="backtest-item">
            <div class="backtest-info">
                <h4>${bt.name}</h4>
                <div class="status status-${bt.status}">${bt.status.toUpperCase()}</div>
            </div>
            <div class="backtest-metrics">
                ${bt.metrics ? `
                    Return: ${formatPercent(bt.metrics.total_return || 0)}<br>
                    Sharpe: ${formatDecimal(bt.metrics.sharpe_ratio || 0, 2)}
                ` : 'Running...'}
            </div>
        </div>
    `).join('');

    container.innerHTML = backteststHtml;
}

// Backtest form submission
async function submitBacktest(event) {
    event.preventDefault();
    
    if (isLoading) {
        showToast("Please wait for current operation to complete", "info");
        return;
    }

    try {
        showLoading();
        
        const formData = new FormData(event.target);
        const request = {
            name: formData.get('backtestName'),
            start_date: formData.get('backtestStart') + 'T00:00:00Z',
            end_date: formData.get('backtestEnd') + 'T23:59:59Z',
            initial_capital: parseFloat(formData.get('initialCapital')),
            scan_frequency: formData.get('scanFreq')
        };

        const response = await safeFetch(`${BACKEND}/backtest/start`, {
            method: 'POST',
            body: JSON.stringify(request)
        });

        showToast(`Backtest started: ${response.test_run_id}`, "success");
        
        // Refresh backtest list
        await loadBacktestData();
        
        // Reset form
        event.target.reset();
        
    } catch (error) {
        showToast(`Backtest failed: ${error.message}`, "error");
    } finally {
        hideLoading();
    }
}

// Helper functions
function getCurrentQuarter() {
    const now = new Date();
    const quarter = Math.floor(now.getUTCMonth() / 3) + 1;
    return `${now.getUTCFullYear()}-Q${quarter}`;
}

// Utility formatting functions
function formatCurrency(value) {
    return new Intl.NumberFormat('en-US', {
        style: 'currency',
        currency: 'USD',
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
    }).format(value || 0);
}

function formatPercent(value) {
    return new Intl.NumberFormat('en-US', {
        style: 'percent',
        minimumFractionDigits: 1,
        maximumFractionDigits: 2
    }).format((value || 0) / 100);
}

function formatPrice(value) {
    return new Intl.NumberFormat('en-US', {
        minimumFractionDigits: 2,
        maximumFractionDigits: 4
    }).format(value || 0);
}

function formatNumber(value) {
    return new Intl.NumberFormat('en-US').format(value || 0);
}

function formatDecimal(value, decimals = 2) {
    return new Intl.NumberFormat('en-US', {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals
    }).format(value || 0);
}

function formatDateTime(dateStr) {
    if (!dateStr) return 'N/A';
    return new Date(dateStr).toLocaleString('en-US', {
        month: 'short',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit'
    });
}

// Initialize when DOM is fully loaded
document.addEventListener('DOMContentLoaded', function() {
    console.log('AstroEdge Mini-App initializing...');
    
    try {
        // Set up stable event listeners with error boundaries
        const btnDashboard = document.getElementById('btnDashboard');
        const btnAspects = document.getElementById('btnAspects');
        const btnOpps = document.getElementById('btnOpps');
        const btnUpcoming = document.getElementById('btnUpcoming');
        const btnCategories = document.getElementById('btnCategories');
        const btnBacktest = document.getElementById('btnBacktest');
        const backtestForm = document.getElementById('backtestForm');

        // Tab button event listeners
        if (btnDashboard) {
            btnDashboard.addEventListener('click', () => {
                try {
                    showTab('dashboard');
                } catch (error) {
                    console.error('Dashboard tab error:', error);
                    showToast('Error switching to dashboard', 'error');
                }
            });
        }

        if (btnAspects) {
            btnAspects.addEventListener('click', () => {
                try {
                    showTab('aspects');
                } catch (error) {
                    console.error('Aspects tab error:', error);
                    showToast('Error switching to aspects', 'error');
                }
            });
        }

        if (btnOpps) {
            btnOpps.addEventListener('click', () => {
                try {
                    showTab('opps');
                } catch (error) {
                    console.error('Opportunities tab error:', error);
                    showToast('Error switching to opportunities', 'error');
                }
            });
        }

        if (btnUpcoming) {
            btnUpcoming.addEventListener('click', () => {
                try { showTab('upcoming'); } catch (e) { console.error(e); }
            });
        }

        if (btnCategories) {
            btnCategories.addEventListener('click', () => {
                try { showTab('categories'); } catch (e) { console.error(e); }
            });
        }

        if (btnBacktest) {
            btnBacktest.addEventListener('click', () => {
                try {
                    showTab('backtest');
                } catch (error) {
                    console.error('Backtest tab error:', error);
                    showToast('Error switching to backtest', 'error');
                }
            });
        }

        // Backtest form submission
        if (backtestForm) {
            backtestForm.addEventListener('submit', submitBacktest);
        }

        // Modal close handlers
        document.querySelectorAll('.close').forEach(closeBtn => {
            closeBtn.addEventListener('click', function() {
                this.closest('.modal')?.classList.add('hidden');
            });
        });

        // Close modals when clicking outside
        document.querySelectorAll('.modal').forEach(modal => {
            modal.addEventListener('click', function(e) {
                if (e.target === this) {
                    this.classList.add('hidden');
                }
            });
        });

        // Initialize Telegram WebApp if available
        if (window.Telegram?.WebApp) {
            window.Telegram.WebApp.ready();
            window.Telegram.WebApp.expand();
        }

        // Load initial quarters data
        loadQuarters().catch(error => {
            console.error('Failed to load quarters:', error);
        });

        // Show dashboard by default
        showTab('dashboard');

        console.log('✅ AstroEdge Mini-App initialized successfully');
        showToast('App loaded successfully', 'success');

    } catch (error) {
        console.error('❌ Initialization error:', error);
        showToast('App initialization failed', 'error');
    }
});

// Load quarters for aspect/opportunity filtering
async function loadQuarters() {
    try {
        // For now, use hardcoded quarters since endpoint may not exist
        const quarters = [
            { id: '2024-Q1', name: '2024 Q1' },
            { id: '2024-Q2', name: '2024 Q2' },
            { id: '2024-Q3', name: '2024 Q3' },
            { id: '2024-Q4', name: '2024 Q4' },
            { id: '2025-Q1', name: '2025 Q1' }
        ];
        
        const quarterSel = document.getElementById('quarterSel');
        
        if (quarterSel && quarters?.length > 0) {
            quarterSel.innerHTML = quarters.map(q => 
                `<option value="${q.id}">${q.name}</option>`
            ).join('');
            quarterSel.disabled = false;
        }
    } catch (error) {
        console.error('Failed to load quarters:', error);
        const quarterSel = document.getElementById('quarterSel');
        if (quarterSel) {
            quarterSel.innerHTML = '<option>Failed to load</option>';
        }
    }
}

// Export for debugging
window.AstroEdge = {
    showTab,
    loadTabData,
    showToast,
    safeFetch,
    api,
    normQuarter,
    dashboardData,
    currentTab,
    onCategoryClick: (cat) => onUpcoming(cat)
};
