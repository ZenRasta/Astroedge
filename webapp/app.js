/**
 * AstroEdge Mini-App JavaScript - Enhanced with Analytics Dashboard
 */

// Configuration
const BACKEND = window.BACKEND_BASE_URL || "http://localhost:8003";

// Global state
let currentQuarter = null;
let aspectsData = [];
let opportunitiesData = [];
let dashboardData = {};

// Utility functions
function currentQuarter(d = new Date()) {
    const q = Math.floor(d.getUTCMonth() / 3) + 1;
    return `${d.getUTCFullYear()}-Q${q}`;
}

function nextQuarter(d = new Date()) {
    let q = Math.floor(d.getUTCMonth() / 3) + 1;
    let y = d.getUTCFullYear();
    q++;
    if (q > 4) {
        q = 1;
        y++;
    }
    return `${y}-Q${q}`;
}

function formatPercent(value) {
    return `${(value * 100).toFixed(1)}%`;
}

function formatCurrency(value) {
    return `$${value.toFixed(2)}`;
}

function formatDateTime(dateStr) {
    try {
        const date = new Date(dateStr);
        return date.toLocaleDateString('en-US', {
            month: '2-digit',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit'
        });
    } catch (e) {
        return dateStr.substring(0, 10);
    }
}

function truncateText(text, maxLength = 60) {
    if (text.length <= maxLength) return text;
    return text.substring(0, maxLength - 3) + '...';
}

// UI functions
function showLoading() {
    document.getElementById('loadingIndicator').classList.remove('hidden');
}

function hideLoading() {
    document.getElementById('loadingIndicator').classList.add('hidden');
}

function showError(message) {
    const modal = document.getElementById('errorModal');
    const messageEl = document.getElementById('errorMessage');
    messageEl.textContent = message;
    modal.classList.remove('hidden');
}

function hideError() {
    document.getElementById('errorModal').classList.add('hidden');
}

function showSection(sectionId) {
    document.querySelectorAll('.tab-content').forEach(el => {
        el.classList.add('hidden');
    });
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.classList.remove('active');
    });
    
    document.getElementById(sectionId).classList.remove('hidden');
    
    // Highlight active button
    const buttonMap = {
        'dashboardSec': 'btnDashboard',
        'aspectsSec': 'btnAspects',
        'oppsSec': 'btnOpps',
        'backtestSec': 'btnBacktest'
    };
    
    if (buttonMap[sectionId]) {
        document.getElementById(buttonMap[sectionId]).classList.add('active');
    }
    
    // Load section data if needed
    if (sectionId === 'dashboardSec') {
        loadDashboardData();
    } else if (sectionId === 'backtestSec') {
        loadBacktestData();
    }
}

// Dashboard API functions
async function loadDashboardData() {
    try {
        showLoading();
        
        // Load all dashboard data in parallel
        const [portfolioKpis, positions, dailyPnl, scatterData] = await Promise.all([
            fetch(`${BACKEND}/kpis`).then(r => r.ok ? r.json() : {}),
            fetch(`${BACKEND}/positions`).then(r => r.ok ? r.json() : []),
            fetch(`${BACKEND}/pnl/daily?days=30`).then(r => r.ok ? r.json() : []),
            fetch(`${BACKEND}/trades/scatter`).then(r => r.ok ? r.json() : [])
        ]);
        
        dashboardData = {
            kpis: portfolioKpis,
            positions: positions,
            dailyPnl: dailyPnl,
            scatterData: scatterData
        };
        
        renderDashboard();
        
    } catch (error) {
        console.error('Error loading dashboard data:', error);
        showError(`Failed to load dashboard: ${error.message}`);
    } finally {
        hideLoading();
    }
}

async function loadBacktestData() {
    try {
        const response = await fetch(`${BACKEND}/backtest/runs?limit=10`);
        if (response.ok) {
            const backtests = await response.json();
            renderBacktests(backtests);
        }
    } catch (error) {
        console.error('Error loading backtest data:', error);
    }
}

// Render functions
function renderDashboard() {
    renderPortfolioSummary();
    renderPerformanceKpis();
    renderPnlChart();
    renderScatterChart();
    renderPositions();
}

function renderPortfolioSummary() {
    const container = document.getElementById('portfolioSummary');
    const kpis = dashboardData.kpis || {};
    
    const equity = kpis.total_return * 1000; // Assuming $1000 base
    const positions = dashboardData.positions?.length || 0;
    const fees = kpis.total_fees || 0;
    const volume = kpis.total_volume || 0;
    
    container.innerHTML = `
        <div class="kpi-item">
            <div class="kpi-value ${equity >= 0 ? 'kpi-positive' : 'kpi-negative'}">${formatCurrency(equity)}</div>
            <div class="kpi-label">Portfolio Value</div>
        </div>
        <div class="kpi-item">
            <div class="kpi-value">${positions}</div>
            <div class="kpi-label">Active Positions</div>
        </div>
        <div class="kpi-item">
            <div class="kpi-value">${formatCurrency(fees)}</div>
            <div class="kpi-label">Total Fees</div>
        </div>
        <div class="kpi-item">
            <div class="kpi-value">${formatCurrency(volume)}</div>
            <div class="kpi-label">Total Volume</div>
        </div>
    `;
}

function renderPerformanceKpis() {
    const container = document.getElementById('performanceKpis');
    const kpis = dashboardData.kpis || {};
    
    container.innerHTML = `
        <div class="kpi-item">
            <div class="kpi-value ${kpis.total_return >= 0 ? 'kpi-positive' : 'kpi-negative'}">${formatPercent(kpis.total_return || 0)}</div>
            <div class="kpi-label">Total Return</div>
        </div>
        <div class="kpi-item">
            <div class="kpi-value">${(kpis.sharpe_ratio || 0).toFixed(2)}</div>
            <div class="kpi-label">Sharpe Ratio</div>
        </div>
        <div class="kpi-item">
            <div class="kpi-value">${formatPercent(kpis.win_rate || 0)}</div>
            <div class="kpi-label">Win Rate</div>
        </div>
        <div class="kpi-item">
            <div class="kpi-value">${kpis.total_trades || 0}</div>
            <div class="kpi-label">Total Trades</div>
        </div>
    `;
}

function renderPnlChart() {
    const container = document.getElementById('pnlChart');
    const pnlData = dashboardData.dailyPnl || [];
    
    if (!pnlData.length) {
        container.innerHTML = '<div class="no-data">No P&L data available</div>';
        return;
    }
    
    // Create simple bar chart
    const maxPnl = Math.max(...pnlData.map(d => Math.abs(d.daily_pnl)));
    const chartHeight = 160;
    
    const bars = pnlData.slice(-30).map(day => {
        const height = maxPnl > 0 ? Math.abs(day.daily_pnl) / maxPnl * chartHeight * 0.8 : 4;
        const className = day.daily_pnl >= 0 ? 'chart-positive' : 'chart-negative';
        return `<div class="chart-bar ${className}" style="height: ${height}px" title="${day.date}: ${formatCurrency(day.daily_pnl)}"></div>`;
    }).join('');
    
    container.innerHTML = `<div class="simple-chart">${bars}</div>`;
}

function renderScatterChart() {
    const container = document.getElementById('scatterChart');
    const scatterData = dashboardData.scatterData || [];
    
    if (!scatterData.length) {
        container.innerHTML = '<div class="no-data">No trade data available</div>';
        return;
    }
    
    // Create simple scatter plot
    const maxHours = Math.max(...scatterData.map(d => d.hold_time_hours));
    const maxPnl = Math.max(...scatterData.map(d => Math.abs(d.pnl)));
    
    const points = scatterData.map(trade => {
        const x = maxHours > 0 ? (trade.hold_time_hours / maxHours) * 90 : 10;
        const y = maxPnl > 0 ? (1 - Math.abs(trade.pnl) / maxPnl) * 90 : 50;
        const className = trade.pnl >= 0 ? 'chart-positive' : 'chart-negative';
        
        return `<div class="scatter-point" style="left: ${x}%; top: ${y}%; background: ${trade.pnl >= 0 ? '#2e7d32' : '#d32f2f'}" title="Hold: ${trade.hold_time_hours.toFixed(1)}h, P&L: ${formatCurrency(trade.pnl)}"></div>`;
    }).join('');
    
    container.innerHTML = `<div class="scatter-chart">${points}</div>`;
}

function renderPositions() {
    const container = document.getElementById('positionsList');
    const positions = dashboardData.positions || [];
    
    if (!positions.length) {
        container.innerHTML = '<div class="no-data">No open positions</div>';
        return;
    }
    
    const positionItems = positions.slice(0, 10).map(pos => {
        const pnlClass = pos.unrealized_pnl >= 0 ? 'kpi-positive' : 'kpi-negative';
        
        return `
            <div class="position-item">
                <div class="position-info">
                    <h4>${truncateText(pos.market_title, 40)}</h4>
                    <div class="position-details">
                        ${pos.qty} shares @ ${formatCurrency(pos.vwap)} | Mark: ${formatCurrency(pos.mark_price)}
                    </div>
                </div>
                <div class="position-pnl ${pnlClass}">
                    ${formatCurrency(pos.unrealized_pnl)}
                </div>
            </div>
        `;
    }).join('');
    
    container.innerHTML = positionItems;
}

function renderBacktests(backtests) {
    const container = document.getElementById('recentBacktests');
    
    if (!backtests.length) {
        container.innerHTML = '<div class="no-data">No backtests found</div>';
        return;
    }
    
    const backestItems = backtests.map(bt => {
        const statusClass = `status-${bt.status}`;
        const metrics = bt.metrics || {};
        const returnStr = metrics.total_return ? formatPercent(metrics.total_return) : '';
        
        return `
            <div class="backtest-item">
                <div class="backtest-info">
                    <h4>${truncateText(bt.name, 25)}</h4>
                    <div class="status ${statusClass}">${bt.status}</div>
                </div>
                <div class="backtest-metrics">
                    ${returnStr && `<div>${returnStr}</div>`}
                    <div>${formatDateTime(bt.created_at)}</div>
                </div>
            </div>
        `;
    }).join('');
    
    container.innerHTML = backestItems;
}

// Backtest form handling
async function handleBacktestSubmit(event) {
    event.preventDefault();
    
    const form = event.target;
    const formData = new FormData(form);
    
    const config = {
        name: formData.get('backtestName'),
        start_date: formData.get('backtestStart') + 'T00:00:00Z',
        end_date: formData.get('backtestEnd') + 'T23:59:59Z',
        initial_capital: parseFloat(formData.get('initialCapital')),
        scan_frequency: formData.get('scanFreq'),
        lambda_gain: 0.10,
        threshold: 0.04,
        lambda_days: 5.0,
        max_positions: 10,
        max_position_size: 0.05,
        fee_bps: 60
    };
    
    try {
        showLoading();
        
        const response = await fetch(`${BACKEND}/backtest/start`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(config)
        });
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const result = await response.json();
        
        // Reset form
        form.reset();
        
        // Show success message
        showError(`Backtest "${config.name}" started successfully! Run ID: ${result.test_run_id.substring(0, 8)}...`);
        
        // Refresh backtest list
        loadBacktestData();
        
    } catch (error) {
        console.error('Error starting backtest:', error);
        showError(`Failed to start backtest: ${error.message}`);
    } finally {
        hideLoading();
    }
}

// Original API functions (kept for aspects and opportunities)
async function fetchAspects(quarter) {
    try {
        showLoading();
        const response = await fetch(`${BACKEND}/astrology/aspects?quarter=${encodeURIComponent(quarter)}`);
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        const data = await response.json();
        aspectsData = Array.isArray(data) ? data : (data.aspects || []);
        renderAspects();
    } catch (error) {
        console.error('Error fetching aspects:', error);
        showError(`Failed to load aspects: ${error.message}`);
    } finally {
        hideLoading();
    }
}

async function scanOpportunities(quarter) {
    try {
        showLoading();
        const scanResponse = await fetch(`${BACKEND}/scan-quarter`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ 
                quarter: quarter,
                lambda_gain: 0.10,
                threshold: 0.04,
                lambda_days: 5,
                orb_limits: { square: 8, opposition: 8, conjunction: 6 },
                K_cap: 5.0
            })
        });
        
        if (!scanResponse.ok) {
            throw new Error(`Scan failed: ${scanResponse.status} ${scanResponse.statusText}`);
        }
        
        const scanResult = await scanResponse.json();
        opportunitiesData = scanResult.opportunities || [];
        renderOpportunities();
    } catch (error) {
        console.error('Error scanning opportunities:', error);
        showError(`Failed to scan opportunities: ${error.message}`);
    } finally {
        hideLoading();
    }
}

async function fetchOpportunityDetail(oppId, quarter) {
    try {
        const response = await fetch(`${BACKEND}/opportunities/${oppId}?quarter=${encodeURIComponent(quarter)}`);
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }
        const data = await response.json();
        showOpportunityDetail(data);
    } catch (error) {
        console.error('Error fetching opportunity detail:', error);
        showError(`Failed to load opportunity details: ${error.message}`);
    }
}

// Original render functions (kept for aspects and opportunities)
function renderAspects() {
    const tbody = document.querySelector('#aspectsTbl tbody');
    const statsEl = document.getElementById('aspectsStats');
    
    if (!aspectsData.length) {
        tbody.innerHTML = '<tr><td colspan="6" class="no-data">No aspects found for this quarter</td></tr>';
        statsEl.innerHTML = '<div class="stat-card"><div class="stat-number">0</div><div class="stat-label">Aspects</div></div>';
        return;
    }
    
    // Render stats
    const majorCount = aspectsData.filter(a => a.severity === 'major').length;
    const eclipseCount = aspectsData.filter(a => a.is_eclipse).length;
    
    statsEl.innerHTML = `
        <div class="stat-card">
            <div class="stat-number">${aspectsData.length}</div>
            <div class="stat-label">Total Aspects</div>
        </div>
        <div class="stat-card">
            <div class="stat-number">${majorCount}</div>
            <div class="stat-label">Major Aspects</div>
        </div>
        <div class="stat-card">
            <div class="stat-number">${eclipseCount}</div>
            <div class="stat-label">Eclipses</div>
        </div>
    `;
    
    // Render table
    tbody.innerHTML = aspectsData.map(aspect => `
        <tr>
            <td>${formatDateTime(aspect.peak_utc)}</td>
            <td><strong>${aspect.planet1}-${aspect.planet2}</strong></td>
            <td>${aspect.aspect}</td>
            <td>${aspect.orb_deg.toFixed(2)}°</td>
            <td><span class="${aspect.severity}">${aspect.severity}</span></td>
            <td>${aspect.is_eclipse ? '🌑☀️' : ''}</td>
        </tr>
    `).join('');
}

function renderOpportunities() {
    const listEl = document.getElementById('oppsList');
    const statsEl = document.getElementById('oppsStats');
    
    if (!opportunitiesData.length) {
        listEl.innerHTML = '<div class="no-data">No opportunities found for this quarter</div>';
        statsEl.innerHTML = '<div class="stat-card"><div class="stat-number">0</div><div class="stat-label">Opportunities</div></div>';
        return;
    }
    
    // Calculate stats
    const buyCount = opportunitiesData.filter(o => o.decision?.toLowerCase() === 'buy').length;
    const sellCount = opportunitiesData.filter(o => o.decision?.toLowerCase() === 'sell').length;
    const avgEdge = opportunitiesData.reduce((sum, o) => sum + (o.edge_net || 0), 0) / opportunitiesData.length;
    
    statsEl.innerHTML = `
        <div class="stat-card">
            <div class="stat-number">${opportunitiesData.length}</div>
            <div class="stat-label">Total Opportunities</div>
        </div>
        <div class="stat-card">
            <div class="stat-number">${buyCount}</div>
            <div class="stat-label">Buy Signals</div>
        </div>
        <div class="stat-card">
            <div class="stat-number">${sellCount}</div>
            <div class="stat-label">Sell Signals</div>
        </div>
        <div class="stat-card">
            <div class="stat-number">${formatPercent(avgEdge)}</div>
            <div class="stat-label">Avg Edge</div>
        </div>
    `;
    
    // Render opportunities
    listEl.innerHTML = opportunitiesData.map(opp => {
        const decision = (opp.decision || 'hold').toLowerCase();
        const decisionClass = decision;
        
        return `
            <div class="card">
                <div class="card-title">${truncateText(opp.title || 'Unknown Market')}</div>
                <div class="card-metrics">
                    <div class="metric">Base: <strong>${formatPercent(opp.p0 || 0)}</strong></div>
                    <div class="metric">Astro: <strong>${formatPercent(opp.p_astro || 0)}</strong></div>
                    <div class="metric">Edge: <strong>${formatPercent(opp.edge_net || 0)}</strong></div>
                    <div class="metric">Size: <strong>${formatPercent(opp.size_fraction || 0)}</strong></div>
                </div>
                <div style="display: flex; justify-content: space-between; align-items: center; margin-top: 12px;">
                    <span class="decision ${decisionClass}">${decision}</span>
                    <button onclick="fetchOpportunityDetail('${opp.id}', '${currentQuarter}')">📊 Details</button>
                </div>
            </div>
        `;
    }).join('');
}

function showOpportunityDetail(data) {
    const modal = document.getElementById('detailModal');
    const content = document.getElementById('detailContent');
    
    const opp = data.opportunity;
    const contribs = data.contributions || [];
    const market = opp.markets || {};
    
    // Sort contributions by absolute value
    const sortedContribs = contribs
        .sort((a, b) => Math.abs(b.contribution || 0) - Math.abs(a.contribution || 0))
        .slice(0, 10); // Top 10
    
    const detailHtml = `
        <div>
            <h4>${truncateText(market.title || 'Unknown Market', 50)}</h4>
            
            <div class="card-metrics" style="margin: 16px 0;">
                <div class="metric">Base Prob: <strong>${formatPercent(opp.p0 || 0)}</strong></div>
                <div class="metric">Astro Score: <strong>${(opp.s_astro || 0).toFixed(2)}</strong></div>
                <div class="metric">Astro Prob: <strong>${formatPercent(opp.p_astro || 0)}</strong></div>
                <div class="metric">Net Edge: <strong>${formatPercent(opp.edge_net || 0)}</strong></div>
                <div class="metric">Size: <strong>${formatPercent(opp.size_fraction || 0)}</strong></div>
            </div>
            
            <p><span class="decision ${(opp.decision || 'hold').toLowerCase()}">${(opp.decision || 'hold').toUpperCase()}</span></p>
            
            <h5>Contributing Aspects (${contribs.length})</h5>
            ${sortedContribs.length ? `
                <div style="max-height: 300px; overflow-y: auto;">
                    ${sortedContribs.map(c => {
                        const ae = c.aspect_events || {};
                        const contribution = c.contribution || 0;
                        const eclipse = ae.is_eclipse ? ' 🌑' : '';
                        
                        return `
                            <div style="padding: 8px; border-bottom: 1px solid #eee; display: flex; justify-content: space-between;">
                                <div>
                                    <strong>${ae.planet1 || '?'}-${ae.aspect || '?'}-${ae.planet2 || '?'}</strong>${eclipse}<br>
                                    <small style="color: var(--hint-color);">${formatDateTime(ae.peak_utc || '')}</small>
                                </div>
                                <div style="text-align: right;">
                                    <strong style="color: ${contribution > 0 ? '#2e7d32' : '#d32f2f'};">
                                        ${contribution > 0 ? '+' : ''}${contribution.toFixed(2)}
                                    </strong>
                                </div>
                            </div>
                        `;
                    }).join('')}
                </div>
            ` : '<p class="no-data">No contributing aspects found</p>'}
        </div>
    `;
    
    content.innerHTML = detailHtml;
    modal.classList.remove('hidden');
}

// Event listeners
function setupEventListeners() {
    // Quarter selector
    const quarterSel = document.getElementById('quarterSel');
    quarterSel.addEventListener('change', (e) => {
        currentQuarter = e.target.value;
        // Clear previous data
        aspectsData = [];
        opportunitiesData = [];
        dashboardData = {};
        
        // Re-render current section
        const currentSection = document.querySelector('.tab-content:not(.hidden)');
        if (currentSection) {
            const sectionId = currentSection.id;
            if (sectionId === 'aspectsSec' && currentQuarter) {
                fetchAspects(currentQuarter);
            } else if (sectionId === 'oppsSec' && currentQuarter) {
                scanOpportunities(currentQuarter);
            } else if (sectionId === 'dashboardSec') {
                loadDashboardData();
            }
        }
    });
    
    // Tab buttons
    document.getElementById('btnDashboard').addEventListener('click', () => {
        showSection('dashboardSec');
    });
    
    document.getElementById('btnAspects').addEventListener('click', () => {
        showSection('aspectsSec');
        if (currentQuarter && !aspectsData.length) {
            fetchAspects(currentQuarter);
        }
    });
    
    document.getElementById('btnOpps').addEventListener('click', () => {
        showSection('oppsSec');
        if (currentQuarter && !opportunitiesData.length) {
            scanOpportunities(currentQuarter);
        }
    });
    
    document.getElementById('btnBacktest').addEventListener('click', () => {
        showSection('backtestSec');
    });
    
    // Backtest form
    document.getElementById('backtestForm').addEventListener('submit', handleBacktestSubmit);
    
    // Modal close buttons
    document.querySelectorAll('.close').forEach(closeBtn => {
        closeBtn.addEventListener('click', (e) => {
            e.target.closest('.modal').classList.add('hidden');
        });
    });
    
    // Close modals when clicking outside
    document.querySelectorAll('.modal').forEach(modal => {
        modal.addEventListener('click', (e) => {
            if (e.target === modal) {
                modal.classList.add('hidden');
            }
        });
    });
}

// Initialize app
function initializeApp() {
    console.log('Initializing AstroEdge Mini-App...');
    
    // Setup Telegram Web App
    if (window.Telegram && window.Telegram.WebApp) {
        const webApp = window.Telegram.WebApp;
        webApp.ready();
        webApp.expand();
        console.log('Telegram WebApp initialized');
    }
    
    // Populate quarter selector
    const quarterSel = document.getElementById('quarterSel');
    const quarters = [currentQuarter(), nextQuarter()];
    
    quarterSel.innerHTML = quarters.map(q => 
        `<option value="${q}">${q}</option>`
    ).join('');
    
    quarterSel.disabled = false;
    currentQuarter = quarters[0];
    
    // Set up form defaults
    const today = new Date().toISOString().split('T')[0];
    document.getElementById('backtestEnd').value = today;
    
    // Setup event listeners
    setupEventListeners();
    
    // Show dashboard by default
    showSection('dashboardSec');
    
    console.log('App initialized successfully');
}

// Start the app when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initializeApp);
} else {
    initializeApp();
}