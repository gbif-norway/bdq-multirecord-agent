// Global variables
let uniqueResults = [];
let tg2Tests = [];
let summaryStats = {};
let currentAmendmentsPage = 1;
let currentValidationsPage = 1;
const itemsPerPage = 10;

// Initialize the dashboard
document.addEventListener('DOMContentLoaded', function() {
    initializeDashboard();
});

async function initializeDashboard() {
    try {
        // Get filename from URL parameter
        const urlParams = new URLSearchParams(window.location.search);
        const uniqueTestResultsFile = urlParams.get('unique_test_results');
        
        if (!uniqueTestResultsFile) {
            showError('No unique_test_results parameter provided in URL');
            return;
        }

        // Set filename in header
        setFileName(uniqueTestResultsFile);

        // Load data files
        await Promise.all([
            loadUniqueResults(`results/${uniqueTestResultsFile}`),
            loadTG2Tests('TG2_tests_small.csv'),
            loadSummaryStats(`results/${uniqueTestResultsFile.replace('unique_test_results', 'summary_stats')}`)
        ]);

        // Process and display data
        const uniqueResultsWithTestContext = joinResultsWithTestContext();
        
        // Debug: Log test IDs to help identify the Unknown issue
        const resultTestIds = [...new Set(uniqueResults.map(r => r.test_id))];
        const tg2TestIds = tg2Tests.map(t => t.test_id).filter(id => id); // Filter out empty strings
        
        console.log('Unique test IDs in results:', resultTestIds);
        console.log('Test IDs in TG2 tests:', tg2TestIds);
        console.log('Available properties in first TG2 test:', Object.keys(tg2Tests[0] || {}));
        
        // Find missing test IDs
        const missingTestIds = resultTestIds.filter(id => !tg2TestIds.includes(id));
        const matchingTestIds = resultTestIds.filter(id => tg2TestIds.includes(id));
        
        console.log('Matching test IDs:', matchingTestIds);
        console.log('Missing test IDs from TG2 file:', missingTestIds);
        console.log(`Summary: ${matchingTestIds.length} matching, ${missingTestIds.length} missing out of ${resultTestIds.length} total`);
        
        // Render dashboard components
        renderSummaryCards();
        renderNeedsAttentionChart(uniqueResultsWithTestContext);
        renderAmendmentsList(uniqueResultsWithTestContext);
        renderValidationsList(uniqueResultsWithTestContext);
        
        // Initialize tooltips
        initializeTooltips();

    } catch (error) {
        console.error('Error initializing dashboard:', error);
        showError('Failed to load dashboard data: ' + error.message);
    }
}

function setFileName(filename) {
    const fileNameElement = document.getElementById('file-name');
    const displayName = filename
        .replace(/[-_]/g, ' ')
        .replace('.csv', '')
        .split(' ')
        .map(word => word.charAt(0).toUpperCase() + word.slice(1))
        .join(' ');
    fileNameElement.textContent = `for ${displayName} file`;
}

async function loadUniqueResults(filePath) {
    return new Promise((resolve, reject) => {
        Papa.parse(filePath, {
            download: true,
            header: true,
            complete: function(results) {
                uniqueResults = results.data;
                resolve();
            },
            error: function(error) {
                reject(new Error(`Failed to load unique results: ${error.message}`));
            }
        });
    });
}

async function loadTG2Tests(filePath) {
    return new Promise((resolve, reject) => {
        Papa.parse(filePath, {
            download: true,
            header: true,
            skipEmptyLines: 'greedy',
            transformHeader: function(header) {
                // Normalize headers to be consistent
                return header.trim()
                    .replace(/\s+/g, ' ')
                    .replace(/^IE Class$/i, 'ie_class')
                    .replace(/^Description$/i, 'description')
                    .replace(/^Notes$/i, 'notes')
                    .replace(/\r$/, ''); // Remove trailing \r
            },
            complete: function(results) {
                tg2Tests = results.data;
                console.log('TG2 Tests loaded:', tg2Tests.length, 'records');
                console.log('First TG2 test record:', tg2Tests[0]);
                console.log('TG2 Tests columns:', results.meta.fields);
                console.log('Parsing errors:', results.errors);
                resolve();
            },
            error: function(error) {
                reject(new Error(`Failed to load TG2 tests: ${error.message}`));
            }
        });
    });
}

async function loadSummaryStats(filePath) {
    return new Promise((resolve, reject) => {
        Papa.parse(filePath, {
            download: true,
            header: true,
            complete: function(results) {
                if (results.data.length > 0) {
                    summaryStats = results.data[0];
                }
                resolve();
            },
            error: function(error) {
                // Summary stats might not exist, so we'll continue without it
                console.warn('Summary stats not found, using defaults');
                summaryStats = {};
                resolve();
            }
        });
    });
}

function joinResultsWithTestContext() {
    return uniqueResults.map(result => {
        const testInfo = tg2Tests.find(test => test.test_id === result.test_id);
        if (!testInfo) {
            console.warn(`Test not found for test_id: ${result.test_id}`);
        }
        return {
            ...result,
            test_description: testInfo ? testInfo.description : 'No description available',
            test_notes: testInfo ? testInfo.notes : 'No notes available',
            ie_class: testInfo ? testInfo.ie_class : 'Unknown'
        };
    });
}

function renderSummaryCards() {
    const cardsContainer = document.getElementById('summary-cards');
    const cards = [
        {
            number: summaryStats.number_of_records_in_dataset || uniqueResults.length,
            label: 'records in dataset'
        },
        {
            number: summaryStats.no_of_tests_run || new Set(uniqueResults.map(r => r.test_id)).size,
            label: 'tests across dataset'
        },
        {
            number: summaryStats.no_of_test_results || uniqueResults.length,
            label: 'results'
        },
        {
            number: (summaryStats.no_of_amendments || 0) + (summaryStats.no_of_filled_in || 0),
            label: 'changes can be applied automatically'
        },
        {
            number: summaryStats.no_of_unique_non_compliant_validations || 0,
            label: 'corrections needing attention'
        },
        {
            number: summaryStats.no_of_unique_issues || 0,
            label: 'fields with potential issues'
        }
    ];

    cardsContainer.innerHTML = cards.map(card => `
        <div class="col-md-4 col-lg-2 mb-3"><div class="card" style="height: 130px;">
            <div class="card-body">
                <h5 class="card-title">${card.number}</h5>
                <p class="card-text">${card.label}</p>
            </div>
        </div></div>
    `).join('');
}

function renderNeedsAttentionChart(uniqueResultsWithTestContext) {
    // Filter for non-compliant and potential issues
    const needsAttention = uniqueResultsWithTestContext.filter(result => 
        result.result === 'NOT_COMPLIANT' || result.result === 'POTENTIAL_ISSUE'
    );

    // Group by IE Class and count
    const ieClassCounts = {};
    needsAttention.forEach(result => {
        const ieClass = result.ie_class ? result.ie_class.replace(/^[^:]+:/, '') : 'Unknown';
        console.log(`Test ID: ${result.test_id}, IE Class: ${result.ie_class}, Processed: ${ieClass}`);
        ieClassCounts[ieClass] = (ieClassCounts[ieClass] || 0) + parseInt(result.count || 1);
    });
    
    console.log('IE Class counts:', ieClassCounts);

    // Create gradient colors between yellow and yellow-green
    const colors = generateGradientColors(Object.keys(ieClassCounts).length);

    const ctx = document.getElementById('needsAttentionChart').getContext('2d');
    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: Object.keys(ieClassCounts),
            datasets: [{
                label: 'Issues Count',
                data: Object.values(ieClassCounts),
                backgroundColor: colors,
                borderColor: colors.map(color => color.replace('0.8', '1')),
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    ticks: {
                        stepSize: 1
                    }
                }
            }
        }
    });
}

function generateGradientColors(count) {
    const colors = [];
    for (let i = 0; i < count; i++) {
        const ratio = i / (count - 1);
        const r = Math.round(254 * (1 - ratio) + 200 * ratio);
        const g = Math.round(222 * (1 - ratio) + 223 * ratio);
        const b = Math.round(0 * (1 - ratio) + 82 * ratio);
        colors.push(`rgba(${r}, ${g}, ${b}, 0.8)`);
    }
    return colors;
}

function renderAmendmentsList(uniqueResultsWithTestContext) {
    const amendments = uniqueResultsWithTestContext.filter(result => 
        result.status === 'AMENDED' || result.status === 'FILLED_IN'
    );

    if (amendments.length === 0) {
        const container = document.getElementById('amendments-list');
        const pagination = document.getElementById('amendments-pagination');
        container.innerHTML = `
            <div class="list-group">
                <div class="list-group-item text-muted text-center py-4">
                    <i class="fas fa-info-circle"></i> No amendments found
                </div>
            </div>
        `;
        pagination.innerHTML = '';
        return;
    }

    renderPaginatedList(amendments, 'amendments', currentAmendmentsPage, 'amendments-pagination');
}

function renderValidationsList(uniqueResultsWithTestContext) {
    const validations = uniqueResultsWithTestContext.filter(result => 
        (result.status === 'RUN_HAS_RESULT' && result.result === 'NOT_COMPLIANT') ||
        (result.status === 'RUN_HAS_RESULT' && (result.result === 'IS_ISSUE' || result.result === 'POTENTIAL_ISSUE'))
    );

    renderPaginatedList(validations, 'validations', currentValidationsPage, 'validations-pagination');
}

function renderPaginatedList(data, containerId, currentPage, paginationId) {
    const container = document.getElementById(containerId + '-list');
    const pagination = document.getElementById(paginationId);
    
    // Group by test_id
    const groupedData = {};
    data.forEach(item => {
        if (!groupedData[item.test_id]) {
            groupedData[item.test_id] = [];
        }
        groupedData[item.test_id].push(item);
    });

    const totalPages = Math.ceil(Object.keys(groupedData).length / itemsPerPage);
    const startIndex = (currentPage - 1) * itemsPerPage;
    const endIndex = startIndex + itemsPerPage;
    const pageData = Object.entries(groupedData).slice(startIndex, endIndex);

    // Render list items using proper Bootstrap list-group structure
    container.innerHTML = `
        <div class="list-group">
            ${pageData.map(([testId, items]) => {
                const totalCount = items.reduce((sum, item) => sum + parseInt(item.count || 1), 0);
                const testInfo = tg2Tests.find(test => test.test_id === testId);
                const topCombos = getTopFieldCombinations(items, 5);

                return `
                    <a href="#" class="list-group-item list-group-item-action" 
                       onclick="openDetailModal('${testId}', '${containerId}'); return false;">
                        <div class="d-flex w-100 justify-content-between">
                            <h5 class="mb-1">${formatTestTitle(testId)}</h5>
                            <small class="text-body-secondary">${totalCount} items</small>
                        </div>
                         <p class="mb-1">${testInfo ? testInfo.description : 'No description available'}</p>
                        <small class="text-body-secondary">
                            ${topCombos.slice(0, 2).map(combo => 
                                `${combo.count} - ${combo.combo}`
                            ).join('; ')}
                            ${topCombos.length > 2 ? '...' : ''}
                        </small>
                    </a>
                `;
            }).join('')}
        </div>
    `;

    // Render pagination
    renderPagination(pagination, currentPage, totalPages, containerId);
}

function renderPagination(paginationElement, currentPage, totalPages, containerType) {
    if (totalPages <= 1) {
        paginationElement.innerHTML = '';
        return;
    }

    let paginationHTML = '';
    
    // Previous button
    if (currentPage > 1) {
        paginationHTML += `<li class="page-item">
            <a class="page-link" href="#" onclick="changePage('${containerType}', ${currentPage - 1}); return false;">Previous</a>
        </li>`;
    }

    // Page numbers
    for (let i = 1; i <= totalPages; i++) {
        if (i === currentPage) {
            paginationHTML += `<li class="page-item active">
                <span class="page-link">${i}</span>
            </li>`;
        } else {
            paginationHTML += `<li class="page-item">
                <a class="page-link" href="#" onclick="changePage('${containerType}', ${i}); return false;">${i}</a>
            </li>`;
        }
    }

    // Next button
    if (currentPage < totalPages) {
        paginationHTML += `<li class="page-item">
            <a class="page-link" href="#" onclick="changePage('${containerType}', ${currentPage + 1}); return false;">Next</a>
        </li>`;
    }

    paginationElement.innerHTML = paginationHTML;
}

function changePage(containerType, page) {
    if (containerType === 'amendments') {
        currentAmendmentsPage = page;
    } else if (containerType === 'validations') {
        currentValidationsPage = page;
    }
    
    // Re-render the appropriate list
    const uniqueResultsWithTestContext = joinResultsWithTestContext();
    if (containerType === 'amendments') {
        renderAmendmentsList(uniqueResultsWithTestContext);
    } else if (containerType === 'validations') {
        renderValidationsList(uniqueResultsWithTestContext);
    }
}

function openDetailModal(testId, containerType) {
    const modal = new bootstrap.Modal(document.getElementById('detailModal'));
    const modalTitle = document.getElementById('detailModalLabel');
    const modalBody = document.getElementById('modal-body');

    modalTitle.textContent = formatTestTitle(testId);

    // Get all data for this test
    const uniqueResultsWithTestContext = joinResultsWithTestContext();
    let testData;
    
    if (containerType === 'amendments') {
        testData = uniqueResultsWithTestContext.filter(result => 
            result.test_id === testId && (result.status === 'AMENDED' || result.status === 'FILLED_IN')
        );
    } else {
        testData = uniqueResultsWithTestContext.filter(result => 
            result.test_id === testId && 
            ((result.status === 'RUN_HAS_RESULT' && result.result === 'NOT_COMPLIANT') ||
             (result.status === 'RUN_HAS_RESULT' && (result.result === 'IS_ISSUE' || result.result === 'POTENTIAL_ISSUE')))
        );
    }

    const testInfo = tg2Tests.find(test => test.test_id === testId);
    const allCombos = getTopFieldCombinations(testData, testData.length);

    modalBody.innerHTML = `
        <div class="mb-3">
            <h6>Test Notes:</h6>
            <p class="text-muted">${testInfo ? testInfo.notes : 'No notes available'}</p>
        </div>
        <div class="mb-3">
            <h6>Description:</h6>
            <p>${testInfo ? testInfo.description : 'No description available'}</p>
        </div>
        <div>
            <h6>All Field Combinations:</h6>
            <div class="field-combinations">
                ${allCombos.map(combo => `
                    <div class="field-combo">
                        <span class="field-combo-count">${combo.count}</span> - ${combo.combo}
                    </div>
                `).join('')}
            </div>
        </div>
    `;

    modal.show();
}

function getTopFieldCombinations(items, limit) {
    const comboCounts = {};
    
    items.forEach(item => {
        const combo = `${item.actedUpon || 'N/A'} | ${item.consulted || 'N/A'}`;
        comboCounts[combo] = (comboCounts[combo] || 0) + parseInt(item.count || 1);
    });

    return Object.entries(comboCounts)
        .map(([combo, count]) => ({ combo, count }))
        .sort((a, b) => b.count - a.count)
        .slice(0, limit);
}

function formatTestTitle(testId) {
    return testId
        .replace(/_/g, ' ')
        .split(' ')
        .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
        .join(' ');
}

function initializeTooltips() {
    // Initialize Bootstrap tooltips
    const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });
}

function showError(message) {
    const container = document.querySelector('.container-fluid');
    container.innerHTML = `
        <div class="error-message">
            <h5><i class="fas fa-exclamation-triangle"></i> Error</h5>
            <p>${message}</p>
        </div>
    `;
}
