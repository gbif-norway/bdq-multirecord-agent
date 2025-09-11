// Global variables
let uniqueResults = [];
let tg2Tests = [];
let currentAmendmentsPage = 1;
let currentValidationsPage = 1;
let currentUseCaseFilter = 'all';
const itemsPerPage = 10;

// Modal pagination and search state
let modalCurrentPage = 1;
const modalItemsPerPage = 20;
let modalSearchTerm = '';
let modalFilteredCombos = [];
let modalAllCombos = [];

// Utility function to format numbers with space separators
function formatNumber(num) {
    if (num === null || num === undefined || isNaN(num)) return '0';
    return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ' ');
}

// Cache for efficient lookups
let amendmentLookupMap = new Map(); // key: "test_id||result" -> {test_id, result, actedUpon, count}

// Helpers for counts from unique results
function sumCounts(filterFn) {
    return uniqueResults
        .filter(filterFn)
        .reduce((sum, r) => sum + (parseInt(r.count || 0) || 0), 0);
}

function getDatasetSize() {
    // For any single test_id, sum(count) equals dataset size (inclusive candidate generation)
    const byTest = new Map();
    uniqueResults.forEach(r => {
        const id = r.test_id || '';
        const c = parseInt(r.count || 0) || 0;
        byTest.set(id, (byTest.get(id) || 0) + c);
    });
    let maxSum = 0;
    for (const v of byTest.values()) {
        if (v > maxSum) maxSum = v;
    }
    return maxSum;
}

// Initialize the dashboard
document.addEventListener('DOMContentLoaded', function() {
    initializeDashboard();
});

async function initializeDashboard() {
    try {
        // Get filenames from URL parameters
        const urlParams = new URLSearchParams(window.location.search);
        const uniqueTestResultsFile = urlParams.get('unique_test_results');
        const amendedDatasetFile = urlParams.get('amended_dataset');
        
        if (!uniqueTestResultsFile) {
            showError('No unique_test_results parameter provided in URL');
            return;
        }

        // Set filename in header
        setFileName(uniqueTestResultsFile);
        
        // Set up download buttons
        setupDownloadButtons(uniqueTestResultsFile, amendedDatasetFile);

        // Load data files
        await Promise.all([
            loadUniqueResults(`results/${uniqueTestResultsFile}`),
            loadTG2Tests('TG2_tests_small.csv')
        ]);

        // Process and display data
        const uniqueResultsWithTestContext = joinResultsWithTestContext();
        
        // Build lookup map for efficient amendment lookups
        buildAmendmentLookupMap();
        
        // Debug: Log test IDs to help identify the Unknown issue
        const resultTestIds = [...new Set(uniqueResults.map(r => r.test_id))];
        const tg2TestIds = tg2Tests.map(t => t.test_id).filter(id => id); // Filter out empty strings
        
        console.log('Unique test IDs in results:', resultTestIds);
        console.log('Test IDs in TG2 tests:', tg2TestIds);
        console.log('Available properties in first TG2 test:', Object.keys(tg2Tests[0] || {}));
        
        // Find missing test IDs using normalized comparison
        const normalizedResultTestIds = resultTestIds.map(id => String(id || '').trim());
        const normalizedTg2TestIds = tg2TestIds.map(id => String(id || '').trim());
        const missingTestIds = normalizedResultTestIds.filter(id => !normalizedTg2TestIds.includes(id));
        const matchingTestIds = normalizedResultTestIds.filter(id => normalizedTg2TestIds.includes(id));
        
        console.log('Matching test IDs (normalized):', matchingTestIds);
        console.log('Missing test IDs (normalized):', missingTestIds);
        console.log(`Summary: ${matchingTestIds.length} matching, ${missingTestIds.length} missing out of ${resultTestIds.length} total`);
        
        // Render dashboard components
        renderSummaryCards();
        renderNeedsAttentionChart(uniqueResultsWithTestContext);
        renderAmendmentsList(uniqueResultsWithTestContext);
        renderValidationsList(uniqueResultsWithTestContext);
        
        // Initialize tooltips
        initializeTooltips();
        
        // Initialize use case filters
        initializeUseCaseFilters();

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

function setupDownloadButtons(uniqueTestResultsFile, amendedDatasetFile) {
    const headerNav = document.querySelector('.bd-navbar .container-xxl');
    
    // Create download buttons container
    const downloadButtonsContainer = document.createElement('div');
    downloadButtonsContainer.className = 'd-flex gap-2';
    
    // Add unique test results button (always present since it's required)
    const uniqueResultsButton = document.createElement('a');
    uniqueResultsButton.type = 'button';
    uniqueResultsButton.className = 'btn btn-outline-light';
    uniqueResultsButton.href = `results/${uniqueTestResultsFile}`;
    uniqueResultsButton.download = uniqueTestResultsFile;
    uniqueResultsButton.innerHTML = '<i class="fas fa-download me-1"></i>Test results';
    downloadButtonsContainer.appendChild(uniqueResultsButton);
    
    // Add amended dataset button if file is provided
    if (amendedDatasetFile) {
        const amendedDatasetButton = document.createElement('a');
        amendedDatasetButton.type = 'button';
        amendedDatasetButton.className = 'btn btn-light';
        amendedDatasetButton.href = `results/${amendedDatasetFile}`;
        amendedDatasetButton.download = amendedDatasetFile;
        amendedDatasetButton.innerHTML = '<i class="fas fa-file-csv me-1"></i>Amended dataset';
        downloadButtonsContainer.appendChild(amendedDatasetButton);
    }
    
    // Add the buttons container to the header (it will be right-aligned due to justify-content-between)
    if (downloadButtonsContainer.children.length > 0) {
        headerNav.appendChild(downloadButtonsContainer);
    }
}

async function loadUniqueResults(filePath) {
    return new Promise((resolve, reject) => {
        Papa.parse(filePath, {
            download: true,
            header: true,
            skipEmptyLines: 'greedy',
            complete: function(results) {
                // Use the file exactly as provided; assume schema is correct
                uniqueResults = results.data || [];
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
                    .replace(/^InformationElement:ActedUpon$/i, 'ie_acted_upon')
                    .replace(/^InformationElement:Consulted$/i, 'ie_consulted')
                    .replace(/^Description$/i, 'description')
                    .replace(/^Notes$/i, 'notes')
                    .replace(/^Type$/i, 'type')
                    .replace(/^UseCases$/i, 'usecases')
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


function joinResultsWithTestContext() {
    return uniqueResults.map(result => {
            // Normalize test_id for comparison (trim whitespace and convert to string)
            const normalizedResultTestId = String(result.test_id || '').trim();
            const testInfo = tg2Tests.find(test => {
                const normalizedTestId = String(test.test_id || '').trim();
                return normalizedTestId === normalizedResultTestId;
            });
            
            if (!testInfo) {
                console.warn(`Test not found for test_id: "${result.test_id}" (normalized: "${normalizedResultTestId}")`);
                // Debug: show available test IDs for comparison
                const availableTestIds = tg2Tests.map(t => `"${String(t.test_id || '').trim()}"`).slice(0, 5);
                console.warn(`Available test IDs (first 5): ${availableTestIds.join(', ')}`);
            }
            
            return {
                ...result,
                test_description: testInfo ? testInfo.description : 'No description available',
                test_notes: testInfo ? testInfo.notes : 'No notes available',
                ie_class: testInfo ? testInfo.ie_class : 'Unknown',
                ie_acted_upon: testInfo ? (testInfo.ie_acted_upon || '') : '',
                ie_consulted: testInfo ? (testInfo.ie_consulted || '') : '',
                test_kind: testInfo ? (testInfo.type || '') : '',
                use_cases: testInfo ? (testInfo.usecases || '') : ''
            };
        });
}

function buildAmendmentLookupMap() {
    amendmentLookupMap.clear();
    uniqueResults.forEach(result => {
        if (result.test_type && result.test_type.toLowerCase() === 'amendment' && 
            (result.status === 'AMENDED' || result.status === 'FILLED_IN')) {
            const key = `${result.test_id}||${result.result || ''}`;
            amendmentLookupMap.set(key, {
                test_id: result.test_id,
                result: result.result,
                actedUpon: result.actedUpon,
                count: parseInt(result.count || 1)
            });
        }
    });
}

function renderSummaryCards() {
    const cardsContainer = document.getElementById('summary-cards');
    
    // Calculate values from uniqueResults data (using counts)
    const uniqueTestIds = new Set(uniqueResults.map(r => r.test_id)).size;
    const datasetSize = getDatasetSize();
    const amendments = sumCounts(r => r.status === 'AMENDED');
    const filledIn = sumCounts(r => r.status === 'FILLED_IN');
    const nonCompliantValidations = sumCounts(r => r.result === 'NOT_COMPLIANT');
    const potentialIssues = sumCounts(r => r.result === 'POTENTIAL_ISSUE');
    const totalResults = sumCounts(() => true);
    
    const cards = [
        { number: datasetSize, label: 'records in dataset' },
        {
            number: uniqueTestIds,
            label: 'tests across dataset'
        },
        { number: totalResults, label: 'results' },
        {
            number: nonCompliantValidations,
            label: 'corrections needing attention'
        },
        {
            number: amendments + filledIn,
            label: 'changes can be applied automatically'
        },
        {
            number: potentialIssues,
            label: 'fields with secondary issues'
        }
    ];

    cardsContainer.innerHTML = cards.map(card => `
        <div class="col-md-4 col-lg-2 mb-3"><div class="card" style="height: 130px;">
            <div class="card-body">
                <h5 class="card-title">${formatNumber(card.number)}</h5>
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

    // Group by IE Class, then by test_id within each class
    const ieClassGroups = {};
    needsAttention.forEach(result => {
        const ieClass = result.ie_class ? result.ie_class.replace(/^[^:]+:/, '') : 'Unknown';
        const testId = result.test_id;
        const count = parseInt(result.count || 1);
        
        if (!ieClassGroups[ieClass]) {
            ieClassGroups[ieClass] = {};
        }
        if (!ieClassGroups[ieClass][testId]) {
            ieClassGroups[ieClass][testId] = 0;
        }
        ieClassGroups[ieClass][testId] += count;
    });
    
    // Convert to stacked chart format
    const ieClassLabels = Object.keys(ieClassGroups).map(ieClass => 
        ieClass === 'Unknown' ? 'Unknown' : ieClass + ' fields'
    );
    
    // Get all unique test IDs across all IE classes
    const allTestIds = new Set();
    Object.values(ieClassGroups).forEach(testGroups => {
        Object.keys(testGroups).forEach(testId => allTestIds.add(testId));
    });
    
    // Create datasets for each test (each test becomes a dataset for stacking)
    const datasets = [];
    const testIds = Array.from(allTestIds);
    
    // Filter out tests that have zero counts across all IE classes
    const testsWithData = testIds.filter(testId => {
        return ieClassLabels.some(ieClassLabel => {
            const ieClass = ieClassLabel.replace(' fields', '');
            const actualIeClass = ieClass === 'Unknown' ? 'Unknown' : ieClass;
            return ieClassGroups[actualIeClass] && ieClassGroups[actualIeClass][testId] > 0;
        });
    });
    
    // Calculate total records for percentage calculation (dataset size)
    const totalRecords = getDatasetSize() || 1;
    
    // Generate darker green to yellow gradient colors
    const testColors = generateStackedGradientColors(testsWithData.length);
    
    testsWithData.forEach((testId, index) => {
        const testData = ieClassLabels.map(ieClassLabel => {
            const ieClass = ieClassLabel.replace(' fields', '');
            const actualIeClass = ieClass === 'Unknown' ? 'Unknown' : ieClass;
            const count = ieClassGroups[actualIeClass] && ieClassGroups[actualIeClass][testId] 
                ? ieClassGroups[actualIeClass][testId] 
                : 0;
            // Convert to percentage
            return (count / totalRecords) * 100;
        });
        
        datasets.push({
            label: formatTestTitle(testId),
            data: testData,
            backgroundColor: testColors[index],
            borderColor: testColors[index].replace('0.8', '1'),
            borderWidth: 1
        });
    });

    const ctx = document.getElementById('needsAttentionChart').getContext('2d');
    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: ieClassLabels,
            datasets: datasets
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            aspectRatio: 2,
            plugins: {
                legend: {
                    display: true,
                    position: 'right',
                    labels: {
                        usePointStyle: true,
                        padding: 10,
                        font: {
                            size: 10
                        }
                    }
                },
                tooltip: {
                    enabled: true,
                    mode: 'index',
                    intersect: false,
                    filter: function(tooltipItem) {
                        // Only show tooltips for non-zero values
                        return tooltipItem.parsed.y > 0;
                    },
                    callbacks: {
                        title: function(context) {
                            return context[0].label;
                        },
                        label: function(context) {
                            const testName = context.dataset.label;
                            const percentage = context.parsed.y;
                            const totalRecords = uniqueResults.length;
                            const count = Math.round((percentage / 100) * totalRecords);
                            return `${testName}: ${percentage.toFixed(1)}% (${formatNumber(count)} records)`;
                        }
                    }
                }
            },
            scales: {
                x: {
                    stacked: true,
                    grid: {
                        display: false
                    }
                },
                y: {
                    stacked: true,
                    beginAtZero: true,
                    grid: {
                        display: false
                    },
                    ticks: {
                        callback: function(value) {
                            return value.toFixed(0) + '%';
                        }
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

function generateStackedGradientColors(count) {
    const colors = [];
    for (let i = 0; i < count; i++) {
        const ratio = i / (count - 1);
        // Much darker green to yellow gradient
        const r = Math.round(0 * (1 - ratio) + 255 * ratio);    // 0 -> 255 (very dark green to yellow)
        const g = Math.round(100 * (1 - ratio) + 255 * ratio);  // 100 -> 255 (dark green to yellow)
        const b = Math.round(0 * (1 - ratio) + 0 * ratio);      // 0 -> 0 (dark green to yellow)
        colors.push(`rgba(${r}, ${g}, ${b}, 0.8)`);
    }
    return colors;
}

// (triage chart removed on this branch)

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

function updateValidationsHeading(validations) {
    // Group by test_id to count unique tests
    const groupedByTestId = {};
    validations.forEach(result => {
        if (!groupedByTestId[result.test_id]) {
            groupedByTestId[result.test_id] = [];
        }
        groupedByTestId[result.test_id].push(result);
    });

    // Calculate x: number of unique tests with results needing attention
    const x = Object.keys(groupedByTestId).length;

    // Calculate y: sum of all record counts
    const y = validations.reduce((sum, result) => sum + parseInt(result.count || 1), 0);

    // Update the heading
    const heading = document.querySelector('h2 i.fa-flag').parentElement;
    heading.innerHTML = `<i class="fa-regular fa-flag" style="color: #f59e0b;"></i> Validations and issues <small>(<strong>${formatNumber(x)}</strong> tests with results needing attention, total of <strong>${formatNumber(y)}</strong> records affected)</small>`;
}

function renderValidationsList(uniqueResultsWithTestContext) {
    let validations = uniqueResultsWithTestContext.filter(result => 
        (result.status === 'RUN_HAS_RESULT' && result.result === 'NOT_COMPLIANT') ||
        (result.status === 'RUN_HAS_RESULT' && result.result === 'POTENTIAL_ISSUE')
    );

    // Apply use case filter if not 'all'
    if (currentUseCaseFilter !== 'all') {
        validations = validations.filter(result => {
            const useCases = result.use_cases || '';
            return useCases.includes(currentUseCaseFilter);
        });
    }

    // Update the heading with dynamic counts
    updateValidationsHeading(validations);

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

    // Sort groups by total count (descending)
    const sortedGroups = Object.entries(groupedData).sort(([, itemsA], [, itemsB]) => {
        const totalCountA = itemsA.reduce((sum, item) => sum + parseInt(item.count || 1), 0);
        const totalCountB = itemsB.reduce((sum, item) => sum + parseInt(item.count || 1), 0);
        return totalCountB - totalCountA; // Descending order
    });

    // Calculate min/max counts for gradient color mapping
    const counts = sortedGroups.map(([, items]) => 
        items.reduce((sum, item) => sum + parseInt(item.count || 1), 0)
    );
    const minCount = Math.min(...counts);
    const maxCount = Math.max(...counts);
    
    // Generate gradient colors (using same logic as chart)
    const gradientColors = generateGradientColors(sortedGroups.length);
    
    // Function to get color for a given count
    const getColorForCount = (count) => {
        if (maxCount === minCount) return gradientColors[0]; // All same count
        const ratio = (count - minCount) / (maxCount - minCount);
        const colorIndex = Math.floor(ratio * (gradientColors.length - 1));
        return gradientColors[colorIndex];
    };

    const totalPages = Math.ceil(sortedGroups.length / itemsPerPage);
    const startIndex = (currentPage - 1) * itemsPerPage;
    const endIndex = startIndex + itemsPerPage;
    const pageData = sortedGroups.slice(startIndex, endIndex);

    // Render list items using proper Bootstrap list-group structure
    container.innerHTML = `
        <div class="list-group">
            ${pageData.map(([testId, items]) => {
                const totalCount = items.reduce((sum, item) => sum + parseInt(item.count || 1), 0);
                const testInfo = tg2Tests.find(test => test.test_id === testId);
                const topCombos = getTopFieldCombinations(items, 5);
                const appliedAmendments = containerId === 'validations' ? getAmendmentsAppliedForValidation(testId) : [];
                const appliedAmendmentTests = containerId === 'validations' ? getAppliedAmendmentTests(appliedAmendments) : [];
                const badgeColor = getColorForCount(totalCount);

                return `
                    <a href="#" class="list-group-item list-group-item-action" 
                       onclick="openDetailModal('${testId}', '${containerId}'); return false;">
                        <div class="d-flex w-100 justify-content-between">
                            <h5 class="mb-1">${formatTestTitle(testId)}</h5>
                            <span class="badge totalcount" style="background-color: ${badgeColor}; color: #000;">${formatNumber(totalCount)} records affected</span>
                        </div>
                         <p class="mb-1">${testInfo ? testInfo.description : 'No description available'}</p>
                        ${containerId === 'validations' && appliedAmendmentTests.length > 0 ? `
                        <div class="mb-1">
                            <small class="text-body-secondary">Amendments: 
                                ${appliedAmendmentTests.slice(0, 5).map(t => `
                                    <span class="badge bg-secondary me-1">${formatTestTitle(t.test_id)}</span>
                                `).join('')}
                                ${appliedAmendmentTests.length > 5 ? `+${appliedAmendmentTests.length - 5} more` : ''}
                            </small>
                        </div>
                        ` : ''}
                        <small class="text-body-secondary">
                            ${topCombos.slice(0, 2).map(combo => {
                                const displayText = containerId === 'amendments' ? 
                                    formatAmendmentDisplay(combo.item) : 
                                    combo.combo;
                                return `Affected ${formatNumber(combo.count)} records with ${displayText}`;
                            }).join('; ')}
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
    const modalFooter = document.querySelector('#detailModal .modal-footer');

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
             (result.status === 'RUN_HAS_RESULT' && result.result === 'POTENTIAL_ISSUE'))
        );
    }

    const testInfo = tg2Tests.find(test => test.test_id === testId);
    const allCombos = getTopFieldCombinations(testData, testData.length);

    // Store data for pagination and search
    modalAllCombos = allCombos;
    modalFilteredCombos = allCombos;
    modalCurrentPage = 1;
    modalSearchTerm = '';

    const appliedAmendments = containerType === 'validations' ? getAmendmentsAppliedForValidation(testId) : [];

    const modalHTML = `
        <blockquote>
            <p class="description"><i class="fas fa-sticky-note text-warning"></i> <strong>${testInfo ? testInfo.description : 'No description available'}</strong></p>
            ${testInfo && testInfo.notes ? `<p><i class="fas fa-info-circle text-primary"></i> ${testInfo.notes}</p>` : ''}
            ${testInfo && testInfo.suggested_fixes ? `<p><i class="fa-solid fa-wrench"></i></i> <strong>Suggested Fixes:</strong> ${testInfo.suggested_fixes}</p>` : ''}
            ${testInfo && testInfo.gbif_autofix_note ? `
                <p class="gbif-autofix-section">
                    <svg class="gbif-logo-icon" viewBox="90 239.1 539.7 523.9" xmlns="http://www.w3.org/2000/svg">
                        <path class="gbif-logo-svg" d="M325.5,495.4c0-89.7,43.8-167.4,174.2-167.4C499.6,417.9,440.5,495.4,325.5,495.4"></path>
                        <path class="gbif-logo-svg" d="M534.3,731c24.4,0,43.2-3.5,62.4-10.5c0-71-42.4-121.8-117.2-158.4c-57.2-28.7-127.7-43.6-192.1-43.6
                        c28.2-84.6,7.6-189.7-19.7-247.4c-30.3,60.4-49.2,164-20.1,248.3c-57.1,4.2-102.4,29.1-121.6,61.9c-1.4,2.5-4.4,7.8-2.6,8.8
                        c1.4,0.7,3.6-1.5,4.9-2.7c20.6-19.1,47.9-28.4,74.2-28.4c60.7,0,103.4,50.3,133.7,80.5C401.3,704.3,464.8,731.2,534.3,731"></path>
                    </svg>
                    <strong>GBIF corrects (on ingestion):</strong> ${testInfo.gbif_autofix_note}
                </p>
            ` : ''}
        </blockquote>
        <div>
            <div class="d-flex justify-content-between align-items-center mb-3">
                <h6 class="mb-0">The following fields in your data were flagged:</h6>
                ${containerType === 'validations' && testData.length > 0 ? `<button type="button" class="btn btn-outline-dark btn-sm btn-dl-custom" onclick="downloadValidationCSV('${testId}')"><i class="fas fa-download me-1"></i>Download</button>` : ''}
            </div>
            
            <!-- Search box -->
            <div class="input-group">
                <span class="input-group-text"><i class="fas fa-search"></i></span>
                <input type="text" 
                        class="form-control" 
                        id="modal-search-input" 
                        placeholder="Search field combinations..." 
                        onkeyup="filterModalCombos(this.value)"
                        value="">
            </div>
            
            <!-- Field combinations container -->
            <div id="modal-field-combinations" 
                 class="field-combinations" 
                 data-container-type="${containerType}" 
                 data-test-id="${testId}">
                <!-- Content will be populated by renderModalFieldCombinations() -->
            </div>
            
            <!-- Pagination info and controls -->
            <div class="d-flex justify-content-between align-items-center mt-3">
                <div id="modal-pagination-info" class="text-muted small">
                    <!-- Pagination info will be populated by renderModalFieldCombinations() -->
                </div>
                <div id="modal-pagination-controls" class="btn-group">
                    <!-- Pagination controls will be populated by renderModalFieldCombinations() -->
                </div>
            </div>
        </div>
    `;

    modalBody.innerHTML = modalHTML;
    
    // Reset modal footer to default
    modalFooter.innerHTML = `
        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
    `;
    
    modal.show();
    
    // Initialize the field combinations display with pagination
    renderModalFieldCombinations();
}

function getTopFieldCombinations(items, limit) {
    const comboCounts = {};
    
    items.forEach(item => {
        const combo = buildNormalizedCombo(item);
        if (!comboCounts[combo]) {
            comboCounts[combo] = { 
                count: 0, 
                comment: item.comment || '',
                item: item // Store the original item for amendment processing
            };
        }
        comboCounts[combo].count += parseInt(item.count || 1);
    });

    return Object.entries(comboCounts)
        .map(([combo, data]) => ({ 
            combo, 
            count: data.count, 
            comment: data.comment,
            item: data.item // Include the original item
        }))
        .sort((a, b) => b.count - a.count)
        .slice(0, limit);
}

// Modal search and pagination functions
function filterModalCombos(searchTerm) {
    modalSearchTerm = searchTerm.toLowerCase();
    modalFilteredCombos = modalAllCombos.filter(combo => {
        const searchableText = `${combo.combo} ${combo.comment || ''}`.toLowerCase();
        return searchableText.includes(modalSearchTerm);
    });
    modalCurrentPage = 1; // Reset to first page when searching
    renderModalFieldCombinations();
}

function getModalPageData() {
    const startIndex = (modalCurrentPage - 1) * modalItemsPerPage;
    const endIndex = startIndex + modalItemsPerPage;
    return modalFilteredCombos.slice(startIndex, endIndex);
}

function getModalTotalPages() {
    return Math.ceil(modalFilteredCombos.length / modalItemsPerPage);
}

function renderModalFieldCombinations() {
    const fieldCombinationsContainer = document.getElementById('modal-field-combinations');
    if (!fieldCombinationsContainer) return;

    const pageData = getModalPageData();
    const totalPages = getModalTotalPages();
    const totalItems = modalFilteredCombos.length;
    const startItem = (modalCurrentPage - 1) * modalItemsPerPage + 1;
    const endItem = Math.min(modalCurrentPage * modalItemsPerPage, totalItems);

    // Get container type from the modal
    const containerType = fieldCombinationsContainer.dataset.containerType || 'validations';
    const testId = fieldCombinationsContainer.dataset.testId || '';

    fieldCombinationsContainer.innerHTML = `
        ${pageData.map(combo => {
            const displayText = containerType === 'amendments' ? 
                formatAmendmentDisplay(combo.item) : 
                combo.combo;
            
            // For validations, check if there are applicable amendments
            let amendmentInfo = '';
            let hasAmendments = false;
            if (containerType === 'validations') {
                const applicableAmendments = getApplicableAmendmentsForValidation(combo.item, testId);
                if (applicableAmendments.length > 0) {
                    hasAmendments = true;
                    amendmentInfo = `
                        <div class="mt-1">
                            <small><i class="fas fa-check-circle text-success"></i> Automatic fix: had the ${applicableAmendments.map(a => `<span class="badge bg-secondary me-1">${formatTestTitle(a.test_id)}</span>`).join('')} amendment applied</small>
                            <div class="mt-1">
                                <small class="text-muted">${applicableAmendments.map(a => formatAmendmentResult(a.result, a.actedUpon)).join('; ')}</small>
                            </div>
                        </div>
                    `;
                }
            }
            
            const fieldComboClass = hasAmendments ? 'field-combo field-combo-amended' : 'field-combo';
            
            return `
            <div class="${fieldComboClass}">
                <span class="field-combo-count">${formatNumber(combo.count)} records flagged</span>, with ${displayText}
                ${combo.comment && combo.comment.trim() ? `
                <div class="mt-1">
                    <small><i class="fas fa-comment"></i> ${combo.comment}</small>
                </div>
                ` : ''}
                ${amendmentInfo}
            </div>
        `;
        }).join('')}
    `;

    // Update pagination info
    const paginationInfo = document.getElementById('modal-pagination-info');
    if (paginationInfo) {
        paginationInfo.textContent = `Showing ${startItem}-${endItem} of ${totalItems} field combinations`;
    }

    // Update pagination controls
    const paginationControls = document.getElementById('modal-pagination-controls');
    if (paginationControls) {
        paginationControls.innerHTML = `
            <button class="btn btn-outline-secondary btn-sm" 
                    onclick="changeModalPage(1)" 
                    ${modalCurrentPage === 1 ? 'disabled' : ''}>
                <i class="fas fa-angle-double-left"></i>
            </button>
            <button class="btn btn-outline-secondary btn-sm" 
                    onclick="changeModalPage(${modalCurrentPage - 1})" 
                    ${modalCurrentPage === 1 ? 'disabled' : ''}>
                <i class="fas fa-angle-left"></i>
            </button>
            <span class="mx-2">Page ${modalCurrentPage} of ${totalPages}</span>
            <button class="btn btn-outline-secondary btn-sm" 
                    onclick="changeModalPage(${modalCurrentPage + 1})" 
                    ${modalCurrentPage === totalPages ? 'disabled' : ''}>
                <i class="fas fa-angle-right"></i>
            </button>
            <button class="btn btn-outline-secondary btn-sm" 
                    onclick="changeModalPage(${totalPages})" 
                    ${modalCurrentPage === totalPages ? 'disabled' : ''}>
                <i class="fas fa-angle-double-right"></i>
            </button>
        `;
    }
}

function changeModalPage(page) {
    const totalPages = getModalTotalPages();
    if (page >= 1 && page <= totalPages) {
        modalCurrentPage = page;
        renderModalFieldCombinations();
    }
}

function formatTestTitle(testId) {
    return testId
        .replace(/_/g, ' ')
        .split(' ')
        .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
        .join(' ')
        .replace(/^Amendment\s+/i, '')
        .replace(/^Validation\s+/i, '');
}

function parseIEList(value) {
    if (!value) return [];
    // Values may be like "dwc:countryCode" or comma-separated list
    return value
        .split(',')
        .map(s => s.trim())
        .filter(Boolean);
}

function getAmendmentsAppliedForValidation(validationTestId) {
    const validationInfo = tg2Tests.find(t => t.test_id === validationTestId);
    if (!validationInfo) return [];
    const vFields = new Set([
        ...parseIEList(validationInfo.ie_acted_upon || ''),
        ...parseIEList(validationInfo.ie_consulted || '')
    ]);
    if (vFields.size === 0) return [];

    // Collect applied amendment rows from uniqueResults
    const appliedRows = uniqueResults.filter(r =>
        r.test_type && r.test_type.toLowerCase() === 'amendment' &&
        (r.status === 'AMENDED' || r.status === 'FILLED_IN')
    );

    // Only keep those amendments whose acted-upon fields intersect with the validation's fields
    const groups = new Map(); // key: test_id||result -> {test_id, result, count}
    appliedRows.forEach(r => {
        const amendInfo = tg2Tests.find(t => t.test_id === r.test_id);
        const aFields = new Set(parseIEList(amendInfo && amendInfo.ie_acted_upon || ''));
        const intersects = [...aFields].some(f => vFields.has(f));
        if (!intersects) return;
        const key = `${r.test_id}||${r.result || ''}`;
        const c = parseInt(r.count || 1);
        if (!groups.has(key)) {
            groups.set(key, { test_id: r.test_id, result: r.result || '', count: 0 });
        }
        const entry = groups.get(key);
        entry.count += isNaN(c) ? 0 : c;
    });

    return Array.from(groups.values()).sort((a, b) => b.count - a.count);
}

function getApplicableAmendmentsForValidation(validationItem, validationTestId) {
    // Parse the validation item's acted upon values
    const validationFields = parseActedUponValues(validationItem.actedUpon || '');
    if (validationFields.length === 0) return [];
    
    // Find amendments that match the same field values
    const applicableAmendments = [];
    
    validationFields.forEach(validationField => {
        // Look for amendments that have the same field and original value
        amendmentLookupMap.forEach((amendmentData, key) => {
            const amendmentResults = parseAmendmentResults(amendmentData.result);
            const amendmentActedUpon = parseActedUponValues(amendmentData.actedUpon);
            
            // Check if this amendment affects the same field with the same original value
            const matchingAmendment = amendmentActedUpon.find(acted => 
                acted.field === validationField.field && acted.value === validationField.value
            );
            
            if (matchingAmendment) {
                const amendmentResult = amendmentResults.find(result => result.field === validationField.field);
                if (amendmentResult) {
                    applicableAmendments.push({
                        test_id: amendmentData.test_id,
                        result: amendmentData.result,
                        actedUpon: amendmentData.actedUpon,
                        count: amendmentData.count
                    });
                }
            }
        });
    });
    
    return applicableAmendments;
}

function truncateText(text, maxLen) {
    if (!text) return '';
    if (text.length <= maxLen) return text;
    return text.slice(0, maxLen - 1) + '…';
}

function getAppliedAmendmentTests(appliedAmendments) {
    const counts = new Map();
    appliedAmendments.forEach(a => {
        counts.set(a.test_id, (counts.get(a.test_id) || 0) + (a.count || 0));
    });
    return Array.from(counts.entries())
        .map(([test_id, count]) => ({ test_id, count }))
        .sort((a, b) => b.count - a.count);
}

function formatAmendmentResult(resultStr, actedUponStr) {
    if (!resultStr) return '';
    const pairs = resultStr.split('|').map(s => s.trim()).filter(Boolean);
    if (pairs.length === 0) return '';
    
    // Parse acted upon values if available
    const actedUponValues = parseActedUponValues(actedUponStr || '');
    
    const parts = pairs.map(p => {
        const idx = p.indexOf('=');
        if (idx === -1) return p;
        const key = p.slice(0, idx);
        const val = p.slice(idx + 1);
        
        // Try to find the original value for before/after display
        const original = actedUponValues.find(acted => acted.field === key);
        if (original) {
            const caseChangeType = getCaseChangeType(original.value, val);
            const caseChangePill = caseChangeType ? 
                `<span class="badge bg-light-green case-change-pill">${caseChangeType}</span>` : '';
            return `${key} amended from ${original.value} → ${val} ${caseChangePill}`;
        } else {
            return `${key} → ${val}`;
        }
    });
    return `Change(s): ${parts.join('; ')}`;
}

function splitFieldPairs(str) {
    if (!str) return [];
    return str.split('|').map(s => s.trim()).filter(Boolean);
}

function buildNormalizedCombo(item) {
    const parts = [];
    const seen = new Set();
    splitFieldPairs(item.actedUpon || '').forEach(p => {
        if (!seen.has(p)) { seen.add(p); parts.push(p); }
    });
    splitFieldPairs(item.consulted || '').forEach(p => {
        if (!seen.has(p)) { seen.add(p); parts.push(p); }
    });
    if (parts.length === 0) return 'N/A';
    return parts.join(' · ');
}

function parseAmendmentResults(resultStr) {
    if (!resultStr) return [];
    return resultStr.split('|').map(s => s.trim()).filter(Boolean).map(pair => {
        const idx = pair.indexOf('=');
        if (idx === -1) return { field: pair, value: '' };
        return { field: pair.slice(0, idx), value: pair.slice(idx + 1) };
    });
}

function parseActedUponValues(actedUponStr) {
    if (!actedUponStr) return [];
    return actedUponStr.split('|').map(s => s.trim()).filter(Boolean).map(pair => {
        const idx = pair.indexOf('=');
        if (idx === -1) return { field: pair, value: '' };
        return { field: pair.slice(0, idx), value: pair.slice(idx + 1) };
    });
}

function isCaseOnlyChange(originalValue, newValue) {
    if (!originalValue || !newValue) return false;
    return originalValue.toLowerCase() === newValue.toLowerCase() && originalValue !== newValue;
}

function getCaseChangeType(originalValue, newValue) {
    if (!isCaseOnlyChange(originalValue, newValue)) return null;
    
    // Check if it's a simple case change
    const originalLower = originalValue.toLowerCase();
    const newLower = newValue.toLowerCase();
    
    if (originalLower === newLower) {
        // Determine the type of case change
        if (originalValue === originalValue.toUpperCase() && newValue === newValue.toLowerCase()) {
            return 'lowercase change';
        } else if (originalValue === originalValue.toLowerCase() && newValue === newValue.toUpperCase()) {
            return 'uppercase change';
        } else if (originalValue !== newValue) {
            return 'case change';
        }
    }
    
    return 'case change';
}

function formatAmendmentDisplay(item) {
    if (item.status !== 'AMENDED' && item.status !== 'FILLED_IN') {
        // For non-amendments, use the original format
        return buildNormalizedCombo(item);
    }
    
    const amendmentResults = parseAmendmentResults(item.result);
    const actedUponValues = parseActedUponValues(item.actedUpon);
    
    if (amendmentResults.length === 0 || actedUponValues.length === 0) {
        return buildNormalizedCombo(item);
    }
    
    const amendments = [];
    
    // Match amendment results with acted upon values
    amendmentResults.forEach(amendment => {
        const original = actedUponValues.find(acted => acted.field === amendment.field);
        if (original) {
            const caseChangeType = getCaseChangeType(original.value, amendment.value);
            amendments.push({
                field: amendment.field,
                originalValue: original.value,
                newValue: amendment.value,
                caseChangeType: caseChangeType
            });
        }
    });
    
    if (amendments.length === 0) {
        return buildNormalizedCombo(item);
    }
    
    return amendments.map(amendment => {
        const caseChangePill = amendment.caseChangeType ? 
            `<span class="badge bg-light-green case-change-pill">${amendment.caseChangeType}</span>` : '';
        return `${amendment.field} amended from ${amendment.originalValue} → ${amendment.newValue} ${caseChangePill}`;
    }).join('; ');
}

function initializeTooltips() {
    // Initialize Bootstrap tooltips
    const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });
}

function initializeUseCaseFilters() {
    // Add event listeners to all filter buttons
    const filterButtons = document.querySelectorAll('[data-use-case]');
    filterButtons.forEach(button => {
        button.addEventListener('click', function() {
            // Remove active class from all buttons
            filterButtons.forEach(btn => btn.classList.remove('active'));
            
            // Add active class to clicked button
            this.classList.add('active');
            
            // Update current filter
            currentUseCaseFilter = this.getAttribute('data-use-case');
            
            // Reset to first page when filtering
            currentValidationsPage = 1;
            
            // Re-render validations list with new filter
            const uniqueResultsWithTestContext = joinResultsWithTestContext();
            renderValidationsList(uniqueResultsWithTestContext);
        });
    });
}

function downloadValidationCSV(testId) {
    // Get all data for this test
    const uniqueResultsWithTestContext = joinResultsWithTestContext();
    const testData = uniqueResultsWithTestContext.filter(result => 
        result.test_id === testId && 
        ((result.status === 'RUN_HAS_RESULT' && result.result === 'NOT_COMPLIANT') ||
         (result.status === 'RUN_HAS_RESULT' && result.result === 'POTENTIAL_ISSUE'))
    );

    if (testData.length === 0) {
        alert('No data available for download');
        return;
    }

    // Get all unique field names from actedUpon and consulted
    const allFields = new Set();
    testData.forEach(item => {
        // Parse actedUpon fields
        if (item.actedUpon) {
            const actedUponFields = item.actedUpon.split('|').map(field => field.trim()).filter(Boolean);
            actedUponFields.forEach(field => {
                const fieldName = field.split('=')[0];
                if (fieldName) allFields.add(fieldName);
            });
        }
        
        // Parse consulted fields
        if (item.consulted) {
            const consultedFields = item.consulted.split('|').map(field => field.trim()).filter(Boolean);
            consultedFields.forEach(field => {
                const fieldName = field.split('=')[0];
                if (fieldName) allFields.add(fieldName);
            });
        }
    });

    // Convert Set to sorted array
    const fieldNames = Array.from(allFields).sort();
    
    // Add Records flagged column
    const headers = [...fieldNames, 'Records flagged'];
    
    // Generate CSV rows
    const rows = testData.map(item => {
        const row = {};
        
        // Initialize all fields with empty values
        fieldNames.forEach(field => {
            row[field] = '';
        });
        
        // Fill in actedUpon values
        if (item.actedUpon) {
            const actedUponFields = item.actedUpon.split('|').map(field => field.trim()).filter(Boolean);
            actedUponFields.forEach(field => {
                const [fieldName, value] = field.split('=');
                if (fieldName && value !== undefined) {
                    row[fieldName] = value;
                }
            });
        }
        
        // Fill in consulted values (only if not already filled by actedUpon)
        if (item.consulted) {
            const consultedFields = item.consulted.split('|').map(field => field.trim()).filter(Boolean);
            consultedFields.forEach(field => {
                const [fieldName, value] = field.split('=');
                if (fieldName && value !== undefined && !row[fieldName]) {
                    row[fieldName] = value;
                }
            });
        }
        
        // Add count
        row['Records flagged'] = item.count || 1;
        
        return row;
    });

    // Convert to CSV format
    const csvContent = generateCSV(headers, rows);
    
    // Create and trigger download
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    const url = URL.createObjectURL(blob);
    link.setAttribute('href', url);
    link.setAttribute('download', `${testId}_items.csv`);
    link.style.visibility = 'hidden';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
}

function generateCSV(headers, rows) {
    // Escape CSV values (handle commas, quotes, newlines)
    function escapeCSVValue(value) {
        if (value === null || value === undefined) return '';
        const stringValue = String(value);
        if (stringValue.includes(',') || stringValue.includes('"') || stringValue.includes('\n')) {
            return '"' + stringValue.replace(/"/g, '""') + '"';
        }
        return stringValue;
    }
    
    // Generate header row
    const headerRow = headers.map(escapeCSVValue).join(',');
    
    // Generate data rows
    const dataRows = rows.map(row => 
        headers.map(header => escapeCSVValue(row[header] || '')).join(',')
    );
    
    return [headerRow, ...dataRows].join('\n');
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
