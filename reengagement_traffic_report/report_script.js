var sortStates = {};

function toggleChart(rowId) {
    var chartCell = document.getElementById("chart-" + rowId);
    chartCell.classList.toggle("show-chart");
}

function expandAll() {
    var charts = document.getElementsByClassName("chart-cell");
    for (var i = 0; i < charts.length; i++) {
        charts[i].classList.add("show-chart");
    }
}

function collapseAll() {
    var charts = document.getElementsByClassName("chart-cell");
    for (var i = 0; i < charts.length; i++) {
        charts[i].classList.remove("show-chart");
    }
}

function sortTable(columnIndex) {
    var table, rows, switching, i, x, y, shouldSwitch;
    table = document.getElementById("reportTable");
    // switching = true;

    var sortState = sortStates[columnIndex];
    if (!sortState) {
        sortState = 'asc';
    } else if (sortState === 'asc') {
        sortState = 'desc';
    } else if (sortState === 'desc') {
        sortState = '';
    }

    var sortIcon = document.getElementById("sort-icon-" + columnIndex);

    // Remove sort indicator from all other columns
    var sortIcons = document.getElementsByClassName("sort-icon");
    for (var j = 0; j < sortIcons.length; j++) {
        sortIcons[j].innerHTML = '';
    }

    rows = table.tBodies[0].getElementsByTagName("tr");
    arrRows = Array.from(rows);
    arrRows.sort(function(a, b) {
        var x = a.getElementsByTagName("td")[columnIndex].getAttribute("data-sort-value");
        var y = b.getElementsByTagName("td")[columnIndex].getAttribute("data-sort-value");
        if (sortState === 'asc') {
            if (isNumeric(x) && isNumeric(y)) {
                return parseFloat(x) - parseFloat(y);
            } else {
                return x.toLowerCase().localeCompare(y.toLowerCase());
            }
        } else if (sortState === 'desc') {
            if (isNumeric(x) && isNumeric(y)) {
                return parseFloat(y) - parseFloat(x);
            } else {
                return y.toLowerCase().localeCompare(x.toLowerCase());
            }
        }
    })

    var newBody = table.createTBody();
    for (const row of arrRows) {
        newBody.appendChild(row);
    }
    table.replaceChild(newBody, table.tBodies[0]);

    sortStates[columnIndex] = sortState;
    sortIcon.innerHTML = sortState === 'asc' ? '&#x25B2;' : sortState === 'desc' ? '&#x25BC;' : '';
}


function isNumeric(value) {
    return !isNaN(parseFloat(value)) && isFinite(value);
}

function filterTable() {
    var geoSelect = document.getElementById("spc-geo-select");
    var bucketSelect = document.getElementById("mean-bucket-select");
    var highPrioritySelect = document.getElementById("high-priority-check");
    var highPrioritySelectValue = highPrioritySelect.checked;
    var geoValue = geoSelect.value.toLowerCase();
    var bucketValue = bucketSelect.value.toLowerCase();

    var table = document.getElementById("reportTable");
    var rows = table.getElementsByTagName("tr");

    for (var i = 1; i < rows.length; i++) {
        var row = rows[i];
        var spcGeo = row.getElementsByTagName("td")[0].innerHTML.toLowerCase();
        var trafficBucket = row.getElementsByTagName("td")[15].innerHTML.toLowerCase();
        var highPriority = row.getElementsByTagName("td")[16].innerHTML.toLowerCase();

        var geoMatch = spcGeo === geoValue || geoValue === "all";
        var bucketMatch = trafficBucket === bucketValue || bucketValue === "all";
        var highPriorityMatch = highPrioritySelectValue ? highPriority === 'true' : true

        if (geoMatch && bucketMatch && highPriorityMatch) {
            row.style.display = "";
        } else {
            row.style.display = "none";
        }
    }
}