function getSVG(data, settings) {

    const { bars, suffix } = settings;
    const svgNS = "http://www.w3.org/2000/svg";
    const svg = document.createElementNS(svgNS, "svg");

    const tooltip = document.getElementById("svg-tooltip");

    if (!data || !data.length) return svg;

    const leftPadding = 17 + Math.max(...data.map((d) => numberToShortString(d).length)) * 4;

    const width = 550;
    const height = 100;
    const numGridLines = 4;

    const padding = 10;  // Top and bottom padding within the SVG

    const graphPadding = 10;

    // Create SVG element
    svg.setAttribute("width", width);
    svg.setAttribute("height", height);

    if (!data || data.length < 2) return svg;

    // Create gradient
    createGradient(svg, "df-gradient", [
        { offset: "0%", color: "#5A3EE0" },
        { offset: "100%", color: "#C53EE0" }
    ]);

    let maxY = Math.max(...data);

    if (suffix == "%" && maxY <= 100) maxY = 100;
    else if (maxY === 0 || (maxY >= 1 && maxY <= 10)) {
        maxY = 10;
    } else {
        const uniqueData = [...new Set(data)];
        maxY = uniqueData.length === 1 ? maxY * 2 : maxY;
    }

    maxY = nextDivisible(maxY, numGridLines);

    let scaledData;
    scaledData = data.map(d => (height - padding) - (d / maxY) * (height - 2 * padding));

    drawGrid();

    if (bars) drawBarGraph();
    else drawGraph();

    return svg;

    function nextDivisible(num, divider) {
        return num % divider === 0 ? num : num + (divider - (num % divider));
    }

    function numberToShortString(num) {
        num = Number(num);
        if (num === 0) return "0";
        const units = ["", "K", "M", "B"];
        const isNegative = num < 0;
        num = Math.abs(num);
        const unit = Math.floor(Math.log10(num) / 3);
        let scaledNum = num / Math.pow(1000, unit);
        scaledNum = scaledNum.toFixed(1);
        let roundedNum = scaledNum.replace(/\.0$/, ''); // Remove unnecessary .0 if the number is whole
        return (isNegative ? '-' : '') + roundedNum + units[unit];
    }

    function drawGrid() {
        // Draw Y-axis grid lines and labels
        for (let i = 0; i <= numGridLines; i++) {
            const lineY = Math.round(padding + (height - 2 * padding) / numGridLines * i);
            const value = maxY * (1 - i / numGridLines); // Calculate the value at each grid line

            // Draw the grid line
            const gridLine = document.createElementNS(svgNS, "line");
            gridLine.setAttribute("x1", leftPadding);
            gridLine.setAttribute("y1", lineY);
            gridLine.setAttribute("x2", width); // Ensure lines span the width of the graph
            gridLine.setAttribute("y2", lineY);
            gridLine.setAttribute("stroke", "#525259");
            gridLine.setAttribute("stroke-width", "1");
            gridLine.setAttribute("shape-rendering", "crispEdges");
            svg.appendChild(gridLine);

            // Add text label for the grid line
            const textLabel = document.createElementNS(svgNS, "text");
            textLabel.setAttribute("x", leftPadding - 5);
            textLabel.setAttribute("y", lineY + 4);
            textLabel.setAttribute("font-size", "10px");
            textLabel.setAttribute("fill", "#525259");
            textLabel.setAttribute("text-anchor", "end");
            textLabel.textContent = numberToShortString(value);
            svg.appendChild(textLabel);
        }
    }

    function drawGraph() {
        // Define the path for the line graph

        const xDist = (width - leftPadding - graphPadding * 2) / HISTORY_WINDOW;
        const lp = leftPadding + graphPadding + (HISTORY_WINDOW - (data.length)) * xDist;

        //const lp = leftPadding * 1.45;


        let pathData = `M ${lp} ${scaledData[0]}`;

        // Check if all y values are the same
        const allYsSame = scaledData.every((y) => y === scaledData[0]);

        const path = document.createElementNS(svgNS, "path");

        if (allYsSame) {
            // Draw a straight horizontal line if all y values are the same
            const xEnd = Math.round(width - graphPadding);
            pathData += ` L ${xEnd} ${scaledData[0]}`;
            path.setAttribute("stroke", "#C53EE0");
        } else {
            // Draw the cubic Bezier curve if y values are not all the same
            for (let i = 1; i < scaledData.length; i++) {
                const x1 = lp + xDist * (i - 1);
                const y1 = scaledData[i - 1];
                const x2 = lp + xDist * i;
                const y2 = scaledData[i];
                const controlDistance = xDist / 2; // Adjust this to control the roundness
                const cx1 = x1 + controlDistance;
                const cy1 = y1;
                const cx2 = x2 - controlDistance;
                const cy2 = y2;
                pathData += ` C ${cx1},${cy1} ${cx2},${cy2} ${x2},${y2}`;
            }
            path.setAttribute("stroke", "url(#df-gradient)");
        }

        path.setAttribute("d", pathData);
        path.setAttribute("stroke-width", "2");
        path.setAttribute("fill", "none");
        svg.appendChild(path);

        data.forEach((value, index) => {
            const x = lp + xDist * index;
            const y = scaledData[index];

            const circle = document.createElementNS(svgNS, "circle");
            circle.setAttribute("cx", x);
            circle.setAttribute("cy", y);
            circle.setAttribute("r", 5);
            circle.setAttribute("fill", "#C53EE0");
            circle.setAttribute("opacity", 0);
            circle.setAttribute("style", "cursor: pointer;");

            addTooltip(circle, value, true);

            svg.appendChild(circle);
        });
    }


    function drawBarGraph() {
        const barPadding = width / data.length * 0.3; // Padding between bars
        const barWidth = (width - leftPadding - barPadding * (data.length + 1)) / data.length; // Calculate bar width

        data.forEach((value, index) => {
            let barHeight = (value / maxY) * (height - 2 * padding);
            if (barHeight == 0) barHeight = 1;

            const x = leftPadding + barPadding + (barWidth + barPadding) * index; // Adjust x to include padding
            const y = height - barHeight - padding; // Y position of the bar

            const rect = document.createElementNS(svgNS, "rect");
            rect.setAttribute("x", x);
            rect.setAttribute("width", barWidth);
            rect.setAttribute("y", y);
            rect.setAttribute("height", barHeight);
            rect.setAttribute("fill", "url(#df-gradient)");
            rect.setAttribute("style", "cursor: pointer;");
            svg.appendChild(rect);

            addTooltip(rect, value);

            svg.appendChild(rect);
        });
    }

    function addTooltip(element, value, hideElement) {
        element.addEventListener("mouseover", (event) => {
            if (hideElement) element.setAttribute("opacity", 1);
            tooltip.style.display = "block";
            tooltip.textContent = formatValue(value, settings);
            const rect = event.target.getBoundingClientRect();
            tooltip.style.left = `${rect.x + window.scrollX + rect.width / 2 - tooltip.clientWidth / 2}px`;
            tooltip.style.top = `${rect.y + window.scrollY - tooltip.clientHeight - 10}px`;
        });

        element.addEventListener("mousemove", (event) => {
            const rect = event.target.getBoundingClientRect();
            tooltip.style.left = `${rect.x + window.scrollX + rect.width / 2 - tooltip.clientWidth / 2}px`;
            tooltip.style.top = `${rect.y + window.scrollY - tooltip.clientHeight - 10}px`;
        });

        element.addEventListener("mouseout", () => {
            if (hideElement) element.setAttribute("opacity", 0); // Hide the dot
            tooltip.style.display = "none";
        });
    }

    function createGradient(svg, id, colorStops) {
        const defs = svg.querySelector('defs') || svg.appendChild(document.createElementNS(svgNS, "defs"));
        const gradient = document.createElementNS(svgNS, "linearGradient");
        gradient.setAttribute("id", id);
        gradient.setAttribute("x1", "0%");
        gradient.setAttribute("y1", "0%");
        gradient.setAttribute("x2", "0%");
        gradient.setAttribute("y2", "100%");

        colorStops.forEach(stopInfo => {
            const stop = document.createElementNS(svgNS, "stop");
            stop.setAttribute("offset", stopInfo.offset);
            stop.setAttribute("stop-color", stopInfo.color);
            gradient.appendChild(stop);
        });

        defs.appendChild(gradient);
    }
}

function formatValue(value, settings) {
    if (!(value || value == 0)) return "";

    if (settings.id == "uptime") {
        const vals = value.split(",");
        if (vals.length > 2) vals.pop();
        return vals.join(",");
    }

    value = Number(value);
    if (settings.id.startsWith("used_memory")) {
        return MemoryDisplay(value);
    }

    return `${value.toLocaleString('en-US')}${settings.suffix || ""}`;

    function MemoryDisplay(bytes) {
        const thresh = 1000;

        if (Math.abs(bytes) < thresh) {
            return bytes + ' B';
        }

        const units = ['kB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
        let u = -1;
        const r = 10;

        do {
            bytes /= thresh;
            ++u;
        } while (Math.round(Math.abs(bytes) * r) / r >= thresh && u < units.length - 1);

        const fixedBytes = bytes.toFixed(1);
        const decimalPart = fixedBytes.split('.')[1];
        const hasNonZeroDecimal = decimalPart && parseInt(decimalPart) !== 0;

        return (hasNonZeroDecimal ? fixedBytes : Math.round(bytes)) + units[u];
    }
}

function openTab(tabName) {
    var i, tabcontent, tablinks;

    tabcontent = document.getElementsByClassName("tab-container");
    for (i = 0; i < tabcontent.length; i++) {
        tabcontent[i].style.display = "none";
    }

    tablinks = document.getElementsByClassName("tablink");
    for (i = 0; i < tablinks.length; i++) {
        tablinks[i].className = tablinks[i].className.replace(" active", "");
    }

    document.getElementById(tabName).style.display = "flex";
    document.getElementById(`${tabName}-link`).className += " active";
    const params = new URLSearchParams(window.location.search);
    params.set('tab', tabName);
    window.history.replaceState({}, '', `${window.location.pathname}?${params}`);
}

function initGraphs() {

    const settings = [
        {
            id: "uptime",
            title: "Up Time",
            graph: false
        },
        {
            id: "total_commands_num",
            title: "Total Commands",
            graph: false
        },
        {
            id: "used_memory_bytes",
            title: "Memory"
        },
        {
            id: "num_connected_clients",
            title: "Connected Clients"
        },
        {
            id: "qps",
            title: "QPS"
        },
        {
            id: "hit_rate",
            title: "Hit Rate",
            suffix: "%"
        },
        {
            id: "used_memory",
            title: "Used Memory",
            bars: true
        },
        {
            id: "key_count",
            title: "Key Count",
            bars: true
        },
        {
            id: "expire_count",
            title: "Expire Count",
            bars: true
        },
        {
            id: "key_reads",
            title: "Key Reads",
            bars: true
        },
    ];

    settings.forEach((s) => initWidget(s));
}

function initWidget(settings) {
    const widget = new Widget(settings);
    document.getElementById(settings.id).appendChild(widget.html.element);
}

function Widget(settings) {

    this.settings = settings;
    this.html = initHtml(settings);

    const me = this;
    setInterval(() => {
        if (!settings.bars) me.updateHeader();
        if (me.settings.graph == undefined || me.settings.graph) me.updateGraph();
    }, UPDATE_INTERVAL)

    this.updateHeader = () => {
        const stats = this.stats();
        this.html.header.textContent = formatValue(stats[stats.length - 1], settings)
    }

    this.updateGraph = () => {
        const data = this.stats();
        const svg = getSVG(data, this.settings);
        me.html.graph.textContent = '';
        me.html.graph.appendChild(svg);
    }

    this.stats = () => {
        return this.settings.bars ? globalStats.shards_stats[this.settings.id] : globalStats[this.settings.id];
    }

    function initHtml({ title, bars }) {

        const html = {};

        const element = document.createElement("div");

        if (!bars) {
            const titleDiv = document.createElement("div");
            titleDiv.textContent = title;
            titleDiv.className = "w-title";
            element.appendChild(titleDiv);
            html.title = titleDiv;
        }

        const containerDiv = document.createElement("div");
        containerDiv.className = "graph-container";
        //containerDiv.style.width = bars ? "240px" : "340px";
        element.appendChild(containerDiv);

        const header = document.createElement("div");
        header.className = "w-header";
        if (bars) header.textContent = title;
        containerDiv.appendChild(header);

        /*
        if (settings.bar) {
            const progressBarBackground = document.createElement("div");
            progressBarBackground.className = "progress-bar-bg";
            const progressBar = document.createElement("div");
            progressBar.className = "progress-bar";
            progressBarBackground.appendChild(progressBar);
            containerDiv.appendChild(progressBarBackground);
            html.progressBar = progressBar;
        }
        */

        if (settings.graph == undefined || settings.graph) {
            const graphContainer = document.createElement("div");
            graphContainer.className = "w-graph";
            containerDiv.appendChild(graphContainer);
            html.graph = graphContainer;
        }

        return {
            ...html,
            element: element,
            header: header,
        }
    }
}

function initTabs() {
    const params = new URLSearchParams(window.location.search);
    const tab = params.get('tab');
    if (tab) openTab(tab);
    else openTab("dashboard");
}

window.addEventListener('DOMContentLoaded', (event) => {
    initTabs();
    initGraphs();
    setInterval(updateStats, UPDATE_INTERVAL);
    setInterval(updateShardStats, UPDATE_INTERVAL);
    loadAchievementsTab();
});