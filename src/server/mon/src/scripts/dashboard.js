function getSVG(data, bars) {
    const svgNS = "http://www.w3.org/2000/svg";
    const svg = document.createElementNS(svgNS, "svg");

    if (!data || !data.length) return svg;

    const leftPadding = 10 + Math.max(...data.map((d)=>numberToShortString(d).length)) * 8;

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

    const maxY = Math.max(...data);
    let yMax;
    let scaledData;

    if (maxY === 0 || (maxY >= 1 && maxY <= 10)) {
        yMax = 10;
    } else {
        const uniqueData = [...new Set(data)];
        yMax = uniqueData.length === 1 ? maxY * 2 : maxY; // Adjust scale if data contains same numbers
    }

    scaledData = data.map(d => (height - padding) - (d / yMax) * (height - 2 * padding));

    drawGrid(yMax);

    if (bars) drawBarGraph();
    else drawGraph();

    return svg;

    function numberToShortString(num) {
        if (num === 0) return "0";
        const units = ["", "K", "M", "B"];
        const isNegative = num < 0;
        num = Math.abs(num);
        const unit = Math.floor((num.toString().length - 1) / 3);
        let scaledNum = num / Math.pow(1000, unit);
        scaledNum = scaledNum.toFixed(1);
        let roundedNum = scaledNum.replace(/\.0$/, ''); // Remove unnecessary .0 if the number is whole
        return (isNegative ? '-' : '') + roundedNum + units[unit];
    }

    function drawGrid(yMax) {
        // Draw Y-axis grid lines and labels
        for (let i = 0; i <= numGridLines; i++) {
            const lineY = Math.round(padding + (height - 2 * padding) / numGridLines * i);
            const value = yMax * (1 - i / numGridLines); // Calculate the value at each grid line

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

        const xDist = (width-leftPadding-graphPadding*2)/HISTORY_WINDOW;
        const lp = leftPadding+graphPadding+(HISTORY_WINDOW-(data.length))*xDist;

        //const lp = leftPadding * 1.45;


        let pathData = `M ${lp} ${scaledData[0]}`;

        // Check if all y values are the same
        const allYsSame = scaledData.every((y) => y === scaledData[0]);

        const path = document.createElementNS(svgNS, "path");

        if (allYsSame) {
            // Draw a straight horizontal line if all y values are the same
            const xEnd = Math.round(width-graphPadding);
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
    }


    function drawBarGraph() {
        const barPadding = width / data.length * 0.3; // Padding between bars
        const barWidth = (width - leftPadding - barPadding * (data.length + 1)) / data.length; // Calculate bar width

        data.forEach((value, index) => {
            const barHeight = (value / yMax) * (height - 2 * padding);
            const x = leftPadding + barPadding + (barWidth + barPadding) * index; // Adjust x to include padding
            const y = height - barHeight - padding; // Y position of the bar

            const rect = document.createElementNS(svgNS, "rect");
            rect.setAttribute("x", x);
            rect.setAttribute("width", barWidth);
            rect.setAttribute("y", y);
            rect.setAttribute("height", barHeight);
            rect.setAttribute("fill", "url(#df-gradient)");
            svg.appendChild(rect);
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
        if (stats[stats.length - 1] || stats[stats.length - 1] == 0) {
            if (settings.id == "uptime") {
                const vals = stats[stats.length - 1].split(",");
                if (vals.length > 2) vals.pop();
                this.html.header.textContent = vals.join(",");
                return;
            }
            const num = Number(stats[stats.length - 1]).toLocaleString('en-US');
            this.html.header.textContent = `${num}${settings.suffix || ""}`;
        }
    }

    this.updateGraph = () => {
        const data = this.stats();
        const svg = getSVG(data, this.settings.bars);
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
    loadWinnerTab();
});