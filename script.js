const API_URL = "/api/flights";
const myRenderer = L.canvas({ padding: 0.5 });

const map = L.map("map", {
    preferCanvas: true, zoomControl: true, renderer: myRenderer
}).setView([39.0, 35.0], 5);

L.tileLayer("https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png", {
    maxZoom: 18, subdomains: 'abcd',
}).addTo(map);

let allFlightsData = [];
const aircraftMarkers = new Map();
const markerRenderState = new Map();
let currentTrackLayer = null;
let selectedFlightId = null;
const STALE_FLIGHT_THRESHOLD = 120 * 1000; 

let currentDisplayMode = "air";
document.getElementById("mode-checkbox").addEventListener("change", (e) => {
    currentDisplayMode = e.target.checked ? "ground" : "air";
    scheduleRenderVisibleFlights();
});

let filterMinAlt = 0; let filterMaxAlt = 60000;
let filterMinSpeed = 0; let filterMaxSpeed = 1000;
let filterCountry = "";

const ALL_COUNTRIES = [
    "Afghanistan", "Albania", "Algeria", "Andorra", "Angola", "Antigua and Barbuda", "Argentina", "Armenia", "Australia", "Austria", "Azerbaijan", "Bahamas", "Bahrain", "Bangladesh", "Barbados", "Belarus", "Belgium", "Belize", "Benin", "Bhutan", "Bolivia", "Bosnia and Herzegovina", "Botswana", "Brazil", "Brunei", "Bulgaria", "Burkina Faso", "Burundi", "Cabo Verde", "Cambodia", "Cameroon", "Canada", "Central African Republic", "Chad", "Chile", "China", "Colombia", "Comoros", "Congo", "Costa Rica", "Croatia", "Cuba", "Cyprus", "Czechia", "Denmark", "Djibouti", "Dominica", "Dominican Republic", "Ecuador", "Egypt", "El Salvador", "Equatorial Guinea", "Eritrea", "Estonia", "Eswatini", "Ethiopia", "Fiji", "Finland", "France", "Gabon", "Gambia", "Georgia", "Germany", "Ghana", "Greece", "Grenada", "Guatemala", "Guinea", "Guinea-Bissau", "Guyana", "Haiti", "Honduras", "Hungary", "Iceland", "India", "Indonesia", "Iran", "Iraq", "Ireland", "Israel", "Italy", "Jamaica", "Japan", "Jordan", "Kazakhstan", "Kenya", "Kiribati", "Kuwait", "Kyrgyzstan", "Laos", "Latvia", "Lebanon", "Lesotho", "Liberia", "Libya", "Liechtenstein", "Lithuania", "Luxembourg", "Madagascar", "Malawi", "Malaysia", "Maldives", "Mali", "Malta", "Marshall Islands", "Mauritania", "Mauritius", "Mexico", "Micronesia", "Moldova", "Monaco", "Mongolia", "Montenegro", "Morocco", "Mozambique", "Myanmar", "Namibia", "Nauru", "Nepal", "Netherlands", "New Zealand", "Nicaragua", "Niger", "Nigeria", "North Korea", "North Macedonia", "Norway", "Oman", "Pakistan", "Palau", "Panama", "Papua New Guinea", "Paraguay", "Peru", "Philippines", "Poland", "Portugal", "Qatar", "Romania", "Russia", "Rwanda", "Saint Kitts and Nevis", "Saint Lucia", "Saint Vincent and the Grenadines", "Samoa", "San Marino", "Sao Tome and Principe", "Saudi Arabia", "Senegal", "Serbia", "Seychelles", "Sierra Leone", "Singapore", "Slovakia", "Slovenia", "Solomon Islands", "Somalia", "South Africa", "South Korea", "South Sudan", "Spain", "Sri Lanka", "Sudan", "Suriname", "Sweden", "Switzerland", "Syria", "Tajikistan", "Tanzania", "Thailand", "Timor-Leste", "Togo", "Tonga", "Trinidad and Tobago", "Tunisia", "Turkey", "Turkmenistan", "Tuvalu", "Uganda", "Ukraine", "United Arab Emirates", "United Kingdom", "United States", "Uruguay", "Uzbekistan", "Vanuatu", "Venezuela", "Vietnam", "Yemen", "Zambia", "Zimbabwe"
];

const countrySelect = document.getElementById("f-country");
ALL_COUNTRIES.forEach(country => {
    let opt = document.createElement("option");
    opt.value = country.toLowerCase();
    opt.textContent = country;
    countrySelect.appendChild(opt);
});

const filterPanel = document.getElementById("filter-panel");
document.getElementById("toggle-filters-btn").addEventListener("click", () => {
    filterPanel.style.display = filterPanel.style.display === "none" || filterPanel.style.display === "" ? "block" : "none";
});

document.getElementById("btn-apply-filters").addEventListener("click", () => {
    filterMinAlt = Number(document.getElementById("f-min-alt").value) || 0;
    filterMaxAlt = Number(document.getElementById("f-max-alt").value) || 60000;
    filterMinSpeed = Number(document.getElementById("f-min-speed").value) || 0;
    filterMaxSpeed = Number(document.getElementById("f-max-speed").value) || 1000;
    filterCountry = document.getElementById("f-country").value.trim().toLowerCase();
    filterPanel.style.display = "none";
    scheduleRenderVisibleFlights();
});

document.getElementById("btn-reset-filters").addEventListener("click", () => {
    document.getElementById("f-min-alt").value = 0;
    document.getElementById("f-max-alt").value = 60000;
    document.getElementById("f-min-speed").value = 0;
    document.getElementById("f-max-speed").value = 1000;
    document.getElementById("f-country").value = "";
    filterMinAlt = 0; filterMaxAlt = 60000;
    filterMinSpeed = 0; filterMaxSpeed = 1000;
    filterCountry = "";
    scheduleRenderVisibleFlights();
});

function getAltitudeColor(altitudeMeters) {
    if (altitudeMeters === null || altitudeMeters === undefined) return '#a4b0be';
    const altitudeFt = altitudeMeters * 3.28084;
    if (altitudeFt < 5000) return '#ff4757';
    if (altitudeFt < 15000) return '#ffa502';
    if (altitudeFt < 25000) return '#eccc68';
    if (altitudeFt < 35000) return '#7bed9f';
    if (altitudeFt < 40000) return '#1e90ff';
    return '#9c88ff';
}

const PLANE_SVG_PATH = "M448 336v-40L288 192V79.2c0-28.5-29.3-47.2-56-31.2-26.7-16-56 2.7-56 31.2V192L16 296v40l160-48v113.6l-48 31.2V480l112-16 112 16v-47.2l-48-31.2V288l160 48z";
const ALTITUDE_COLORS = ['#a4b0be', '#ff4757', '#ffa502', '#eccc68', '#7bed9f', '#1e90ff', '#9c88ff'];

function renderPlaneIconPNG(fillColor, size = 24) {
    const canvas = document.createElement('canvas');
    canvas.width = size; canvas.height = size;
    const ctx = canvas.getContext('2d');
    const scale = size / 512;
    ctx.save(); ctx.scale(scale, scale);
    const path = new Path2D(PLANE_SVG_PATH);
    ctx.fillStyle = fillColor; ctx.strokeStyle = '#191919';
    ctx.lineWidth = 2 / scale; ctx.fill(path); ctx.stroke(path); ctx.restore();
    return canvas.toDataURL('image/png');
}

const planeImageCache = {};
ALTITUDE_COLORS.forEach(color => { 
    const img = new Image(); img.src = renderPlaneIconPNG(color, 24);
    planeImageCache[color] = img; 
});
const selectedImg = new Image(); selectedImg.src = renderPlaneIconPNG('#ffffff', 24);
planeImageCache.selected = selectedImg;

function planeImageFor(altitude, isSelected) {
    return isSelected ? planeImageCache.selected : planeImageCache[getAltitudeColor(altitude)];
}

L.CanvasPlaneMarker = L.CircleMarker.extend({
    _updatePath: function () {
        const renderer = this._renderer; const p = this._point; const ctx = renderer._ctx;
        if (!ctx) return;
        if (this.options.image && this.options.image.complete) {
            ctx.save(); ctx.translate(p.x, p.y); ctx.rotate((this.options.heading || 0) * Math.PI / 180);
            ctx.drawImage(this.options.image, -12, -12, 24, 24); ctx.restore();
        }
    }
});
L.canvasPlaneMarker = function (latlng, options) { return new L.CanvasPlaneMarker(latlng, options); };

function closeInfoPanel() {
    if (selectedFlightId && aircraftMarkers.has(selectedFlightId)) {
        const marker = aircraftMarkers.get(selectedFlightId);
        const state = markerRenderState.get(selectedFlightId);
        const flight = allFlightsData.find(f => f.icao24 === selectedFlightId);
        if(flight && state) {
            state.selected = false;
            marker.setStyle({ image: planeImageFor(flight.altitude, false) });
        }
    }
    selectedFlightId = null;
    updateInfoPanel(null);
    if (currentTrackLayer) { map.removeLayer(currentTrackLayer); currentTrackLayer = null; }
}

function updateInfoPanel(flight) {
    const panel = document.getElementById("info-panel");
    if (!flight) { panel.style.display = "none"; return; }

    const altitudeFeet = Number((flight.altitude || 0) * 3.28084).toFixed(0);
    const velocityKnots = Number((flight.velocity || 0) * 1.94384).toFixed(0);

    const html = `
        <div class="close-btn" onclick="closeInfoPanel()">✕</div>
        <div class="info-header">${flight.callsign}</div>
        <div class="info-row"><span class="info-label">ICAO24</span> <span class="info-value">${flight.icao24}</span></div>
        <div class="info-row"><span class="info-label">Country</span> <span class="info-value">${flight.origin_country}</span></div>
        <div class="info-row"><span class="info-label">Latitude</span> <span class="info-value">${flight.latitude ? flight.latitude.toFixed(4) : 'N/A'}°</span></div>
        <div class="info-row"><span class="info-label">Longitude</span> <span class="info-value">${flight.longitude ? flight.longitude.toFixed(4) : 'N/A'}°</span></div>
        <div class="info-row"><span class="info-label">Altitude</span> <span class="info-value">${altitudeFeet} ft</span></div>
        <div class="info-row"><span class="info-label">Speed</span> <span class="info-value">${velocityKnots} kts</span></div>
        <div class="info-row"><span class="info-label">Heading</span> <span class="info-value">${(flight.heading || 0).toFixed(0)}°</span></div>
        <div class="info-row"><span class="info-label">Last Updated</span> <span class="info-value">${flight.last_updated} UTC</span></div>
        
        <div class="history-section">
            <div class="info-row" style="margin-top: 15px; margin-bottom: 5px;">
                <span class="info-label">Track History Date</span>
            </div>
            <div class="history-inputs">
                <input type="date" id="history-date-input" class="history-date-picker">
                <button class="filter-btn btn-apply" id="btn-search-history" onclick="onSearchHistoryClick('${flight.icao24}')">Search</button>
            </div>
            <div style="margin-top: 10px;">
                <button class="filter-btn btn-apply" onclick="drawHistoryTrack('${flight.icao24}')" style="width: 100%;">Show Full History</button>
            </div>
        </div>
    `;
    document.getElementById("info-content").innerHTML = html;

    flatpickr("#history-date-input", {
        altInput: true,
        altFormat: "d-m-Y",
        dateFormat: "Y-m-d",
        defaultDate: "today"
    });

    panel.style.display = "block";
}

async function onSearchHistoryClick(icao24) {
    const dateInput = document.getElementById("history-date-input");
    const selectedDate = dateInput.value;

    if (!selectedDate) {
        alert("Please select a date!");
        return;
    }

    const [year, month, day] = selectedDate.split('-');
    const displayDate = `${day}-${month}-${year}`;

    try {
        const response = await fetch(`/api/flights/${icao24}/history-by-date?date=${selectedDate}`);
        if (!response.ok) throw new Error("HTTP " + response.status);
        const data = await response.json();
        
        if (currentTrackLayer) map.removeLayer(currentTrackLayer);
        
        if (data.path && data.path.length > 1) {
            currentTrackLayer = L.layerGroup();
            for (let i = 0; i < data.path.length - 1; i++) {
                const p1 = data.path[i]; 
                const p2 = data.path[i+1];
                const segment = L.polyline([[p1[0], p1[1]], [p2[0], p2[1]]], {
                    color: getAltitudeColor(p1[2]), weight: 3, opacity: 0.8, smoothFactor: 1
                });
                currentTrackLayer.addLayer(segment);
            }
            currentTrackLayer.addTo(map);
        } else {
            alert(`${displayDate} date for ${icao24} flight not found.`);
        }
    } catch (error) {
        console.error("Error loading history track:", error);
    }
}

const searchInput = document.getElementById("search-input");
const searchResults = document.getElementById("search-results");
let searchDebounceTimeout = null;

searchInput.addEventListener("input", (e) => {
    const query = e.target.value.trim().toLowerCase();
    clearTimeout(searchDebounceTimeout);
    if (query.length < 1) { searchResults.style.display = "none"; searchResults.innerHTML = ""; return; }

    searchDebounceTimeout = setTimeout(() => {
        const filteredFlights = allFlightsData.filter(flight => {
            const callsignMatch = flight.callsign && flight.callsign.toLowerCase().includes(query);
            const icaoMatch = flight.icao24 && flight.icao24.toLowerCase().includes(query);
            return callsignMatch || icaoMatch;
        }).slice(0, 10); 

        searchResults.innerHTML = "";
        if (filteredFlights.length > 0) {
            filteredFlights.forEach(flight => {
                const item = document.createElement("div"); item.className = "search-item";
                item.innerHTML = `<span class="callsign-text">${flight.callsign}</span> <span class="icao-text">(${flight.icao24})</span> <div style="font-size: 11px; color: #888;">${flight.origin_country}</div>`;
                item.addEventListener("click", () => {
                    selectAndFocusFlight(flight); searchResults.style.display = "none"; searchInput.value = `${flight.callsign} (${flight.icao24})`;
                });
                searchResults.appendChild(item);
            });
            searchResults.style.display = "block";
        } else {
            searchResults.innerHTML = `<div class="search-item" style="color: #aaa;">No flights found</div>`; searchResults.style.display = "block";
        }
    }, 300);
});

function selectAndFocusFlight(flight) {
    map.flyTo([flight.latitude, flight.longitude], 9, { animate: true, duration: 1.5 });
    selectedFlightId = flight.icao24; drawFlightTrack(flight.icao24);
    const fullFlightData = allFlightsData.find(f => f.icao24 === flight.icao24) || flight;
    updateInfoPanel(fullFlightData);
    
    if (aircraftMarkers.has(flight.icao24)) {
        const marker = aircraftMarkers.get(flight.icao24);
        const state = markerRenderState.get(flight.icao24);
        if (state) {
            state.selected = true;
            marker.setStyle({ image: planeImageFor(fullFlightData.altitude, true) });
        }
    }
}

document.addEventListener("click", (e) => {
    if (!document.getElementById("search-container").contains(e.target)) searchResults.style.display = "none";
});

async function drawFlightTrack(icao24) {
    try {
        const response = await fetch(`/api/flights/${icao24}/track`);
        if (!response.ok) throw new Error("HTTP " + response.status);
        const data = await response.json();
        
        if (currentTrackLayer) map.removeLayer(currentTrackLayer);
        
        if (data.path && data.path.length > 1) {
            currentTrackLayer = L.layerGroup();
            for (let i = 0; i < data.path.length - 1; i++) {
                const p1 = data.path[i]; const p2 = data.path[i+1];
                const segment = L.polyline([[p1[0], p1[1]], [p2[0], p2[1]]], {
                    color: getAltitudeColor(p1[2]), weight: 3, opacity: 0.8, smoothFactor: 1
                });
                currentTrackLayer.addLayer(segment);
            }
            currentTrackLayer.addTo(map);
        }
    } catch (error) { console.error(error); }
}

async function drawHistoryTrack(icao24) {
    try {
        const response = await fetch(`/api/flights/${icao24}/history`);
        if (!response.ok) throw new Error("HTTP " + response.status);
        const data = await response.json();
        
        if (currentTrackLayer) map.removeLayer(currentTrackLayer);
        
        if (data.path && data.path.length > 1) {
            currentTrackLayer = L.layerGroup();
            for (let i = 0; i < data.path.length - 1; i++) {
                const p1 = data.path[i]; const p2 = data.path[i+1];
                const segment = L.polyline([[p1[0], p1[1]], [p2[0], p2[1]]], {
                    color: getAltitudeColor(p1[2]), weight: 3, opacity: 0.8, smoothFactor: 1
                });
                currentTrackLayer.addLayer(segment);
            }
            currentTrackLayer.addTo(map);
        }
    } catch (error) { console.error(error); }
}

function renderVisibleFlights() {
    if (!allFlightsData.length) {
        aircraftMarkers.forEach((marker) => map.removeLayer(marker));
        aircraftMarkers.clear(); markerRenderState.clear();
        document.getElementById("visible-count").textContent = 0; document.getElementById("flight-count").textContent = 0;
        return;
    }

    const bounds = map.getBounds();
    const activeIds = new Set();
    let visibleCount = 0;

    for (const flight of allFlightsData) {
        const id = flight.icao24;
        if (!id) continue;
    
        const isOnGround = flight.on_ground === true || (flight.altitude !== null && flight.altitude <= 0);
        if (currentDisplayMode === "air" && isOnGround) continue;
        if (currentDisplayMode === "ground" && !isOnGround) continue;

        const altitudeFt = (flight.altitude || 0) * 3.28084;
        const speedKts = (flight.velocity || 0) * 1.94384;

        if (filterCountry) {
            const country = (flight.origin_country || "").toLowerCase();
            if (country !== filterCountry) continue;
        }

        if (altitudeFt < filterMinAlt || altitudeFt > filterMaxAlt) continue;
        if (speedKts < filterMinSpeed || speedKts > filterMaxSpeed) continue;

        const latLng = L.latLng(flight.latitude, flight.longitude);
        const heading = flight.heading || 0;
        const altitude = flight.altitude || 0;
    
        if (bounds.contains(latLng)) {
            visibleCount++; activeIds.add(id);
            const isSelected = (id === selectedFlightId);
        
            let marker = aircraftMarkers.get(id);
            let state = markerRenderState.get(id);
        
            if (marker) {
                marker.setLatLng(latLng);
                if (!state || state.heading !== heading || state.altitude !== altitude || state.selected !== isSelected) {
                    marker.setStyle({ image: planeImageFor(altitude, isSelected), heading: heading });
                    if (state) { state.heading = heading; state.altitude = altitude; state.selected = isSelected; }
                }
                if (isSelected) updateInfoPanel(flight);
            } else {
                marker = L.canvasPlaneMarker(latLng, {
                    renderer: myRenderer, image: planeImageFor(altitude, isSelected), heading: heading, radius: 12, fillOpacity: 0, weight: 0 
                });
                markerRenderState.set(id, { heading, altitude, selected: isSelected });
            
                marker.on('click', () => {
                    if (selectedFlightId && aircraftMarkers.has(selectedFlightId)) {
                        const oldMarker = aircraftMarkers.get(selectedFlightId);
                        const oldState = markerRenderState.get(selectedFlightId);
                        const oldFlight = allFlightsData.find(f => f.icao24 === selectedFlightId);
                        if (oldFlight && oldState) {
                            oldState.selected = false; oldMarker.setStyle({ image: planeImageFor(oldFlight.altitude, false) });
                        }
                    }
                    selectedFlightId = id;
                    const currentState = markerRenderState.get(id); currentState.selected = true;
                    marker.setStyle({ image: planeImageFor(altitude, true) });
                    drawFlightTrack(id);
                    const currentFlightData = allFlightsData.find(f => f.icao24 === id);
                    if (currentFlightData) updateInfoPanel(currentFlightData);
                });
                marker.addTo(map); aircraftMarkers.set(id, marker);
            }
        }
    }

    aircraftMarkers.forEach((marker, id) => {
        if (!activeIds.has(id)) {
            map.removeLayer(marker); aircraftMarkers.delete(id); markerRenderState.delete(id);
        }
    });
    document.getElementById("visible-count").textContent = visibleCount;
    document.getElementById("flight-count").textContent = allFlightsData.length;
}

let renderScheduled = false;
function scheduleRenderVisibleFlights() {
    if (renderScheduled) return;
    renderScheduled = true;
    requestAnimationFrame(() => { renderScheduled = false; renderVisibleFlights(); });
}

map.on("moveend zoomend", scheduleRenderVisibleFlights);
map.on('click', function(e) { if (e.originalEvent.target.id === 'map' && selectedFlightId) closeInfoPanel(); });

let ws; let pendingStreamUpdates = false;
function connectWebSocket() {
    const wsUrl = `${window.location.protocol === 'https:' ? 'wss:' : 'ws:'}//${window.location.host}/ws/flights`;
    ws = new WebSocket(wsUrl);
    ws.onopen = () => { document.getElementById("connection").textContent = "Kafka Connected"; document.getElementById("dot").classList.remove("error"); };
    ws.onmessage = (event) => {
        const flight = JSON.parse(event.data); flight._lastSeen = Date.now(); 
        const existingIndex = allFlightsData.findIndex(f => f.icao24 === flight.icao24);
        if (existingIndex > -1) allFlightsData[existingIndex] = flight; else allFlightsData.push(flight);
        pendingStreamUpdates = true;
    };
    ws.onclose = () => {
        document.getElementById("connection").textContent = "Disconnected, Retrying..."; document.getElementById("dot").classList.add("error");
        setTimeout(connectWebSocket, 3000); 
    };
    ws.onerror = (error) => { console.error("WebSocket Error:", error); ws.close(); };
}
connectWebSocket();

setInterval(() => {
    if (pendingStreamUpdates) { scheduleRenderVisibleFlights(); pendingStreamUpdates = false; }
}, 500);

setInterval(() => {
    const now = Date.now(); const originalLength = allFlightsData.length;
    allFlightsData = allFlightsData.filter(f => (now - (f._lastSeen || 0)) < STALE_FLIGHT_THRESHOLD);
    if (allFlightsData.length !== originalLength) {
        if (selectedFlightId && !allFlightsData.find(f => f.icao24 === selectedFlightId)) closeInfoPanel();
        scheduleRenderVisibleFlights();
    }
}, 10000);

const airportLayer = L.layerGroup().addTo(map);

async function fetchAndRenderAirports() {
    try {
        const response = await fetch('/api/airports');
        if (!response.ok) throw new Error("HTTP " + response.status);
        const data = await response.json();
        
        data.airports.forEach(apt => {
            let radius = 2;
            let color = '#aaaaaa';
            
            if (apt.type === 'large_airport') {
                radius = 4;
                color = '#2ebd59';
            } else if (apt.type === 'medium_airport') {
                radius = 3;
                color = '#f39c12';
            }

            const popupContent = `
                <div style="font-size: 14px; color: #191919;">
                    <b style="font-size: 16px;">${apt.name}</b><br>
                    <b>ICAO:</b> ${apt.icao} <br>
                    <b>IATA:</b> ${apt.iata || 'N/A'} <br>
                    <b>Type:</b> ${apt.type.replace('_', ' ')}
                </div>
            `;
            
            L.circleMarker([apt.lat, apt.lon], {
                renderer: myRenderer,
                radius: radius,
                color: color,
                weight: 1,
                fillColor: color,
                fillOpacity: 0.7
            }).bindPopup(popupContent, { className: 'airport-popup' }).addTo(airportLayer);
        });
    } catch (error) {}
}

fetchAndRenderAirports();