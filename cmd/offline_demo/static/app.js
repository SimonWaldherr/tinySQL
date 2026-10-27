const $ = selector => document.querySelector(selector);

async function api(url) {
  const response = await fetch(url);
  const data = await response.json();
  if (!response.ok) throw new Error(data.error || 'Anfrage fehlgeschlagen');
  return data;
}

function el(tag, text) {
  const node = document.createElement(tag);
  if (text !== undefined) node.textContent = text;
  return node;
}

function draw(items, center) {
  const svg = $('#map');
  svg.replaceChildren();
  const points = center ? [...items, center] : items;
  if (!points.length) return;
  const lons = points.map(p => p.lon), lats = points.map(p => p.lat);
  const minX = Math.min(...lons), maxX = Math.max(...lons);
  const minY = Math.min(...lats), maxY = Math.max(...lats);
  const spanX = Math.max(.01, maxX - minX), spanY = Math.max(.01, maxY - minY);
  const x = lon => minX === maxX ? 340 : 35 + (lon - minX) / spanX * 610;
  const y = lat => minY === maxY ? 210 : 385 - (lat - minY) / spanY * 350;
  items.forEach(p => {
    const point = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
    point.setAttribute('cx', x(p.lon));
    point.setAttribute('cy', y(p.lat));
    point.setAttribute('r', '7');
    point.setAttribute('class', 'point');
    const title = document.createElementNS('http://www.w3.org/2000/svg', 'title');
    title.textContent = p.name;
    point.append(title);
    svg.append(point);
  });
  if (center) {
    const marker = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
    marker.setAttribute('cx', x(center.lon));
    marker.setAttribute('cy', y(center.lat));
    marker.setAttribute('r', '12');
    marker.setAttribute('class', 'center');
    const title = document.createElementNS('http://www.w3.org/2000/svg', 'title');
    title.textContent = 'Suchzentrum';
    marker.append(title);
    svg.append(marker);
  }
}

function render(items, center) {
  $('#count').textContent = `${items.length} Treffer`;
  const box = $('#results');
  box.replaceChildren();
  items.forEach(p => {
    const item = el('article');
    item.className = 'poi';
    const link = el('a', 'In OpenStreetMap öffnen');
    link.href = `https://www.openstreetmap.org/?mlat=${p.lat}&mlon=${p.lon}#map=16/${p.lat}/${p.lon}`;
    link.target = '_blank';
    link.rel = 'noopener';
    const distance = Number.isFinite(p.distance_m) ? ` · ${Math.round(p.distance_m)} m` : '';
    item.append(el('strong', p.name), el('small', `${p.category} · ${p.city}${distance}`), link);
    box.append(item);
  });
  draw(items, center);
}

async function search() {
  const params = new URLSearchParams({q: $('#query').value});
  const lon = $('#lon').value.trim(), lat = $('#lat').value.trim(), radius = $('#radius').value.trim();
  let center = null;
  if (lon || lat || radius) {
    if (!lon || !lat || !radius) throw new Error('Für die Umkreissuche Länge, Breite und Radius ausfüllen.');
    params.set('lon', lon);
    params.set('lat', lat);
    params.set('radius_m', radius);
    center = {lon: Number(lon), lat: Number(lat)};
  }
  const data = await api('/api/search?' + params);
  render(data.results, center);
}

$('#search').onsubmit = event => {
  event.preventDefault();
  search().catch(error => alert(error.message));
};
$('#all').onclick = () => {
  for (const id of ['query', 'lon', 'lat', 'radius']) $('#' + id).value = '';
  search().catch(error => alert(error.message));
};
api('/api/status')
  .then(data => $('#status').textContent = `${data.pois} POIs · ${data.source} · ${data.read_only ? 'schreibgeschützt' : 'beschreibbar'}`)
  .catch(() => {});
search().catch(error => alert(error.message));
