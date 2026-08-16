const $ = (s) => document.querySelector(s);
let state = { vehicles: [], zones: [], events: [] };

function request(url, options) {
  return fetch(url, {headers: {'Content-Type': 'application/json'}, ...options}).then(async r => {
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(data.error || 'Anfrage fehlgeschlagen');
    return data;
  });
}
function text(node, value) { node.textContent = value; return node; }
function time(unix) { return unix ? new Date(unix * 1000).toLocaleString('de-DE') : '–'; }

function extent(values) {
  const min = Math.min(...values), max = Math.max(...values), span = Math.max(max - min, .01);
  return [min - span * .12, max + span * .12];
}
function coordinates(geometry, into) {
  if (!geometry) return;
  if (geometry.type === 'Polygon') geometry.coordinates.forEach(r => r.forEach(p => into.push(p)));
  if (geometry.type === 'MultiPolygon') geometry.coordinates.forEach(poly => poly.forEach(r => r.forEach(p => into.push(p))));
}
function drawMap() {
  const svg = $('#map'); svg.replaceChildren();
  const points = [];
  state.zones.forEach(z => { try { coordinates(JSON.parse(z.geometry), points); } catch {} });
  state.vehicles.filter(v => v.recorded_at).forEach(v => points.push([v.last_lon, v.last_lat]));
  if (!points.length) return;
  const [minX,maxX] = extent(points.map(p => p[0])), [minY,maxY] = extent(points.map(p => p[1]));
  const point = ([x,y]) => `${35+(x-minX)/(maxX-minX)*730},${395-(y-minY)/(maxY-minY)*360}`;
  state.zones.forEach(z => {
    try { const g = JSON.parse(z.geometry); const rings = g.type === 'Polygon' ? g.coordinates : g.coordinates.flat(); rings.forEach(r => { const el = document.createElementNS('http://www.w3.org/2000/svg','polygon'); el.setAttribute('points',r.map(point).join(' ')); el.setAttribute('class','zone'); el.setAttribute('aria-label',z.name); svg.append(el); }); } catch {}
  });
  state.vehicles.filter(v => v.recorded_at).forEach(v => { const [cx,cy]=point([v.last_lon,v.last_lat]); const c=document.createElementNS('http://www.w3.org/2000/svg','circle'); c.setAttribute('cx',cx);c.setAttribute('cy',cy);c.setAttribute('r','8');c.setAttribute('class','vehicle');const title=document.createElementNS('http://www.w3.org/2000/svg','title');title.textContent=`${v.name} (${time(v.recorded_at)})`;c.append(title);svg.append(c); });
}
function render() {
  const select = $('#vehicle'), prior = select.value; select.replaceChildren();
  state.vehicles.forEach(v => { const o=document.createElement('option');o.value=v.id;text(o,v.name);select.append(o); }); select.value=prior;
  const body=$('#events');body.replaceChildren();
  state.events.forEach(e => { const tr=document.createElement('tr'); [time(e.occurred_at),e.vehicle,e.kind,e.zone,`${e.lat.toFixed(5)}, ${e.lon.toFixed(5)}`].forEach((v,i)=>{const td=document.createElement('td');text(td,v);if(i===2)td.className=e.kind;tr.append(td)});body.append(tr); });
  drawMap();
}
async function refresh() { state = await request('/api/state'); render(); }
$('#refresh').onclick=refresh;
$('#position-form').onsubmit=async e=>{e.preventDefault();const f=new FormData(e.target);const msg=$('#position-message');try{const r=await request('/api/positions',{method:'POST',body:JSON.stringify({vehicle_id:+f.get('vehicle_id'),lon:+f.get('lon'),lat:+f.get('lat')})});text(msg,r.events.length?`Gespeichert: ${r.events.map(x=>x.kind).join(', ')}`:'Gespeichert; kein Zonenwechsel.');await refresh()}catch(err){text(msg,err.message)}};
$('#zone-form').onsubmit=async e=>{e.preventDefault();const f=new FormData(e.target);try{await request('/api/zones',{method:'POST',body:JSON.stringify({name:f.get('name'),geometry:JSON.parse(f.get('geometry'))})});e.target.reset();await refresh()}catch(err){alert(err.message)}};
$('#vehicle-form').onsubmit=async e=>{e.preventDefault();const f=new FormData(e.target);try{await request('/api/vehicles',{method:'POST',body:JSON.stringify({name:f.get('name')})});e.target.reset();await refresh()}catch(err){alert(err.message)}};
refresh().catch(err=>alert(err.message));
