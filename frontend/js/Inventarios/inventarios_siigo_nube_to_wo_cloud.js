// ══════════════════════════════════════════════════════════════════
// ETL: Inventarios — Siigo Nube → World Office Cloud
// Basado en: dev.zip / convertidor_escritorio_cloud.py
// Entrada : Listado_de_productos___Servicios.xlsx
// Salida  : 43 columnas — "Importación Productos"
// ══════════════════════════════════════════════════════════════════

// ── Estado global ─────────────────────────────────────────────────
let INV_CLD_WB = null;
const INV_CLD_LOG = [];
const INV_CLD_EXCL = [];
let _invCldPriceMap = null;  // {p1:{col,nombre}, p2:{col,nombre}, p3:{col,nombre}}
let _invCldFileData = null;  // cached file data

// ── 43 columnas destino WO Cloud ──────────────────────────────────
const INV_CLD_COLS = [
  'Código *','Descripción *','Unidad Medida *','Tipo Impuesto Ventas *',
  'Valor IVA *','Suma al Costo*','Bodega 1 *','Maneja Talla - Color',
  'Maneja Lotes','Maneja Seriales','Ver en POS*','Pertenece a un Producto*',
  'Facturar Sin Existencias','Centro de Costos','Grupo Inventario',
  'Existencia Máxima Permitida','Existencia Mínima Permitida','Existencia Mínima Reorden',
  'Otros impuestos 1','Valor Impuestos 1','Otros Impuestos 2','Valor Impuestos 2',
  'Bodega 2','Bodega 3','Utilidad Estimada','Favoritos POS','Código de Barras',
  'Codigo Fabricación','Activo',
  'Nombre Lista Precios 1','Valor Lista Precios 1',
  'Nombre Lista Precios 2','Valor Lista Precios 2',
  'Nombre Lista Precios 3','Valor Lista Precios 3',
  'Nombre Lista Precios 4','Valor Lista Precios 4',
  'Nombre Lista Precios 5','Valor Lista Precios 5',
  'Nombre Lista Precios 6','Valor Lista Precios 6',
  'Tallas','Colores'
];

// ── Valores fijos por defecto (igual que dev.zip DEFAULTS) ────────
const INV_CLD_DEFAULTS = {
  'Maneja Talla - Color': 'No',
  'Maneja Lotes':         'No',
  'Maneja Seriales':      'No',
  'Ver en POS*':          'Si',
  'Pertenece a un Producto*': 'No',
  'Facturar Sin Existencias': 'Si',
  'Favoritos POS':        'No',
  'Activo':               'Si',
};

// ── Diccionarios de precios ───────────────────────────────────────
const INV_CLD_P1 = [
  'al detal','precio venta 1','precio de venta 1',
  'precio de venta fabrica','precio fabrica','precio venta fabrica','precio 1'
];
const INV_CLD_P2 = ['precio venta 2','precio de venta 2','precio 2','al por mayor menor'];
const INV_CLD_P3 = ['al mayor','precio venta 3','precio de venta 3','precio 3','precio mayorista'];

// ── Mapeo Unidad de Medida ────────────────────────────────────────
// Origen → Cloud  (agregar alias aquí)
const INV_CLD_UNIDAD = {
  'und.':         'Unidad',
  'und':          'Unidad',
  'unidad':       'Unidad',
  'unidades':     'Unidad',
  'log':          'Día',
  'dia':          'Día',
  'kg':           'Kilogramo',
  'kilogramo':    'Kilogramo',
  'gr':           'Gramo',
  'gramo':        'Gramo',
  'lt':           'Litro',
  'litro':        'Litro',
  'ml':           'Mililitro',
  'mililitro':    'Mililitro',
  'mt':           'Metro',
  'metro':        'Metro',
  'metros':       'Metro',
  'cm':           'Centímetro',
  'hora':         'Hora',
  'hr':           'Hora',
  'horas':        'Hora',
  'cj':           'Caja',
  'caja':         'Caja',
  'pq':           'Paquete',
  'paquete':      'Paquete',
  'par':          'Par',
  'docena':       'Docena',
  'galon':        'Galón',
  'gal':          'Galón',
};

// ── Tipo Impuesto Ventas: WO Cloud acepta ─────────────────────────
// IVA | NO_GRAVADO | EXCLUIDO | EXENTO | IMPOCONSUMO
const INV_CLD_TIPO_IMP = {
  'iva 19%':        {tipo:'Gravado',    valor:19},
  'iva 18%':        {tipo:'Gravado',    valor:18},
  'iva 17%':        {tipo:'Gravado',    valor:17},
  'iva 16%':        {tipo:'Gravado',    valor:16},
  'iva 5%':         {tipo:'Gravado',    valor:5},
  'iva 0%':         {tipo:'No Gravado', valor:0},
  'excluido':       {tipo:'Excluido',   valor:0},
  'exento':         {tipo:'Exento',     valor:0},
  'no aplica':      {tipo:'No Gravado', valor:0},
  'impoconsumo 8%': {tipo:'No Gravado', valor:0},
  'impoconsumo':    {tipo:'No Gravado', valor:0},
};

// ── Helpers ───────────────────────────────────────────────────────
function invCldNorm(h){
  return String(h||'').trim().toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g,'');
}
function invCldLog(msg,lvl='i',fase=''){
  const ts=new Date().toISOString();
  INV_CLD_LOG.push({ts,fase,lvl,msg});
  const panel=document.getElementById('inv-logp');
  if(!panel)return;
  const now=new Date().toLocaleTimeString('es-CO',{hour12:false});
  const css={i:'li',w:'lw',o:'lo',e:'le-e'}[lvl]||'li';
  panel.innerHTML+=`<div class="le"><span class="lt">${now}</span><span class="${css}">${msg}</span></div>`;
  panel.scrollTop=panel.scrollHeight;
}
function invCldSleep(ms){return new Promise(r=>setTimeout(r,ms));}

function invCldSetStep(n){
  for(let i=1;i<=4;i++){
    const wz=document.getElementById('inv-wt'+i);
    if(wz)wz.className='wz'+(i<n?' done':i===n?' on':'');
    const sec=document.getElementById('inv-s'+i);
    if(sec)sec.style.display=(i===n)?'block':'none';
  }
}
function invCldSetPStep(n){
  for(let i=0;i<=5;i++){
    const el=document.getElementById('inv-ps'+i);
    if(!el)continue;
    el.classList.toggle('act',i===n);
    el.classList.toggle('don',i<n);
  }
}
function invCldSetPct(pct,msg){
  const pb=document.getElementById('inv-pbar');
  const pp=document.getElementById('inv-ppct');
  const ph=document.getElementById('inv-pph');
  if(pb)pb.style.width=pct+'%';
  if(pp)pp.textContent=pct+'%';
  if(ph&&msg)ph.textContent=msg;
}

// ── Transformaciones ──────────────────────────────────────────────
function invCldMapUnidad(v){
  if(!v)return 'Unidad';
  const k=invCldNorm(v);
  // Try exact match first
  if(INV_CLD_UNIDAD[k])return INV_CLD_UNIDAD[k];
  // Try partial match
  for(const[pat,val] of Object.entries(INV_CLD_UNIDAD))
    if(k.includes(pat))return val;
  // Return original trimmed if not found
  return String(v).trim();
}

function invCldMapImpuesto(ivaRaw, iva2Raw){
  const raw1=String(ivaRaw||'').trim();
  const raw2=String(iva2Raw||'').trim();
  const k1=invCldNorm(raw1);
  const k2=invCldNorm(raw2);
  const esImpo1=k1.includes('impoconsumo');
  const esImpo2=k2.includes('impoconsumo');

  // Impoconsumo → ALWAYS No Gravado / 0
  if(esImpo1||esImpo2){
    const impoOrigen=esImpo1?raw1:raw2;
    const m=String(impoOrigen).match(/(\d+(?:\.\d+)?)\s*%/);
    return {tipo:'No Gravado',valor:0,esImpo:true,pctImpo:m?parseFloat(m[1]):'',impoOrigen};
  }

  // Normal IVA
  let tipo='No Gravado',valor=0;
  for(const[p,mp] of Object.entries(INV_CLD_TIPO_IMP)){
    if(k1===invCldNorm(p)||k1.includes(invCldNorm(p))){
      tipo=mp.tipo;valor=mp.valor;break;
    }
  }
  return {tipo,valor,esImpo:false,pctImpo:'',impoOrigen:''};
}
function invCldMapActivo(v){
  const s=String(v||'').toLowerCase().trim();
  return ['activo','active','true','1','-1'].includes(s)?'Si':'No';
}

function invCldLimpiarNum(v){
  if(v===null||v===undefined||v==='')return '';
  const n=parseFloat(String(v).replace(/[^0-9.,\-]/g,'').replace(',','.'));
  return isNaN(n)?'':Math.round(n*100)/100;
}

function invCldEsServicio(tipo){
  return String(tipo||'').toLowerCase().includes('servicio');
}

// ── Generar código grupo ──────────────────────────────────────────
function invCldGenCod(nombre,usados){
  if(!nombre||!nombre.trim())return '';
  const words=(nombre.trim().match(/\w+/g)||[]);
  if(!words.length)return '';
  let base=(words.length===1
    ?words[0].substring(0,3)
    :words.map(w=>w.substring(0,3)).join('')
  ).toUpperCase();
  let cod=base,n=1;
  while(usados.has(cod)){cod=base+n;n++;}
  return cod;
}
function invCldBuildGrupos(rows){
  const g1Map=new Map(),g2Map=new Map();
  const u1=new Set(),u2=new Set();
  rows.forEach(r=>{
    const g1=String(r['Grupo Inventario']||'').trim();
    const g2=String(r['_categ']||'').trim();
    if(g1&&!g1Map.has(g1)){const c=invCldGenCod(g1,u1);if(c){u1.add(c);g1Map.set(g1,c);}}
    if(g2&&!g2Map.has(g2)){const c=invCldGenCod(g2,u2);if(c){u2.add(c);g2Map.set(g2,c);}}
  });
  return{
    g1:[...g1Map.entries()].sort((a,b)=>a[0].localeCompare(b[0])).map(([n,c])=>({cod:c,nom:n})),
    g2:[...g2Map.entries()].sort((a,b)=>a[0].localeCompare(b[0])).map(([n,c])=>({cod:c,nom:n}))
  };
}

// ── Leer Excel ───────────────────────────────────────────────────
async function invCldReadFile(file){
  return new Promise((res,rej)=>{
    const reader=new FileReader();
    reader.onload=e=>{
      try{
        const wb=XLSX.read(new Uint8Array(e.target.result),{type:'array',raw:false,cellText:true});
        const ws=wb.Sheets[wb.SheetNames[0]];
        const all=XLSX.utils.sheet_to_json(ws,{header:1,defval:''});
        let hi=0;
        for(let i=0;i<Math.min(all.length,15);i++){
          const rn=all[i].map(v=>invCldNorm(v));
          if(rn.includes('codigo')||rn.includes('tipo')){hi=i;break;}
        }
        const hdrs=all[hi].map(v=>String(v||'').trim());
        const rows=all.slice(hi+1).filter(r=>{
          if(!r.some(v=>v!==''&&v!==null&&v!==undefined))return false;
          const first=String(r[0]||'').trim().toLowerCase();
          if(first.startsWith('procesado en'))return false;
          return true;
        });
        res({hdrs,rows});
      }catch(err){rej(err);}
    };
    reader.readAsArrayBuffer(file);
  });
}

// ── ETL principal ─────────────────────────────────────────────────
async function startInvCldETL(){
  const file=S.files&&S.files['inv-maestro']?S.files['inv-maestro']:null;
  if(!file){alert('Carga el archivo Listado_de_productos___Servicios.xlsx');return;}

  // Read file headers first to populate price modal
  try{
    const data=await invCldReadFile(file);
    _invCldFileData=data;
    invCldShowPriceModal(data.hdrs);
  }catch(err){
    alert('Error leyendo archivo: '+err.message);
  }
}

async function _startInvCldETLRun(){
  const file=S.files&&S.files['inv-maestro']?S.files['inv-maestro']:null;
  if(!file) return;

  invCldSetStep(3);
  INV_CLD_LOG.length=0; INV_CLD_EXCL.length=0;
  const panel=document.getElementById('inv-logp');
  if(panel)panel.innerHTML='';
  invCldSetPStep(0); invCldSetPct(0,'Iniciando...');
  const t0=Date.now();

  try{
    invCldSetPStep(1); invCldSetPct(15,'Leyendo archivo...');
    invCldLog('📂 Leyendo archivo origen...','i','Lectura');
    await invCldSleep(30);

    const data=_invCldFileData;
    invCldLog(`   ${data.rows.length} registros encontrados`,'i','Lectura');

    // ── Detectar columnas ────────────────────────────────────────
    const H=data.hdrs;
    const HN=H.map(invCldNorm);
    const fi=(terms)=>HN.findIndex(h=>terms.some(t=>h===invCldNorm(t)||h.includes(invCldNorm(t))));

    const cTipo = HN.indexOf('tipo');
    const cCod  = HN.indexOf('codigo');
    const cNom  = HN.indexOf('nombre');
    const cEst  = HN.indexOf('estado');
    const cUnd  = fi(['unidad de medida','unidad medida','unidad']);
    const cStk  = fi(['stock minimo','stock min']);
    // Use user-selected price columns from modal, fallback to dictionary
    const cPV1  = _invCldPriceMap?.p1
                  ? HN.indexOf(invCldNorm(_invCldPriceMap.p1))
                  : fi(INV_CLD_P1);
    const cPV2  = _invCldPriceMap?.p2
                  ? HN.indexOf(invCldNorm(_invCldPriceMap.p2))
                  : fi(INV_CLD_P2);
    const cPV3  = _invCldPriceMap?.p3
                  ? HN.indexOf(invCldNorm(_invCldPriceMap.p3))
                  : fi(INV_CLD_P3);
    const cIva  = HN.findIndex(h=>h.includes('impuesto cargo')&&!h.includes('2'));
    const cIva2 = HN.findIndex(h=>h==='impuesto cargo 2'||h==='impuesto cargo2');
    const cInv  = fi(['inventariable']);
    const cIncl = fi(['es incluido']);
    const cCateg= fi(['categoria']);
    const cBarr = HN.findIndex(h=>h==='codigo de barras'||h==='codigo barras');
    const cRefF = fi(['referencia fabrica','ref fab']);

    invCldLog(`   Cols → Cód[${cCod}] Und[${cUnd}] IVA[${cIva}] IVA2[${cIva2}] P1[${cPV1}] P2[${cPV2}]`,'i','Lectura');

    invCldSetPStep(2); invCldSetPct(35,'Consolidando...');
    invCldLog('🔗 Consolidando registros...','i','Consolidación');
    await invCldSleep(30);

    const seen=new Set(), out=[], defaults=[];
    const total=data.rows.length;

    for(const r of data.rows){
      const cod=cCod>=0?String(r[cCod]||'').trim():'';
      const nom=cNom>=0?String(r[cNom]||'').trim():'';
      if(!cod){INV_CLD_EXCL.push({cod:'',nom,motivo:'Sin código'});continue;}
      if(seen.has(cod)){INV_CLD_EXCL.push({cod,nom,motivo:'Código duplicado'});continue;}
      seen.add(cod);

      const tipo   = cTipo>=0  ? String(r[cTipo]||'').trim()  : 'Producto';
      const estado = cEst>=0   ? r[cEst]   : 'Activo';
      const undRaw = cUnd>=0   ? String(r[cUnd]||'').trim()   : '';
      const stockM = cStk>=0   ? r[cStk]   : 0;
      const pv1raw = cPV1>=0   ? r[cPV1]   : '';
      const pv2raw = cPV2>=0   ? r[cPV2]   : '';
      const pv3raw = cPV3>=0   ? r[cPV3]   : '';
      const ivaRaw = cIva>=0   ? String(r[cIva]||'').trim()   : '';
      const iva2Raw= cIva2>=0  ? String(r[cIva2]||'').trim()  : '';
      const invRaw = cInv>=0   ? r[cInv]   : '';
      const inclRaw= cIncl>=0  ? r[cIncl]  : '';
      const categ  = cCateg>=0 ? String(r[cCateg]||'').trim() : '';
      const barras = cBarr>=0  ? String(r[cBarr]||'').trim()  : '';
      const refFab = cRefF>=0  ? String(r[cRefF]||'').trim()  : '';

      // Transformar
      const activoVal   = invCldMapActivo(estado);
      const unidadVal   = invCldMapUnidad(undRaw);
      const impuesto    = invCldMapImpuesto(ivaRaw, iva2Raw);
      const p1n         = invCldLimpiarNum(pv1raw);
      const p2n         = invCldLimpiarNum(pv2raw);
      const p3n         = invCldLimpiarNum(pv3raw);
      const stockN      = invCldLimpiarNum(stockM)||0;
      const esServ      = invCldEsServicio(tipo);

      // Facturar sin existencias
      const invStr=String(invRaw||'').toUpperCase().trim();
      const factSinEx=['SI','SÍ','YES','1','TRUE','-1'].includes(invStr)?'No':'Si';
      const inclStr=String(inclRaw||'').toUpperCase().trim();
      const pertVal=['SI','SÍ','YES','1','TRUE','-1'].includes(inclStr)?'Si':'No';

      // Impoconsumo → IVA=0/No Gravado + Otros impuestos 1
      const otrosImp1 = impuesto.esImpo ? 'IMPOCONSUMO' : '';
      // Valor Impuestos 1 = percentage as integer (e.g. 8 for 8%)
      const valImp1   = impuesto.esImpo && impuesto.pctImpo !== ''
        ? impuesto.pctImpo   // already extracted as number e.g. 8
        : '';

      // Track defaults
      if(!ivaRaw) defaults.push({cod,nombre:nom,campo:'Tipo Impuesto / IVA',valor:'NO_GRAVADO / 0',motivo:'Sin impuesto en origen'});
      if(!undRaw) defaults.push({cod,nombre:nom,campo:'Unidad Medida',valor:'Unidad',motivo:'Unidad vacía → Unidad por defecto'});

      // Classification key for grouping sheets
      const tipoClasi = esServ?'Servicio':'Producto';
      // Clave para hoja: Tipo x IVA%
      const ivaKey = impuesto.esImpo         ? 'Impoconsumo'
                   : impuesto.tipo==='Gravado'? `IVA ${impuesto.valor}%`
                   : impuesto.tipo==='Excluido'? 'Excluido'
                   : impuesto.tipo==='Exento'  ? 'Exento'
                   : 'No Gravado';

      out.push({
        'Código *':                   cod,
        'Descripción *':              nom,
        'Unidad Medida *':            unidadVal,
        'Tipo Impuesto Ventas *':     impuesto.tipo,
        'Valor IVA *':                impuesto.valor,
        'Suma al Costo*':             'No',
        'Bodega 1 *':                 'PRINCIPAL',
        'Maneja Talla - Color':       'No',
        'Maneja Lotes':               'No',
        'Maneja Seriales':            'No',
        'Ver en POS*':                'Si',
        'Pertenece a un Producto*':   pertVal,
        'Facturar Sin Existencias':   factSinEx,
        'Centro de Costos':           '',
        'Grupo Inventario':           categ||tipo||'',
        'Existencia Máxima Permitida': 0,
        'Existencia Mínima Permitida': stockN,
        'Existencia Mínima Reorden':   0,
        'Otros impuestos 1':          otrosImp1,
        'Valor Impuestos 1':          valImp1,
        'Otros Impuestos 2':          '',
        'Valor Impuestos 2':          '',
        'Bodega 2':                   '',
        'Bodega 3':                   '',
        'Utilidad Estimada':          '',
        'Favoritos POS':              'No',
        'Código de Barras':           barras||'',
        'Codigo Fabricación':         refFab||'',
        'Activo':                     activoVal,
        // Precios: nombre SOLO si hay valor en esa fila
        'Nombre Lista Precios 1':     (_invCldPriceMap?.p1&&p1n!==''&&p1n!==0)?_invCldPriceMap.p1:'',
        'Valor Lista Precios 1':      (_invCldPriceMap?.p1&&p1n!==''&&p1n!==0)?p1n:'',
        'Nombre Lista Precios 2':     (_invCldPriceMap?.p2&&p2n!==''&&p2n!==0)?_invCldPriceMap.p2:'',
        'Valor Lista Precios 2':      (_invCldPriceMap?.p2&&p2n!==''&&p2n!==0)?p2n:'',
        'Nombre Lista Precios 3':     (_invCldPriceMap?.p3&&p3n!==''&&p3n!==0)?_invCldPriceMap.p3:'',
        'Valor Lista Precios 3':      (_invCldPriceMap?.p3&&p3n!==''&&p3n!==0)?p3n:'',
        'Nombre Lista Precios 4':     '','Valor Lista Precios 4':0,
        'Nombre Lista Precios 5':     '','Valor Lista Precios 5':0,
        'Nombre Lista Precios 6':     '','Valor Lista Precios 6':0,
        'Tallas':                     '',
        'Colores':                    '',
        // Internal fields for sheet grouping
        '_tipoClasi':  tipoClasi,
        '_ivaKey':     ivaKey,
        '_categ':      categ,
      });
    }

    invCldLog(`✅ ${out.length} registros transformados`,'o','Consolidación');
    if(INV_CLD_EXCL.length) invCldLog(`   ⚠ ${INV_CLD_EXCL.length} excluidos`,'w','Consolidación');

    invCldSetPStep(4); invCldSetPct(85,'Generando Excel...');
    invCldLog('📊 Construyendo archivo Excel...','i','Escritura');
    await invCldSleep(30);

    const grupos=invCldBuildGrupos(out);
    INV_CLD_WB=invCldBuildWB(out,INV_CLD_LOG,
      {registros_entrada:total,registros_salida:out.length},
      INV_CLD_EXCL,defaults,grupos);

    const dur=((Date.now()-t0)/1000).toFixed(1);
    invCldSetPct(100,'¡Listo!'); invCldSetPStep(5);
    invCldLog(`✅ Excel listo en ${dur}s — ${out.length} productos`,'o','Escritura');

    const fn=invCldBuildFN();
    const fnEl=document.getElementById('inv-dl-fn'); if(fnEl)fnEl.textContent=fn;
    const stIn=document.getElementById('inv-st-in'); if(stIn)stIn.textContent=total;
    const stOk=document.getElementById('inv-st-ok'); if(stOk)stOk.textContent=out.length;

    try{
      await api('POST','/migrations',{
        filename_out:fn,orig_soft:'Siigo Nube',dest_soft:'World Office Cloud',
        module:'Inventarios',records_in:total,records_out:out.length,
        errors:0,warnings:INV_CLD_EXCL.length,duration_sec:parseFloat(dur),status:'completed'
      },AUTH.token);
    }catch(e){}

    invCldSetStep(4);

  }catch(err){
    invCldLog(`❌ Error: ${err.message}`,'e','Pipeline');
    console.error(err);
  }
}

// ── Modal de mapeo de precios ─────────────────────────────────────
function invCldShowPriceModal(hdrs){
  // Filter headers that could be price columns (non-empty, non-system)
  const skip=['tipo','codigo','nombre','estado','referencia','categoria',
    'unidad','impuesto','descripcion','inventariable','incluido','saldo',
    'visible','stock','retencion','dian','marca','arancelario','modelo',
    'almacen','barras','procesado'];
  const priceOpts = hdrs.filter(h=>{
    if(!h||!h.trim()) return false;
    const k = invCldNorm(h);
    return !skip.some(s=>k.includes(s));
  });

  // Build modal HTML
  const makeSelect = (id,label)=>`
    <div style="margin-bottom:20px">
      <div style="font-size:13px;font-weight:600;color:#fff;margin-bottom:8px">${label}</div>
      <select id="${id}" style="width:100%;background:rgba(255,255,255,.08);border:1px solid rgba(255,255,255,.2);
        border-radius:8px;padding:10px 14px;color:#fff;font-size:13px;outline:none">
        <option value="">— No asignar —</option>
        ${priceOpts.map(h=>`<option value="${h}">${h}</option>`).join('')}
      </select>
      <div style="font-size:11px;color:rgba(255,255,255,.4);margin-top:5px">
        El nombre de la lista será el título de la columna seleccionada
      </div>
    </div>`;

  let modal = document.getElementById('inv-price-modal');
  if(!modal){
    modal = document.createElement('div');
    modal.id = 'inv-price-modal';
    modal.style.cssText='position:fixed;inset:0;z-index:9500;overflow:auto;display:none';
    document.body.appendChild(modal);
  }

  modal.innerHTML=`
    <div style="position:fixed;inset:0;background:rgba(0,0,0,.5);z-index:0" onclick="invCldClosePriceModal()"></div>
    <div style="position:relative;z-index:1;min-height:100vh;display:flex;align-items:center;justify-content:center;padding:40px 20px">
      <div style="width:100%;max-width:500px;background:linear-gradient(135deg,#1a1060,#2d1b8e,#4a2db8);
        border:1px solid rgba(255,255,255,.15);border-radius:16px;padding:32px;
        box-shadow:0 20px 60px rgba(0,0,0,.5)">
        <div style="text-align:center;margin-bottom:28px">
          <img src="img/wo_cloud_logo.png" style="width:70px;height:auto;margin-bottom:12px;filter:drop-shadow(0 4px 16px rgba(100,80,255,.5))">
          <div style="font-size:20px;font-weight:700;color:#fff">Configurar Listas de Precios</div>
          <div style="font-size:12px;color:rgba(255,255,255,.5);margin-top:6px">
            Selecciona qué columnas del archivo usar como precios.<br>Todos los campos son opcionales.
          </div>
        </div>
        ${makeSelect('inv-price-sel-1','Nombre Lista Precios 1')}
        ${makeSelect('inv-price-sel-2','Nombre Lista Precios 2')}
        ${makeSelect('inv-price-sel-3','Nombre Lista Precios 3')}
        <div style="display:flex;gap:12px;margin-top:8px">
          <button onclick="invCldClosePriceModal()" style="flex:1;padding:12px;
            background:rgba(255,255,255,.1);border:1px solid rgba(255,255,255,.2);
            border-radius:10px;color:#fff;font-size:14px;cursor:pointer">
            Cancelar
          </button>
          <button onclick="invCldConfirmPriceModal()" style="flex:2;padding:12px;
            background:linear-gradient(135deg,#5b4fcf,#7c6ef0);
            border:1px solid rgba(255,255,255,.15);border-radius:10px;
            color:#fff;font-size:14px;font-weight:700;cursor:pointer;
            box-shadow:0 4px 16px rgba(91,79,207,.4)">
            ✅ Continuar Migración
          </button>
        </div>
      </div>
    </div>`;

  modal.style.display='block';
}

function invCldClosePriceModal(){
  const modal=document.getElementById('inv-price-modal');
  if(modal) modal.style.display='none';
}

async function invCldConfirmPriceModal(){
  const get=id=>document.getElementById(id)?.value||'';
  _invCldPriceMap={
    p1: get('inv-price-sel-1'),
    p2: get('inv-price-sel-2'),
    p3: get('inv-price-sel-3'),
  };
  invCldClosePriceModal();
  await _startInvCldETLRun();
}

// ── Nombre archivo ────────────────────────────────────────────────
function invCldBuildFN(){
  const d=new Date();
  return `inventarios_siigo_nube_wo_cloud_${d.getFullYear()}_${String(d.getMonth()+1).padStart(2,'0')}_${String(d.getDate()).padStart(2,'0')}.xlsx`;
}

// ── Construir Workbook ────────────────────────────────────────────
function invCldBuildWB(rows,logEntries,stats,excluded,defaults,grupos){
  const wb=XLSX.utils.book_new();

  // Helper: aoa con fila vacía + headers + datos
  function buildAoa(subset){
    const aoa=[Array(INV_CLD_COLS.length).fill(''), INV_CLD_COLS.slice()];
    subset.forEach(r=>aoa.push(INV_CLD_COLS.map(c=>{const v=r[c];return v===undefined?'':v??'';})));
    return aoa;
  }

  // Hoja 1: todos los registros
  XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet(buildAoa(rows)),'Importación Productos');

  // Hojas por Tipo (Producto/Servicio) × valor IVA
  // Nombre hoja: "Producto IVA 19%" / "Servicio No Gravado" / "Servicio Excluido"
  const grupos_iva=new Map();
  rows.forEach(r=>{
    const tipo  = r['_tipoClasi'] || 'Producto';      // Producto | Servicio
    const ivaK  = r['_ivaKey']   || 'No Gravado';     // IVA 19% | No Gravado | etc
    const name  = (`${tipo} ${ivaK}`).substring(0,31);
    if(!grupos_iva.has(name))grupos_iva.set(name,[]);
    grupos_iva.get(name).push(r);
  });
  // Sort: Productos first, then Servicios; within each by IVA value
  const sortKey=n=>{
    const isServ=n.startsWith('Servicio')?1:0;
    const m=n.match(/(\d+)/); const num=m?parseInt(m[1]):0;
    return `${isServ}_${String(num).padStart(3,'0')}_${n}`;
  };
  [...grupos_iva.entries()]
    .sort((a,b)=>sortKey(a[0]).localeCompare(sortKey(b[0])))
    .forEach(([name,sub])=>{
      XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet(buildAoa(sub)),name);
    });

  // Hoja Grupos (igual que escritorio: A/B = Grupo 1, D/E = Grupo 2)
  try{
    if(grupos){
      const maxLen=Math.max(grupos.g1.length,grupos.g2.length);
      const gAoa=[['Codigo','Grupo 1','','Codigo','Grupo 2']];
      for(let i=0;i<maxLen;i++){
        const g1=grupos.g1[i]||{cod:'',nom:''};
        const g2=grupos.g2[i]||{cod:'',nom:''};
        gAoa.push([g1.cod,g1.nom,'',g2.cod,g2.nom]);
      }
      XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet(gAoa),'Grupos');
    }
  }catch(eg){console.warn('Grupos sheet:',eg);}

  // Logs
  const logsAoa=[['Timestamp','Fase','Nivel','Mensaje']];
  (logEntries||[]).forEach(e=>logsAoa.push([e.ts,e.fase||'',
    e.lvl==='e'?'ERROR':e.lvl==='w'?'WARN':'INFO',e.msg]));
  XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet(logsAoa),'Logs');

  // Estadísticas
  const s=stats||{};
  XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet([
    ['Métrica','Valor'],
    ['Registros entrada',s.registros_entrada||0],
    ['Registros salida',rows.length],
    ['Excluidos',excluded?excluded.length:0],
  ]),'Estadísticas');

  // Excluidos
  const exAoa=[['Código','Nombre','Motivo']];
  (excluded||[]).forEach(e=>exAoa.push([e.cod,e.nom,e.motivo]));
  XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet(exAoa),'Excluidos');

  // Campos por Defecto
  if(defaults&&defaults.length){
    const defAoa=[['Código','Nombre','Campo','Valor Asignado','Motivo']];
    defaults.forEach(d=>defAoa.push([d.cod,d.nombre,d.campo,d.valor,d.motivo]));
    XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet(defAoa),'Campos por Defecto');
  }

  return wb;
}

// ── Descarga ──────────────────────────────────────────────────────
function invCldDoDownload(){
  if(!INV_CLD_WB){alert('Primero ejecuta el proceso ETL');return;}
  XLSX.writeFile(INV_CLD_WB,invCldBuildFN());
}

// ── Reset ─────────────────────────────────────────────────────────
function invCldReset(){
  INV_CLD_WB=null; INV_CLD_LOG.length=0; INV_CLD_EXCL.length=0;
}
