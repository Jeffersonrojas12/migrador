// ══════════════════════════════════════════════════════════════════
// ETL: Inventarios — Siigo Nube → World Office Escritorio
// Basado en: dev.zip / inventarios_siigoNube_woEscritorio.json
// Entrada : Listado_de_productos___Servicios.xlsx
// Salida  : 88 columnas
// ══════════════════════════════════════════════════════════════════

// ── Estado global ─────────────────────────────────────────────────
let INV_WB = null;
const INV_LOG = [];
const INV_EXCL = [];

// ── 88 columnas destino (orden exacto plantilla WO Escritorio) ────
const INV_COLS = [
  'Código','Descripción','Activo','Exis. Máxima','Exis. Mínima',
  'Punto Reorden','Unid. Medida','Precio 1','Precio 2','Precio 3',
  'Precio 4','Grupo Uno','Iva ','Tipo Iva','Clasificación',
  'Clasificación Niif','Producto','Facturar sin Existen.','Pertenece Produc.',
  'Producto Proceso','Maneja Seriales','Observaciones','Verificar Utilidad',
  'Utilidad Estimada','Arancel','Impoconsumo','Porcentaje de impoconsumo',
  'ImpoConsumo al Costo','Iva Mayor Vr al costo','Gasto que afecta el Costo',
  'Centro Costos','Código Barras','Favorito POS','Imagen POS','Impresora',
  'Ocultar Imprimir','Código Internacional','Grupo Dos','Grupo Tres',
  'Grupo Cuatro','Grupo Cinco','Grupo Seis','Grupo Siete','Grupo Ocho',
  'Grupo Nueve','Grupo Diez',
  'Precio 5','Precio 6','Precio 7','Precio 8','Precio 9','Precio 10',
  'Precio 11','Precio 12','Precio 13','Precio 14','Precio 15','Precio 16',
  'Precio 17','Precio 18','Precio 19','Precio 20','Precio 21','Precio 22',
  'Precio 23','Precio 24','Precio 25','Precio 26','Precio 27','Precio 28',
  'Precio 29','Precio 30',
  'Personalizado 1','Personalizado 2','Personalizado 3','Personalizado 4',
  'Personalizado 5','Personalizado 6','Personalizado 7','Personalizado 8',
  'Personalizado 9','Personalizado 10','Personalizado 11','Personalizado 12',
  'Personalizado 13','Personalizado 14','Personalizado 15','Código Centro Costos'
];

// ── Diccionarios de precios (agregar alias aquí) ───────────────────
const INV_P1 = ['al detal','precio venta 1','precio de venta 1','precio de venta fabrica',
                'precio fabrica','precio venta fabrica','precio 1'];
const INV_P2 = ['precio venta 2','precio de venta 2','precio 2','al por mayor menor'];
const INV_P3 = ['al mayor','precio venta 3','precio de venta 3','precio 3','precio mayorista'];

// ── Mapeos IVA ────────────────────────────────────────────────────
const INV_IVA_VAL = {
  'iva 19%':0.19,'iva 18%':0.18,'iva 17%':0.17,'iva 16%':0.16,
  'iva 5%':0.05,'iva 0%':0,'excluido':0,'exento':0,'no aplica':0
};
const INV_IVA_TIPO = {
  'iva 19%':'Gravado','iva 18%':'Gravado','iva 17%':'Gravado','iva 16%':'Gravado',
  'iva 5%':'Gravado','iva 0%':'Excluido','excluido':'Excluido',
  'exento':'Exento','no aplica':'No Gravados'
};

// ── Helpers ───────────────────────────────────────────────────────
function invNorm(h){
  return String(h||'').trim().toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g,'');
}

function invLog(msg,lvl='i',fase=''){
  const ts=new Date().toISOString();
  INV_LOG.push({ts,fase,lvl,msg});
  const panel=document.getElementById('inv-logp');
  if(!panel)return;
  const now=new Date().toLocaleTimeString('es-CO',{hour12:false});
  const css={i:'li',w:'lw',o:'lo',e:'le-e'}[lvl]||'li';
  panel.innerHTML+=`<div class="le"><span class="lt">${now}</span><span class="${css}">${msg}</span></div>`;
  panel.scrollTop=panel.scrollHeight;
}
function invSleep(ms){return new Promise(r=>setTimeout(r,ms));}

function invSetStep(n){
  for(let i=1;i<=4;i++){
    const wz=document.getElementById('inv-wt'+i);
    if(wz)wz.className='wz'+(i<n?' done':i===n?' on':'');
    const sec=document.getElementById('inv-s'+i);
    if(sec)sec.style.display=(i===n)?'block':'none';
  }
}
function invSetPStep(n){
  for(let i=0;i<=5;i++){
    const el=document.getElementById('inv-ps'+i);
    if(!el)continue;
    el.classList.toggle('act',i===n);
    el.classList.toggle('don',i<n);
  }
}
function invSetPct(pct,msg){
  const pb=document.getElementById('inv-pbar');
  const pp=document.getElementById('inv-ppct');
  const ph=document.getElementById('inv-pph');
  if(pb)pb.style.width=pct+'%';
  if(pp)pp.textContent=pct+'%';
  if(ph&&msg)ph.textContent=msg;
}

// ── Transformaciones ──────────────────────────────────────────────
function invMapActivo(v){
  const s=String(v||'').toLowerCase().trim();
  return ['activo','active','true','1','-1'].includes(s)?-1:0;
}
function invLimpiarNum(v){
  if(v===null||v===undefined||v==='')return 0;
  const n=parseFloat(String(v).replace(/[^0-9.,\-]/g,'').replace(',','.'));
  return isNaN(n)?0:n;
}
function invMapClasi(v){
  return String(v||'').toLowerCase().includes('servicio')?'Servicio':'Producto';
}
function invMapProducto(v){
  return String(v||'').toLowerCase().includes('servicio')?0:-1;
}
function invMapSiNo(v,si=-1,no=0){
  const s=String(v||'').trim().toUpperCase();
  return ['SI','SÍ','YES','1','TRUE','-1'].includes(s)?si:no;
}
function invEsImpo(v){
  return String(v||'').toLowerCase().includes('impoconsumo');
}
function invMapIvaVal(v){
  if(!v)return 0;
  if(invEsImpo(v))return 0;
  const k=invNorm(v);
  for(const[p,val] of Object.entries(INV_IVA_VAL))
    if(k.includes(invNorm(p)))return val;
  const m=k.match(/(\d+(?:\.\d+)?)\s*%/);
  return m?parseFloat(m[1])/100:0;
}
function invMapIvaTipo(v){
  if(!v)return 'No Gravados';
  if(invEsImpo(v))return 'No Gravados';
  const k=invNorm(v);
  for(const[p,tipo] of Object.entries(INV_IVA_TIPO))
    if(k.includes(invNorm(p)))return tipo;
  const m=k.match(/(\d+(?:\.\d+)?)\s*%/);
  if(m)return parseFloat(m[1])>0?'Gravado':'Excluido';
  return 'No Gravados';
}
function invMapImpoVal(v){
  // Returns -1 if contains Impoconsumo, else ''
  return invEsImpo(v)?-1:'';
}
function invMapImpoPct(v){
  // Extracts pct: 'Impoconsumo 8%' → 0.08
  if(!invEsImpo(v))return '';
  const m=String(v).match(/(\d+(?:\.\d+)?)\s*%/);
  return m?parseFloat(m[1])/100:'';
}

// ── Generar código de grupo ───────────────────────────────────────
function invGenCod(nombre,usados){
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
function invBuildGrupos(rows){
  const g1Map=new Map(),g2Map=new Map();
  const u1=new Set(),u2=new Set();
  rows.forEach(r=>{
    const g1=String(r['Grupo Uno']||'').trim();
    const g2=String(r['Grupo Dos']||'').trim();
    if(g1&&!g1Map.has(g1)){const c=invGenCod(g1,u1);if(c){u1.add(c);g1Map.set(g1,c);}}
    if(g2&&!g2Map.has(g2)){const c=invGenCod(g2,u2);if(c){u2.add(c);g2Map.set(g2,c);}}
  });
  return{
    g1:[...g1Map.entries()].sort((a,b)=>a[0].localeCompare(b[0])).map(([n,c])=>({cod:c,nom:n})),
    g2:[...g2Map.entries()].sort((a,b)=>a[0].localeCompare(b[0])).map(([n,c])=>({cod:c,nom:n}))
  };
}

// ── Leer Excel ───────────────────────────────────────────────────
async function invReadFile(file){
  return new Promise((res,rej)=>{
    const reader=new FileReader();
    reader.onload=e=>{
      try{
        const wb=XLSX.read(new Uint8Array(e.target.result),{type:'array',raw:true});
        const ws=wb.Sheets[wb.SheetNames[0]];
        const all=XLSX.utils.sheet_to_json(ws,{header:1,defval:''});
        // Find header row: contains 'Código' or 'Tipo'
        let hi=0;
        for(let i=0;i<Math.min(all.length,15);i++){
          const rn=all[i].map(v=>invNorm(v));
          if(rn.includes('codigo')||rn.includes('tipo')){hi=i;break;}
        }
        const hdrs=all[hi].map(v=>String(v||'').trim());
        const rows=all.slice(hi+1).filter(r=>{
          if(!r.some(v=>v!==''&&v!==null&&v!==undefined))return false;
          // Skip footer rows like "Procesado en:..."
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
async function startInvETL(){
  const file=S.files&&S.files['inv-maestro']?S.files['inv-maestro']:null;
  if(!file){alert('Carga el archivo Listado_de_productos___Servicios.xlsx');return;}

  invSetStep(3);
  INV_LOG.length=0; INV_EXCL.length=0;
  const panel=document.getElementById('inv-logp');
  if(panel)panel.innerHTML='';
  invSetPStep(0); invSetPct(0,'Iniciando...');
  const t0=Date.now();

  try{
    // ── Paso 1: Lectura ──────────────────────────────────────────
    invSetPStep(1); invSetPct(15,'Leyendo archivo...');
    invLog('📂 Leyendo archivo origen...','i','Lectura');
    await invSleep(30);

    const data=await invReadFile(file);
    invLog(`   ${data.rows.length} registros encontrados`,'i','Lectura');

    // ── Detectar columnas ────────────────────────────────────────
    const H=data.hdrs;
    const HN=H.map(invNorm);

    const fi=(terms)=>HN.findIndex(h=>terms.some(t=>h===invNorm(t)||h.includes(invNorm(t))));

    const cTipo  = HN.indexOf('tipo');
    const cCod   = HN.indexOf('codigo');
    const cNom   = HN.indexOf('nombre');
    const cEst   = HN.indexOf('estado');
    const cStk   = fi(['stock minimo','stock min']);
    const cPV1   = fi(INV_P1);
    const cPV2   = fi(INV_P2);
    const cPV3   = fi(INV_P3);
    const cIva   = HN.findIndex(h=>h.includes('impuesto cargo')&&!h.includes('2'));
    const cIva2  = HN.findIndex(h=>h==='impuesto cargo 2'||h==='impuesto cargo2');
    const cInv   = fi(['inventariable']);
    const cIncl  = fi(['es incluido']);
    const cDescL = fi(['descripcion larga']);
    const cRefF  = fi(['referencia fabrica','referencia fab']);
    const cCateg = fi(['categoria']);
    const cBarr  = HN.findIndex(h=>h==='codigo de barras'||h==='codigo barras');

    invLog(`   Cols → Código[${cCod}] Nombre[${cNom}] Tipo[${cTipo}] IVA[${cIva}] IVA2[${cIva2}] P1[${cPV1}] P2[${cPV2}] P3[${cPV3}]`,'i','Lectura');

    // ── Paso 2: Consolidación ────────────────────────────────────
    invSetPStep(2); invSetPct(35,'Consolidando...');
    invLog('🔗 Consolidando registros...','i','Consolidación');
    await invSleep(30);

    const seen=new Set(), out=[], defaults=[];
    const total=data.rows.length;

    for(const r of data.rows){
      const cod=cCod>=0?String(r[cCod]||'').trim():'';
      if(!cod){INV_EXCL.push({cod:'',nom:String(r[cNom>=0?cNom:0]||''),motivo:'Sin código'});continue;}
      if(seen.has(cod)){INV_EXCL.push({cod,nom:String(r[cNom>=0?cNom:0]||''),motivo:'Código duplicado'});continue;}
      seen.add(cod);

      // Leer campos origen
      const nombre = cNom>=0  ? String(r[cNom]||'').trim()  : '';
      const estado = cEst>=0  ? r[cEst]  : 'Activo';
      const stockM = cStk>=0  ? r[cStk]  : 0;
      const pv1raw = cPV1>=0  ? r[cPV1]  : 0;
      const pv2raw = cPV2>=0  ? r[cPV2]  : 0;
      const pv3raw = cPV3>=0  ? r[cPV3]  : 0;
      const tipo   = cTipo>=0 ? String(r[cTipo]||'').trim() : 'Producto';
      const ivaRaw = cIva>=0  ? String(r[cIva]||'').trim()  : '';
      const iva2Raw= cIva2>=0 ? String(r[cIva2]||'').trim() : '';
      const invRaw = cInv>=0  ? r[cInv]  : '';
      const inclRaw= cIncl>=0 ? r[cIncl] : '';
      const descL  = cDescL>=0? String(r[cDescL]||'').trim(): '';
      const refFab = cRefF>=0 ? String(r[cRefF]||'').trim() : '';
      const categ  = cCateg>=0? String(r[cCateg]||'').trim(): '';
      const barras = cBarr>=0 ? String(r[cBarr]||'').trim() : '';

      // Determinar fuente de impoconsumo: cIva o cIva2
      const ivaImpo = invEsImpo(ivaRaw)?ivaRaw:(invEsImpo(iva2Raw)?iva2Raw:'');

      // Transformar
      const activoVal  = invMapActivo(estado);
      const stockN     = invLimpiarNum(stockM);
      const p1n        = invLimpiarNum(pv1raw);
      const p2n        = invLimpiarNum(pv2raw);
      const p3n        = invLimpiarNum(pv3raw);
      const ivaVal     = invMapIvaVal(ivaRaw);
      const tipoIvaVal = invMapIvaTipo(ivaRaw);
      const clasi      = invMapClasi(tipo);
      const prodVal    = invMapProducto(tipo);
      const factSinEx  = invMapSiNo(invRaw, -1, 0);  // Inventariable SI→-1
      const pertVal    = invMapSiNo(inclRaw, -1, ''); // Es incluido SI→-1 else ''
      const impoVal    = invMapImpoVal(ivaImpo);
      const pctImpoVal = invMapImpoPct(ivaImpo);

      // Campos por defecto
      if(!String(estado||'').trim())
        defaults.push({cod,nombre,campo:'Activo',valor:'-1',motivo:'Estado vacío → activo'});
      if(!ivaRaw)
        defaults.push({cod,nombre,campo:'Iva / Tipo Iva',valor:'0 / No Gravados',motivo:'Sin impuesto → no gravado'});
      if(ivaImpo)
        defaults.push({cod,nombre,campo:'Impoconsumo + Porcentaje',valor:`-1 / ${pctImpoVal}`,motivo:`Derivado de: ${ivaImpo}`});

      out.push({
        'Código':                  cod,
        'Descripción':             nombre,
        'Activo':                  activoVal,
        'Exis. Máxima':            0,
        'Exis. Mínima':            stockN,
        'Punto Reorden':           0,
        'Unid. Medida':            'Und.',
        'Precio 1':                p1n,
        'Precio 2':                p2n,
        'Precio 3':                p3n,
        'Precio 4':                0,
        'Grupo Uno':               tipo,
        'Iva ':                    ivaVal,
        'Tipo Iva':                tipoIvaVal,
        'Clasificación':           clasi,
        'Clasificación Niif':      clasi,
        'Producto':                prodVal,
        'Facturar sin Existen.':   factSinEx,
        'Pertenece Produc.':       pertVal,
        'Producto Proceso':        '',
        'Maneja Seriales':         0,
        'Observaciones':           descL||null,
        'Verificar Utilidad':      0,
        'Utilidad Estimada':       '',
        'Arancel':                 '',
        'Impoconsumo':             impoVal,
        'Porcentaje de impoconsumo': pctImpoVal,
        'ImpoConsumo al Costo':    '',
        'Iva Mayor Vr al costo':   '',
        'Gasto que afecta el Costo':'',
        'Centro Costos':           '',
        'Código Barras':           barras||'',
        'Favorito POS':            '',
        'Imagen POS':              '',
        'Impresora':               '',
        'Ocultar Imprimir':        '',
        'Código Internacional':    '',
        'Grupo Dos':               categ||'',
        'Grupo Tres':'','Grupo Cuatro':'','Grupo Cinco':'','Grupo Seis':'',
        'Grupo Siete':'','Grupo Ocho':'','Grupo Nueve':'','Grupo Diez':'',
        'Precio 5':0,'Precio 6':0,'Precio 7':0,'Precio 8':0,'Precio 9':0,
        'Precio 10':0,'Precio 11':0,'Precio 12':0,'Precio 13':0,'Precio 14':0,
        'Precio 15':0,'Precio 16':0,'Precio 17':0,'Precio 18':0,'Precio 19':0,
        'Precio 20':0,'Precio 21':0,'Precio 22':0,'Precio 23':0,'Precio 24':0,
        'Precio 25':0,'Precio 26':0,'Precio 27':0,'Precio 28':0,'Precio 29':0,
        'Precio 30':0,
        'Personalizado 1':  refFab||'',
        'Personalizado 2':'','Personalizado 3':'','Personalizado 4':'',
        'Personalizado 5':'','Personalizado 6':'','Personalizado 7':'',
        'Personalizado 8':'','Personalizado 9':'','Personalizado 10':'',
        'Personalizado 11':'','Personalizado 12':'','Personalizado 13':'',
        'Personalizado 14':'','Personalizado 15':'','Código Centro Costos':''
      });
    }

    invLog(`✅ ${out.length} registros transformados`,'o','Consolidación');
    if(INV_EXCL.length) invLog(`   ⚠ ${INV_EXCL.length} excluidos`,'w','Consolidación');

    // ── Paso 3: Generación Excel ─────────────────────────────────
    invSetPStep(4); invSetPct(85,'Generando Excel...');
    invLog('📊 Construyendo archivo Excel...','i','Escritura');
    await invSleep(30);

    invLog("📦 Iniciando construcción Excel...","i","Escritura");
    INV_WB=invBuildWB(out,INV_LOG,{registros_entrada:total,registros_salida:out.length},INV_EXCL,defaults);

    const dur=((Date.now()-t0)/1000).toFixed(1);
    invSetPct(100,'¡Listo!'); invSetPStep(5);
    invLog(`✅ Excel listo en ${dur}s — ${out.length} productos`,'o','Escritura');

    const fn=invBuildFN();
    const fnEl=document.getElementById('inv-dl-fn'); if(fnEl)fnEl.textContent=fn;
    const stIn=document.getElementById('inv-st-in'); if(stIn)stIn.textContent=total;
    const stOk=document.getElementById('inv-st-ok'); if(stOk)stOk.textContent=out.length;

    try{
      await api('POST','/migrations',{
        filename_out:fn,orig_soft:'Siigo Nube',dest_soft:'World Office Escritorio',
        module:'Inventarios',records_in:total,records_out:out.length,
        errors:0,warnings:INV_EXCL.length,duration_sec:parseFloat(dur),status:'completed'
      },AUTH.token);
    }catch(e){}

    invSetStep(4);

  }catch(err){
    invLog(`❌ Error: ${err.message}`,'e','Pipeline');
    console.error(err);
  }
}

// ── Construir nombre archivo ──────────────────────────────────────
function invBuildFN(){
  const d=new Date();
  return `inventarios_siigo_nube_wo_escritorio_${d.getFullYear()}_${String(d.getMonth()+1).padStart(2,'0')}_${String(d.getDate()).padStart(2,'0')}.xlsx`;
}

// ── Construir Workbook ────────────────────────────────────────────
function invBuildWB(rows,logEntries,stats,excluded,defaults){
  const wb=XLSX.utils.book_new();

  // Hoja 1: Crear Productos y servicios (88 cols)
  const aoa=[INV_COLS.slice()];
  rows.forEach(r=>aoa.push(INV_COLS.map(c=>{
    const v=r[c];
    return (v===undefined||v==='')?null:(v??null);
  })));
  XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet(aoa),'Crear Productos y servicios');

  // Hoja 2: Logs
  const logsAoa=[['Timestamp','Fase','Nivel','Mensaje']];
  (logEntries||[]).forEach(e=>logsAoa.push([
    e.ts,e.fase||'',e.lvl==='e'?'ERROR':e.lvl==='w'?'WARN':'INFO',e.msg
  ]));
  XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet(logsAoa),'Logs');

  // Hoja 4: Estadísticas
  const s=stats||{};
  XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet([
    ['Métrica','Valor'],
    ['Registros entrada',s.registros_entrada||0],
    ['Registros salida',rows.length],
    ['Excluidos',excluded?excluded.length:0],
  ]),'Estadísticas');

  // Hoja 5: Excluidos
  const exAoa=[['Código','Nombre','Motivo']];
  (excluded||[]).forEach(e=>exAoa.push([e.cod,e.nom,e.motivo]));
  XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet(exAoa),'Excluidos');

  // Hoja 6: Campos por Defecto
  if(defaults&&defaults.length){
    const defAoa=[['Código','Nombre','Campo','Valor Asignado','Motivo']];
    defaults.forEach(d=>defAoa.push([d.cod,d.nombre,d.campo,d.valor,d.motivo]));
    XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet(defAoa),'Campos por Defecto');
  }

  return wb;
}

// ── Descarga ──────────────────────────────────────────────────────
function invDoDownload(){
  if(!INV_WB){alert('Primero ejecuta el proceso ETL');return;}
  try{
    XLSX.writeFile(INV_WB,invBuildFN());
  }catch(e){
    console.error('Download error:',e);
    alert('Error al descargar: '+e.message);
  }
}

// ── Reset ─────────────────────────────────────────────────────────
function invReset(){
  INV_WB=null; INV_LOG.length=0; INV_EXCL.length=0;
  if(typeof S!=='undefined'&&S.files) delete S.files['inv-maestro'];
  const sl=document.getElementById('sl-inv-m'); if(sl)sl.className='fslot';
  const nm=document.getElementById('nm-inv-m'); if(nm)nm.textContent='';
  const fi=document.getElementById('f-inv-m'); if(fi)fi.value='';
  const logp=document.getElementById('inv-logp'); if(logp)logp.innerHTML='';
  const pb=document.getElementById('inv-pbar'); if(pb)pb.style.width='0%';
  const pp=document.getElementById('inv-ppct'); if(pp)pp.textContent='0%';
  const ph=document.getElementById('inv-pph'); if(ph)ph.textContent='Iniciando...';
  // Reset selectors
  ['inv-sorig','inv-sdest','inv-smod'].forEach(id=>{
    const el=document.getElementById(id); if(el)el.value='';
  });
  invSetStep(1);
}
