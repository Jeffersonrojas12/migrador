// ══════════════════════════════════════════════════════════════════
// ETL: Empleados — Siigo Nube → World Office Escritorio
// Archivo: empleados_siigo_nube_to_wo_escritorio.js
// Entradas: Modelo_Importacion_Empleados.xlsx + Modelo_Importacion_Contratos.xlsx
// Salida : plantilla-nomina_woEscritorio.xlsx → hoja "Información Empleados"
// ══════════════════════════════════════════════════════════════════

// ── Estado ───────────────────────────────────────────────────────
const EMP_ESC = { files:{} };
let EMP_ESC_WB = null;
const EMP_ESC_LOG = [];
const EMP_ESC_EXCL = [];
// Pending data waiting for modal confirmation
let _empEscPending = null;

// ── Columnas salida (55 cols) ─────────────────────────────────────
const EMP_ESC_COLS = [
  'Tipo Identificación','Identificación','Identificación Ciudad',
  'Primer Nombre','Segundo Nombre','Primer Apellido','Segundo Apellido',
  'Tipo Contrato','Fecha Ingreso','Area','Clase','Empresa','Cargo','Sueldo',
  'Periodo Pago','Centro Costos','Clasificación Dian',
  'Cesantias','IntCesantias','Prima ','Vacaciones','Ret. Fte',
  'Fecha Nacimiento','Ciudad','Tipo Dirección','Direccion','Teléfono','E_Mail',
  'Número hijos','Estado Civil','Declarante','Dotación',
  'Tipo Cuenta ','Número Cuenta ','Banco Cuenta',
  'Tipo Sena','Fecha Fin Periodo Prueba','Fecha Fin Contrato',
  'ARL','Fecha Afil. ARL','Tarifa ARL',
  'EPS','Fecha Afil. EPS',
  'Pensión','Fecha Afil. AFP',
  'Fondo Cesantias','Fecha Afil. Fondo Cesantías',
  'Caja de Compensacion','Fecha Afil. Caja',
  'Código Centro Costos','Libreta Militar No.','Sexo',
  'Centro De Trabajo','Tipo Cotizante','Sub tipo Cotizante'
];

// ── Mapeos ────────────────────────────────────────────────────────
const EMP_ESC_TIPO_ID = {
  'cédula de ciudadanía':'CC','cedula de ciudadania':'CC',
  'tarjeta de identidad':'TI','cédula de extranjería':'Cédula de extranjería',
  'cedula de extranjeria':'Cédula de extranjería','pasaporte':'PASAPORTE',
  'nit':'NIT','registro civil':'REGISTRO CIVIL'
};
const EMP_ESC_CONTRATO = {
  'indefinido':'Indefinido','fijo':'Fijo',
  'obra o labor':'Labor Contratada','aprendizaje':'Definido menos de un año'
};
const EMP_ESC_AREA = {
  'administración':'Administrativa','administracion':'Administrativa',
  'producción':'Produccion','produccion':'Produccion','ventas':'Ventas'
};
const EMP_ESC_TIPO_CUENTA = { 'ahorros':'Ahorros','corriente':'Corriente','no aplica':'' };
const EMP_ESC_BANCO = {
  'bancolombia s.a.':'BANCOLOMBIA','banco de bogotá':'BANCO DE BOGOTÁ',
  'banco av villas':'BANCO AV VILLAS','banco agrario de colombia s.a.':'BANCO AGRARIO DE COLOMBIA',
  'banco falabella s.a.':'BANCO FALABELLA','banco caja social - bcsc s.a.':'BANCO CAJA SOCIAL BCSC',
  'banco de occidente':'BANCO DE OCCIDENTE','nequi':'BANCOLOMBIA',
  'daviplata':'BANCO DAVIVIENDA','bancamia':'BANCOLOMBIA'
};
const EMP_ESC_ARL = {
  'alfa':'ARP ALFA','colmena':'COLMENA RIESGOS PROFESIONALES',
  'colpatria arp':'ARL SEGUROS DE VIDA COLPATRIA SA',
  'la equidad seguros':'LA EQUIDAD SEGUROS DE VIDA',
  'liberty':'LIBERTY SEGUROS DE VIDA',
  'mapfre colombia vida seguros s.a.':'MAPFRE COLOMBIA VIDA SEGUROS SA',
  'positiva compañía de seguros':'POSITIVA COMPAÑÍA DE SEGUROS',
  'seguros bolívar':'COMPAÑÍA DE SEGUROS BOLIVAR SA',
  'seguros de riesgos laborales suramericana s.a.':'ARL SURA',
  'seguros de vida aurora':'SEGUROS DE VIDA AURORA',
  'seguros de vida suramericana':'ARL SURA'
};
const EMP_ESC_EPS = {
  'anas wayuu':'ANAS WAYUU EPSI','ars convida':'CONVIDA','asmet salud':'ASMET  SALUD',
  'capital salud eps':'CAPITAL SALUD EPS-S','capresoca':'CAPRESOCA EPS',
  'comfacor':'COMFACOR EPS S','comfamiliar chocó':'COMFACHOCO EPS S',
  'comfamiliar huila':'COMFAMILIAR HUILA EPS - CCF','comfenalco valle':'COMFENALCO VALLE DE LA GENTE Y EPS',
  'compensar':'COMPENSAR EPS','coomeva':'COOMEVA EPS','coosalud':'COOSALUD EPS-S',
  'cruz blanca eps':'CRUZ BLANCA EPS','eps famisanar':'FAMISANAR EPS CAFAM - COLSUBSIDIO',
  'eps sura':'SURA','nueva eps':'NUEVA EPS','salud total eps':'SALUD TOTAL EPS',
  'saludvida':'SALUD VIDA EPS','sanitas':'SANITAS EPS','aliansalud':'ALIANSALUD EPS'
};
const EMP_ESC_PENSION = {
  'colfondos':'COLFONDOS S.A. AFPC','colpensiones':'COLPENSIONES',
  'old mutual':'OLD MUTUAL PENSIONES Y CESANTÍAS S.A.',
  'pensiones de antioquia':'PENSIONES DE ANTIOQUIA',
  'porvenir s.a':'PORVENIR S.A. AFPC','protección s.a':'PROTECCIÓN S.A. AFPC'
};
const EMP_ESC_CAJA = {
  'cafam':'CAFAM','cafasur':'CAFASUR','colsubsidio':'CAJA COLOMBIANA DE SUBSIDIO FAMILIAR COLSUBSIDIO',
  'comfama':'CAJA DE COMPENSACION FAMILIAR DE ANTIOQUIA COMFAMA',
  'comfamiliar risaralda':'COMFAMILIAR RISARALDA','comfandi':'COMFANDI',
  'comfenalco':'COMFENALCO ANTIOQUIA','compensar':'COMPENSAR EPS'
};
const EMP_ESC_TIPO_COT = {
  'dependiente - contrato laboral':'Dependiente','independiente':'Independiente',
  'servicio doméstico':'Servicio Domestico','aprendiz sena':'Aprendices del SENA en etapa lectiva'
};
const EMP_ESC_SUB_COT = {
  'sin subtipo':'Ninguno',
  'cotizante con requisitos cumplidos para pensión':'Cotizante con requisitos cumplidos para pension'
};

// ── Helpers ───────────────────────────────────────────────────────
function empEscMap(val, map, def=''){
  const k=String(val||'').toLowerCase().trim();
  if(!k) return def;
  if(map[k]!==undefined) return map[k];
  // partial match
  for(const mk of Object.keys(map)){
    if(k.includes(mk)||mk.includes(k)) return map[mk];
  }
  return String(val||'')||def;
}
function empEscFecha(val, fmt='dd/MM/yyyy'){
  if(!val) return '';
  const s=String(val).trim().substring(0,10);
  const fmts=[/^(\d{4})-(\d{2})-(\d{2})$/,/^(\d{2})\/(\d{2})\/(\d{4})$/];
  const m1=s.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if(m1){
    if(fmt==='dd/MM/yyyy') return `${m1[3]}/${m1[2]}/${m1[1]}`;
    return s.substring(0,10);
  }
  return s;
}
function empEscNormCiudad(raw){
  // Use escritorio cities list if available
  if(!raw) return 'Bogota D.C.';
  const s=String(raw).trim();
  if(typeof ESC_CIUDADES !== 'undefined'){
    const ex=ESC_CIUDADES.find(c=>c.toLowerCase()===s.toLowerCase());
    if(ex) return ex;
    const st=ESC_CIUDADES.find(c=>c.toLowerCase().startsWith(s.toLowerCase().substring(0,5)));
    if(st) return st;
  }
  return s;
}
function empLog(msg,lvl='i',fase=''){
  const ts=new Date().toISOString();
  EMP_ESC_LOG.push({ts,fase,lvl,msg});
  const panel=document.getElementById('emp-logp');
  if(!panel)return;
  const now=new Date().toLocaleTimeString('es-CO',{hour12:false});
  const css={i:'li',w:'lw',o:'lo',e:'le-e'}[lvl]||'li';
  panel.innerHTML+=`<div class="le"><span class="lt">${now}</span><span class="${css}">${msg}</span></div>`;
  panel.scrollTop=panel.scrollHeight;
}
function empEscSleep(ms){return new Promise(r=>setTimeout(r,ms));}

// ── Leer Excel genérico ───────────────────────────────────────────
async function empEscReadXlsx(file, headerRowHint){
  return new Promise((resolve,reject)=>{
    const reader=new FileReader();
    reader.onload=e=>{
      try{
        const wb=XLSX.read(new Uint8Array(e.target.result),{type:'array',cellText:true,raw:false});
        const ws=wb.Sheets[wb.SheetNames[0]];
        const all=XLSX.utils.sheet_to_json(ws,{header:1,defval:''});
        // Auto-detect header row
        let hdrIdx=headerRowHint||0;
        for(let i=0;i<Math.min(all.length,5);i++){
          if(all[i].some(v=>String(v).length>5&&!/^\d+$/.test(String(v)))){
            hdrIdx=i; break;
          }
        }
        const hdrs=all[hdrIdx].map(h=>String(h||'').trim());
        const rows=all.slice(hdrIdx+1).filter(r=>r.some(v=>v!==''&&v!==null));
        resolve({hdrs,rows});
      }catch(err){reject(err);}
    };
    reader.readAsArrayBuffer(file);
  });
}

// ── ETL Principal Escritorio ──────────────────────────────────────
async function startEmpEscETL(){
  const empFile = S.files && S.files['emp-maestro'] ? S.files['emp-maestro'] : null;
  const ctrFile = S.files && S.files['emp-contratos'] ? S.files['emp-contratos'] : null;
  if(!empFile){ alert('Carga el archivo Modelo_Importacion_Empleados.xlsx'); return; }
  if(!ctrFile){ alert('Carga el archivo Modelo_Importacion_Contratos.xlsx'); return; }

  empSetStep(3);
  EMP_ESC_LOG.length=0; EMP_ESC_EXCL.length=0;
  const panel=document.getElementById('emp-logp');
  if(panel) panel.innerHTML='';
  empSetPStep(0); empSetPct(0,'Iniciando...');
  const t0=Date.now();

  try{
    // ── Lectura ──────────────────────────────────────────────────
    empLog('📂 Leyendo archivos...','i','Lectura');
    empSetPStep(1); empSetPct(10,'Leyendo empleados...');
    await empEscSleep(30);

    const emp = await empEscReadXlsx(empFile, 1);
    empLog(`   Empleados: ${emp.rows.length} registros`,'i','Lectura');

    empSetPct(20,'Leyendo contratos...');
    const ctr = await empEscReadXlsx(ctrFile, 1);
    empLog(`   Contratos: ${ctr.rows.length} registros`,'i','Lectura');

    // ── Índices columnas empleados ────────────────────────────────
    const EH = emp.hdrs;
    const eN1  = EH.findIndex(h=>/primer nombre/i.test(h));
    const eN2  = EH.findIndex(h=>/segundo nombre/i.test(h));
    const eA1  = EH.findIndex(h=>/primer apellido/i.test(h));
    const eA2  = EH.findIndex(h=>/segundo apellido/i.test(h));
    const eTI  = EH.findIndex(h=>/tipo de documento/i.test(h));
    const eID  = EH.findIndex(h=>/n[uú]mero de documento/i.test(h));
    const eEm  = EH.findIndex(h=>/correo electr/i.test(h));
    const eTel = EH.findIndex(h=>/n[uú]mero de celular|n[uú]mero de tel/i.test(h));
    const eCiu = EH.findIndex(h=>/ciudad de residencia/i.test(h));
    const eDir = EH.findIndex(h=>/direcci[oó]n de residencia/i.test(h));
    const ePago= EH.findIndex(h=>/m[eé]todo de pago/i.test(h));
    const eBanco=EH.findIndex(h=>/entidad bancaria/i.test(h));
    const eTCta= EH.findIndex(h=>/tipo de cuenta/i.test(h));
    const eNCta= EH.findIndex(h=>/n[uú]mero de cuenta/i.test(h));
    const eCiuOf=EH.findIndex(h=>/ciudad de oficina/i.test(h));
    const eDirOf=EH.findIndex(h=>/direcci[oó]n de oficina/i.test(h));

    // ── Índices columnas contratos ────────────────────────────────
    const CH = ctr.hdrs;
    const cNom = CH.findIndex(h=>/nombre del empleado/i.test(h));
    const cID  = CH.findIndex(h=>/n[uú]mero de identificaci/i.test(h));
    const cTC  = CH.findIndex(h=>/tipo de contrato/i.test(h));
    const cFI  = CH.findIndex(h=>/fecha inicio de contrato/i.test(h));
    const cFF  = CH.findIndex(h=>/fecha fin de contrato/i.test(h));
    const cSuel= CH.findIndex(h=>/sueldo/i.test(h));
    const cGN  = CH.findIndex(h=>/grupo de n[oó]mina/i.test(h));
    const cCarg= CH.findIndex(h=>/cargo/i.test(h));
    const cCC  = CH.findIndex(h=>/centro de costo/i.test(h));
    const cTCot= CH.findIndex(h=>/tipo de cotizante/i.test(h));
    const cSTCot=CH.findIndex(h=>/subtipo de cotizante/i.test(h));
    const cEPS = CH.findIndex(h=>/fondo de salud/i.test(h));
    const cPen = CH.findIndex(h=>/fondo de pensi[oó]n/i.test(h));
    const cARL = CH.findIndex(h=>/fondo arl/i.test(h));
    const cRiesgo=CH.findIndex(h=>/clase de riesgo/i.test(h));
    const cCaja=CH.findIndex(h=>/caja de compensaci/i.test(h));
    const cFCes=CH.findIndex(h=>/fondo de cesant/i.test(h));

    // ── Índice contratos por ID ───────────────────────────────────
    const ctrMap={};
    for(const r of ctr.rows){
      const id=String(r[cID]||'').replace(/[^0-9]/g,'').trim();
      if(id) ctrMap[id]=r;
    }

    // ── Consolidar ───────────────────────────────────────────────
    empSetPStep(2); empSetPct(35,'Consolidando...');
    await empEscSleep(30);

    const seen=new Set(), out=[];
    const cargosSet=new Set(), centrosTrabSet=new Set(), centrosCostSet=new Set();
    const camposPorDefecto=[];
    const totalEntrada=emp.rows.length;

    for(const r of emp.rows){
      const id=String(r[eID]||'').replace(/[^0-9]/g,'').trim();
      if(!id){ EMP_ESC_EXCL.push({id:'',nombre:String(r[eN1]||''),motivo:'sin-id'}); continue; }
      if(seen.has(id)){ EMP_ESC_EXCL.push({id,nombre:String(r[eN1]||''),motivo:'duplicado'}); continue; }
      seen.add(id);

      const c = ctrMap[id] || [];
      const tipoId = empEscMap(r[eTI], EMP_ESC_TIPO_ID, 'CC');
      const fechaIngreso = c[cFI] ? empEscFecha(c[cFI]) : '';
      const fechaFin = c[cFF] ? empEscFecha(c[cFF]) : '';

      const row={
        'Tipo Identificación': tipoId,
        'Identificación': id,
        'Identificación Ciudad': empEscNormCiudad(r[eCiu]),
        'Primer Nombre': String(r[eN1]||'').toUpperCase().trim(),
        'Segundo Nombre': String(r[eN2]||'').toUpperCase().trim()||null,
        'Primer Apellido': String(r[eA1]||'').toUpperCase().trim(),
        'Segundo Apellido': String(r[eA2]||'').toUpperCase().trim()||null,
        'Tipo Contrato': c[cTC] ? empEscMap(c[cTC], EMP_ESC_CONTRATO, 'Indefinido') : null,
        'Fecha Ingreso': fechaIngreso||null,
        'Area': c[cGN] ? empEscMap(c[cGN], EMP_ESC_AREA, 'Administrativa') : null,
        'Clase': 'Normal',
        'Empresa': null,
        'Cargo': c[cCarg] ? String(c[cCarg]).trim() : null,
        'Sueldo': c[cSuel] ? String(c[cSuel]).replace(/[^0-9]/g,'') : null,
        'Periodo Pago': 'Mensual',
        'Centro Costos': c[cCC] ? String(c[cCC]).split('-').pop().trim() : null,
        'Clasificación Dian': 'Normal',
        'Cesantias': -1, 'IntCesantias': -1, 'Prima ': -1, 'Vacaciones': -1, 'Ret. Fte': -1,
        'Fecha Nacimiento': null,
        'Ciudad': empEscNormCiudad(r[eCiu]),
        'Tipo Dirección': 'Casa',
        'Direccion': String(r[eDir]||'').toUpperCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'').trim()||null,
        'Teléfono': String(r[eTel]||'').replace(/[^0-9]/g,'')||null,
        'E_Mail': String(r[eEm]||'').trim().toLowerCase()||null,
        'Número hijos': null,
        'Estado Civil': null,
        'Declarante': null,
        'Dotación': null,
        'Tipo Cuenta ': r[eTCta] ? empEscMap(r[eTCta], EMP_ESC_TIPO_CUENTA) : null,
        'Número Cuenta ': r[eNCta] ? String(r[eNCta]).replace(/\.0$/,'').trim() : null,
        'Banco Cuenta': r[eBanco] ? empEscMap(r[eBanco], EMP_ESC_BANCO) : null,
        'Tipo Sena': null,
        'Fecha Fin Periodo Prueba': null,
        'Fecha Fin Contrato': fechaFin||null,
        'ARL': c[cARL] ? empEscMap(c[cARL], EMP_ESC_ARL) : null,
        'Fecha Afil. ARL': fechaIngreso||null,
        'Tarifa ARL': c[cRiesgo] ? (String(c[cRiesgo]).match(/(\d+[,.]\d+)/)||[''])[0].replace('.',',') : null,
        'EPS': c[cEPS] ? empEscMap(c[cEPS], EMP_ESC_EPS) : null,
        'Fecha Afil. EPS': fechaIngreso||null,
        'Pensión': c[cPen] ? empEscMap(c[cPen], EMP_ESC_PENSION) : null,
        'Fecha Afil. AFP': fechaIngreso||null,
        'Fondo Cesantias': c[cFCes] ? empEscMap(c[cFCes], EMP_ESC_PENSION) : null,
        'Fecha Afil. Fondo Cesantías': fechaIngreso||null,
        'Caja de Compensacion': c[cCaja] ? empEscMap(c[cCaja], EMP_ESC_CAJA) : null,
        'Fecha Afil. Caja': fechaIngreso||null,
        'Código Centro Costos': null,
        'Libreta Militar No.': null,
        'Sexo': null,
        'Centro De Trabajo': r[eDirOf] ? String(r[eDirOf]).trim() : null,
        'Tipo Cotizante': c[cTCot] ? empEscMap(c[cTCot], EMP_ESC_TIPO_COT, 'Dependiente') : 'Dependiente',
        'Sub tipo Cotizante': c[cSTCot] ? empEscMap(c[cSTCot], EMP_ESC_SUB_COT, 'Ninguno') : 'Ninguno'
      };
      out.push(row);

      // Track unique cargos, centros de trabajo, centros de costo
      if(row['Cargo']) cargosSet.add(String(row['Cargo']).trim());
      if(row['Centro De Trabajo']) centrosTrabSet.add(String(row['Centro De Trabajo']).trim());
      if(row['Centro Costos']) centrosCostSet.add(String(row['Centro Costos']).trim());

      // Track campos por defecto
      if(!c[cFI]) camposPorDefecto.push({id:row['Identificación'],nombre:`${row['Primer Nombre']||''} ${row['Primer Apellido']||''}`.trim(),campo:'Fechas (Ingreso/Afiliaciones)',valor:'Fecha vacía — no se asigna',motivo:'Contrato sin fecha de inicio'});
      if(!c[cTC]) camposPorDefecto.push({id:row['Identificación'],nombre:`${row['Primer Nombre']||''} ${row['Primer Apellido']||''}`.trim(),campo:'Tipo Contrato',valor:'Indefinido',motivo:'Campo vacío en contratos'});
      if(!c[cGN]) camposPorDefecto.push({id:row['Identificación'],nombre:`${row['Primer Nombre']||''} ${row['Primer Apellido']||''}`.trim(),campo:'Area',valor:'Administrativa',motivo:'Grupo nómina vacío'});
      if(row['Clase']==='Normal') camposPorDefecto.push({id:row['Identificación'],nombre:`${row['Primer Nombre']||''} ${row['Primer Apellido']||''}`.trim(),campo:'Clase',valor:'Normal',motivo:'Valor fijo por defecto'});
      if(row['Periodo Pago']==='Mensual') camposPorDefecto.push({id:row['Identificación'],nombre:`${row['Primer Nombre']||''} ${row['Primer Apellido']||''}`.trim(),campo:'Periodo Pago',valor:'Mensual',motivo:'Valor fijo por defecto'});
      if(row['Clasificación Dian']==='Normal') camposPorDefecto.push({id:row['Identificación'],nombre:`${row['Primer Nombre']||''} ${row['Primer Apellido']||''}`.trim(),campo:'Clasificación Dian',valor:'Normal',motivo:'Valor fijo por defecto'});
      if(!row['Teléfono']||row['Teléfono']==='') camposPorDefecto.push({id:row['Identificación'],nombre:`${row['Primer Nombre']||''} ${row['Primer Apellido']||''}`.trim(),campo:'Teléfono',valor:row['Teléfono']||'vacío',motivo:'Teléfono vacío en origen'});
    }

    empLog(`✅ Consolidados: ${out.length} registros`,'o','Consolidación');
    const sinContrato = [...seen].filter(id=>!ctrMap[id]).length;
    if(sinContrato>0) empLog(`   ⚠ ${sinContrato} empleados sin contrato (campos de contrato vacíos)`,'w','Consolidación');

    // ── Transformación ───────────────────────────────────────────
    empSetPStep(3); empSetPct(65,'Transformando...');
    await empEscSleep(30);
    empLog(`🔄 Transformados: ${out.length} registros`,'o','Transformación');

    // ── Generación Excel ─────────────────────────────────────────
    empSetPStep(4); empSetPct(85,'Generando Excel...');
    await empEscSleep(30);
    empLog('📊 Construyendo archivo Excel...','i','Escritura');

    const dur=((Date.now()-t0)/1000).toFixed(1);
    empSetPct(95,'Preparando mapeo...'); empSetPStep(4);
    empLog('🔗 Abriendo ventana de mapeo de entidades...','i','Mapeo');

    // Store pending data and show mapping modal
    _empEscPending = {out, stats:{registros_entrada:totalEntrada,registros_salida:out.length},
      excluded:EMP_ESC_EXCL, cargos:cargosSet, centrosTrab:centrosTrabSet,
      centrosCost:centrosCostSet, defaults:camposPorDefecto, dur, totalEntrada};
    empShowMapModal(out);
    return; // Will continue in empMapConfirm()

    // (code below runs after modal - see empMapConfirm)
    empSetPct(100,'¡Listo!'); empSetPStep(5);
    empLog(`✅ Excel generado en ${dur}s — ${out.length} registros`,'o','Escritura');

    const fn=empEscBuildFN();
    const fnEl=document.getElementById('emp-dl-fn');
    if(fnEl) fnEl.textContent=fn;
    const stIn=document.getElementById('emp-st-in');
    const stOk=document.getElementById('emp-st-ok');
    if(stIn) stIn.textContent=totalEntrada;
    if(stOk) stOk.textContent=out.length;

    try{
      await api('POST','/migrations',{
        filename_out:fn,orig_soft:'Siigo Nube',dest_soft:'World Office Escritorio',
        module:'Empleados',records_in:totalEntrada,records_out:out.length,
        errors:0,warnings:sinContrato,duration_sec:parseFloat(dur),status:'completed'
      },AUTH.token);
    }catch(e){}

    empSetStep(4);
  }catch(err){
    empLog(`❌ Error: ${err.message}`,'e','Pipeline');
    console.error(err);
  }
}

function empEscBuildFN(){
  const d=new Date();
  return `empleados_siigo_nube_wo_escritorio_${d.getFullYear()}_${String(d.getMonth()+1).padStart(2,'0')}_${String(d.getDate()).padStart(2,'0')}.xlsx`;
}

function empEscBuildWB(rows, logEntries, stats, excluded, cargosSet, centrosTrabSet, centrosCostSet, camposPorDefecto){
  const wb=XLSX.utils.book_new();

  // Hoja 1: Información Empleados
  const DATE_COLS=new Set(['Fecha Ingreso','Fecha Nacimiento','Fecha Fin Periodo Prueba',
    'Fecha Fin Contrato','Fecha Afil. ARL','Fecha Afil. EPS','Fecha Afil. AFP',
    'Fecha Afil. Fondo Cesantías','Fecha Afil. Caja']);
  const aoa=[EMP_ESC_COLS.slice()];
  rows.forEach(r=>aoa.push(EMP_ESC_COLS.map(c=>{const v=r[c];return v===undefined||v===''?null:v??null;})));
  const ws1=XLSX.utils.aoa_to_sheet(aoa);
  // Force date columns to string so Excel shows dd/MM/yyyy not a number
  const rng=XLSX.utils.decode_range(ws1['!ref']||'A1');
  EMP_ESC_COLS.forEach((col,ci)=>{
    if(!DATE_COLS.has(col))return;
    for(let ri=1;ri<=rng.e.r;ri++){
      const addr=XLSX.utils.encode_cell({r:ri,c:ci});
      if(ws1[addr]&&ws1[addr].v!==null&&ws1[addr].v!==undefined){
        ws1[addr].t='s';
        ws1[addr].v=String(ws1[addr].v);
        delete ws1[addr].z;
      }
    }
  });
  XLSX.utils.book_append_sheet(wb,ws1,'Información Empleados');

  // Hoja 2: Logs
  const logsAoa=[['timestamp','fase','nivel','mensaje']];
  (logEntries||[]).forEach(e=>logsAoa.push([e.ts,e.fase,e.lvl==='e'?'ERROR':e.lvl==='w'?'WARN':'INFO',e.msg]));
  XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet(logsAoa),'Logs');

  // Hoja 3: Estadísticas
  const s=stats||{};
  XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet([
    ['Métrica','Valor'],
    ['Registros entrada',s.registros_entrada||0],
    ['Registros salida',rows.length],
  ]),'Estadísticas');

  // Hoja 4: Excluidos
  const exAoa=[['Identificación','Nombre','Motivo']];
  (excluded||[]).forEach(e=>exAoa.push([e.id,e.nombre,e.motivo]));
  XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet(exAoa),'Excluidos');

  // Hoja: Cargos
  if(cargosSet && cargosSet.size > 0){
    const cargosAoa=[['Cargo']];
    [...cargosSet].sort().forEach(c=>cargosAoa.push([c]));
    XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet(cargosAoa), 'Cargos');
  }

  // Hoja: Centro de Trabajo
  if(centrosTrabSet && centrosTrabSet.size > 0){
    const ctAoa=[['Centro de Trabajo']];
    [...centrosTrabSet].sort().forEach(c=>ctAoa.push([c]));
    XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet(ctAoa), 'Centro de Trabajo');
  }

  // Hoja: Centro de Costos
  if(centrosCostSet && centrosCostSet.size > 0){
    const ccAoa=[['Centro de Costos']];
    [...centrosCostSet].sort().forEach(c=>ccAoa.push([c]));
    XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet(ccAoa), 'Centro de Costos');
  }

  // Hoja: Campos por Defecto
  const defAoa=[['Identificación','Nombre','Campo','Valor Asignado','Motivo']];
  if(camposPorDefecto && camposPorDefecto.length > 0){
    camposPorDefecto.forEach(d=>defAoa.push([d.id, d.nombre, d.campo, d.valor, d.motivo]));
  }
  XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet(defAoa), 'Campos por Defecto');

  return wb;
}


// ── Mapping Modal ─────────────────────────────────────────────────
const EMP_MAP_COLS = {
  'ARL':             {label:'ARL / Administradora de Riesgos Laborales', rowKey:'ARL'},
  'EPS':             {label:'EPS / Fondo de Salud',                       rowKey:'EPS'},
  'Pensión':         {label:'Fondo de Pensión',                           rowKey:'Pensión'},
  'Fondo Cesantias': {label:'Fondo de Cesantías',                         rowKey:'Fondo Cesantias'},
  'Caja de Compensacion': {label:'Caja de Compensación',                  rowKey:'Caja de Compensacion'},
};

function empShowMapModal(rows){
  // Collect unique values per entity column
  const entities = {};
  Object.keys(EMP_MAP_COLS).forEach(col=>{
    const vals = new Set();
    rows.forEach(r=>{ if(r[col]) vals.add(String(r[col]).trim()); });
    if(vals.size>0) entities[col] = [...vals].sort();
  });

  // Build modal sections
  const container = document.getElementById('emp-map-sections');
  if(!container) return;
  container.innerHTML='';

  Object.entries(entities).forEach(([col, vals])=>{
    const cfg = EMP_MAP_COLS[col];
    const section = document.createElement('div');
    section.style.cssText='background:rgba(255,255,255,.07);border:1px solid rgba(255,255,255,.12);border-radius:12px;padding:20px;margin-bottom:20px';
    section.innerHTML = `
      <div style="font-size:14px;font-weight:700;color:#fff;margin-bottom:14px;display:flex;align-items:center;gap:8px">
        <span style="background:rgba(255,255,255,.15);padding:3px 10px;border-radius:20px;font-size:11px;font-family:monospace">${col}</span>
        <span>${cfg.label}</span>
      </div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;font-size:12px;color:rgba(255,255,255,.5);margin-bottom:8px;padding:0 4px">
        <div>Valor en Siigo Nube (origen)</div><div>Nombre en World Office Escritorio</div>
      </div>
      ${vals.map(v=>`
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:8px">
          <div style="background:rgba(0,0,0,.2);border:1px solid rgba(255,255,255,.1);border-radius:8px;
            padding:10px 14px;color:rgba(255,255,255,.8);font-size:13px;display:flex;align-items:center">${v}</div>
          <input type="text" placeholder="Nombre exacto en WO Escritorio..."
            data-col="${col}" data-val="${v.replace(/"/g,'&quot;')}"
            style="background:rgba(255,255,255,.08);border:1px solid rgba(255,255,255,.2);border-radius:8px;
              padding:10px 14px;color:#fff;font-size:13px;outline:none;width:100%;box-sizing:border-box"
            oninput="this.style.borderColor=this.value?'rgba(74,222,128,.6)':'rgba(255,255,255,.2)'">
        </div>`).join('')}
    `;
    container.appendChild(section);
  });

  // Show modal and start canvas particles
  const modal = document.getElementById('emp-map-modal');
  if(modal){ modal.style.display='block'; modal.scrollTop=0; }
  setTimeout(()=>{ if(typeof makeParticles==='function') makeParticles('modal-canvas',60,0.5,0.5); },100);
}

function empMapConfirm(){
  const modal = document.getElementById('emp-map-modal');
  const errEl = document.getElementById('emp-map-err');
  const inputs = modal ? modal.querySelectorAll('input[data-col]') : [];

  // Validate all filled
  let missing = false;
  inputs.forEach(inp=>{ if(!inp.value.trim()){ inp.style.borderColor='rgba(239,68,68,.6)'; missing=true; } });
  if(missing){ if(errEl) errEl.style.display='block'; return; }
  if(errEl) errEl.style.display='none';

  // Build mapping
  const mapping = {}; // { col: { siigo_val: wo_val } }
  inputs.forEach(inp=>{
    const col = inp.dataset.col;
    const val = inp.dataset.val;
    if(!mapping[col]) mapping[col] = {};
    mapping[col][val] = inp.value.trim();
  });

  // Apply mapping to pending rows
  if(!_empEscPending){ if(modal) modal.style.display='none'; return; }
  const {out, stats, excluded, cargos, centrosTrab, centrosCost, defaults, dur, totalEntrada} = _empEscPending;

  out.forEach(row=>{
    Object.keys(EMP_MAP_COLS).forEach(col=>{
      const key = EMP_MAP_COLS[col].rowKey;
      if(row[key] && mapping[col] && mapping[col][row[key]]){
        row[key] = mapping[col][row[key]];
      }
    });
  });

  // Build workbook
  EMP_ESC_WB = empEscBuildWB(out, EMP_ESC_LOG, stats, excluded, cargos, centrosTrab, centrosCost, defaults);

  // Close modal
  if(modal) modal.style.display='none';

  // Finish ETL
  empSetPct(100,'¡Listo!'); empSetPStep(5);
  empLog('✅ Excel generado en '+dur+'s — '+out.length+' registros','o','Escritura');

  const fn = empEscBuildFN();
  const fnEl = document.getElementById('emp-dl-fn'); if(fnEl) fnEl.textContent=fn;
  const stIn = document.getElementById('emp-st-in'); if(stIn) stIn.textContent=totalEntrada;
  const stOk = document.getElementById('emp-st-ok'); if(stOk) stOk.textContent=out.length;

  try{
    api('POST','/migrations',{
      filename_out:fn,orig_soft:'Siigo Nube',dest_soft:'World Office Escritorio',
      module:'Empleados',records_in:totalEntrada,records_out:out.length,
      errors:0,warnings:0,duration_sec:parseFloat(dur),status:'completed'
    },AUTH.token);
  }catch(e){}

  empSetStep(4);
  _empEscPending = null;
}

function empEscDoDownload(){
  if(!EMP_ESC_WB){alert('Primero ejecuta el proceso ETL');return;}
  XLSX.writeFile(EMP_ESC_WB, empEscBuildFN());
}
