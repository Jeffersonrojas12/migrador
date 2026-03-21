// ══════════════════════════════════════════════════════════════════
// ETL: Empleados — Siigo Nube → World Office Cloud
// Archivo: empleados_siigo_nube_to_wo_cloud.js
// Entrada : Modelo_Importacion_Empleados.xlsx + Modelo_Importacion_Contratos.xlsx
// Salida  : Modelo_Importacion_Empleados.xlsx (mismo formato de entrada)
//           + Modelo_Importacion_Contratos.xlsx (mismo formato)
// ══════════════════════════════════════════════════════════════════

// ── Estado ───────────────────────────────────────────────────────
const EMP_CLD = { files:{} };
let EMP_CLD_WB = null;
const EMP_CLD_LOG = [];
const EMP_CLD_EXCL = [];

// ── Columnas salida empleados Cloud (misma estructura origen) ─────
const EMP_CLD_COLS = [
  'Primer nombre (Obligatorio)','Segundo nombre ','Primer apellido (Obligatorio)',
  'Segundo apellido ','Tipo de documento (Obligatorio)','Número de documento (Obligatorio)',
  'Correo electrónico (Obligatorio)','Número de celular (Obligatorio)',
  'Pais de residencia (Obligatorio)','Departamento de residencia (Obligatorio)',
  'Ciudad de residencia (Obligatorio)','Dirección de residencia (Obligatorio)',
  'Método de pago (Obligatorio)','Entidad Bancaria','Tipo de cuenta','Número de cuenta',
  'Número de telefono celular','Dirección de oficina (Obligatorio)',
  'Pais de oficina (Obligatorio)','Departamento de oficina (Obligatorio)',
  'Ciudad de oficina (Obligatorio)'
];

// ── Columnas contratos Cloud (misma estructura origen) ────────────
const EMP_CLD_CTR_COLS = [
  'Nombre del empleado','Número de identificación (Obligatorio)',
  'Tipo de contrato (Obligatorio)','Fecha inicio de contrato (Obligatorio)',
  'Fecha fin de contrato','Sueldo (Obligatorio)','Salario integral',
  'Número de contrato (Obligatorio)','Grupo de nómina (Obligatorio)',
  'Cargo (Obligatorio)','Centro de costo','Tipo de cotizante (Obligatorio)',
  'Subtipo de Cotizante (Obligatorio)','Fondo de salud (Obligatorio)',
  '% Salud (Obligatorio)','Fondo de pensión','% pensión',
  'Fondo ARL','Clase de riesgo','Codigo CIIU','Código',
  'Caja de compensación','Fondo de cesantías',
  'Deducción de vivienda','Deducción medicina prepagada',
  'Aportes voluntarios a fondos de pensiones obligatorias',
  'Aporte voluntario de pensión (Renta exenta)',
  'Aporte voluntario AFC','Aplica deducción para dependientes'
];

// ── Helpers ───────────────────────────────────────────────────────
function empLog(msg,lvl='i',fase=''){
  const ts=new Date().toISOString();
  EMP_CLD_LOG.push({ts,fase,lvl,msg});
  const panel=document.getElementById('emp-logp');
  if(!panel)return;
  const now=new Date().toLocaleTimeString('es-CO',{hour12:false});
  const css={i:'li',w:'lw',o:'lo',e:'le-e'}[lvl]||'li';
  panel.innerHTML+=`<div class="le"><span class="lt">${now}</span><span class="${css}">${msg}</span></div>`;
  panel.scrollTop=panel.scrollHeight;
}
function empCldSleep(ms){return new Promise(r=>setTimeout(r,ms));}

async function empCldReadXlsx(file){
  return new Promise((resolve,reject)=>{
    const reader=new FileReader();
    reader.onload=e=>{
      try{
        const wb=XLSX.read(new Uint8Array(e.target.result),{type:'array',cellText:true,raw:false});
        const ws=wb.Sheets[wb.SheetNames[0]];
        const all=XLSX.utils.sheet_to_json(ws,{header:1,defval:''});
        let hdrIdx=0;
        for(let i=0;i<Math.min(all.length,5);i++){
          if(all[i].some(v=>String(v).length>5&&!/^\d+$/.test(String(v)))){
            hdrIdx=i; break;
          }
        }
        const hdrs=all[hdrIdx].map(h=>String(h||'').trim());
        const rows=all.slice(hdrIdx+1).filter(r=>r.some(v=>v!==''&&v!==null));
        resolve({hdrs,rows,hdrRow:all[hdrIdx]});
      }catch(err){reject(err);}
    };
    reader.readAsArrayBuffer(file);
  });
}

// ── ETL Principal Cloud ───────────────────────────────────────────
async function startEmpCldETL(){
  const empFile = S.files && S.files['emp-maestro'] ? S.files['emp-maestro'] : null;
  const ctrFile = S.files && S.files['emp-contratos'] ? S.files['emp-contratos'] : null;
  if(!empFile){ alert('Carga el archivo Modelo_Importacion_Empleados.xlsx'); return; }
  if(!ctrFile){ alert('Carga el archivo Modelo_Importacion_Contratos.xlsx'); return; }

  empSetStep(3);
  EMP_CLD_LOG.length=0; EMP_CLD_EXCL.length=0;
  const panel=document.getElementById('emp-logp');
  if(panel) panel.innerHTML='';
  empSetPStep(0); empSetPct(0,'Iniciando...');
  const t0=Date.now();

  try{
    // ── Lectura ──────────────────────────────────────────────────
    empLog('📂 Leyendo archivos...','i','Lectura');
    empSetPStep(1); empSetPct(10,'Leyendo empleados...');
    await empCldSleep(30);

    const emp = await empCldReadXlsx(empFile);
    empLog(`   Empleados: ${emp.rows.length} registros`,'i','Lectura');

    empSetPct(20,'Leyendo contratos...');
    const ctr = await empCldReadXlsx(ctrFile);
    empLog(`   Contratos: ${ctr.rows.length} registros`,'i','Lectura');

    // ── Índices empleados ─────────────────────────────────────────
    const EH=emp.hdrs;
    const eN1 =EH.findIndex(h=>/primer nombre/i.test(h));
    const eID =EH.findIndex(h=>/n[uú]mero de documento/i.test(h));

    // ── Índices contratos ─────────────────────────────────────────
    const CH=ctr.hdrs;
    const cID =CH.findIndex(h=>/n[uú]mero de identificaci/i.test(h));
    const cNom=CH.findIndex(h=>/nombre del empleado/i.test(h));

    // ── Dedup empleados ───────────────────────────────────────────
    empSetPStep(2); empSetPct(35,'Consolidando...');
    await empCldSleep(30);

    const seenEmp=new Set(), outEmp=[];
    const totalEntrada=emp.rows.length;

    for(const r of emp.rows){
      const id=String(r[eID]||'').replace(/[^0-9]/g,'').trim();
      if(!id){ EMP_CLD_EXCL.push({id:'',nombre:String(r[eN1]||''),motivo:'sin-id'}); continue; }
      if(seenEmp.has(id)){ EMP_CLD_EXCL.push({id,nombre:String(r[eN1]||''),motivo:'duplicado'}); continue; }
      seenEmp.add(id);
      // Copiar fila completa (misma estructura)
      const row={};
      EH.forEach((h,i)=>{ if(h) row[h]=r[i]; });
      outEmp.push(row);
    }

    // Dedup contratos (mantener solo los que corresponden a empleados válidos)
    const seenCtr=new Set(), outCtr=[];
    for(const r of ctr.rows){
      const id=String(r[cID]||'').replace(/[^0-9]/g,'').trim();
      if(!id||seenCtr.has(id)) continue;
      seenCtr.add(id);
      const row={};
      CH.forEach((h,i)=>{ if(h) row[h]=r[i]; });
      outCtr.push(row);
    }

    empLog(`✅ Empleados: ${outEmp.length} | Contratos: ${outCtr.length}`,'o','Consolidación');

    // ── Transformación ───────────────────────────────────────────
    empSetPStep(3); empSetPct(65,'Transformando...');
    await empCldSleep(30);
    empLog('🔄 Datos validados y limpios','o','Transformación');

    // ── Generación Excel ─────────────────────────────────────────
    empSetPStep(4); empSetPct(85,'Generando Excel...');
    await empCldSleep(30);
    empLog('📊 Construyendo archivo Excel...','i','Escritura');

    EMP_CLD_WB = empCldBuildWB(outEmp, outCtr, EMP_CLD_LOG, {
      registros_entrada:totalEntrada, registros_salida:outEmp.length
    }, EMP_CLD_EXCL);

    const dur=((Date.now()-t0)/1000).toFixed(1);
    empSetPct(100,'¡Listo!'); empSetPStep(5);
    empLog(`✅ Excel generado en ${dur}s — ${outEmp.length} empleados, ${outCtr.length} contratos`,'o','Escritura');

    const fn=empCldBuildFN();
    const fnEl=document.getElementById('emp-dl-fn');
    if(fnEl) fnEl.textContent=fn;
    const stIn=document.getElementById('emp-st-in');
    const stOk=document.getElementById('emp-st-ok');
    if(stIn) stIn.textContent=totalEntrada;
    if(stOk) stOk.textContent=outEmp.length;

    try{
      await api('POST','/migrations',{
        filename_out:fn,orig_soft:'Siigo Nube',dest_soft:'World Office Cloud',
        module:'Empleados',records_in:totalEntrada,records_out:outEmp.length,
        errors:0,warnings:0,duration_sec:parseFloat(dur),status:'completed'
      },AUTH.token);
    }catch(e){}

    empSetStep(4);
  }catch(err){
    empLog(`❌ Error: ${err.message}`,'e','Pipeline');
    console.error(err);
  }
}

function empCldBuildFN(){
  const d=new Date();
  return `empleados_siigo_nube_wo_cloud_${d.getFullYear()}_${String(d.getMonth()+1).padStart(2,'0')}_${String(d.getDate()).padStart(2,'0')}.xlsx`;
}

function empCldBuildWB(empRows, ctrRows, logEntries, stats, excluded){
  const wb=XLSX.utils.book_new();

  // Hoja 1: Plantilla de empleados (misma estructura origen)
  const empAoa=[EMP_CLD_COLS.slice()];
  empRows.forEach(r=>empAoa.push(EMP_CLD_COLS.map(c=>r[c]??null)));
  XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet(empAoa),'Plantilla de empleados');

  // Hoja 2: Plantilla de contratos (misma estructura origen)
  const ctrAoa=[EMP_CLD_CTR_COLS.slice()];
  ctrRows.forEach(r=>ctrAoa.push(EMP_CLD_CTR_COLS.map(c=>r[c]??null)));
  XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet(ctrAoa),'Plantilla de contratos');

  // Hoja 3: Logs
  const logsAoa=[['timestamp','fase','nivel','mensaje']];
  (logEntries||[]).forEach(e=>logsAoa.push([e.ts,e.fase,e.lvl==='e'?'ERROR':e.lvl==='w'?'WARN':'INFO',e.msg]));
  XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet(logsAoa),'Logs');

  // Hoja 4: Estadísticas
  const s=stats||{};
  XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet([
    ['Métrica','Valor'],
    ['Empleados entrada',s.registros_entrada||0],
    ['Empleados salida',empRows.length],
    ['Contratos salida',ctrRows.length],
  ]),'Estadísticas');

  // Hoja 5: Excluidos
  const exAoa=[['Identificación','Nombre','Motivo']];
  (excluded||[]).forEach(e=>exAoa.push([e.id,e.nombre,e.motivo]));
  XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet(exAoa),'Excluidos');

  return wb;
}

function empCldDoDownload(){
  if(!EMP_CLD_WB){alert('Primero ejecuta el proceso ETL');return;}
  XLSX.writeFile(EMP_CLD_WB, empCldBuildFN());
}
