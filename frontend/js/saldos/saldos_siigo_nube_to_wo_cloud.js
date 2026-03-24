// ══════════════════════════════════════════════════════════════════
// ETL: Saldos Iniciales Contables — Siigo Nube → World Office
// Basado en: dev.zip / saldos_contables_siigo_nube_wo
// Entrada : Balance de prueba por tercero (Siigo Nube)
//           Hoja datos: Sheet1, encabezados en fila 5
// Salida  : 40 columnas (Encab + Detalle)
// ══════════════════════════════════════════════════════════════════

// ── Estado ────────────────────────────────────────────────────────
let SAL_CLD_WB   = null;
const SAL_CLD_LOG  = [];
const SAL_CLD_EXCL = [];

// ── Columnas destino (Plantilla WO Cloud — hoja Contabilidad) ─────
const SAL_CLD_COLS = [
  'Cuenta *','Concepto','Tercero *','Débito *','Crédito *',
  'Centro costos','Fecha de Vencimiento *','Base Ret','% Ret','Vendedor ','Cuentas Originales'
];

// ── Prefijos de cuentas ───────────────────────────────────────────
// Naturaleza débito: 1(Activos) 5(Gastos) 6(Costos venta) 7(Costos prod)
// Naturaleza crédito: 2(Pasivos) 3(Patrimonio) 4(Ingresos)
// Sin tercero (agrupar): 11,12,14,16,18,19,24,29
const SAL_CLD_SIN_TERCERO = ['11','12','14','16','18','19','24','29'];
const SAL_CLD_NAT_DEBITO  = ['1','5','6','7'];
const SAL_CLD_NAT_CREDITO = ['2','3','4'];

// ── Helpers ───────────────────────────────────────────────────────
function salCldNorm(h){
  return String(h||'').trim().toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g,'');
}
function salCldLog(msg,lvl='i',fase=''){
  const ts=new Date().toISOString();
  SAL_CLD_LOG.push({ts,fase,lvl,msg});
  const panel=document.getElementById('sal-logp');
  if(!panel)return;
  const now=new Date().toLocaleTimeString('es-CO',{hour12:false});
  const css={i:'li',w:'lw',o:'lo',e:'le-e'}[lvl]||'li';
  panel.innerHTML+=`<div class="le"><span class="lt">${now}</span><span class="${css}">${msg}</span></div>`;
  panel.scrollTop=panel.scrollHeight;
}
function salCldSleep(ms){return new Promise(r=>setTimeout(r,ms));}

function salCldSetStep(n){
  for(let i=1;i<=4;i++){
    const wz=document.getElementById('sal-wt'+i);
    if(wz)wz.className='wz'+(i<n?' done':i===n?' on':'');
    const sec=document.getElementById('sal-s'+i);
    if(sec)sec.style.display=(i===n)?'block':'none';
  }
}
function salCldSetPStep(n){
  for(let i=0;i<=5;i++){
    const el=document.getElementById('sal-ps'+i);
    if(!el)continue;
    el.classList.toggle('act',i===n);
    el.classList.toggle('don',i<n);
  }
}
function salCldSetPct(pct,msg){
  const pb=document.getElementById('sal-pbar');
  const pp=document.getElementById('sal-ppct');
  const ph=document.getElementById('sal-pph');
  if(pb)pb.style.width=pct+'%';
  if(pp)pp.textContent=pct+'%';
  if(ph&&msg)ph.textContent=msg;
}

// ── Limpiar código de cuenta (quitar decimales) ───────────────────
function salCldLimpiarCuenta(v){
  if(v===null||v===undefined||v==='')return '';
  return String(v).split('.')[0].replace(/\D/g,'');
}

// ── Limpiar identificación ────────────────────────────────────────
function salCldLimpiarId(v){
  if(v===null||v===undefined||v==='')return '';
  return String(v).split('.')[0].replace(/\D/g,'');
}

// ── Determinar Débito/Crédito ─────────────────────────────────────
// Regla: saldo positivo → Débito, saldo negativo → Crédito
// (aplica para todos los prefijos — simplificado del dev.zip)
function salCldDebitoCred(cuenta, saldo){
  const s=parseFloat(saldo)||0;
  const abs=Math.abs(s);
  if(s>=0) return {deb:abs, cred:0};
  return {deb:0, cred:abs};
}

// ── ¿Cuenta sin tercero? ──────────────────────────────────────────
function salCldEsSinTercero(cuenta){
  return SAL_CLD_SIN_TERCERO.some(p=>String(cuenta).startsWith(p));
}

// ── Leer Excel ───────────────────────────────────────────────────
async function salCldReadFile(file){
  return new Promise((res,rej)=>{
    const reader=new FileReader();
    reader.onload=e=>{
      try{
        const wb=XLSX.read(new Uint8Array(e.target.result),{type:'array',raw:false,cellText:true});
        // Hoja Sheet1
        const wsName = wb.SheetNames.find(n=>n==='Sheet1')||wb.SheetNames[0];
        const ws=wb.Sheets[wsName];
        const all=XLSX.utils.sheet_to_json(ws,{header:1,defval:''});

        // Parámetros: leer NIT empresa de filas iniciales
        let nitEmpresa='', fechaCorte='', nombreEmpresa='';
        // Siigo pone datos de empresa en primeras filas
        for(let i=0;i<6;i++){
          const row=all[i]||[];
          const txt=row.join(' ').trim();
          if(/^\d{6,12}-?\d?$/.test(row[0])||/nit/i.test(txt)){
            nitEmpresa=String(row[0]||'').split('.')[0].replace(/\D/g,'');
          }
          if(row[0]&&String(row[0]).includes('/')){
            fechaCorte=String(row[0]).trim();
          }
          if(row[0]&&String(row[0]).length>3&&!nitEmpresa){
            nombreEmpresa=String(row[0]).trim();
          }
        }

        // Encabezados en fila 5 (índice 4)
        let hdrIdx=4;
        for(let i=0;i<Math.min(all.length,10);i++){
          const rn=all[i].map(v=>salCldNorm(v));
          if(rn.some(v=>v.includes('transaccional')||v.includes('codigo cuenta'))){
            hdrIdx=i; break;
          }
        }
        const hdrs=all[hdrIdx].map(v=>String(v||'').trim());
        const rows=all.slice(hdrIdx+1).filter(r=>r.some(v=>v!==''&&v!==null&&v!==undefined));

        res({hdrs,rows,nitEmpresa,fechaCorte,nombreEmpresa});
      }catch(err){rej(err);}
    };
    reader.readAsArrayBuffer(file);
  });
}

// ── ETL principal ─────────────────────────────────────────────────
async function startSalCldETL(){
  const file=S.files&&S.files['sal-maestro']?S.files['sal-maestro']:null;
  if(!file){alert('Carga el archivo Balance de prueba por tercero');return;}

  // Pedir parámetros al usuario antes de ejecutar
  salCldShowParamModal();
}

async function _startSalCldETLRun(params){
  const file=S.files&&S.files['sal-maestro']?S.files['sal-maestro']:null;
  if(!file)return;

  // Force step 3 visible immediately
  salCldSetStep(3);
  SAL_CLD_LOG.length=0; SAL_CLD_EXCL.length=0;
  const panel=document.getElementById('sal-logp');
  if(panel)panel.innerHTML='';
  salCldSetPStep(0); salCldSetPct(0,'Iniciando...');
  const t0=Date.now();
  await salCldSleep(50); // let DOM update

  try{
    // ── Paso 1: Lectura ──────────────────────────────────────────
    salCldSetPStep(1); salCldSetPct(10,'Leyendo archivo...');
    salCldLog('📂 Leyendo Balance de prueba por tercero...','i','Lectura');
    await salCldSleep(30);

    const data=await salCldReadFile(file);
    salCldLog(`   ${data.rows.length} filas encontradas`,'i','Lectura');

    const empresa  = params.empresa  || data.nombreEmpresa || 'EMPRESA';
    const nit      = params.nit      || data.nitEmpresa    || '';
    const fecha    = params.fecha    || data.fechaCorte    || new Date().toLocaleDateString('es-CO');

    salCldLog(`   Empresa: ${empresa} | NIT: ${nit} | Fecha: ${fecha}`,'i','Lectura');

    // ── Detectar columnas ────────────────────────────────────────
    const H=data.hdrs;
    const HN=H.map(salNorm);
    const fi=(terms)=>HN.findIndex(h=>terms.some(t=>h===salCldNorm(t)||h.includes(salCldNorm(t))));

    const cTrans  = fi(['transaccional']);
    const cCuenta = fi(['codigo cuenta contable','cuenta contable','codigo cuenta']);
    const cNomCta = fi(['nombre cuenta contable','nombre cuenta']);
    const cId     = fi(['identificacion','identificación','nit','tercero id']);
    const cNomTer = fi(['nombre tercero','nombre del tercero']);
    const cSaldo  = fi(['saldo final','saldo_final','saldo']);

    salCldLog(`   Cols → Trans[${cTrans}] Cuenta[${cCuenta}] ID[${cId}] Saldo[${cSaldo}]`,'i','Lectura');

    if(cCuenta<0||cSaldo<0){
      throw new Error('No se encontraron columnas requeridas: Código cuenta contable / Saldo final');
    }

    // ── Paso 2: Filtrar transaccionales ──────────────────────────
    salCldSetPStep(2); salCldSetPct(25,'Filtrando transaccionales...');
    salCldLog('🔗 Filtrando registros transaccionales...','i','Filtrado');
    await salCldSleep(30);

    let filas=data.rows;
    let totalOrigen=filas.length;

    // Filtrar Transaccional = 'Sí'
    if(cTrans>=0){
      filas=filas.filter(r=>{
        const v=salCldNorm(r[cTrans]);
        return v==='si'||v==='sí'||v==='yes'||v==='1'||v==='true';
      });
      salCldLog(`   Transaccionales: ${filas.length} de ${totalOrigen}`,'i','Filtrado');
    }

    // Filtrar saldo cero
    const antCero=filas.length;
    filas=filas.filter(r=>Math.abs(parseFloat(r[cSaldo])||0)>0.001);
    salCldLog(`   Saldos cero eliminados: ${antCero-filas.length}`,'w','Filtrado');

    // ── Paso 3: Marcar padres con auxiliares ─────────────────────
    salCldSetPStep(3); salCldSetPct(45,'Procesando cuentas...');
    salCldLog('🔧 Aplicando reglas de negocio...','i','Transformación');
    await salCldSleep(30);

    // Obtener set de cuentas con saldo
    const cuentasConSaldo=new Set(filas.map(r=>salCldLimpiarCuenta(r[cCuenta])));

    // Identificar cuentas padre (tienen auxiliares con 2 dígitos más)
    const padresConAuxiliares=new Set();
    cuentasConSaldo.forEach(c=>{
      if(c.length>=4){
        const padre=c.slice(0,-2);
        if(cuentasConSaldo.has(padre)) padresConAuxiliares.add(padre);
      }
    });
    if(padresConAuxiliares.size>0){
      filas=filas.filter(r=>!padresConAuxiliares.has(salCldLimpiarCuenta(r[cCuenta])));
      salCldLog(`   Cuentas padre excluidas (tienen auxiliares): ${padresConAuxiliares.size}`,'w','Transformación');
    }

    // ── Paso 4: Separar con/sin tercero y agrupar sin tercero ────
    const conTercero=[], sinTercero={};

    filas.forEach(r=>{
      const cuenta=salCldLimpiarCuenta(r[cCuenta]);
      const id    =salCldLimpiarId(cId>=0?r[cId]:'');
      const saldo =parseFloat(String(r[cSaldo]).replace(/[^0-9.\-]/g,''))||0;

      if(salCldEsSinTercero(cuenta)){
        // Agrupar por cuenta
        if(!sinTercero[cuenta]){
          sinTercero[cuenta]={cuenta,saldo:0,nombre:cNomCta>=0?String(r[cNomCta]||'').trim():''};
        }
        sinTercero[cuenta].saldo+=saldo;
      } else {
        conTercero.push({cuenta,id,saldo});
      }
    });

    salCldLog(`   Con tercero: ${conTercero.length}, Sin tercero (agrupadas): ${Object.keys(sinTercero).length}`,'i','Transformación');

    // ── Paso 5: Generar filas destino ────────────────────────────
    salCldSetPStep(4); salCldSetPct(70,'Generando registros destino...');
    await salCldSleep(30);

    const out=[];
    const nota=`SALDOS INICIALES A ${fecha}`;

    const encabBase={
      'Encab: Empresa':          empresa,
      'Encab: Tipo Documento':   'SI',
      'Encab: Prefijo':          '',
      'Encab: Documento Número': 1,
      'Encab: Fecha':            fecha,
      'Encab: Tercero Interno':  nit,
      'Encab: Tercero Externo':  nit,
      'Encab: Nota':             nota,
      'Encab: FormaPago':        'Saldos Iniciales',
      'Encab: Verificado':       '',
      'Encab: Anulado':          0,
      'Encab: Personalizado1':'','Encab: Personalizado2':'','Encab: Personalizado3':'',
      'Encab: Personalizado4':'','Encab: Personalizado5':'','Encab: Personalizado6':'',
      'Encab: Personalizado7':'','Encab: Personalizado8':'','Encab: Personalizado9':'',
      'Encab: Personalizado10':'','Encab: Personalizado11':'','Encab: Personalizado12':'',
      'Encab: Personalizado13':'','Encab: Personalizado14':'','Encab: Personalizado15':'',
    };

    // Registros con tercero
    conTercero.forEach(({cuenta,id,saldo})=>{
      const {deb,cred}=salCldDebitoCred(cuenta,saldo);
      out.push({
        'Cuenta *':                cuenta,
        'Concepto':                nota,
        'Tercero *':               id||nit,
        'Débito *':                deb,
        'Crédito *':               cred,
        'Centro costos':           '',
        'Fecha de Vencimiento *':  fecha,
        'Base Ret':                '',
        '% Ret':                   '',
        'Vendedor ':               '',
        'Cuentas Originales':      cuenta,
      });
    });

    // Cuentas sin tercero agrupadas
    Object.values(sinTercero).forEach(({cuenta,saldo})=>{
      if(Math.abs(saldo)<=0.001)return;
      const {deb,cred}=salCldDebitoCred(cuenta,saldo);
      out.push({
        ...encabBase,
        'Detalle: CuentaContable':     cuenta,
        'Detalle: Nota':               nota,
        'Detalle: TerceroExterno':     nit,
        'Detalle: Débito':             deb,
        'Detalle: Crédito':            cred,
        'Detalle: Vencimiento':        fecha,
        'Detalle: Vendedor':           '',
        'Detalle: Cheque':             '',
        'Detalle: Banco Cheque':       '',
        'Detalle: Centro Costos':      '',
        'Detalle: PorcentajeRetención':'',
        'Detalle: BaseRetención':      '',
        'Detalle: PagoRetención':      '',
        'Detalle: Tipo Base':          '',
        'Detalle: Código Centro Costos':'',
      });
    });

    // Estadísticas
    const sumDeb=out.reduce((a,r)=>a+(r['Detalle: Débito']||0),0);
    const sumCred=out.reduce((a,r)=>a+(r['Detalle: Crédito']||0),0);
    salCldLog(`✅ ${out.length} registros generados`,'o','Transformación');
    salCldLog(`   Débitos:  $${sumDeb.toLocaleString('es-CO',{minimumFractionDigits:2})}`,'i','Estadísticas');
    salCldLog(`   Créditos: $${sumCred.toLocaleString('es-CO',{minimumFractionDigits:2})}`,'i','Estadísticas');
    const dif=Math.abs(sumDeb-sumCred);
    if(dif>0.01) salCldLog(`   ⚠ Diferencia D-C: $${dif.toLocaleString('es-CO',{minimumFractionDigits:2})}`,'w','Estadísticas');
    else         salCldLog(`   ✓ Débitos = Créditos (cuadrado)`,'o','Estadísticas');

    // ── Paso 5: Excel ────────────────────────────────────────────
    salCldSetPStep(5); salCldSetPct(90,'Generando Excel...');
    salCldLog('📊 Construyendo archivo Excel...','i','Escritura');
    await salCldSleep(30);

    SAL_CLD_WB=salCldBuildWB(out,SAL_CLD_LOG,{
      registros_entrada:totalOrigen,
      registros_salida:out.length,
      suma_debitos:sumDeb,
      suma_creditos:sumCred,
      diferencia:dif
    });

    const dur=((Date.now()-t0)/1000).toFixed(1);
    salCldSetPct(100,'¡Listo!');
    salCldLog(`✅ Excel listo en ${dur}s`,'o','Escritura');

    const fn=salCldBuildFN();
    const fnEl=document.getElementById('sal-dl-fn'); if(fnEl)fnEl.textContent=fn;
    const stIn=document.getElementById('sal-st-in'); if(stIn)stIn.textContent=totalOrigen;
    const stOk=document.getElementById('sal-st-ok'); if(stOk)stOk.textContent=out.length;
    const stD=document.getElementById('sal-st-deb');
    if(stD)stD.textContent='$'+Math.round(sumDeb).toLocaleString('es-CO');

    try{
      await api('POST','/migrations',{
        filename_out:fn,orig_soft:'Siigo Nube',dest_soft:'World Office Cloud',
        module:'Saldos Iniciales Cloud',records_in:totalOrigen,records_out:out.length,
        errors:0,warnings:dif>0.01?1:0,duration_sec:parseFloat(dur),status:'completed'
      },AUTH.token);
    }catch(e){}

    salCldSetStep(4);

  }catch(err){
    salCldLog(`❌ Error: ${err.message}`,'e','Pipeline');
    salCldSetPct(0,'Error');
    console.error('SAL_CLD ETL Error:', err);
    alert('Error en migración: ' + err.message);
  }
}

// ── Modal de parámetros ───────────────────────────────────────────
function salCldShowParamModal(){
  let modal=document.getElementById('sal-cld-param-modal');
  if(!modal){
    modal=document.createElement('div');
    modal.id='sal-cld-param-modal';
    modal.style.cssText='position:fixed;inset:0;z-index:9500;overflow:auto;display:none';
    document.body.appendChild(modal);
  }

  modal.innerHTML=`
    <div style="position:fixed;inset:0;background:rgba(0,0,0,.5);z-index:0" onclick="salCldCloseParamModal()"></div>
    <div style="position:relative;z-index:1;min-height:100vh;display:flex;align-items:center;justify-content:center;padding:40px 20px">
      <div style="width:100%;max-width:480px;background:linear-gradient(135deg,#1a1060,#2d1b8e,#4a2db8);
        border:1px solid rgba(255,255,255,.15);border-radius:16px;padding:32px;
        box-shadow:0 20px 60px rgba(0,0,0,.5)">
        <div style="text-align:center;margin-bottom:24px">
          <img src="img/wo_cloud_logo.png" style="width:70px;height:auto;margin-bottom:12px;filter:drop-shadow(0 4px 16px rgba(100,80,255,.5))">
          <div style="font-size:20px;font-weight:700;color:#fff">Parámetros de Migración</div>
          <div style="font-size:12px;color:rgba(255,255,255,.5);margin-top:6px">Datos para el encabezado del documento</div>
        </div>
        <div style="margin-bottom:16px">
          <div style="font-size:12px;font-weight:600;color:rgba(255,255,255,.7);margin-bottom:6px;text-transform:uppercase;letter-spacing:.06em">Nombre Empresa *</div>
          <input id="sal-cld-p-empresa" type="text" placeholder="Nombre de la empresa"
            style="width:100%;background:rgba(255,255,255,.08);border:1px solid rgba(255,255,255,.2);
            border-radius:8px;padding:10px 14px;color:#fff;font-size:13px;outline:none;box-sizing:border-box">
        </div>
        <div style="margin-bottom:16px">
          <div style="font-size:12px;font-weight:600;color:rgba(255,255,255,.7);margin-bottom:6px;text-transform:uppercase;letter-spacing:.06em">NIT Empresa *</div>
          <input id="sal-cld-p-nit" type="text" placeholder="NIT sin dígito de verificación"
            style="width:100%;background:rgba(255,255,255,.08);border:1px solid rgba(255,255,255,.2);
            border-radius:8px;padding:10px 14px;color:#fff;font-size:13px;outline:none;box-sizing:border-box">
        </div>
        <div style="margin-bottom:24px">
          <div style="font-size:12px;font-weight:600;color:rgba(255,255,255,.7);margin-bottom:6px;text-transform:uppercase;letter-spacing:.06em">Fecha de Corte *</div>
          <input id="sal-cld-p-fecha" type="date"
            style="width:100%;background:rgba(255,255,255,.08);border:1px solid rgba(255,255,255,.2);
            border-radius:8px;padding:10px 14px;color:#fff;font-size:13px;outline:none;box-sizing:border-box">
        </div>
        <div id="sal-cld-p-err" style="color:#fca5a5;font-size:12px;margin-bottom:12px;display:none">⚠ Completa los campos requeridos</div>
        <div style="display:flex;gap:12px">
          <button id="sal-cld-btn-cancel" style="flex:1;padding:12px;
            background:rgba(255,255,255,.1);border:1px solid rgba(255,255,255,.2);
            border-radius:10px;color:#fff;font-size:14px;cursor:pointer">Cancelar</button>
          <button id="sal-cld-btn-confirm" style="flex:2;padding:12px;
            background:linear-gradient(135deg,#5b4fcf,#7c6ef0);
            border:1px solid rgba(255,255,255,.15);border-radius:10px;
            color:#fff;font-size:14px;font-weight:700;cursor:pointer;
            box-shadow:0 4px 16px rgba(91,79,207,.4)">✅ Ejecutar Migración</button>
        </div>
      </div>
    </div>`;

  // Set today as default date
  const today=new Date().toISOString().split('T')[0];
  modal.style.display='block';
  setTimeout(()=>{
    const df=document.getElementById('sal-cld-p-fecha');
    if(df)df.value=today;
    const btnCancel=document.getElementById('sal-cld-btn-cancel');
    const btnConfirm=document.getElementById('sal-cld-btn-confirm');
    if(btnCancel)btnCancel.onclick=salCldCloseParamModal;
    if(btnConfirm)btnConfirm.onclick=salCldConfirmParamModal;
  },50);
}

function salCldCloseParamModal(){
  const modal=document.getElementById('sal-cld-param-modal');
  if(modal)modal.style.display='none';
}

async function salCldConfirmParamModal(){
  const empresa=(document.getElementById('sal-cld-p-empresa')?.value||'').trim();
  const nit=(document.getElementById('sal-cld-p-nit')?.value||'').trim();
  const fechaRaw=(document.getElementById('sal-cld-p-fecha')?.value||'').trim();
  const errEl=document.getElementById('sal-cld-p-err');

  if(!empresa||!nit||!fechaRaw){
    if(errEl)errEl.style.display='block';
    return;
  }
  if(errEl)errEl.style.display='none';

  // Format date dd/MM/yyyy
  const [y,m,d]=fechaRaw.split('-');
  const fecha=`${d}/${m}/${y}`;

  salCldCloseParamModal();
  await _startSalCldETLRun({empresa,nit,fecha});
}

// ── Nombre archivo ────────────────────────────────────────────────
function salCldBuildFN(){
  const d=new Date();
  return `saldos_iniciales_siigo_nube_wo_cloud_${d.getFullYear()}_${String(d.getMonth()+1).padStart(2,'0')}_${String(d.getDate()).padStart(2,'0')}.xlsx`;
}

// ── Construir Workbook ────────────────────────────────────────────
function salCldBuildWB(rows,logEntries,stats){
  const wb=XLSX.utils.book_new();

  // Hoja 1: Saldos Iniciales
  const aoa=[SAL_CLD_COLS.slice()];
  rows.forEach(r=>aoa.push(SAL_CLD_COLS.map(c=>{
    const v=r[c]; return (v===undefined)?'':v??'';
  })));
  XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet(aoa),'Contabilidad');

  // Hoja 2: Estadísticas
  const s=stats||{};
  XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet([
    ['Métrica','Valor'],
    ['Registros entrada',s.registros_entrada||0],
    ['Registros salida',s.registros_salida||0],
    ['Suma Débitos',s.suma_debitos||0],
    ['Suma Créditos',s.suma_creditos||0],
    ['Diferencia D-C',s.diferencia||0],
  ]),'Estadísticas');

  // Hoja 3: Logs
  const logsAoa=[['Timestamp','Fase','Nivel','Mensaje']];
  (logEntries||[]).forEach(e=>logsAoa.push([e.ts,e.fase||'',
    e.lvl==='e'?'ERROR':e.lvl==='w'?'WARN':'INFO',e.msg]));
  XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet(logsAoa),'Logs');

  return wb;
}

// ── Descarga ──────────────────────────────────────────────────────
function salCldDoDownload(){
  if(!SAL_CLD_WB){alert('Primero ejecuta el proceso ETL');return;}
  XLSX.writeFile(SAL_CLD_WB,salCldBuildFN());
}

// ── Reset ─────────────────────────────────────────────────────────
function salCldReset(){
  SAL_CLD_WB=null; SAL_CLD_LOG.length=0; SAL_CLD_EXCL.length=0;
  if(typeof S!=='undefined'&&S.files) delete S.files['sal-maestro'];
  const sl=document.getElementById('sl-sal-m'); if(sl)sl.className='fslot';
  const nm=document.getElementById('nm-sal-m'); if(nm)nm.textContent='';
  const fi=document.getElementById('f-sal-m');  if(fi)fi.value='';
  const logp=document.getElementById('sal-logp'); if(logp)logp.innerHTML='';
  const pb=document.getElementById('sal-pbar');   if(pb)pb.style.width='0%';
  const pp=document.getElementById('sal-ppct');   if(pp)pp.textContent='0%';
  const ph=document.getElementById('sal-pph');    if(ph)ph.textContent='Iniciando...';
  ['sal-sorig','sal-sdest','sal-smod'].forEach(id=>{
    const el=document.getElementById(id); if(el)el.value='';
  });
  salCldSetStep(1);
}
