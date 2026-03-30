// ── ETL STATE ─────────────────────────────────────────────
const S={orig:'',dest:'',mod:'',files:{}};
let WB=null;
const LOGENTRIES=[];
const EXCLUDED_RECORDS=[];
const DEFAULTS_LOG=[];

['sorig','sdest','smod'].forEach(id=>document.getElementById(id).addEventListener('change',()=>{
  const o=document.getElementById('sorig').value,d=document.getElementById('sdest').value,m=document.getElementById('smod').value;
  ['sorig','sdest','smod'].forEach(x=>document.getElementById(x).classList.remove('err'));
  document.getElementById('a1').classList.remove('vis');
  const c=document.getElementById('compat');
  if(o&&d&&o!==d&&m){c.style.display='block';document.getElementById('ctxt').textContent=o+' -> '+d+' - '+m;}
  else c.style.display='none';

}));

function setStep(n){
  [1,2,3,4].forEach(i=>{
    const wz=document.getElementById('wt'+i);
    if(wz){wz.className='wz'+(i<n?' done':i===n?' on':'');}
    const sec=document.getElementById('s'+i);
    if(sec)sec.style.display=(i===n)?'':'none';
  });
  window.scrollTo({top:0,behavior:'smooth'});
}
function showA(id,m){const e=document.getElementById(id);if(e){e.classList.remove('hide');const em=document.getElementById(id+'m');if(em)em.textContent=m}}
function hideA(id){const e=document.getElementById(id);if(e)e.classList.add('hide')}

function goFiles(){
  const o=document.getElementById('sorig').value,d=document.getElementById('sdest').value,m=document.getElementById('smod').value;
  if(!o){document.getElementById('sorig').classList.add('err');showA('a1','Selecciona software de origen.');return}
  if(!d){document.getElementById('sdest').classList.add('err');showA('a1','Selecciona software de destino.');return}
  if(o===d){['sorig','sdest'].forEach(x=>document.getElementById(x).classList.add('err'));showA('a1','Origen y destino no pueden ser iguales.');return}
  if(!m){document.getElementById('smod').classList.add('err');showA('a1','Selecciona modulo.');return}
  S.orig=o;S.dest=d;S.mod=m;
  document.getElementById('fsub').textContent=o+' -> '+d+' - Modulo '+m;
  setStep(2);
}

function onFile(input,slotId,nameId,key){
  if(!input.files||!input.files.length)return;
  const f=input.files[0];
  S.files[key]=f;
  document.getElementById(slotId).className='fslot ok';
  document.getElementById(nameId).textContent=f.name;
  hideA('a2');
}

function addLog(msg,lvl,fase){
  const p=document.getElementById('logp');
  const ts=new Date().toISOString();
  const td=ts.substring(11,19);
  const cls=lvl==='w'?'lw':lvl==='o'?'lo':lvl==='e'?'le-e':'li';
  const d=document.createElement('div');d.className='le';
  d.innerHTML='<span class="lt">'+td+'</span><span class="'+cls+' le-msg">'+msg+'</span>';
  p.appendChild(d);p.scrollTop=p.scrollHeight;
  LOGENTRIES.push([ts,fase||null,lvl==='e'?'ERROR':lvl==='w'?'WARN':lvl==='o'?'OK':'INFO',msg,null,null]);
}
function setPStep(id,st){
  // Handle both: setPStep('ps0','ps act') and setPStep(2) numeric form
  if(typeof id==='number'){
    for(let i=0;i<=5;i++){
      const el=document.getElementById('ps'+i);
      if(!el)continue;
      el.classList.toggle('act',i===id);
      el.classList.toggle('don',i<id);
    }
  } else {
    const el=document.getElementById(id);
    if(el)el.className='ps '+st;
  }
}
function setPct(pct,ph){
  const pb=document.getElementById('pbar');
  const pph=document.getElementById('pph');
  const ppct=document.getElementById('ppct');
  if(pb)pb.style.width=pct+'%';
  if(pph)pph.textContent=ph||'';
  if(ppct)ppct.textContent=pct+'%';
}
const sleep=ms=>new Promise(r=>setTimeout(r,ms));

function readMaestro(file){
  return new Promise((res,rej)=>{
    const fr=new FileReader();
    fr.onload=e=>{
      try{
        const wb=XLSX.read(new Uint8Array(e.target.result),{type:'array',cellText:true,raw:false,sheetStubs:true});
        const ws=wb.Sheets[wb.SheetNames[0]];

        // ── PROBLEMA CONOCIDO CON SIIGO: el archivo declara !ref=A1:A380
        // aunque tiene 45 columnas reales. Calculamos el rango real
        // escaneando todas las claves del objeto ws.
        let maxR=0, maxC=0;
        Object.keys(ws).forEach(addr=>{
          if(addr.startsWith('!')) return;
          const dec=XLSX.utils.decode_cell(addr);
          if(dec.r>maxR) maxR=dec.r;
          if(dec.c>maxC) maxC=dec.c;
        });
        const nCols=maxC+1;
        const nRows=maxR+1;

        // Función robusta para leer celda
        function getCell(r,c){
          const addr=XLSX.utils.encode_cell({r,c});
          const cell=ws[addr];
          if(!cell) return '';
          if(cell.w!==undefined&&cell.w!==null&&String(cell.w).trim()!=='') return String(cell.w).trim();
          if(cell.v!==undefined&&cell.v!==null) return String(cell.v).trim();
          return '';
        }

        // Buscar fila de headers en las primeras 20 filas, escaneando todas las columnas reales
        let hRow=-1;
        for(let r=0;r<Math.min(nRows,20);r++){
          for(let c=0;c<nCols;c++){
            const v=getCell(r,c).toLowerCase().trim();
            if(v==='nombre tercero'||v==='identificación'||v==='identificacion'){
              hRow=r; break;
            }
          }
          if(hRow>=0) break;
        }
        if(hRow<0){rej(new Error('No se encontró la fila de encabezados (máximo 20 filas revisadas). Verifica que el archivo sea la exportación Búsqueda de Terceros de Siigo.'));return}

        // Leer headers
        const hdrs=[];
        for(let c=0;c<nCols;c++) hdrs.push(getCell(hRow,c));

        // Leer datos
        const data=[];
        for(let r=hRow+1;r<nRows;r++){
          let hasData=false;
          for(let c=0;c<nCols;c++){if(getCell(r,c)){hasData=true;break}}
          if(!hasData) continue;
          const o={};
          hdrs.forEach((h,c)=>{ o[h]=getCell(r,c); });
          data.push(o);
        }
        res({hdrs,data});
      }catch(ex){rej(ex)}
    };
    fr.onerror=rej;
    fr.readAsArrayBuffer(file);
  });
}

function readCSV(file){
  return new Promise((res,rej)=>{
    const fr=new FileReader();
    fr.onload=e=>{
      try{
        const lines=e.target.result.split(/\r?\n/);
        let hRow=-1;
        for(let i=0;i<Math.min(lines.length,10);i++){
          if(lines[i].toUpperCase().includes('IDENTIFICACI')){hRow=i;break}
        }
        if(hRow<0){res({hdrs:[],data:[]});return}
        const hdrs=parseCSVLine(lines[hRow]);
        const data=[];
        for(let i=hRow+1;i<lines.length;i++){
          const l=lines[i].trim();if(!l)continue;
          const vals=parseCSVLine(l);if(vals.length<2)continue;
          const o={};hdrs.forEach((h,j)=>o[h.trim()]=(vals[j]||'').trim());
          data.push(o);
        }
        res({hdrs,data});
      }catch(ex){rej(ex)}
    };
    fr.onerror=rej;
    fr.readAsText(file,'latin1');
  });
}
function parseCSVLine(l){
  const r=[];let c='',inQ=false;
  for(let i=0;i<l.length;i++){
    if(l[i]==='"'){inQ=!inQ}
    else if(l[i]===';'&&!inQ){r.push(c);c='';}
    else c+=l[i];
  }
  r.push(c);return r;
}
function normId(x){
  const s=String(x??'').replace(/[,\s]/g,'').trim();
  if(!s||s==='nan'||s==='undefined')return '';
  const n=parseFloat(s);
  return(!isNaN(n)&&isFinite(n)&&String(s).length<20)?String(Math.round(n)):s;
}
const TIPO_ID_MAP={'cedula de ciudadania':'Cedula de ciudadania','cedula de ciudadanía':'Cédula de ciudadanía','nit':'NIT','tarjeta de identidad':'Tarjeta de Identidad','cedula de extranjeria':'Cedula de Extranjeria','cédula de extranjería':'Cédula de Extranjería','permiso proteccion temporal ppt':'Permiso Especial de Permanencia','permiso especial de permanencia':'Permiso Especial de Permanencia','pasaporte':'Pasaporte','registro civil':'Registro Civil','tarjeta de extranjeria':'Tarjeta de Extranjeria'};
function mapTipoId(raw){
  const k=String(raw||'').toLowerCase().trim();
  return TIPO_ID_MAP[k]||String(raw||'');
}
function isNIT(t){return String(t||'').toLowerCase().includes('nit')}
// ── Separación de nombres (dev.zip AINameSplitter) ───────────────
// Reglas:
//   NIT/empresa (SAS,LTDA,CORP,INC) → razón social completa en p1
//   Conectores (DE,DEL,LA,LAS,LOS,Y,E,I) forman token propio con la palabra siguiente
//   1 token  → p1
//   2 tokens → p1, a1
//   3 tokens → si token[1] es nombre conocido → p1,p2,a1 / si no → p1,a1,a2
//   4+ tokens → p1, p2, a1, a2

const TERC_NOMBRES_SET = new Set([
  'ALEXANDER','ALEJANDRO','ALEXIS','ANDRES','ANGEL','ANTONIO','ARTURO',
  'CAMILO','CARLOS','CRISTIAN','CRISTINA','DANIEL','DAVID','DIEGO',
  'EDGAR','EDUARDO','ELENA','ELIANA','EMILIO','FABIAN','FELIPE','FERNANDO',
  'FRANCISCO','FREDDY','GABRIEL','GERMAN','GLORIA','GUSTAVO','HAROLD',
  'HECTOR','HERNAN','HUGO','IVAN','JAVIER','JEFFERSON','JENNIFER','JESSICA',
  'JHON','JORGE','JOSE','JUAN','JULIAN','JULIO','KAREN','LAURA','LEIDY',
  'LEONARDO','LILIANA','LINA','LUIS','LUZ','MANUEL','MARCELA','MARIA',
  'MARIO','MARTHA','MAURICIO','MIGUEL','NATALIA','NICOLAS','OSCAR','PABLO',
  'PAOLA','PATRICIA','PEDRO','RAFAEL','RAUL','RICARDO','ROBERTO','RODRIGO',
  'ROSA','RUBEN','SAMUEL','SANDRA','SANTIAGO','SERGIO','SILVIA','SOFIA',
  'STEPHANIE','TATIANA','VALENTINA','VICTOR','WILLIAM','WILSON','XIOMARA',
  'YESENIA','YOLANDA','ZULMA','ALBA','BEATRIZ','BRAYAN','BRYAN','CAROLINA',
  'CESAR','CLAUDIA','DIANA','DUVAN','ELISA','FELIX','GIOVANNY',
  'ISABELLA','JACKELINE','JAIME','JENNY','JHONATAN','JOHANA','JONATHAN',
  'KATHERINE','KELLY','LEANDRO','LORENA','LUISA','MANUELA','MARIANA',
  'MELISSA','MICHAEL','MILLER','NELSON','OLGA','OMAR','ORLANDO',
  'RUBIELA','SEBASTIAN','VANESSA','VERONICA','VIVIANA','YAMILE',
  'YEISON','YENNY','ASTRID','BLANCA','CONSUELO','DARIO','ELIZABETH',
  'ERNESTO','ESPERANZA','ESTEBAN','EUGENIO','FRANCO','FREDY','GIOVANNI',
  'GLADYS','GONZALO','IGNACIO','JAIRO','LISETH','LISSETTE','LUCRECIA',
  'MARGARITA','MARINA','MARISOL','MILTON','MIRIAM','MONICA','NANCY',
  'NIDIA','NORMA','NUBIA','PIEDAD','PILAR','ROMARIO','SONIA','SUSANA',
  'TERESA','URSULA','VIVIAN','YURI','ZAIDA','JHONY','LEIDY','NELLY',
  'ALBA','AMPARO','BERTHA','CECILIA','ESPERANZA','FLOR','GRACIELA',
  'INÉS','INES','LUZ','MAGDALENA','MERCEDES','NOHORA','RUBY','SOCORRO'
,
  'MILENA','ADRIANA','ALEJANDRA','ALICIA','AMPARO','BERTHA','BIBIANA','BRIGITTE','CONSUELO','DEISY','DOLORES','EMILIA','FABIOLA','FLOR','FRANCY','GINA','HEIDY','HELENA','INGRID','IRENE','ISABEL','JANA','JAZMIN','JOHANA','JOHANNA','JOSEFINA','JUANA','LEILA','LEONOR','LINEY','LORENA','LORENZA','LUCIA','LUISA','LYDA','MABEL','MAGNOLIA','MAIERLY','MANUELA','MARGARITA','MARIANA','MARIELA','MARLENY','MARLEN','MAYERLY','MELISSA','MIREYA','NATALY','NELLY','NIDIA','NUBIA','OFELIA','OLGA','ORIANNA','ORFA','PAULA','PILAR','PIEDAD','ROBERTA','ROSALBA','ROSANA','ROSARIO','ROSAURA','RUBY','RUTH','SARAH','SILVIA','SOLEDAD','SONIA','STEFANIA','SUSANA','TERESA','URSULA','VANESSA','VERONICA','VIVIAN','WENDY','XIOMARA','YAMILE','YENNY','YESENIA','YOLANDA','YULIANA','YURANI','YURI','ZAIDA','ZULMA','VIOLETA','YARIT','LISETH','ANN','DAYANA','DIANA','DIXIE','FLAVIA','HELLEN','ILSE','LISSETH','LILIANA','MARCELA','MARINELA','MARISOL','MIRIAM','MONICA','NANCY','NORMA','RUBIELA','ROSEMARY','SOCORRA'
,
  'ABEL','AGUSTINA','ALBERTO','ALEXANDRA','ALFONSO','AMANDA','ANDREA','ANGEE','ANGELA','ANGELICA','ANNIE','ARMANDO','AUGUSTO','AURELIO','BRAIAN','BRANDON','BRIALLAN','CAMILA','CARMEN','CAROL','CAROLYN','CATALINA','CRISTIN','DANIELA','DANILO','DANNA','DIDIER','DUVER','EDISON','EFREN','ELVIRA','ENRIQUE','ERICK','ERIKA','EUGENIA','EZEQUIEL','FERNANDA','FRANKLIN','GABRIELA','GERSON','GILBERTO','GINNA','GINO','HENRY','HERNANDO','HUBER','HUMBERTO','IVETH','JACKSON','JADER','JAIR','JARY','JEAN','JEFERSON','JEIMI','JEIMY','JEISON','JEISSON','JESUS','JHAN','JHOAN','JHONNY','JOAN','JOEL','JOHAN','JOHN','JONATAN','JORDAN','JULY','KELLIN','KEVIN','LEIDER','LUCAS','LUNA','MAICOL','MARBY','MARCO','MARLON','MARTIN','MATEO','MIRLEN','MISAEL','NELSI','NICK','NICOLE','NIVIA','OCTAVIO','RICHARD','ROCIO','ROGER','ROLANDO','SILENA','STEVEN','STIVEN','TANIA','VALERIA','VICENTE','VICTORIA','XIMENA','YAIR','YANETH','YEFREN','YEIMI','YENI','YESID','YESSICA','YINNA','YISETTE','YUBER','YURANY'
]);

function tercSplitName(nombre, tipoId){
  const n=String(nombre||'').trim().toUpperCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g,'');
  const vacio={p1:'',p2:'',a1:'',a2:''};
  if(!n)return vacio;

  // Empresa: NIT o sufijo empresarial (word boundary)
  const esNIT=tipoId&&/nit/i.test(String(tipoId));
  const tieneSufijo=/\b(SAS|LTDA|LIMITADA|CORP|INC)\b/.test(n);
  if(esNIT||tieneSufijo)return{p1:String(nombre).trim(),p2:'',a1:'',a2:''};

  // Tokenizar y agrupar conectores hacia adelante
  const CONN=new Set(['DE','DEL','LA','LAS','LOS','Y','E','I','VAN','VON']);
  const raw=n.split(/\s+/).filter(Boolean);
  const parts=[];
  let i=0;
  while(i<raw.length){
    if(CONN.has(raw[i])){
      // Conector forma token propio con la(s) siguiente(s) palabras
      let grupo=raw[i]; i++;
      while(i<raw.length&&CONN.has(raw[i])){ grupo+=' '+raw[i]; i++; }
      if(i<raw.length){ grupo+=' '+raw[i]; i++; }
      parts.push(grupo);
    }else{
      parts.push(raw[i]); i++;
    }
  }

  const n2=parts.length;
  if(n2===0)return vacio;
  if(n2===1)return{p1:parts[0],p2:'',a1:'',a2:''};
  if(n2===2)return{p1:parts[0],p2:'',a1:parts[1],a2:''};
  if(n2===3){
    // Token del medio: si es nombre conocido → p1,p2,a1  / si no → p1,a1,a2
    const midEsNombre=TERC_NOMBRES_SET.has(parts[1]);
    if(midEsNombre)return{p1:parts[0],p2:parts[1],a1:parts[2],a2:''};
    return{p1:parts[0],p2:'',a1:parts[1],a2:parts[2]};
  }
  return{p1:parts[0],p2:parts[1],a1:parts[2],a2:parts.slice(3).join(' ')};
}

function tercSplitName(nombre, tipoId){
  const n=String(nombre||'').trim().toUpperCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g,'');
  const vacio={p1:'',p2:'',a1:'',a2:''};
  if(!n)return vacio;

  // Empresa: NIT o sufijo empresarial (word boundary)
  const esNIT=tipoId&&/nit/i.test(String(tipoId));
  const tieneSufijo=/\b(SAS|LTDA|LIMITADA|CORP|INC)\b/.test(n);
  if(esNIT||tieneSufijo)return{p1:String(nombre).trim(),p2:'',a1:'',a2:''};

  // Tokenizar
  const CONN=new Set(['DE','DEL','LA','LAS','LOS','Y','E','I','VAN','VON']);
  const raw=n.split(/\s+/).filter(Boolean);

  // Agrupar conectores con palabras adyacentes
  const parts=[];
  let i=0;
  while(i<raw.length){
    const tok=raw[i];
    if(CONN.has(tok)&&parts.length>0&&i+1<raw.length){
      const prev=parts.pop();
      const nxt=raw[i+1];
      parts.push(prev+' '+tok+' '+nxt);
      i+=2;
    }else{
      parts.push(tok);
      i++;
    }
  }

  const n2=parts.length;
  if(n2===0)return vacio;
  if(n2===1)return{p1:parts[0],p2:'',a1:'',a2:''};
  if(n2===2)return{p1:parts[0],p2:'',a1:parts[1],a2:''};

  if(n2===3){
    // Si el token del medio es un nombre conocido → p1, p2, a1
    // Si no → p1, a1, a2 (colombiano estándar: 1 nombre + 2 apellidos)
    const midEsNombre=TERC_NOMBRES.has(parts[1]);
    if(midEsNombre)return{p1:parts[0],p2:parts[1],a1:parts[2],a2:''};
    return{p1:parts[0],p2:'',a1:parts[1],a2:parts[2]};
  }

  // 4+ tokens → p1, p2, a1, a2
  return{p1:parts[0],p2:parts[1],a1:parts[2],a2:parts.slice(3).join(' ')};
}

function tercSplitName(nombre, tipoId){
  const n=String(nombre||'').trim().toUpperCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g,'');
  const vacio={p1:'',p2:'',a1:'',a2:''};
  if(!n)return vacio;
  // Empresa por NIT
  const esNIT=tipoId&&/nit/i.test(String(tipoId));
  // Empresa por sufijo (word boundary - evita falsos positivos como CIA en GARCIA)
  const tieneSufijo=/\b(SAS|LTDA|LIMITADA|CORP|INC)\b/.test(n);
  if(esNIT||tieneSufijo)return{p1:String(nombre).trim(),p2:'',a1:'',a2:''};
  // Tokenizar y agrupar conectores
  const CONN=new Set(['DE','DEL','LA','LAS','LOS','Y','E','I','VAN','VON']);
  const raw=n.split(/\s+/).filter(Boolean);
  const parts=[];
  let i=0;
  while(i<raw.length){
    const tok=raw[i];
    if(CONN.has(tok)&&parts.length>0&&i+1<raw.length){
      const prev=parts.pop();
      const nxt=raw[i+1];
      parts.push(prev+' '+tok+' '+nxt);
      i+=2;
    }else{
      parts.push(tok);
      i++;
    }
  }
  const n2=parts.length;
  if(n2===0)return vacio;
  if(n2===1)return{p1:parts[0],p2:'',a1:'',a2:''};
  if(n2===2)return{p1:parts[0],p2:'',a1:parts[1],a2:''};
  if(n2===3)return{p1:parts[0],p2:'',a1:parts[1],a2:parts[2]};
  return{p1:parts[0],p2:parts[1],a1:parts[2],a2:parts.slice(3).join(' ')};
}
function tipoContrib(t,r){
  if(isNIT(t))return 'Persona Juridica';
  const rv=String(r||'').toLowerCase();
  if(rv.includes('responsable')&&!rv.includes('no responsable'))return 'Persona Natural Responsable del IVA';
  return 'Persona Natural No Responsable del IVA';
}
const CITY_MAP={'bogotá':'Bogotá, D.C.','bogota':'Bogotá, D.C.','bogota d.c.':'Bogotá, D.C.','bogotá d.c.':'Bogotá, D.C.'};
function normCiudad(c){const k=String(c||'').trim().toLowerCase();if(!k||k==='nan'||k==='-')return 'Bogotá, D.C.';return CITY_MAP[k]||String(c||'').trim()||'Bogotá, D.C.';}
function normTel(raw){
  const s=String(raw??'').trim();
  if(!s||s==='nan'||s==='-'||s==='--'||s==='---')return '3000000000';
  // Remove leading/trailing dashes and clean up
  const d=s.replace(/^-+|-+$/g,'').replace(/[^0-9\-\s\+\(\)]/g,'').trim();
  if(!d)return '';
  // Remove all non-digit for validation
  const digits=d.replace(/\D/g,'');
  if(!digits||/^0+$/.test(digits))return '';
  return digits; // Return clean digits as string
}
const ADDR_REP=[[/\bcarrera\b/gi,'CR'],[/\bcra\.?\s*/gi,'CR '],[/\bcalle\b/gi,'CL'],[/\bcll\.?\s*/gi,'CL '],[/\bcl\.?\s*/gi,'CL '],[/\btransversal\b/gi,'TV'],[/\bdiagonal\b/gi,'DG'],[/\bavenida\b/gi,'AV'],[/\bav\.?\s*/gi,'AV '],[/\bnumero\b/gi,''],[/\bnum\.?\s*/gi,''],[/\bno\.?\s*/gi,' '],[/#/g,' '],[/-/g,' '],[/\./g,' '],[/,/g,' '],[/\s{2,}/g,' ']];
function normAddr(raw){
  const s=String(raw||'').trim();
  if(!s||s.toLowerCase()==='nan'||/^no aplica$/i.test(s)||s==='0'||s.length<3)return 'DIRECCION NO INFORMADA';
  let r=s;
  ADDR_REP.forEach(([p,rep])=>r=r.replace(p,rep));
  r=r.trim().replace(/\s{2,}/g,' ').toUpperCase();
  return r.replace(/\s/g,'')?r:'DIRECCION NO INFORMADA';
}
function mapActivo(e){return /^activo$/i.test(String(e||'').trim())?'Si':'No'}

async function startETL(){
  hideA('a2');
  if(!S.files.maestro){document.getElementById('sl-m').className='fslot bad';showA('a2','El archivo Busqueda de Terceros es obligatorio.');return}
  LOGENTRIES.length=0;
  EXCLUDED_RECORDS.length=0;
  DEFAULTS_LOG.length=0;
  setStep(3);
  const t0=Date.now();
  try{
    setPStep('ps0','ps act');setPct(5,'Cargando configuracion...');
    addLog('INICIANDO PIPELINE ETL','o',null);
    addLog('   '+S.orig+' -> '+S.dest+' - Modulo '+S.mod,'i',null);
    addLog('Plantilla: plantilla-terceros_woCloud.xlsx - 2 hojas','i','Configuracion');
    await sleep(250);setPStep('ps0','ps don');

    setPStep('ps1','ps act');setPct(15,'Leyendo archivos...');
    addLog('Iniciando lectura de archivos','i','Lectura de archivos');
    const maestro=await readMaestro(S.files.maestro);
    addLog('   Maestro: '+maestro.data.length+' filas, '+maestro.hdrs.filter(Boolean).length+' columnas','i','Lectura de archivos');
    addLog('   Columnas detectadas: Nombre='+maestro.hdrs.find(h=>/nombre tercero/i.test(h))+' | ID='+maestro.hdrs.find(h=>/^identificaci/i.test(h.trim()))+' | Dir='+maestro.hdrs.find(h=>/^direcci/i.test(h.trim()))+' | Tel='+maestro.hdrs.find(h=>/^tel/i.test(h.trim())),'i','Lectura de archivos');
    let cliRows=[],proRows=[];
    const totalEntrada=maestro.data.length;
    addLog('Lectura: '+totalEntrada+' registros totales','o','Lectura de archivos');
    // Empleados opcional
    let cloudEmpIds=new Set();
    if(S.files && S.files['empleados']){
      try{
        const empData=await escReadEmpleados(S.files['empleados']);
        cloudEmpIds=empData.ids;
        addLog('   Empleados: '+cloudEmpIds.size+' cédulas cargadas','i','Lectura de archivos');
      }catch(e){addLog('   Empleados: error leyendo archivo','w','Lectura de archivos');}
    }
    await sleep(250);setPStep('ps1','ps don');

    const H=maestro.hdrs;
    const cN=H.find(h=>/nombre tercero/i.test(h))||'Nombre tercero';
    const cTI=H.find(h=>/tipo de identificaci/i.test(h))||'Tipo de identificacion';
    const cID=H.find(h=>/^identificaci/i.test(h.trim()))||'Identificacion';
    const cReg=H.find(h=>/regimen/i.test(h))||'Tipo de regimen IVA';
    const cDir=H.find(h=>/^direcci/i.test(h.trim()))||'Direccion';
    const cCiu=H.find(h=>/^ciudad$/i.test(h.trim()))||'Ciudad';
    const cTel=H.find(h=>/^tel/i.test(h.trim()))||'Telefono';
    const cEst=H.find(h=>/^estado$/i.test(h.trim()))||'Estado';
    const cCliente=H.find(h=>/^cliente$/i.test(h.trim()))||'Cliente';
    const cProveedor=H.find(h=>/^proveedor$/i.test(h.trim()))||'Proveedor';
    const cEmail=H.find(h=>/correo electr/i.test(h))||null;

    setPStep('ps2','ps act');setPct(35,'Consolidando...');
    addLog('Iniciando consolidacion','i','Consolidacion de datos');
    const emailMap={};
    [...cliRows,...proRows].forEach(r=>{
      const kk=Object.keys(r);
      const idKey=kk.find(k=>/^identificaci/i.test(k.trim()))||kk[0];
      const emailKey=kk.find(k=>/correo/i.test(k)&&!/contacto/i.test(k));
      const principalKey=kk.find(k=>/principal/i.test(k));
      const id=normId(r[idKey]||'');
      if(!id)return;
      const esPrincipal=principalKey?String(r[principalKey]||'').toLowerCase().startsWith('s'):true;
      if(esPrincipal&&emailKey){const em=String(r[emailKey]||'').trim();if(em&&!emailMap[id])emailMap[id]=em;}
    });
    const filtered=[],seen=new Set(),nitExcluidos=[];
    maestro.data.forEach(r=>{
      const id=normId(r[cID]);
      if(!id||seen.has(id))return;
      if(NITS_EXCLUIR.has(id)){nitExcluidos.push({id,nombre:String(r[cN]||'')});return;}
      seen.add(id);filtered.push({id,r});
    });
    addLog('Consolidacion: '+totalEntrada+' -> '+filtered.length+' registros unicos','o','Consolidacion de datos');
    if(nitExcluidos.length>0) addLog('   '+nitExcluidos.length+' NITs excluidos (lista predefinida)','w','Consolidacion de datos');
    addLog('   Emails enriquecidos: '+Object.keys(emailMap).length,'i','Consolidacion de datos');
    await sleep(250);setPStep('ps2','ps don');

    setPStep('ps3','ps act');setPct(58,'Transformando...');
    addLog('Iniciando transformacion','i','Transformacion de datos');
    await sleep(150);
    const out=[];let warns=0,errCount=0;
    filtered.forEach(({id,r})=>{
      const nombre=String(r[cN]||id).trim();
      const tipoRaw=String(r[cTI]||'Cedula de ciudadania').trim();
      const tipoId=mapTipoId(tipoRaw)||'Cedula de ciudadania';
      const juridica=isNIT(tipoId);
      const regimen=String(r[cReg]||'No responsable de IVA');
      const estado=mapActivo(String(r[cEst]||'Activo'));
      const _rawTel=String(r[cTel]??'').trim();
      const _rawDir=String(r[cDir]||'').trim();
      const _rawCiu=String(r[cCiu]||'').trim();
      const telefono=normTel(_rawTel);
      const dir=normAddr(_rawDir);
      const ciudad=normCiudad(_rawCiu);
      const _defaults=[];
      if(!_rawCiu||_rawCiu==='nan'||_rawCiu==='-') _defaults.push({campo:'Ciudad Identificación / Ciudad Dirección',valor_aplicado:'Bogotá, D.C.',motivo:'Campo vacío en origen'});
      if(!_rawDir||_rawDir==='nan'||_rawDir.length<3) _defaults.push({campo:'Dirección',valor_aplicado:'DIRECCION NO INFORMADA',motivo:'Campo vacío en origen'});
      if(!_rawTel||_rawTel==='nan'||_rawTel==='-') _defaults.push({campo:'Teléfonos',valor_aplicado:'3000000000',motivo:'Campo vacío en origen'});
      const email=cEmail?String(r[cEmail]||'').trim():(emailMap[id]||'');
      let p1='',p2='',a1='',a2='';
      if(juridica){p1=nombre;}
      else{const sn=tercSplitName(nombre,tipoId);p1=sn.p1;p2=sn.p2;a1=sn.a1;a2=sn.a2;if(!a1)warns++;}
      const idNum=(!isNaN(id)&&id!=='')?Number(id):id;
      out.push({
        'Tipo Identificacion *':tipoId,
        'Identificacion *':idNum,
        'Ciudad Identificacion*':ciudad,
        'Primer Nombre o Razon Social*':p1,
        'Segundo Nombre':p2||null,
        'Primer Apellido *':a1||null,
        'Segundo Apellido':a2||null,
        'Tipo Tercero *':(()=>{
          const esC=String(r[cCliente]||'').trim().toLowerCase().startsWith('s');
          const esP=String(r[cProveedor]||'').trim().toLowerCase().startsWith('s');
          if(esC&&esP) return 'Cliente,Proveedor';
          if(esC)      return 'Cliente';
          if(esP)      return 'Proveedor';
          return 'Cliente,Proveedor'; // ambos No → defecto WO
        })(),
        'Codigo':null,
        'Activo':estado,
        'Actividad Economica':null,
        'Tipo Contribuyente *':tipoContrib(tipoId,regimen),
        'Clasi. Administrador Impuesto *':'Normal',
        'Excepcion Impuesto':null,
        'Tarifa Reteica Compras':9.66,
        'Aplica Reteica Ventas':null,
        'Maneja Cupo Credito':null,
        'Vendedor':null,'Lista Precios':null,'Forma Pago':null,
        'Plazo Dias':0,'Porcentaje Descuento':null,
        'Tipo Direccion *':juridica?'Empresa/Oficina':'Casa',
        'Nombre Direccion *':'Principal',
        'Direccion *':dir,
        'Telefonos *':telefono,
        'Email *':email||null,
        'Ciudad Direccion *':ciudad,
        'Zona':null,'Barrio':null
      })
      if(_defaults.length>0) _defaults.forEach(d=>DEFAULTS_LOG.push({id:idNum,nombre:p1,campo:d.campo,valor_aplicado:d.valor_aplicado,motivo:d.motivo}));;
    });
    addLog('Transformacion: '+out.length+' registros, 30 campos','o','Transformacion de datos');
    if(warns>0)addLog('   '+warns+' personas naturales sin apellido','w','Transformacion de datos');
    await sleep(250);setPStep('ps3','ps don');

    setPStep('ps4','ps act');setPct(74,'Validando...');
    addLog('Iniciando validacion','i','Validacion de datos');
    out.forEach(r=>{if(!r['Tipo Identificacion *']||!r['Identificacion *']||!r['Primer Nombre o Razon Social*'])errCount++;});
    addLog('Validacion: '+errCount+' errores, '+warns+' advertencias','o','Validacion de datos');
    await sleep(200);setPStep('ps4','ps don');

    setPStep('ps5','ps act');setPct(88,'Generando Excel...');
    addLog('Generando Excel con plantilla WO Cloud','i','Escritura de archivo');
    addLog('   Importacion Terceros: '+out.length+' registros','i','Escritura de archivo');
    addLog('   LISTAS + Listas01 + Logs + Estadisticas + Excluidos','i','Escritura de archivo');

    const _stats={archivos_entrada:1,registros_entrada:totalEntrada,
      registros_consolidados:filtered.length,registros_transformados:out.length,
      errores:errCount,warnings:warns};
    WB=buildWB(out, LOGENTRIES, _stats, EXCLUDED_RECORDS, cloudEmpIds, DEFAULTS_LOG, nitExcluidos);
    const dur=((Date.now()-t0)/1000).toFixed(1);
    addLog('Excel generado en '+dur+'s - 7 hojas','o','Escritura de archivo');
    addLog('=================================','o',null);
    addLog('PIPELINE COMPLETADO | '+out.length+' terceros migrados','o',null);
    setPStep('ps5','ps don');setPct(100,'Completado');
    await sleep(500);

    const fn=buildFN();
    document.getElementById('dl-fn').textContent=fn;
    const dlMeta=document.getElementById('dl-meta');if(dlMeta)dlMeta.textContent='Excel - 2 hojas - '+out.length+' registros - WO Cloud';
    document.getElementById('rsub').textContent=S.orig+' -> '+S.dest+' - '+S.mod+' - '+dur+'s';
    document.getElementById('st-in').textContent=out.length.toLocaleString('es-CO');
    document.getElementById('st-ok').textContent=(out.length-errCount).toLocaleString('es-CO');
    document.getElementById('st-w').textContent=warns+errCount;
    document.getElementById('st-t').textContent=dur;
    document.getElementById('rlog').innerHTML=document.getElementById('logp').innerHTML;
    setStep(4);
    logMigrationToBackend({filename_out:fn,orig_soft:S.orig,dest_soft:S.dest,module:S.mod,records_in:totalEntrada,records_out:out.length,errors:errCount,warnings:warns,duration_sec:parseFloat(dur),status:'completed'});
  }catch(err){
    addLog('ERROR: '+err.message,'e',null);
    console.error(err);
    setStep(2);showA('a2','Error en pipeline: '+err.message);
  }
}

const COLS=['Tipo Identificacion *','Identificacion *','Ciudad Identificacion*','Primer Nombre o Razon Social*','Segundo Nombre','Primer Apellido *','Segundo Apellido','Tipo Tercero *','Codigo','Activo','Actividad Economica','Tipo Contribuyente *','Clasi. Administrador Impuesto *','Excepcion Impuesto','Tarifa Reteica Compras','Aplica Reteica Ventas','Maneja Cupo Credito','Vendedor','Lista Precios','Forma Pago','Plazo Dias','Porcentaje Descuento','Tipo Direccion *','Nombre Direccion *','Direccion *','Telefonos *','Email *','Ciudad Direccion *','Zona','Barrio'];

const NITS_EXCLUIR = new Set(["891800330","899999063","890203183","890399010","891500319","890102257","800118954","891080031","890480123","890980040","800197268","800148514","805001157","899999034","890903790","860022137","800256161","860002503","899999061","860034594","900604350","830074184","800130907","900914254","800229739","860011153","800224808","809008362","800216278","830125132","800253055","900156264","830054904","837000084","860008645","830008686","890903937","899999001","901037916","899999734","899999284","800112806","800088702","901543761","800251440","830003564","890904996","901093846","824001398","222222222","830009783","900226715","805000427","900406150","890806490","860066942","804002105","901469580","800219488","890303093","890700148","890201578","890000381","890480023","890900842","800211025","892200015","891200337","890500675","890500516","890303208","891480000","891180008","891600091","890480110","890900840","890101994","890900841","860045904","891080005","892399989","891500182","844003392","891190047","891800213","800231969","890102002","860007336","860002183","800226175","800227940","892000146","800149496","891280008","892115006","860007379","891856000","900298372","890200106","892400320","890102044","891780093","860013570","800140949","890704737","800003122","890270275","800147502","860003020","890903938","860043186","900200960","860007738","890200756","860050750","890300279","860002964","860034313","890203088","860035827","860051135","860007335","800037800","806008394","900935126","899999107","839000495","830113831","860503617","800138188","900336004","817001773"]);
const COLS_DISPLAY=['Tipo Identificaci\u00f3n *','Identificaci\u00f3n *','Ciudad Identificaci\u00f3n*','Primer Nombre \u00f3 Razon Social*','Segundo Nombre','Primer Apellido *','Segundo Apellido','Tipo Tercero\u00a0*','C\u00f3digo','Activo','Actividad Econ\u00f3mica','Tipo Contribuyente *','Clasi. Administrador Impuesto *','Excepci\u00f3n Impuesto','Tarifa Reteica Compras','Aplica Reteica Ventas','Maneja Cupo Cr\u00e9dito','Vendedor','Lista Precios','Forma Pago','Plazo D\u00edas','Porcentaje Descuento','Tipo Direcci\u00f3n *','Nombre Direcci\u00f3n *','Direcci\u00f3n *','Tel\u00e9fonos *','Email *','Ciudad Direcci\u00f3n *','Zona','Barrio'];
const TIPOS_ID=['C\u00e9dula de ciudadan\u00eda','C\u00e9dula de Extranjer\u00eda','Documento de Identificaci\u00f3n Extranjero','NIT','Pasaporte','Permiso Especial de Permanencia','Registro Civil','Sin ID del exterior o para uso definido por DIAN','Tarjeta de Extranjer\u00eda','Tarjeta de Identidad'];
const TIPOS_CONTRIB=['Persona Natural Responsable del IVA','Persona Natural No Responsable del IVA','Persona Jur\u00eddica','Grande Contribuyente No Autorretenedor','Grande Contribuyente Autorretenedor','Persona Jur\u00eddica Autorretenedor','Persona Natural Autorretenedor','Tercero del Exterior','Proveedor Sociedades de Comercio Internacional','R\u00e9gimen Simple de Tributaci\u00f3n Persona Jur\u00eddica','R\u00e9gimen Simple de Tributaci\u00f3n Persona Natural','Entidades Sin Animo De Lucro','Persona Natural Regimen Com\u00fan Agente Retenedor','Persona Natural o Jur\u00eddica Ley 1429','Instituciones del Estado Publicos y Otros'];
const CLASI_ADMIN=['Exportador','Importador','Importador Zona Franca','Normal','Sociedad de Comercializaci\u00f3n Internacional','Tercero en Zona Franca','Zonas Exentas','Zonas y Terceros Excluidos'];
const TIPOS_DIR=['Bodega','Casa','Empresa/Oficina','Sucursal'];
const MANEJA_CUPO=['Si','No'];
const EXCEPCION=['Contribuyente Exento','Zona Afectada Terremoto','Zona Franca'];
const CIUDADES=["Abejorral","Abrego","Abriaqu\u00ed","Acac\u00edas","Acand\u00ed","Acevedo","Ach\u00ed","Agrado","Agua De Dios","Aguachica","Aguada","Aguadas","Aguazul","Agust\u00edn Codazzi","Aipe","Alb\u00e1n","Albania","Alcal\u00e1","Aldana","Alejandr\u00eda","Algarrobo","Algeciras","Almaguer","Almeida","Alpujarra","Altamira","Alto Baud\u00f3","Altos Del Rosario","Alvarado","Amag\u00e1","Amalfi","Ambalema","Anapoima","Ancuy\u00e1","Andaluc\u00eda","Andes","Angel\u00f3polis","Angostura","Anolaima","Anor\u00ed","Anserma","Ansermanuevo","Anz\u00e1","Anzo\u00e1tegui","Apartad\u00f3","Ap\u00eda","Apulo","Aquitania","Aracataca","Aranzazu","Aratoca","Arauca","Arauquita","Arbe\u00e1ez","Arboleda","Arboledas","Arboletes","Arcabuco","Arenal","Argelia","Ariguan\u00ed","Arjona","Armenia","Armero","Arroyohondo","Astrea","Ataco","Atrato","Ayapel","Bagad\u00f3","Bah\u00eda Solano","Bajo Baud\u00f3","Balboa","Baranoa","Baraya","Barbacoas","Barbosa","Barichara","Barranca De Up\u00eda","Barrancabermeja","Barrancas","Barranco De Loba","Barranco Minas","Barranquilla","Becerril","Belalc\u00e1zar","Bel\u00e9n","Bel\u00e9n De Bajir\u00e1","Bel\u00e9n De Los Andaquies","Bel\u00e9n De Umbr\u00eda","Bello","Belmira","Beltr\u00e1n","Berbeo","Betania","Bet\u00e9itiva","Betulia","Bituima","Boavita","Bochalema","Bogot\u00e1, D.C.","Bojac\u00e1","Boyay\u00e1","Bol\u00edvar","Bosconia","Boyac\u00e1","Brice\u00f1o","Bucaramanga","Bucarasica","Buenaventura","Buenavista","Buenos Aires","Buesaco","Bugalagrande","Buritic\u00e1","Busb\u00e1nz\u00e1","Cabrera","Cabuyaro","Cacahual","C\u00e1ceres","Cachipay","Cachir\u00e1","C\u00e1cota","Caicedo","Caicedonia","Caimito","Cajamarca","Cajibi\u00f3","Cajic\u00e1","Calamar","Calarc\u00e1","Caldas","Caldono","Cali","California","Calima","Caloto","Campamento","Campo De La Cruz","Campoalegre","Campohermoso","Canalete","Candelaria","Cantagallo","Ca\u00f1asgordas","Caparrap\u00ed","Capitanejo","C\u00e1queza","Caracol\u00ed","Caramanta","Carcase\u00ed","Carepa","Carmen De Apical\u00e1","Carmen De Carupa","Carmen Del Dari\u00e9n","Carolina","Cartagena de Indias","Cartagena Del Chair\u00e1","Cartago","Caru\u00fa","Casabianca","Castilla La Nueva","Caucasia","Cep\u00edt\u00e1","Ceret\u00e9","Cerinza","Cerrito","Cerro San Antonio","C\u00e9rtegui","Chachag\u00fc\u00ed","Chagu\u00e1ni","Chal\u00e1n","Chameza","Chaparral","Charal\u00e1","Charta","Ch\u00eda","Chibolo","Chigorod\u00f3","Chima","Chim\u00e1","Chimichagua","Chin\u00e1cota","Chinavita","Chinchi\u00e1","Chin\u00fa","Chipaque","Chipat\u00e1","Chiquinquir\u00e1","Ch\u00edquiza","Chiriguan\u00e1","Chiscas","Chita","Chitag\u00e1","Chitaraque","Chivat\u00e1","Chivor","Choach\u00ed","Chocont\u00e1","Cicuco","Ci\u00e9naga","Ci\u00e9naga De Oro","Ci\u00e9nega","Cimitarra","Circasia","Cisneros","Ciudad Bol\u00edvar","Clemencia","Cocorn\u00e1","Coello","Cogua","Colombia","Col\u00f3n","Coloso","C\u00f3mbita","Concepci\u00f3n","Concordia","Condoto","Confines","Consaca","Contadero","Contrataci\u00f3n","Convenci\u00f3n","Copacabana","Coper","C\u00f3rdoba","Corinto","Coromoro","Corozal","Corrales","Cota","Cotorra","Covarach\u00eda","Cove\u00f1as","Coyaima","Cravo Norte","Cuaspud","Cubar\u00e1","Cubarral","Cucaita","Cucunub\u00e1","C\u00facuta","Cucutilla","Cu\u00edtiva","Cumaral","Cumaribo","Cumbal","Cumbitara","Cunday","Curillo","Cur\u00edt\u00ed","Curuman\u00ed","Dabeiba","Dagua","Dibulla","Distracci\u00f3n","Dolores","Don Mat\u00edas","Dosquebradas","Duitama","Durania","Eb\u00e9jico","El \u00c1guila","El Bagre","El Banco","El Cairo","El Calvario","El Cant\u00f3n Del San Pablo","El Carmen","El Carmen De Atrato","El Carmen De Bol\u00edvar","El Carmen De Chucur\u00ed","El Carmen De Viboral","El Castillo","El Cerrito","El Charco","El Cocuy","El Colegio","El Copey","El Doncello","El Dorado","El Dovio","El Encanto","El Espino","El Guacamayo","El Guamo","El Litoral Del San Juan","El Molino","El Paso","El Paujil","El Pe\u00f1ol","El Pe\u00f1\u00f3n","El Pi\u00f1on","El Play\u00f3n","El Ret\u00e9n","El Retorno","El Roble","El Rosal","El Rosario","El Santuario","El Tabl\u00f3n De G\u00f3mez","El Tambo","El Tarra","El Zulia","El\u00edas","Encino","Enciso","Entrerr\u00edos","Envigado","Espinal","Facatativ\u00e1","Falan","Filadelfia","Filandia","Firavitoba","Flandes","Florencia","Floresta","Flori\u00e1n","Florida","Floridablanca","Fomeque","Fonseca","Fortul","Fosca","Francisco Pizarro","Fredonia","Fresno","Frontino","Fuente De Oro","Fundaci\u00f3n","Funes","Funza","F\u00faquene","Fusagasug\u00e1","Gachal\u00e1","Gachancip\u00e1","Gachantiv\u00e1","Gachet\u00e1","Gal\u00e1n","Galapa","Galeras","Gama","Gamarra","Gambita","G\u00e1meza","Garagoa","Garz\u00f3n","G\u00e9nova","Gigante","Ginebra","Giraldo","Girardot","Girardota","Gir\u00f3n","G\u00f3mez Plata","Gonz\u00e1lez","Gramalote","Granada","Guaca","Guacamayas","Guacar\u00ed","Guachen\u00e9","Guachet\u00e1","Guachucal","Guadalajara De Buga","Guadalupe","Guaduas","Guaitarilla","Gualmatan","Guamal","Guamo","Guapi","Guapot\u00e1","Guaranda","Guarne","Guasca","Guatap\u00e9","Guataqu\u00ed","Guatavita","Guateque","Gu\u00e1tica","Guavat\u00e1","Guayabal De Siquima","Guayabetal","Guayat\u00e1","G\u00fcepsa","G\u00fcic\u00e1n","Guti\u00e9rrez","Hacar\u00ed","Hatillo De Loba","Hato","Hato Corozal","Hatonuevo","Heliconia","Herr\u00e1n","Herveo","Hispania","Hobo","Honda","Ibagu\u00e9","Icononzo","Iles","Imu\u00e9s","In\u00edrida","Inz\u00e1","Ipiales","Iquira","Isnos","Istmina","Itag\u00fc\u00ed","Ituango","Iza","Jambal\u00f3","Jamund\u00ed","Jard\u00edn","Jenesano","Jeric\u00f3","Jerusal\u00e9n","Jes\u00fas Mar\u00eda","Jord\u00e1n","Juan De Acosta","Jun\u00edn","Jurad\u00f3","La Apartada","La Argentina","La Belleza","La Calera","La Capilla","La Ceja","La Celia","La Chorrera","La Cruz","La Cumbre","La Dorada","La Esperanza","La Estrella","La Florida","La Gloria","La Guadalupe","La Jagua De Ibirico","La Jagua Del Pilar","La Llanada","La Macarena","La Merced","La Mesa","La Monta\u00f1ita","La Palma","La Paz","La Pedrera","La Pe\u00f1a","La Pintada","La Plata","La Playa","La Primavera","La Salina","La Sierra","La Tebaida","La Tola","La Uni\u00f3n","La Uvita","La Vega","La Victoria","La Virginia","Labateca","Labranzagrande","Land\u00e1zuri","Lebr\u00edja","Legu\u00edzamo","Leiva","Lejan\u00edas","Lenguazaque","L\u00e9rida","Leticia","L\u00edbano","Liborina","Linares","Llor\u00f3","L\u00f3pez","Lorica","Los Andes","Los C\u00f3rdobas","Los Palmitos","Los Patios","Los Santos","Lourdes","Luruaco","Macanal","Macaravita","Maceo","Macheta","Madrid","Mangan\u00e9","Mag\u00fci","Mahates","Maicao","Majagual","M\u00e1laga","Malambo","Mallama","Manat\u00ed","Manaure","Man\u00ed","Manizales","Manta","Manzanares","Mapirip\u00e1n","Mapiripana","Margarita","Mar\u00eda La Baja","Marinilla","Marip\u00ed","Mariquita","Marmato","Marquetalia","Marsella","Marulanda","Matanza","Medell\u00edn","Medina","Medio Atrato","Medio Baud\u00f3","Medio San Juan","Melgar","Mercaderes","Mesetas","Mil\u00e1n","Miraflores","Miranda","Mirat\u00ed - Paran\u00e1","Mistrat\u00f3","Mit\u00fa","Mocoa","Mogotes","Molagavita","Momil","Momp\u00f3s","Mongua","Mong\u00fc\u00ed","Moniquir\u00e1","Montebello","Montecristo","Montel\u00edbano","Montenegro","Monter\u00eda","Monterrey","Mo\u00f1itos","Morales","Morelia","Morichal","Morroa","Mosquera","Motavita","Murillo","Murind\u00f3","Mutat\u00e1","Mutiscua","Muzo","Nari\u00f1o","N\u00e1taga","Natagaima","Nech\u00ed","Neclocl\u00ed","Neira","Neiva","Nemoc\u00f3n","Nilo","Nimaima","Nobsa","Nocaima","Norcasia","Noros\u00ed","N\u00f3vita","Nueva Granada","Nuevo Col\u00f3n","Nunch\u00eda","Nqu\u00ed","Obando","Ocamonte","Oca\u00f1a","Oiba","Oicat\u00e1","Olaya","Olaya Herrera","Onzaga","Oporapa","Orito","Oroc\u00fantre","Ortega","Ospina","Otanche","Ovejas","Pachavita","Pacho","Pacoa","P\u00e1cora","Padilla","P\u00e1ez","Paicol","Pailitas","Paime","Paipa","Pajarito","Palermo","Palestina","Palmar","Palmar De Varela","Palmas Del Socorro","Palmira","Palmito","Palocabildo","Pamplona","Pamplonita","Pana Pana","Pandi","Panqueba","Papunaua","P\u00e1ramo","Paratebueno","Pasca","Pasto","Pati\u00e1","Pauna","Paya","Paz De Ariporo","Paz De R\u00edo","Pedraza","Pelaya","Pensilvania","Pe\u00f1ol","Peque","Pereira","Pesca","Piamonte","Piedecuesta","Piedras","Piendamo","Pijao","Piji\u00f1o Del Carmen","Pinchote","Pinillos","Pioj\u00f3","Pisba","Pital","Pitalito","Pivijay","Planadas","Planeta Rica","Plato","Policarpa","Polonuevo","Ponedera","Popay\u00e1n","Pore","Potos\u00ed","Pradera","Prado","Providencia","Pueblo Bello","Pueblo Nuevo","Pueblo Rico","Pueblorrico","Puebloviejo","Puente Nacional","Puerres","Puerto Alegr\u00eda","Puerto Arica","Puerto As\u00eds","Puerto Berr\u00edo","Puerto Boyac\u00e1","Puerto Caicedo","Puerto Carre\u00f1o","Puerto Colombia","Puerto Concordia","Puerto Escondido","Puerto Gait\u00e1n","Puerto Guzm\u00e1n","Puerto Libertador","Puerto Lleras","Puerto L\u00f3pez","Puerto Nare","Puerto Nari\u00f1o","Puerto Parra","Puerto Rico","Puerto Rond\u00f3n","Puerto Salgar","Puerto Santander","Puerto Tejada","Puerto Triunfo","Puerto Wilches","Pul\u00ed","Pupiales","Purac\u00e9","Purificaci\u00f3n","Pur\u00edsima","Quebradanegra","Quetame","Quibd\u00f3","Quimbaya","Quinch\u00eda","Qu\u00edpama","Quipile","Ragonvalia","Ramiriqu\u00ed","R\u00e1quira","Recetor","Regidor","Remedios","Remolino","Repel\u00f3n","Restrepo","Retiro","Ricaurte","R\u00edo De Oro","R\u00edo Iro","R\u00edo Quito","R\u00edo Viejo","Rioblanco","Riofr\u00edo","Riohacha","Rionegro","Riosucio","Risaralda","Rivera","Roberto Pay\u00e1n","Roldanillo","Roncesvalles","Rond\u00f3n","Rosas","Rovira","Sabana De Torres","Sabanagrande","Sabanalarga","Sabanas De San \u00c1ngel","Sabaneta","Saboy\u00e1","S\u00e1cama","S\u00e1chica","Sahag\u00fan","Saladoblanco","Salamina","Salazar","Salda\u00f1a","Salento","Salgar","Samac\u00e1","Saman\u00e1","Samaniego","Sampu\u00e9s","San Agust\u00edn","San Alberto","San Andr\u00e9s","San Andr\u00e9s Sotavento","San Antero","San Antonio","San Antonio Del Tequendama","San Benito","San Benito Abad","San Bernardo","San Bernardo Del Viento","San Calixto","San Carlos","San Carlos De Guaroa","San Cayetano","San Crist\u00f3bal","San Diego","San Eduardo","San Estanislao","San Felipe","San Fernando","San Francisco","San Gil","San Jacinto","San Jacinto Del Cauca","San Jer\u00f3nimo","San Joaqu\u00edn","San Jos\u00e9","San Jos\u00e9 De La Monta\u00f1a","San Jos\u00e9 De Miranda","San Jos\u00e9 De Pare","San Jos\u00e9 de Ur\u00e9","San Jos\u00e9 Del Fragua","San Jos\u00e9 Del Guaviare","San Jos\u00e9 Del Palmar","San Juan De Arama","San Juan De Betulia","San Juan De R\u00edo Seco","San Juan De Urab\u00e1","San Juan Del Cesar","San Juan Nepomuceno","San Juanito","San Lorenzo","San Luis","San Luis De Gaceno","San Luis De Palenque","San Marcos","San Mart\u00edn","San Mart\u00edn De Loba","San Mateo","San Miguel","San Miguel De Sema","San Onofre","San Pablo","San Pablo De Borbur","San Pedro","San Pedro De Cartago","San Pedro De Urab\u00e1","San Pelayo","San Rafael","San Roque","San Sebasti\u00e1n","San Sebasti\u00e1n De Buenavista","San Vicente","San Vicente De Chucur\u00ed","San Vicente Del Cagu\u00e1n","San Zen\u00f3n","Sandon\u00e1","Santa Ana","Santa B\u00e1rbara","Santa B\u00e1rbara De Pinto","Santa Catalina","Santa Helena Del Op\u00f3n","Santa Isabel","Santa Luc\u00eda","Santa Mar\u00eda","Santa Marta","Santa Rosa","Santa Rosa De Cabal","Santa Rosa De Osos","Santa Rosa De Viterbo","Santa Rosa Del Sur","Santa Rosal\u00eda","Santa Sof\u00eda","Santacruz","Santaf\u00e9 De Antioquia","Santana","Santander De Quilichao","Santiago","Santiago De Tol\u00fa","Santo Domingo","Santo Tom\u00e1s","Santuario","Sapuyes","Saravena","Sardinata","Sasaima","Sativanorte","Sativasur","Segovia","Sesquil\u00e9","Sevilla","Siachoque","Sibat\u00e9","Sibundoy","Silos","Silvania","Silvia","Simacota","Simijaca","Simit\u00ed","Sinc\u00e9","Sincelejo","Sip\u00ed","Sitionuevo","Soacha","Soat\u00e1","Socha","Socorro","Socot\u00e1","Sogamoso","Solano","Soledad","Solita","Somondoco","Sons\u00f3n","Sopetr\u00e1n","Soplaviento","Sop\u00f3","Sora","Sorac\u00e1","Sotaquir\u00e1","Sotar\u00e1","Suaita","Suan","Su\u00e1rez","Suaza","Subachoque","Sucre","Suesca","Supat\u00e1","Sup\u00eda","Surat\u00e1","Susa","Susc\u00f3n","Sutamarch\u00e1n","Sutatausa","Sutatenza","Tabio","Tad\u00f3","Talaigua Nuevo","Tamalameque","T\u00e1mara","Tame","T\u00e1mesis","Taminango","Tangua","Taraira","Tarapac\u00e1","Taraz\u00e1","Tarqui","Tarso","Tasco","Tauramena","Tausa","Tello","Tena","Tenerife","Tenjo","Tenza","Teorama","Teruel","Tesalia","Tibacuy","Tiban\u00e1","Tibasosa","Tibirita","Tib\u00fa","Tierralta","Timan\u00e1","Timbio","Timbiqui","Tinjac\u00e1","Tipacoque","Tiquisio","Titirib\u00ed","Toca","Tocaima","Tocancip\u00e1","Tog\u00fc\u00ed","Toledo","Tol\u00fa Viejo","Tona","T\u00f3paga","Topaip\u00ed","Toribio","Toro","Tota","Totor\u00f3","Trinidad","Trujillo","Tubar\u00e1","Tuch\u00edn","Tulu\u00e1","Tumaco","Tunja","Tunungu\u00e1","T\u00faquerres","Turbaco","Turban\u00e1","Turbo","Turmequ\u00e9","Tuta","Tutaz\u00e1","Ubal\u00e1","Ubaque","Ulloa","Umbita","Une","Ungu\u00eda","Uni\u00f3n Panamericana","Uramita","Uribe","Uribia","Urrao","Urumita","Usiacur\u00ed","\u00datica","Valdivia","Valencia","Valle De San Jos\u00e9","Valle De San Juan","Valle Del Guamuez","Valledupar","Valpara\u00edso","Vegach\u00ed","V\u00e9lez","Venadillo","Venecia","Ventaquemada","Vergara","Versalles","Vetas","Vian\u00ed","Victoria","Vig\u00eda Del Fuerte","Vijes","Villa Caro","Villa De Leyva","Villa De San Diego De Ubat\u00e9","Villa Del Rosario","Villa Rica","Villagarz\u00f3n","Villag\u00f3mez","Villahermosa","Villamar\u00eda","Villanueva","Villap\u00ednz\u00f3n","Villarrica","Villavicencio","Villavieja","Villeta","Viot\u00e1","Viracach\u00e1","Vistahermosa","Viterbo","Yacop\u00ed","Yacuanquer","Yagu\u00e1ra","Yal\u00ed","Yarumal","Yavarat\u00e9","Yolomb\u00f3","Yond\u00f3","Yopal","Yotoco","Yumbo","Zambrano","Zapatoca","Zapay\u00e1n","Zaragoza","Zarzal","Zetaquira","Zipac\u00f3n","Zipaquir\u00e1","Zona Bananera"];
const ACTIVIDADES=["0010",1011,1012,1020,1030,1040,1051,1052,1061,1062,1063,1071,1072,1081,1082,1083,1084,1089,1090,1101,1102,1103,1104,"0111","0112","0113","0114","0115","0119",1200,"0121","0122","0123","0124","0125","0126","0127","0128","0129","0130",1311,1312,1313,1391,1392,1393,1394,1399,"0141",1410,"0142",1420,"0143",1430,"0144","0145","0149","0150",1511,1512,1513,1521,1522,1523,"0161",1610,"0162",1620,"0163",1630,"0164",1640,1690,"0170",1701,1702,1709,1811,1812,1820,1910,1921,1922,2011,2012,2013,2014,2021,2022,2023,2029,2030,"0210",2100,"0220",2211,2212,2219,2221,2229,"0230",2310,2391,2392,2393,2394,2395,2396,2399,"0240",2410,2421,2429,2431,2432,2511,2512,2513,2520,2591,2592,2593,2599,2610,2620,2630,2640,2651,2652,2660,2670,2680,2711,2712,2720,2731,2732,2740,2750,2790,2811,2812,2813,2814,2815,2816,2817,2818,2819,2821,2822,2823,2824,2825,2826,2829,2910,2920,2930,3011,3012,3020,3030,3040,3091,3092,3099,"0311",3110,"0312",3120,"0321",3210,"0322",3220,3230,3240,3250,3290,3311,3312,3313,3314,3315,3319,3320,3511,3512,3513,3514,3520,3530,3600,3700,3811,3812,3821,3822,3830,3900,4111,4112,4210,4220,4290,4311,4312,4321,4322,4329,4330,4390,4511,4512,4520,4530,4541,4542,4610,4620,4631,4632,4641,4642,4643,4644,4645,4649,4651,4652,4653,4659,4661,4662,4663,4664,4665,4669,4690,4711,4719,4721,4722,4723,4724,4729,4731,4732,4741,4742,4751,4752,4753,4754,4755,4759,4761,4762,4769,4771,4772,4773,4774,4775,4781,4782,4789,4791,4792,4799,4911,4912,4921,4922,4923,4930,5011,5012,5021,5022,"0510",5111,5112,5121,5122,"0520",5210,5221,5222,5223,5224,5229,5310,5320,5511,5512,5513,5514,5519,5520,5530,5590,5611,5612,5613,5619,5621,5629,5630,5811,5812,5813,5819,5820,5911,5912,5913,5914,5920,6010,6020,"0610",6110,6120,6130,6190,"0620",6201,6202,6209,6311,6312,6391,6399,6411,6412,6421,6422,6423,6424,6431,6432,6491,6492,6493,6494,6495,6499,6511,6512,6513,6514,6521,6522,6531,6532,6611,6612,6613,6614,6615,6619,6621,6629,6630,6810,6820,6910,6920,7010,7020,"0710",7110,7111,7112,7120,"0721",7210,"0722",7220,"0723","0729",7310,7320,7410,7420,7490,7500,7710,7721,7722,7729,7730,7740,7810,7820,7830,7911,7912,7990,8010,8020,8030,"0081","0811",8110,"0812",8121,8129,8130,"0082","0820",8211,8219,8220,8230,8291,8292,8299,8411,8412,8413,8414,8415,8421,8422,8423,8424,8430,8511,8512,8513,8521,8522,8523,8530,8541,8542,8543,8544,8551,8552,8553,8559,8560,8610,8621,8622,8691,8692,8699,8710,8720,8730,8790,8810,8890,"0891","0892","0899","0090",9001,9002,9003,9004,9005,9006,9007,9008,"0910",9101,9102,9103,9200,9311,9312,9319,9321,9329,9411,9412,9420,9491,9492,9499,9511,9512,9521,9522,9523,9524,9529,9601,9602,9603,9609,9700,9810,9820,"0990",9900];

let LISTAS01_DATA=null;


function buildWB(rows, logEntries, stats, excluded, empIds, defaultsLog, nitExcluidos){
  const wb=XLSX.utils.book_new();
  const aoa=[];
  const sec=new Array(30).fill(null);sec[0]='Ficha Principal';sec[22]='Ficha Dirección';
  aoa.push(sec);
  aoa.push([...COLS_DISPLAY]);
  rows.forEach(r=>{
    const rowData=COLS.map(c=>{const v=r[c];return(v==='')?null:v??null});
    aoa.push(rowData);
  });
  const ws1=XLSX.utils.aoa_to_sheet(aoa);
  ws1['!cols']=[{wch:24},{wch:18},{wch:18},{wch:28},{wch:14},{wch:18},{wch:16},{wch:18},{wch:8},{wch:6},{wch:18},{wch:40},{wch:36},{wch:20},{wch:20},{wch:20},{wch:18},{wch:16},{wch:14},{wch:12},{wch:11},{wch:20},{wch:18},{wch:18},{wch:30},{wch:16},{wch:30},{wch:18},{wch:8},{wch:8}];
  ws1['!merges']=[{s:{r:0,c:0},e:{r:0,c:21}},{s:{r:0,c:22},e:{r:0,c:29}}];
  XLSX.utils.book_append_sheet(wb,ws1,'Importación Terceros');
  const maxL=Math.max(TIPOS_ID.length,TIPOS_CONTRIB.length,CLASI_ADMIN.length,CIUDADES.length,TIPOS_DIR.length,ACTIVIDADES.length);
  const lAoa=[['Tipo de Identificación*','Tipo Contribuyente *','Clasi. Administrador Impuesto *','Ciudad*','Tipo Dirección *','Actividad Económica','Maneja cupo credito','Excepción Impuesto']];
  for(let i=0;i<maxL;i++){lAoa.push([TIPOS_ID[i]??null,TIPOS_CONTRIB[i]??null,CLASI_ADMIN[i]??null,CIUDADES[i]??null,TIPOS_DIR[i]??null,ACTIVIDADES[i]??null,MANEJA_CUPO[i]??null,EXCEPCION[i]??null]);}
  const ws2=XLSX.utils.aoa_to_sheet(lAoa);
  ws2['!cols']=[{wch:34},{wch:46},{wch:38},{wch:26},{wch:18},{wch:18},{wch:18},{wch:28}];
  XLSX.utils.book_append_sheet(wb,ws2,'LISTAS');
  if(LISTAS01_DATA){XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet(LISTAS01_DATA),'Listas01');}
  XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet([['Identificación','Sucursal']]),'sucursales Clientes');
  const tsNow=new Date().toISOString();
  const logsAoa=[['timestamp','fase','nivel','mensaje','tipo','detalle']];
  (logEntries||[]).forEach(e=>logsAoa.push([e.ts||tsNow,e.fase||'',e.lvl==='e'?'ERROR':e.lvl==='w'?'WARN':'INFO',e.msg||'','','']));
  XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet(logsAoa),'Logs');
  const s2=stats||{};
  const estAoa=[['Métrica','Valor'],['archivos_entrada',s2.archivos_entrada||1],['registros_entrada',s2.registros_entrada||0],['registros_consolidados',s2.registros_consolidados||0],['registros_transformados',s2.registros_transformados||0],['registros_salida',rows.length],['errores_validacion',s2.errores||0],['warnings_validacion',s2.warnings||0]];
  XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet(estAoa),'Estadísticas');
  const exclAoa=[['Identificación','Nombre o Razón Social','Propiedad Activa']];
  (excluded||[]).forEach(e=>exclAoa.push([e.id,e.nombre,e.tipo]));
  XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet(exclAoa),'IdentificacionesExcluidas');
  
  // Hoja empleados (solo si se cargó archivo de empleados)
  if(empIds && empIds.size > 0){
    const empRows = rows.filter(r => {
      const id = String(r[1]||'').replace(/[^0-9]/g,'');
      return empIds.has(id);
    });
    if(empRows.length > 0){
      const empAoa = [COLS.slice()];
      empRows.forEach(r => {
        const rowCopy = r.slice();
        // Col 7 = Tipo Tercero / Propiedad Activa → solo "Empleado"
        rowCopy[7] = 'Empleado';
        empAoa.push(rowCopy);
      });
      const wsEmp = XLSX.utils.aoa_to_sheet(empAoa);
      XLSX.utils.book_append_sheet(wb, wsEmp, 'Empleados');
    }
  }


  // Hoja: Campos aplicados por defecto
  if(defaultsLog && defaultsLog.length > 0){
    const defAoa=[['Identificación','Nombre o Razón Social','Campo','Valor Aplicado','Motivo']];
    defaultsLog.forEach(d=>defAoa.push([d.id, d.nombre, d.campo, d.valor_aplicado, d.motivo]));
    XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet(defAoa), 'Campos por Defecto');
  }

  // Hoja: NITs Excluidos
  if(nitExcluidos && nitExcluidos.length > 0){
    const nitAoa=[['Identificación','Nombre o Razón Social','Motivo']];
    nitExcluidos.forEach(d=>nitAoa.push([d.id, d.nombre, 'NIT excluido — lista predefinida']));
    XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet(nitAoa), 'NITs Excluidos');
  }

return wb;
}

function buildFN(){
  const n=new Date(),p=x=>String(x).padStart(2,'0');
  const d=n.getFullYear()+p(n.getMonth()+1)+p(n.getDate());
  const t=p(n.getHours())+p(n.getMinutes())+p(n.getSeconds());
  return 'terceros_migrados_'+d+'_'+t+'_cloud.xlsx';
}
function doDownload(){
  if(!WB){alert('Primero ejecuta la migracion.');return}
  XLSX.writeFile(WB,document.getElementById('dl-fn').textContent||buildFN());
}
function resetAll(){
  WB=null;S.files={};LOGENTRIES.length=0;
  EXCLUDED_RECORDS.length=0;
  ['f-m','f-c','f-p'].forEach(id=>{const el=document.getElementById(id);if(el)el.value=''});
  ['sl-m','sl-c','sl-p'].forEach(id=>{const el=document.getElementById(id);if(el)el.className='fslot'});
  ['nm-m','nm-c','nm-p'].forEach(id=>{const el=document.getElementById(id);if(el)el.textContent=''});
  ['ps0','ps1','ps2','ps3','ps4','ps5'].forEach(id=>document.getElementById(id).className='ps');
  document.getElementById('pbar').style.width='0%';
  document.getElementById('logp').innerHTML='';
  document.getElementById('sorig').value='';document.getElementById('sdest').value='';document.getElementById('smod').value='';
  document.getElementById('compat').style.display='none';
  setStep(1);
}
