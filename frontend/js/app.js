
// ── Partículas (login + app background) ──────────────────────────
function makeParticles(canvasId, N, speed, alpha){
  const canvas=document.getElementById(canvasId);
  if(!canvas)return;
  const ctx=canvas.getContext('2d');
  let W,H,dots=[];

  function resize(){
    W=canvas.width=canvas.offsetWidth||window.innerWidth;
    H=canvas.height=canvas.offsetHeight||window.innerHeight;
  }
  resize();
  window.addEventListener('resize',resize);

  for(let i=0;i<N;i++){
    dots.push({
      x:Math.random()*W, y:Math.random()*H,
      r:Math.random()*2+0.5,
      vx:(Math.random()-.5)*speed, vy:(Math.random()-.5)*speed,
      a:Math.random()*alpha+0.2
    });
  }

  function draw(){
    ctx.clearRect(0,0,W,H);
    for(let i=0;i<dots.length;i++){
      for(let j=i+1;j<dots.length;j++){
        const dx=dots[i].x-dots[j].x, dy=dots[i].y-dots[j].y;
        const dist=Math.sqrt(dx*dx+dy*dy);
        if(dist<120){
          ctx.beginPath();
          ctx.strokeStyle=`rgba(255,255,255,${.12*(1-dist/120)})`;
          ctx.lineWidth=.5;
          ctx.moveTo(dots[i].x,dots[i].y);
          ctx.lineTo(dots[j].x,dots[j].y);
          ctx.stroke();
        }
      }
    }
    dots.forEach(d=>{
      ctx.beginPath();
      ctx.arc(d.x,d.y,d.r,0,Math.PI*2);
      ctx.fillStyle=`rgba(255,255,255,${d.a})`;
      ctx.fill();
      d.x+=d.vx; d.y+=d.vy;
      if(d.x<0||d.x>W)d.vx*=-1;
      if(d.y<0||d.y>H)d.vy*=-1;
    });
    requestAnimationFrame(draw);
  }
  draw();
}

function initParticles(){
  makeParticles('auth-canvas', 120, 0.8, 0.7);
}

function initAppParticles(){
  makeParticles('app-canvas', 80, 0.4, 0.5);
}

// ════ API ════
const API = window.WO_API_URL || (window.location.hostname==='localhost'||window.location.hostname==='127.0.0.1' ? window.location.origin+'/api' : window.location.origin+'/api');
async function api(method,path,body,token){
  const h={'Content-Type':'application/json'};
  if(token)h['Authorization']='Bearer '+token;
  const r=await fetch(API+path,{method,headers:h,body:body?JSON.stringify(body):null});
  const d=await r.json().catch(()=>({}));
  if(!r.ok)throw new Error(d.error||'Error HTTP '+r.status);
  return d;
}

// ── ETL Router — despacha según software destino ─────────────────
function routeETL(){
  const dest = document.getElementById('sdest') ? document.getElementById('sdest').value : '';
  if(dest === 'World Office Escritorio'){
    // Update download label
    const lbl=document.getElementById('dl-dest-label');
    if(lbl)lbl.textContent='Para importar en World Office Escritorio';
    startEscETL();
  } else {
    const lbl=document.getElementById('dl-dest-label');
    if(lbl)lbl.textContent='Para importar en World Office Cloud';
    startETL();
  }
}

function routeDownload(){
  const dest = document.getElementById('sdest') ? document.getElementById('sdest').value : '';
  if(dest === 'World Office Escritorio'){
    escDoDownload();
  } else {
    doDownload();
  }
}

// ── Empleados: wizard steps ──────────────────────────────────────
function empSetPct(pct,msg){
  const pb=document.getElementById('emp-pbar');
  const pp=document.getElementById('emp-ppct');
  const ph=document.getElementById('emp-pph');
  if(pb) pb.style.width=pct+'%';
  if(pp) pp.textContent=pct+'%';
  if(ph&&msg) ph.textContent=msg;
}

// ── Empleados: pasos propios ──────────────────────────────────────
function empSetStep(n){
  for(let i=1;i<=4;i++){
    const wz=document.getElementById('emp-wt'+i);
    if(wz)wz.className='wz'+(i<n?' done':i===n?' on':'');
    const sec=document.getElementById('emp-s'+i);
    if(sec)sec.style.display=(i===n)?'':'none';
  }
}
function empSetPStep(n){
  for(let i=0;i<=5;i++){
    const el=document.getElementById('emp-ps'+i);
    if(!el)continue;
    el.classList.toggle('act',i===n);
    el.classList.toggle('don',i<n);
  }
}
function empSetPct(pct,msg){
  const pb=document.getElementById('emp-pbar');
  const pp=document.getElementById('emp-ppct');
  const ph=document.getElementById('emp-pph');
  if(pb)pb.style.width=pct+'%';
  if(pp)pp.textContent=pct+'%';
  if(ph&&msg)ph.textContent=msg;
}
function empLog(msg,lvl='i',fase=''){
  const panel=document.getElementById('emp-logp');
  if(!panel)return;
  const now=new Date().toLocaleTimeString('es-CO',{hour12:false});
  const css={i:'li',w:'lw',o:'lo',e:'le-e'}[lvl]||'li';
  panel.innerHTML+=`<div class="le"><span class="lt">${now}</span><span class="${css}">${msg}</span></div>`;
  panel.scrollTop=panel.scrollHeight;
}

// ── Empleados: ir a archivos ──────────────────────────────────────
function empGoFiles(){
  const orig=document.getElementById('emp-sorig').value;
  const dest=document.getElementById('emp-sdest').value;
  const mod=document.getElementById('emp-smod').value;
  const a=document.getElementById('emp-a1');
  if(!orig){ if(a){a.textContent='Selecciona el software de origen.';a.style.display='block';} return; }
  if(!dest){ if(a){a.textContent='Selecciona el software de destino.';a.style.display='block';} return; }
  if(!mod){  if(a){a.textContent='Selecciona el módulo.';a.style.display='block';} return; }
  if(a) a.style.display='none';
  const fsub=document.getElementById('emp-fsub');
  if(fsub) fsub.textContent=orig+' → '+dest+' - Módulo '+mod;
  empSetStep(2);
}

// ── Empleados: router ETL ─────────────────────────────────────────
function routeEmpETL(){
  const dest=document.getElementById('emp-sdest').value;
  const lbl=document.getElementById('emp-dl-dest');
  if(dest==='World Office Escritorio'){
    if(lbl) lbl.textContent='Para importar en World Office Escritorio';
    startEmpEscETL();
  } else {
    if(lbl) lbl.textContent='Para importar en World Office Cloud';
    startEmpCldETL();
  }
}

// ── Empleados: router descarga ────────────────────────────────────
function routeEmpDownload(){
  const dest=document.getElementById('emp-sdest').value;
  if(dest==='World Office Escritorio') empEscDoDownload();
  else empCldDoDownload();
}

// ── Empleados: reset ──────────────────────────────────────────────
function empReset(){
  // Reset files
  if(typeof S!=='undefined'&&S.files){ 
    delete S.files['emp-maestro']; 
    delete S.files['emp-contratos']; 
  }
  // Reset WB objects
  try{ if(typeof EMP_CLD_WB!=='undefined') window.EMP_CLD_WB=null; }catch(e){}
  try{ if(typeof EMP_ESC_WB!=='undefined') window.EMP_ESC_WB=null; }catch(e){}
  // Reset log arrays
  try{ if(typeof EMP_ESC_LOG!=='undefined'&&EMP_ESC_LOG) EMP_ESC_LOG.length=0; }catch(e){}
  try{ if(typeof EMP_ESC_EXCL!=='undefined'&&EMP_ESC_EXCL) EMP_ESC_EXCL.length=0; }catch(e){}
  try{ if(typeof EMP_CLD_LOG!=='undefined'&&EMP_CLD_LOG) EMP_CLD_LOG.length=0; }catch(e){}
  try{ if(typeof EMP_CLD_EXCL!=='undefined'&&EMP_CLD_EXCL) EMP_CLD_EXCL.length=0; }catch(e){}
  // Reset log panel and progress
  const logp=document.getElementById('emp-logp'); if(logp) logp.innerHTML='';
  const pb=document.getElementById('emp-pbar'); if(pb) pb.style.width='0%';
  const pp=document.getElementById('emp-ppct'); if(pp) pp.textContent='0%';
  const ph=document.getElementById('emp-pph'); if(ph) ph.textContent='Iniciando...';
  // Reset file slots
  ['sl-emp-m','sl-emp-c'].forEach(id=>{const el=document.getElementById(id);if(el)el.className='fslot';});
  ['nm-emp-m','nm-emp-c'].forEach(id=>{const el=document.getElementById(id);if(el)el.textContent='';});
  ['f-emp-m','f-emp-c'].forEach(id=>{const el=document.getElementById(id);if(el)el.value='';});
  // Reset stats
  ['emp-st-in','emp-st-ok'].forEach(id=>{const el=document.getElementById(id);if(el)el.textContent='—';});
  // Reset selectors
  ['emp-sorig','emp-sdest','emp-smod'].forEach(id=>{const el=document.getElementById(id);if(el)el.value='';});
  // Reset alert
  const a=document.getElementById('emp-a1'); if(a) a.style.display='none';
  // Close mapping modal if open
  const modal=document.getElementById('emp-map-modal'); if(modal) modal.style.display='none';
  // Go to step 1
  empSetStep(1);
}

// ════ SESSION ════
let AUTH={userId:null,token:null,user:null};
function loadSess(){try{const s=JSON.parse(localStorage.getItem('wo_s')||'null');if(s&&s.token){AUTH={...AUTH,...s};return true}}catch(e){}return false}
function saveSess(){localStorage.setItem('wo_s',JSON.stringify({token:AUTH.token,user:AUTH.user,userId:AUTH.userId}))}
function clearSess(){localStorage.removeItem('wo_s');AUTH={userId:null,token:null,user:null}}

// ════ AUTH UI ════
function togglePw(){const i=document.getElementById('inp-pass');i.type=i.type==='password'?'text':'password'}
function showErr(id,m){const e=document.getElementById(id);e.textContent=m;e.classList.add('vis')}
function hideErr(id){document.getElementById(id).classList.remove('vis')}
function authStep(id){const el=document.getElementById(id);if(el)el.classList.add('vis');}

async function doLogin(){
  hideErr('login-err');
  const email=document.getElementById('inp-email').value.trim();
  const pass=document.getElementById('inp-pass').value;
  ['inp-email','inp-pass'].forEach(id=>document.getElementById(id).classList.remove('err'));
  if(!email){document.getElementById('inp-email').classList.add('err');showErr('login-err','Ingresa tu correo.');return}
  if(!pass){document.getElementById('inp-pass').classList.add('err');showErr('login-err','Ingresa tu contraseña.');return}
  const btn=document.getElementById('login-btn');
  btn.disabled=true;btn.textContent='Ingresando...';
  try{
    const d=await api('POST','/auth/login',{email,password:pass});
    AUTH.token=d.token;
    AUTH.user=d.user||null;
    AUTH.userId=d.user?d.user.id:null;
    saveSess();enterApp();
  }catch(e){
    ['inp-email','inp-pass'].forEach(id=>document.getElementById(id).classList.add('err'));
    showErr('login-err',e.message);
  }finally{btn.disabled=false;btn.textContent='Ingresar →'}
}



// ════ ENTER APP ════
function enterApp(){
  if(!AUTH.user||!AUTH.token){clearSess();return;}
  const authEl=document.getElementById('auth-screen');
  const appEl=document.getElementById('app');
  if(authEl)authEl.classList.add('hidden');
  if(appEl){appEl.classList.add('vis');appEl.style.display='flex';}
  setTimeout(initAppParticles, 100);
  const u=AUTH.user;
  const navAv=document.getElementById('nav-av');
  const navUn=document.getElementById('nav-un');
  if(navAv)navAv.textContent=u.initials||u.name[0];
  if(navUn)navUn.textContent=(u.email||'').split('@')[0];
  const snU=document.getElementById('sn-usuarios');
  if(snU&&u.role!=='admin'){snU.style.display='none';}
  showPg('terceros');
}

async function doLogout(){
  try{await api('POST','/auth/logout',{},AUTH.token)}catch(e){}
  clearSess();
  document.getElementById('app').classList.remove('vis');
  const _a=document.getElementById('auth-screen');if(_a){_a.classList.remove('hidden');_a.style.display='flex';}
  document.getElementById('inp-email').value='';
  document.getElementById('inp-pass').value='';
  authStep('st-login');resetAll();
}

// ════ NAVIGATION ════
function showPg(name){
  document.querySelectorAll('.pg').forEach(p=>p.classList.remove('vis'));
  document.querySelectorAll('.sn-item').forEach(i=>i.classList.remove('on'));
  const pg=document.getElementById('pg-'+name);
  const sn=document.getElementById('sn-'+name);
  if(pg)pg.classList.add('vis');
  if(sn)sn.classList.add('on');
  if(name==='usuarios')loadUsers();
  if(name==='historial')loadMigs();
  if(name==='cuenta')loadPerfil();
  // Reset module state when navigating
  if(name==='terceros'){
    // Reset terceros to step 1
    if(typeof S !== 'undefined') S.files={};
    ['sl-m'].forEach(id=>{const el=document.getElementById(id);if(el)el.className='fslot';});
    ['nm-m'].forEach(id=>{const el=document.getElementById(id);if(el)el.textContent='';});
    ['f-m'].forEach(id=>{const el=document.getElementById(id);if(el)el.value='';});
    if(typeof setStep==='function') setStep(1);
    ['sorig','sdest','smod'].forEach(id=>{const el=document.getElementById(id);if(el)el.value='';});
  }
  if(name==='empleados'){
    // Full reset of empleados state
    empReset();
  }
}

// ════ MIGRATION LOG ════
async function logMigrationToBackend(data){
  if(!AUTH.token)return;
  try{await api('POST','/migrations',data,AUTH.token)}catch(e){console.warn('Log:',e.message)}
}

// ════ USUARIOS ════
// [gestion_usuarios.js]


// ════ MI CUENTA ════
async function loadPerfil(){
  try{
    const u=await api('GET','/auth/me',null,AUTH.token);
    document.getElementById('perfil-data').innerHTML=`
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:6px 28px">
        <div><span style="color:var(--t3)">Nombre:</span> <b style="color:var(--t1)">${u.name}</b></div>
        <div><span style="color:var(--t3)">Correo:</span> <span style="color:var(--t1)">${u.email}</span></div>
        <div><span style="color:var(--t3)">Teléfono:</span> <span style="color:var(--t1)">${u.phone||'No registrado'}</span></div>
        <div><span style="color:var(--t3)">Rol:</span> <span style="color:var(--accent);font-weight:600">${u.role==='admin'?'Administrador':'Usuario'}</span></div>
        <div><span style="color:var(--t3)">Último acceso:</span> <span style="color:var(--t1)">${(u.last_login||'').substring(0,16)||'Primer acceso'}</span></div>
      </div>`;
  }catch(e){}
}

async function changePass(){
  const cur=document.getElementById('cp-cur').value;
  const nw=document.getElementById('cp-new').value;
  const con=document.getElementById('cp-con').value;
  const errEl=document.getElementById('cp-err');const okEl=document.getElementById('cp-ok');
  errEl.classList.add('hide');okEl.classList.add('hide');
  if(!cur||!nw||!con){document.getElementById('cp-errm').textContent='Todos los campos son requeridos.';errEl.classList.remove('hide');return}
  if(nw!==con){document.getElementById('cp-errm').textContent='Las contraseñas no coinciden.';errEl.classList.remove('hide');return}
  try{
    await api('POST','/auth/change-password',{current_password:cur,new_password:nw},AUTH.token);
    document.getElementById('cp-okm').textContent='Contraseña actualizada correctamente.';
    okEl.classList.remove('hide');
    ['cp-cur','cp-new','cp-con'].forEach(id=>document.getElementById(id).value='');
  }catch(e){document.getElementById('cp-errm').textContent=e.message;errEl.classList.remove('hide')}
}

// ════ INIT ════
window.addEventListener('DOMContentLoaded',async()=>{
  initParticles();
  if(loadSess()&&AUTH.token){
    try{
      await api('GET','/auth/me',null,AUTH.token);
      enterApp();
    }catch(e){
      clearSess();
    }
  }
});
document.addEventListener('keydown',e=>{if(e.key==='Escape')closeEdit()});
