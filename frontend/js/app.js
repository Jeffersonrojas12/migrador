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
