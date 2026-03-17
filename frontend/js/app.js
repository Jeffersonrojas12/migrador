// ════ API ════
const API = window.WO_API_URL ||(window.location.port==='5050'?window.location.origin+'/api':'http://localhost:5050/api');
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
function authStep(id){['st-login','st-otp'].forEach(s=>{const el=document.getElementById(s);if(el)el.classList.toggle('vis',s===id)})}

async function doLogin(){
  hideErr('login-err');
  const email=document.getElementById('inp-email').value.trim();
  const pass=document.getElementById('inp-pass').value;
  ['inp-email','inp-pass'].forEach(id=>document.getElementById(id).classList.remove('err'));
  if(!email){document.getElementById('inp-email').classList.add('err');showErr('login-err','Ingresa tu correo.');return}
  if(!pass){document.getElementById('inp-pass').classList.add('err');showErr('login-err','Ingresa tu contraseña.');return}
  const btn=document.getElementById('login-btn');
  btn.disabled=true;btn.textContent='Enviando código...';
  try{
    const d=await api('POST','/auth/login',{email,password:pass});
    AUTH.userId=d.user_id;
    document.getElementById('otp-dest').textContent=d.masked_email;
    document.querySelectorAll('.otp-inp').forEach(i=>{i.value='';i.classList.remove('filled')});
    hideErr('otp-err');
    authStep('st-otp');
    setTimeout(()=>{const f=document.querySelector('.otp-inp');if(f)f.focus()},150);
  }catch(e){
    ['inp-email','inp-pass'].forEach(id=>document.getElementById(id).classList.add('err'));
    showErr('login-err',e.message);
  }finally{btn.disabled=false;btn.textContent='Ingresar →'}
}

async function verifyOTP(){
  hideErr('otp-err');
  const inps=document.querySelectorAll('.otp-inp');
  const code=Array.from(inps).map(i=>i.value).join('');
  if(code.length<6){showErr('otp-err','Ingresa los 6 dígitos.');return}
  const btn=document.getElementById('otp-btn');
  btn.disabled=true;btn.textContent='Verificando...';
  try{
    const d=await api('POST','/auth/otp/verify',{user_id:AUTH.userId,code});
    AUTH.token=d.token;AUTH.user=d.user;
    saveSess();enterApp();
  }catch(e){
    inps.forEach(i=>{i.style.borderColor='var(--red)';setTimeout(()=>i.style.borderColor='',800)});
    showErr('otp-err',e.message);
  }finally{btn.disabled=false;btn.textContent='Verificar →'}
}

async function resendCode(){
  try{
    await api('POST','/auth/resend',{user_id:AUTH.userId});
    document.querySelectorAll('.otp-inp').forEach(i=>{i.value='';i.classList.remove('filled')});
    const lbl=document.getElementById('otp-dest');
    const orig=lbl.textContent;
    lbl.textContent='✓ Reenviado';lbl.style.color='var(--green)';
    setTimeout(()=>{lbl.textContent=orig;lbl.style.color=''},3000);
    const f=document.querySelector('.otp-inp');if(f)f.focus();
  }catch(e){showErr('otp-err','No se pudo reenviar: '+e.message)}
}
function backLogin(){authStep('st-login')}

// OTP keyboard logic
document.querySelectorAll('.otp-inp').forEach((inp,i,arr)=>{
  inp.addEventListener('input',e=>{
    const v=e.target.value.replace(/\D/g,'');e.target.value=v.slice(-1);
    e.target.classList.toggle('filled',!!e.target.value);
    if(v&&i<arr.length-1)arr[i+1].focus();
    if(Array.from(arr).every(x=>x.value))setTimeout(verifyOTP,80);
  });
  inp.addEventListener('keydown',e=>{if(e.key==='Backspace'&&!inp.value&&i>0)arr[i-1].focus()});
  inp.addEventListener('paste',e=>{
    e.preventDefault();
    const p=(e.clipboardData||window.clipboardData).getData('text').replace(/\D/g,'').slice(0,6);
    p.split('').forEach((ch,j)=>{if(arr[j]){arr[j].value=ch;arr[j].classList.add('filled')}});
    if(p.length===6)setTimeout(verifyOTP,80);
  });
});
document.getElementById('inp-pass').addEventListener('keydown',e=>{if(e.key==='Enter')doLogin()});
document.getElementById('inp-email').addEventListener('keydown',e=>{if(e.key==='Enter')document.getElementById('inp-pass').focus()});

// ════ ENTER APP ════
function enterApp(){
  document.getElementById('auth-screen').style.display='none';
  document.getElementById('app').classList.add('vis');
  const u=AUTH.user;
  document.getElementById('nav-av').textContent=u.initials||u.name[0];
  document.getElementById('nav-un').textContent=u.email.split('@')[0];
  if(u.role!=='admin'){
    document.getElementById('sn-usuarios').style.display='none';
  }
  showPg('terceros');
}

async function doLogout(){
  try{await api('POST','/auth/logout',{},AUTH.token)}catch(e){}
  clearSess();
  document.getElementById('app').classList.remove('vis');
  document.getElementById('auth-screen').style.display='flex';
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
  if(loadSess()){
    try{
      await api('GET','/auth/me',null,AUTH.token);
      enterApp();
    }catch(e){
      clearSess();
    }
  }
});
document.addEventListener('keydown',e=>{if(e.key==='Escape')closeEdit()});