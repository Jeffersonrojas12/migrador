async function loadUsers(){
  const tb=document.getElementById('users-tb');if(!tb)return;
  try{
    const users=await api('GET','/users',null,AUTH.token);
    if(!users.length){tb.innerHTML='<tr><td colspan="8" style="text-align:center;color:var(--t3);padding:20px;font-family:var(--mono);font-size:10px">Sin usuarios</td></tr>';return}
    tb.innerHTML=users.map(u=>`<tr>
      <td><div style="width:28px;height:28px;background:linear-gradient(135deg,var(--accent),var(--accent2));border-radius:50%;display:flex;align-items:center;justify-content:center;font-family:var(--mono);font-size:9px;font-weight:700;color:#fff">${u.initials}</div></td>
      <td style="font-weight:600">${u.name}</td>
      <td style="color:var(--t2);font-family:var(--mono);font-size:10px">${u.email}</td>
      <td style="font-family:var(--mono);font-size:10px">${u.phone||'—'}</td>
      <td><span class="badge ${u.role==='admin'?'b-admin':'b-user'}">${u.role==='admin'?'Admin':'Usuario'}</span></td>
      <td><span class="dot ${u.active?'dot-on':'dot-off'}"></span>${u.active?'Activo':'Inactivo'}</td>
      <td style="font-size:10px;color:var(--t3);font-family:var(--mono)">${(u.last_login||'').substring(0,16)||'—'}</td>
      <td><div class="act-row">
        <button class="ib" onclick="openEdit(${u.id})" title="Editar">✏️</button>
        <button class="ib del" onclick="deactivateUser(${u.id},'${u.email}')" ${u.id===AUTH.user.id?'disabled':''} title="Desactivar">🗑</button>
      </div></td>
    </tr>`).join('');
  }catch(e){tb.innerHTML=`<tr><td colspan="8" style="color:var(--red);text-align:center;padding:16px;font-family:var(--mono);font-size:10px">Error: ${e.message}</td></tr>`}
}

async function loadMigs(){
  const tb=document.getElementById('mig-tb');if(!tb)return;
  try{
    const migs=await api('GET','/migrations',null,AUTH.token);
    if(!migs.length){tb.innerHTML='<tr><td colspan="7" style="text-align:center;color:var(--t3);padding:20px;font-family:var(--mono);font-size:10px">Sin migraciones</td></tr>';return}
    tb.innerHTML=migs.slice(0,50).map(m=>`<tr>
      <td style="font-family:var(--mono);font-size:10px;color:var(--t2)">${m.user_email||m.user_id}</td>
      <td><span class="badge b-mod">${m.module||'—'}</span></td>
      <td style="font-family:var(--mono);font-size:10px">${m.records_in||0} → ${m.records_out||0}</td>
      <td style="font-family:var(--mono);font-size:10px;color:${(m.errors||0)>0?'var(--red)':'var(--t3)'}">${m.errors||0}e / ${m.warnings||0}w</td>
      <td style="font-family:var(--mono);font-size:10px">${m.duration_sec?m.duration_sec.toFixed(1)+'s':'—'}</td>
      <td><span class="badge ${m.status==='completed'?'b-done':'b-err'}">${m.status}</span></td>
      <td style="font-size:10px;color:var(--t3);font-family:var(--mono)">${(m.created_at||'').substring(0,16)}</td>
    </tr>`).join('');
  }catch(e){tb.innerHTML=`<tr><td colspan="7" style="color:var(--red);text-align:center;padding:16px;font-family:var(--mono);font-size:10px">Error: ${e.message}</td></tr>`}
}

function toggleNuForm(){
  const f=document.getElementById('nu-form');
  f.style.display=f.style.display==='none'?'block':'none';
}

async function createUser(){
  const name=document.getElementById('nu-name').value.trim();
  const email=document.getElementById('nu-email').value.trim();
  const pass=document.getElementById('nu-pass').value;
  const phone=document.getElementById('nu-phone').value.trim();
  const role=document.getElementById('nu-role').value;
  const errEl=document.getElementById('nu-err');errEl.classList.add('hide');
  if(!name||!email||!pass){document.getElementById('nu-errm').textContent='Nombre, email y contraseña son requeridos.';errEl.classList.remove('hide');return}
  try{
    await api('POST','/users',{name,email,password:pass,phone,role},AUTH.token);
    ['nu-name','nu-email','nu-pass','nu-phone'].forEach(id=>document.getElementById(id).value='');
    document.getElementById('nu-form').style.display='none';
    await loadUsers();
  }catch(e){document.getElementById('nu-errm').textContent=e.message;errEl.classList.remove('hide')}
}

let editUID=null;
async function openEdit(id){
  try{
    const users=await api('GET','/users',null,AUTH.token);
    const u=users.find(x=>x.id===id);if(!u)return;
    editUID=id;
    document.getElementById('eu-name').value=u.name;
    document.getElementById('eu-phone').value=u.phone||'';
    document.getElementById('eu-role').value=u.role;
    document.getElementById('eu-active').value=String(u.active);
    document.getElementById('eu-pass').value='';
    document.getElementById('eu-email-lbl').textContent=u.email;
    document.getElementById('eu-err').classList.add('hide');
    document.getElementById('edit-modal').classList.add('vis');
  }catch(e){alert(e.message)}
}
function closeEdit(){document.getElementById('edit-modal').classList.remove('vis');editUID=null}
async function saveEdit(){
  if(!editUID)return;
  const body={name:document.getElementById('eu-name').value.trim(),phone:document.getElementById('eu-phone').value.trim(),
    role:document.getElementById('eu-role').value,active:parseInt(document.getElementById('eu-active').value)};
  const pass=document.getElementById('eu-pass').value;if(pass)body.password=pass;
  const errEl=document.getElementById('eu-err');errEl.classList.add('hide');
  try{
    await api('PUT','/users/'+editUID,body,AUTH.token);
    closeEdit();await loadUsers();
  }catch(e){document.getElementById('eu-errm').textContent=e.message;errEl.classList.remove('hide')}
}
async function deactivateUser(id,email){
  if(!confirm('¿Desactivar usuario '+email+'?'))return;
  try{await api('DELETE','/users/'+id,null,AUTH.token);await loadUsers()}
  catch(e){alert(e.message)}
}