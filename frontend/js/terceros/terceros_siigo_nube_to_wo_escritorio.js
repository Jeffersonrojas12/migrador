// ══════════════════════════════════════════════════════════════════
// ETL: Terceros — Siigo Nube → World Office Escritorio
// Módulo: terceros_siigo_nube_to_wo_escritorio.js
// ══════════════════════════════════════════════════════════════════

// ── Estado ETL Escritorio ─────────────────────────────────────────
const ESC_S = {orig:'',dest:'',mod:'',files:{}};
let ESC_WB = null;
const ESC_LOG = [];
const ESC_EXCLUDED = [];

// ── Ciudades WO Escritorio ────────────────────────────────────────
const ESC_CIUDADES = ["Abejorral", "Abrego", "Abriaquí", "Acacías", "Acandí", "Acevedo", "Achí", "Agrado", "Agua De Dios", "Aguachica", "Aguada", "Aguadas", "Aguazul", "Agustín Codazzi", "Aipe", "Albán", "Albania", "Alcalá", "Aldana", "Alejandría", "Algarrobo", "Algeciras", "Almaguer", "Almeida", "Alpujarra", "Altamira", "Alto Baudo", "Altos Del Rosario", "Alvarado", "Amagá", "Amalfi", "Ambalema", "Anapoima", "Ancuyá", "Andalucía", "Andes", "Angelópolis", "Angostura", "Anolaima", "Anorí", "Anserma", "Ansermanuevo", "Anza", "Anzoátegui", "Apartadó", "Apía", "Apulo", "Aquitania", "Aracataca", "Aranzazu", "Aratoca", "Arauca", "Arauquita", "Arbeláez", "Arboleda", "Arboledas", "Arboletes", "Arcabuco", "Arenal", "Argelia", "Ariguaní", "Arjona", "Armenia", "Armero", "Arroyohondo", "Astrea", "Ataco", "Atrato", "Ayapel", "Bagadó", "Bahía Solano", "Bajo Baudó", "Balboa", "Baranoa", "Baraya", "Barbacoas", "Barbosa", "Barichara", "Barranca De Upía", "Barrancabermeja", "Barrancas", "Barranco De Loba", "Barranco Minas", "Barranquilla", "Becerril", "Belalcázar", "Belén", "Belén De Bajirá", "Belén De Los Andaquies", "Belén De Umbría", "Bello", "Belmira", "Beltrán", "Berbeo", "Betania", "Betéitiva", "Betulia", "Bituima", "Boavita", "Bochalema", "Bogota D.C.", "Bojacá", "Bojaya", "Bolívar", "Bosconia", "Boyacá", "Briceño", "Bucaramanga", "Bucarasica", "Buenaventura", "Buenavista", "Buenos Aires", "Buesaco", "Bugalagrande", "Buriticá", "Busbanzá", "Cabrera", "Cabuyaro", "Cacahual", "Cáceres", "Cachipay", "Cachirá", "Cácota", "Caicedo", "Caicedonia", "Caimito", "Cajamarca", "Cajibío", "Cajicá", "Calamar", "Calarca", "Caldas", "Caldono", "Cali", "California", "Calima", "Caloto", "Campamento", "Campo De La Cruz", "Campoalegre", "Campohermoso", "Canalete", "Candelaria", "Cantagallo", "Cañasgordas", "Caparrapí", "Capitanejo", "Caqueza", "Caracolí", "Caramanta", "Carcasí", "Carepa", "Carmen De Apicalá", "Carmen De Carupa", "Carmen Del Darien", "Carolina", "Cartagena", "Cartagena Del Chairá", "Cartago", "Caruru", "Casabianca", "Castilla La Nueva", "Caucasia", "Cepitá", "Cereté", "Cerinza", "Cerrito", "Cerro San Antonio", "Cértegui", "Chachagüí", "Chaguaní", "Chalán", "Chameza", "Chaparral", "Charalá", "Charta", "Chía", "Chibolo", "Chigorodó", "Chima", "Chimá", "Chimichagua", "Chinácota", "Chinavita", "Chinchiná", "Chinú", "Chipaque", "Chipatá", "Chiquinquirá", "Chíquiza", "Chiriguaná", "Chiscas", "Chita", "Chitagá", "Chitaraque", "Chivatá", "Chivor", "Choachí", "Chocontá", "Cicuco", "Ciénaga", "Ciénaga De Oro", "Ciénega", "Cimitarra", "Circasia", "Cisneros", "Ciudad Bolívar", "Clemencia", "Cocorná", "Coello", "Cogua", "Colombia", "Colón", "Coloso", "Cómbita", "Concepción", "Concordia", "Condoto", "Confines", "Consaca", "Contadero", "Contratación", "Convención", "Copacabana", "Coper", "Córdoba", "Corinto", "Coromoro", "Corozal", "Corrales", "Cota", "Cotorra", "Covarachía", "Coveñas", "Coyaima", "Cravo Norte", "Cuaspud", "Cubará", "Cubarral", "Cucaita", "Cucunubá", "Cúcuta", "Cucutilla", "Cuítiva", "Cumaral", "Cumaribo", "Cumbal", "Cumbitara", "Cunday", "Curillo", "Curití", "Curumaní", "Dabeiba", "Dagua", "Dibulla", "Distracción", "Dolores", "Don Matías", "Dosquebradas", "Duitama", "Durania", "Ebéjico", "El Águila", "El Bagre", "El Banco", "El Cairo", "El Calvario", "El Cantón Del San Pablo", "El Carmen", "El Carmen De Atrato", "El Carmen De Bolívar", "El Carmen De Chucurí", "El Carmen De Viboral", "El Castillo", "El Cerrito", "El Charco", "El Cocuy", "El Colegio", "El Copey", "El Doncello", "El Dorado", "El Dovio", "El Encanto", "El Espino", "El Guacamayo", "El Guamo", "El Litoral Del San Juan", "El Molino", "El Paso", "El Paujil", "El Peñol", "El Peñón", "El Piñon", "El Playón", "El Retén", "El Retorno", "El Roble", "El Rosal", "El Rosario", "El Santuario", "El Tablón De Gómez", "El Tambo", "El Tarra", "El Zulia", "Elías", "Encino", "Enciso", "Entrerrios", "Envigado", "Espinal", "Facatativá", "Falan", "Filadelfia", "Filandia", "Firavitoba", "Flandes", "Florencia", "Floresta", "Florián", "Florida", "Floridablanca", "Fomeque", "Fonseca", "Fortul", "Fosca", "Francisco Pizarro", "Fredonia", "Fresno", "Frontino", "Fuente De Oro", "Fundación", "Funes", "Funza", "Fúquene", "Fusagasugá", "Gachala", "Gachancipá", "Gachantivá", "Gachetá", "Galán", "Galapa", "Galeras", "Gama", "Gamarra", "Gambita", "Gameza", "Garagoa", "Garzón", "Génova", "Gigante", "Ginebra", "Giraldo", "Girardot", "Girardota", "Girón", "Gómez Plata", "González", "Gramalote", "Granada", "Guaca", "Guacamayas", "Guacarí", "Guachetá", "Guachucal", "Guadalajara De Buga", "Guadalupe", "Guaduas", "Guaitarilla", "Gualmatán", "Guamal", "Guamo", "Guapi", "Guapotá", "Guaranda", "Guarne", "Guasca", "Guatape", "Guataquí", "Guatavita", "Guateque", "Guática", "Guavatá", "Guayabal De Siquima", "Guayabetal", "Guayatá", "Güepsa", "Güicán", "Gutiérrez", "Hacarí", "Hatillo De Loba", "Hato", "Hato Corozal", "Hatonuevo", "Heliconia", "Herrán", "Herveo", "Hispania", "Hobo", "Honda", "Ibagué", "Icononzo", "Iles", "Imués", "Inírida", "Inzá", "Ipiales", "Iquira", "Isnos", "Istmina", "Itagui", "Ituango", "Iza", "Jambaló", "Jamundí", "Jardín", "Jenesano", "Jericó", "Jerusalén", "Jesús María", "Jordán", "Juan De Acosta", "Junín", "Juradó", "La Apartada", "La Argentina", "La Belleza", "La Calera", "La Capilla", "La Ceja", "La Celia", "La Chorrera", "La Cruz", "La Cumbre", "La Dorada", "La Esperanza", "La Estrella", "La Florida", "La Gloria", "La Guadalupe", "La Jagua De Ibirico", "La Jagua Del Pilar", "La Llanada", "La Macarena", "La Merced", "La Mesa", "La Montañita", "La Palma", "La Paz", "La Pedrera", "La Peña", "La Pintada", "La Plata", "La Playa", "La Primavera", "La Salina", "La Sierra", "La Tebaida", "La Tola", "La Unión", "La Uvita", "La Vega", "La Victoria", "La Virginia", "Labateca", "Labranzagrande", "Landázuri", "Lebríja", "Leguízamo", "Leiva", "Lejanías", "Lenguazaque", "Lérida", "Leticia", "Líbano", "Liborina", "Linares", "Lloró", "López", "Lorica", "Los Andes", "Los Córdobas", "Los Palmitos", "Los Patios", "Los Santos", "Lourdes", "Luruaco", "Macanal", "Macaravita", "Maceo", "Macheta", "Madrid", "Magangué", "Magüi", "Mahates", "Maicao", "Majagual", "Málaga", "Malambo", "Mallama", "Manatí", "Manaure", "Maní", "Manizales", "Manta", "Manzanares", "Mapiripán", "Mapiripana", "Margarita", "María La Baja", "Marinilla", "Maripí", "Mariquita", "Marmato", "Marquetalia", "Marsella", "Marulanda", "Matanza", "Medellín", "Medina", "Medio Atrato", "Medio Baudó", "Medio San Juan", "Melgar", "Mercaderes", "Mesetas", "Milán", "Miraflores", "Miranda", "Miriti - Paraná", "Mistrató", "Mitú", "Mocoa", "Mogotes", "Molagavita", "Momil", "Mompós", "Mongua", "Monguí", "Moniquirá", "Montebello", "Montecristo", "Montelíbano", "Montenegro", "Montería", "Monterrey", "Moñitos", "Morales", "Morelia", "Morichal", "Morroa", "Mosquera", "Motavita", "Murillo", "Murindó", "Mutatá", "Mutiscua", "Muzo", "Nariño", "Nátaga", "Natagaima", "Nechí", "Necoclí", "Neira", "Neiva", "Nemocón", "Nilo", "Nimaima", "Nobsa", "Nocaima", "Norcasia", "Nóvita", "Nueva Granada", "Nuevo Colón", "Nunchía", "Nuquí", "Obando", "Ocamonte", "Ocaña", "Oiba", "Oicatá", "Olaya", "Olaya Herrera", "Onzaga", "Oporapa", "Orito", "Orocué", "Ortega", "Ospina", "Otanche", "Ovejas", "Pachavita", "Pacho", "Pacoa", "Pácora", "Padilla", "Paez", "Páez", "Paicol", "Pailitas", "Paime", "Paipa", "Pajarito", "Palermo", "Palestina", "Palmar", "Palmar De Varela", "Palmas Del Socorro", "Palmira", "Palmito", "Palocabildo", "Pamplona", "Pamplonita", "Pana Pana", "Pandi", "Panqueba", "Papunaua", "Páramo", "Paratebueno", "Pasca", "Pasto", "Patía", "Pauna", "Paya", "Paz De Ariporo", "Paz De Río", "Pedraza", "Pelaya", "Pensilvania", "Peñol", "Peque", "Pereira", "Pesca", "Piamonte", "Piedecuesta", "Piedras", "Piendamó", "Pijao", "Pijiño Del Carmen", "Pinchote", "Pinillos", "Piojó", "Pisba", "Pital", "Pitalito", "Pivijay", "Planadas", "Planeta Rica", "Plato", "Policarpa", "Polonuevo", "Ponedera", "Popayán", "Pore", "Potosí", "Pradera", "Prado", "Providencia", "Pueblo Bello", "Pueblo Nuevo", "Pueblo Rico", "Pueblorrico", "Puebloviejo", "Puente Nacional", "Puerres", "Puerto Alegría", "Puerto Arica", "Puerto Asís", "Puerto Berrío", "Puerto Boyacá", "Puerto Caicedo", "Puerto Carreño", "Puerto Colombia", "Puerto Concordia", "Puerto Escondido", "Puerto Gaitán", "Puerto Guzmán", "Puerto Libertador", "Puerto Lleras", "Puerto López", "Puerto Nare", "Puerto Nariño", "Puerto Parra", "Puerto Rico", "Puerto Rondón", "Puerto Salgar", "Puerto Santander", "Puerto Tejada", "Puerto Triunfo", "Puerto Wilches", "Pulí", "Pupiales", "Puracé", "Purificación", "Purísima", "Quebradanegra", "Quetame", "Quibdó", "Quimbaya", "Quinchía", "Quípama", "Quipile", "Ragonvalia", "Ramiriquí", "Ráquira", "Recetor", "Regidor", "Remedios", "Remolino", "Repelón", "Restrepo", "Retiro", "Ricaurte", "Río De Oro", "Río Iro", "Río Quito", "Río Viejo", "Rioblanco", "Riofrío", "Riohacha", "Rionegro", "Riosucio", "Risaralda", "Rivera", "Roberto Payán", "Roldanillo", "Roncesvalles", "Rondón", "Rosas", "Rovira", "Sabana De Torres", "Sabanagrande", "Sabanalarga", "Sabanas De San Angel", "Sabaneta", "Saboyá", "Sácama", "Sáchica", "Sahagún", "Saladoblanco", "Salamina", "Salazar", "Saldaña", "Salento", "Salgar", "Samacá", "Samaná", "Samaniego", "Sampués", "San Agustín", "San Alberto", "San Andrés", "San Andrés Sotavento", "San Antero", "San Antonio", "San Antonio Del Tequendama", "San Benito", "San Benito Abad", "San Bernardo", "San Bernardo Del Viento", "San Calixto", "San Carlos", "San Carlos De Guaroa", "San Cayetano", "San Cristóbal", "San Diego", "San Eduardo", "San Estanislao", "San Felipe", "San Fernando", "San Francisco", "San Gil", "San Jacinto", "San Jacinto Del Cauca", "San Jerónimo", "San Joaquín", "San José", "San José De La Montaña", "San José De Miranda", "San José De Pare", "San José Del Fragua", "San José Del Guaviare", "San José Del Palmar", "San Juan De Arama", "San Juan De Betulia", "San Juan De Río Seco", "San Juan De Urabá", "San Juan Del Cesar", "San Juan Nepomuceno", "San Juanito", "San Lorenzo", "San Luis", "San Luis De Gaceno", "San Luis De Palenque", "San Marcos", "San Martín", "San Martín De Loba", "San Mateo", "San Miguel", "San Miguel De Sema", "San Onofre", "San Pablo", "San Pablo De Borbur", "San Pedro", "San Pedro De Cartago", "San Pedro De Uraba", "San Pelayo", "San Rafael", "San Roque", "San Sebastián", "San Sebastián De Buenavista", "San Vicente", "San Vicente De Chucurí", "San Vicente Del Caguán", "San Zenón", "Sandoná", "Santa Ana", "Santa Bárbara", "Santa Bárbara De Pinto", "Santa Catalina", "Santa Helena Del Opón", "Santa Isabel", "Santa Lucía", "Santa María", "Santa Marta", "Santa Rosa", "Santa Rosa De Cabal", "Santa Rosa De Osos", "Santa Rosa De Viterbo", "Santa Rosa Del Sur", "Santa Rosalía", "Santa Sofía", "Santacruz", "Santafé De Antioquia", "Santana", "Santander De Quilichao", "Santiago", "Santiago De Tolú", "Santo Domingo", "Santo Tomás", "Santuario", "Sapuyes", "Saravena", "Sardinata", "Sasaima", "Sativanorte", "Sativasur", "Segovia", "Sesquilé", "Sevilla", "Siachoque", "Sibaté", "Sibundoy", "Silos", "Silvania", "Silvia", "Simacota", "Simijaca", "Simití", "Sincé", "Sincelejo", "Sipí", "Sitionuevo", "Soacha", "Soatá", "Socha", "Socorro", "Socotá", "Sogamoso", "Solano", "Soledad", "Solita", "Somondoco", "Sonson", "Sopetrán", "Soplaviento", "Sopó", "Sora", "Soracá", "Sotaquirá", "Sotara", "Suaita", "Suan", "Suárez", "Suaza", "Subachoque", "Sucre", "Suesca", "Supatá", "Supía", "Suratá", "Susa", "Susacón", "Sutamarchán", "Sutatausa", "Sutatenza", "Tabio", "Tadó", "Talaigua Nuevo", "Tamalameque", "Támara", "Tame", "Támesis", "Taminango", "Tangua", "Taraira", "Tarapacá", "Tarazá", "Tarqui", "Tarso", "Tasco", "Tauramena", "Tausa", "Tello", "Tena", "Tenerife", "Tenjo", "Tenza", "Teorama", "Teruel", "Tesalia", "Tibacuy", "Tibaná", "Tibasosa", "Tibirita", "Tibú", "Tierralta", "Timaná", "Timbío", "Timbiquí", "Tinjacá", "Tipacoque", "Tiquisio", "Titiribí", "Toca", "Tocaima", "Tocancipá", "Togüí", "Toledo", "Tolú Viejo", "Tona", "Tópaga", "Topaipí", "Toribio", "Toro", "Tota", "Totoró", "Trinidad", "Trujillo", "Tubará", "Tuluá", "Tumaco", "Tunja", "Tununguá", "Túquerres", "Turbaco", "Turbaná", "Turbo", "Turmequé", "Tuta", "Tutazá", "Ubalá", "Ubaque", "Ulloa", "Umbita", "Une", "Unguía", "Unión Panamericana", "Uramita", "Uribe", "Uribia", "Urrao", "Urumita", "Usiacurí", "Útica", "Valdivia", "Valencia", "Valle De San José", "Valle De San Juan", "Valle Del Guamuez", "Valledupar", "Valparaíso", "Vegachí", "Vélez", "Venadillo", "Venecia", "Ventaquemada", "Vergara", "Versalles", "Vetas", "Vianí", "Victoria", "Vigía Del Fuerte", "Vijes", "Villa Caro", "Villa De Leyva", "Villa De San Diego De Ubate", "Villa Del Rosario", "Villa Rica", "Villagarzón", "Villagómez", "Villahermosa", "Villamaría", "Villanueva", "Villapinzón", "Villarrica", "Villavicencio", "Villavieja", "Villeta", "Viotá", "Viracachá", "Vistahermosa", "Viterbo", "Yacopí", "Yacuanquer", "Yaguará", "Yalí", "Yarumal", "Yavaraté", "Yolombó", "Yondó", "Yopal", "Yotoco", "Yumbo", "Zambrano", "Zapatoca", "Zapayán", "Zaragoza", "Zarzal", "Zetaquira", "Zipacón", "Zipaquirá", "Zona Bananera"];

// ── Mapeo Tipo Identificación ─────────────────────────────────────
const ESC_TIPO_ID_MAP = {
  'tarjeta de identidad':'TI',
  'registro civil':'REGISTRO CIVIL',
  'cédula de ciudadanía':'CC',
  'cedula de ciudadania':'CC',
  'tarjeta de extranjería':'TE',
  'cédula de extranjería':'Cédula de extranjería',
  'nit':'NIT',
  'pasaporte':'PASAPORTE',
  'documento de identificación extranjero':'Documento de identificación extranjero',
  'nuip':'NUIP',
  'permiso especial de permanencia pep':'Permiso especial de permanencia',
  'permiso protección temporal ppt':'Permiso especial de permanencia',
  'sin identificación del exterior o para uso definido por la dian':'Sin identificación del exterior o para uso definido por la DIAN',
  'nit de otro país / sin identificación del exterior (43 medios magnéticos)':'Documento de Identificación extranjero Persona Jurídica',
  'salvoconducto de permanencia':'OTRO'
};

// ── Mapeo Régimen Fiscal → Propiedad Retención ────────────────────
const ESC_REGIMEN_MAP = {
  'responsable de iva':'Persona Natural Responsable del IVA',
  'no responsable de iva':'Persona Natural No Responsable del IVA'
};

const ESC_NITS_EXCLUIR = new Set(["222222222","800003122","800037800","800088702","800112806","800118954","800130907","800138188","800140949","800147502","800148514","800149496","800197268","800211025","800216278","800219488","800224808","800226175","800227940","800229739","800231969","800251440","800253055","800256161","804002105","805000427","805001157","806008394","809008362","817001773","824001398","830003564","830008686","830009783","830054904","830074184","830113831","830125132","837000084","839000495","844003392","860002183","860002503","860002964","860003020","860007335","860007336","860007379","860007738","860008645","860011153","860013570","860022137","860034313","860034594","860035827","860043186","860045904","860050750","860051135","860066942","860503617","890000381","890101994","890102002","890102044","890102257","890200106","890200756","890201578","890203088","890203183","890270275","890300279","890303093","890303208","890399010","890480023","890480110","890480123","890500516","890500675","890700148","890704737","890806490","890900840","890900841","890900842","890903790","890903937","890903938","890904996","890980040","891080005","891080031","891180008","891190047","891200337","891280008","891480000","891500182","891500319","891600091","891780093","891800213","891800330","891856000","892000146","892115006","892200015","892399989","892400320","899999001","899999034","899999061","899999063","899999107","899999284","899999734","900156264","900200960","900226715","900298372","900336004","900406150","900604350","900914254","900935126","901037916","901093846","901469580","901543761"]);

// ── Columnas salida (plantilla WO Escritorio) ─────────────────────
const ESC_COLS = [
  'Tipo Identificación','No. Identificación','Ciudad Identificación',
  '1er. Nombre o Razón Social','2do. Nombre','1re. Apellido','2do.Apellido',
  'Propiedad Activa','Activo','Propiedad Retención','Fecha Creación',
  'Plazo','Clasificación Dian','Actividad Económica','Matricula',
  'Tipos_Responsabilidades','Aplica ReteIca','% Ica','Maneja Cupo Crédito',
  'Cupo Crédito','Código','Fecha Aniversario','Forma de Pago','Lista Precios',
  'Nota','% Descuento','Vendedor','Clasificación Uno','Clasificación Dos',
  'Clasificación Tres','Zona Uno','Zona Dos',
  'Personalizado 1','Personalizado 2','Personalizado 3','Personalizado 4',
  'Personalizado 5','Personalizado 6','Personalizado 7','Personalizado 8',
  'Personalizado 9','Personalizado 10','Personalizado 11','Personalizado 12',
  'Personalizado 13','Personalizado 14','Personalizado 15',
  'Tipo Dirección','Ciudad Dirección','Dirección','Dirección Principal',
  'Teléfonos','Código Postal','Fax','Movil 1','Movil 2',
  'E_Mail','E_Mail 2','E_Mail 3','Página Web','Observaciones','Sucursal'
];

// ── Helpers ───────────────────────────────────────────────────────
function escNormId(x){
  const s=String(x??'').replace(/[,\s]/g,'').trim();
  if(!s||s==='nan'||s==='undefined')return '';
  return s.replace(/[^0-9A-Za-z\-]/g,'');
}
function escMapTipoId(raw, idNum){
  const k=String(raw||'').toLowerCase().trim();
  if(!k){
    // Inferir por longitud del número: NIT tiene 9 dígitos, CC entre 6-10
    const n=String(idNum||'').replace(/\D/g,'');
    if(n.length===9)return 'NIT';
    return 'CC';
  }
  return ESC_TIPO_ID_MAP[k]||String(raw||'');
}
function escIsNIT(t){return String(t||'').toLowerCase().includes('nit');}
// NIT empresa: 9 dígitos que inicia en 8 o 9
function escNitEsEmpresa(idNum){
  const n=String(idNum||'').replace(/[^0-9]/g,'');
  return n.length===9 && (n[0]==='8'||n[0]==='9');
}
function escLimpiarParte(p){
  // Eliminar si es solo un caracter especial, numero o simbolo
  if(!p) return '';
  // Si tiene 1 caracter y no es letra: descartar
  if(p.length===1&&!/[A-Za-zÁÉÍÓÚÑáéíóúñ]/i.test(p)) return '';
  // Si termina en caracter especial/numero, quitarlo
  return p.replace(/[^A-Za-zÁÉÍÓÚÑáéíóúñ]+$/, '').trim();
}
function escSplitName(nombre){
  const parts=String(nombre||'').trim().toUpperCase().split(/\s+/).map(escLimpiarParte).filter(Boolean);
  const conectores=new Set(['DE','DEL','LA','LAS','LOS','Y','VAN','VON']);
  const out={p1:'',p2:'',a1:'',a2:''};
  if(parts.length===0) return out;
  if(parts.length===1){out.p1=parts[0];return out;}
  if(parts.length===2){out.p1=parts[0];out.a1=parts[1];return out;}
  if(parts.length===3){out.p1=parts[0];out.a1=parts[1];out.a2=parts[2];return out;}
  // 4+ parts
  let i=0;
  out.p1=parts[i++];
  if(i<parts.length-2&&!conectores.has(parts[i]))out.p2=parts[i++];
  out.a1=parts[i++]||'';
  out.a2=parts.slice(i).join(' ')||'';
  return out;
}
function escNormCiudad(raw){
  if(!raw)return 'Bogota D.C.';
  const s=String(raw).trim();
  // exact match
  const exact=ESC_CIUDADES.find(c=>c.toLowerCase()===s.toLowerCase());
  if(exact)return exact;
  // starts with
  const starts=ESC_CIUDADES.find(c=>c.toLowerCase().startsWith(s.toLowerCase().substring(0,5)));
  if(starts)return starts;
  // contains
  const contains=ESC_CIUDADES.find(c=>c.toLowerCase().includes(s.toLowerCase().substring(0,5)));
  if(contains)return contains;
  return 'Bogota D.C.';
}
function escNormTel(x){
  if(!x)return '6050000000';
  const d=String(x).replace(/\D/g,'');
  return d||'6050000000';
}
function escNormAddr(x){
  if(!x)return 'DIRECCION NO INFORMADA';
  let s=String(x).toUpperCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g,'')
    .replace(/[#°/.,\-()"']/g,' ')
    .replace(/\s+/g,' ').trim();
  return s||'DIRECCION NO INFORMADA';
}
function escMapRegimen(regimen, tipoId, idNum){
  const esNIT=escIsNIT(tipoId)&&escNitEsEmpresa(idNum);
  if(esNIT)return 'Persona Juridica';
  const k=String(regimen||'').toLowerCase().trim();
  return ESC_REGIMEN_MAP[k]||'Persona Juridica';
}
function escMapActivo(estado){
  const s=String(estado||'').toLowerCase().trim();
  if(s==='activo'||s==='active'||s==='1'||s==='true')return -1;
  if(s==='inactivo'||s==='inactive'||s==='0'||s==='false')return 0;
  return -1;
}
function escFechaHoy(){
  const d=new Date();
  const dd=String(d.getDate()).padStart(2,'0');
  const mm=String(d.getMonth()+1).padStart(2,'0');
  const yyyy=d.getFullYear();
  return `${dd}/${mm}/${yyyy}`;
}
function escLog(msg,lvl='i',fase=''){
  const ts=new Date().toISOString();
  ESC_LOG.push({ts,fase,lvl,msg});
  const panel=document.getElementById('logp');
  if(!panel)return;
  const now=new Date().toLocaleTimeString('es-CO',{hour12:false});
  const css={i:'li',w:'lw',o:'lo',e:'le-e'}[lvl]||'li';
  panel.innerHTML+=`<div class="le"><span class="lt">${now}</span><span class="${css}">${msg}</span></div>`;
  panel.scrollTop=panel.scrollHeight;
}
function escSleep(ms){return new Promise(r=>setTimeout(r,ms));}

// setStep/setPStep/setPct defined in terceros_siigo_nube_to_wo_cloud.js

// ── File input ────────────────────────────────────────────────────
function escOnFile(input,slotId,nameId,key){
  const f=input.files[0];if(!f)return;
  const slot=document.getElementById(slotId);
  const nm=document.getElementById(nameId);
  if(slot)slot.className='fslot ok';
  if(nm)nm.textContent=f.name;
  ESC_S.files[key]=f;
}
function escGoStart(){
  setStep(2);
  const el=document.getElementById('esc-fn');
  if(el)el.textContent='';
}
function escGoBack(){setStep(1);}
function escReset(){
  ESC_S.files={};ESC_LOG.length=0;ESC_EXCLUDED.length=0;ESC_WB=null;
  resetAll();
  return;

  setStep(1);
}

// ── Leer maestro ──────────────────────────────────────────────────
async function escReadMaestro(file){
  return new Promise((resolve,reject)=>{
    const reader=new FileReader();
    reader.onload=e=>{
      try{
        const data=new Uint8Array(e.target.result);
        const wb=XLSX.read(data,{type:'array',sheetStubs:true,cellText:true,raw:false});
        const ws=wb.Sheets[wb.SheetNames[0]];
        // Compute real range
        let maxR=0,maxC=0;
        Object.keys(ws).forEach(k=>{
          if(k[0]==='!')return;
          const m=k.match(/^([A-Z]+)(\d+)$/);
          if(!m)return;
          let c=0;for(let i=0;i<m[1].length;i++)c=c*26+m[1].charCodeAt(i)-64;
          maxR=Math.max(maxR,parseInt(m[2]));
          maxC=Math.max(maxC,c);
        });
        ws['!ref']=`A1:${XLSX.utils.encode_col(maxC-1)}${maxR}`;
        // Find header row (row with 'Identificación' or similar)
        let headerRow=7;
        for(let r=1;r<=15;r++){
          const cell=ws[XLSX.utils.encode_cell({r:r-1,c:1})];
          if(cell&&String(cell.v||'').toLowerCase().includes('identificaci')){
            headerRow=r;break;
          }
        }
        const rows=XLSX.utils.sheet_to_json(ws,{header:1,range:headerRow-1,defval:''});
        const hdrs=rows[0].map(h=>String(h||'').trim());
        const data_rows=rows.slice(1).filter(r=>r.some(v=>v!==''&&v!==null&&v!==undefined));
        resolve({hdrs,rows:data_rows});
      }catch(err){reject(err);}
    };
    reader.readAsArrayBuffer(file);
  });
}


// ── Leer archivo Empleados ────────────────────────────────────────
async function escReadEmpleados(file){
  return new Promise((resolve,reject)=>{
    const reader=new FileReader();
    reader.onload=e=>{
      try{
        const data=new Uint8Array(e.target.result);
        const wb=XLSX.read(data,{type:'array',cellText:true,raw:false});
        const ws=wb.Sheets[wb.SheetNames[0]];
        const rows=XLSX.utils.sheet_to_json(ws,{header:1,defval:''});

        // Validar: buscar en fila 2 (índice 1) la columna "Número de documento (Obligatorio)"
        const row2 = rows[1] || [];
        const tieneColumnaId = row2.some(v=>String(v).toLowerCase().includes('número de documento')||
                                            String(v).toLowerCase().includes('numero de documento'));
        if(!tieneColumnaId){
          // Archivo no tiene la estructura esperada — ignorar
          resolve({ids:new Set(),nombres:{},valido:false});
          return;
        }

        // Buscar fila de encabezado: la que contenga "Primer nombre (Obligatorio)"
        let hdrIdx=-1;
        for(let i=0;i<rows.length;i++){
          if(rows[i].some(v=>String(v).toLowerCase().includes('primer nombre'))){
            hdrIdx=i; break;
          }
        }
        if(hdrIdx<0){resolve({ids:new Set(),nombres:{},valido:false});return;}

        const hdrs=rows[hdrIdx].map(h=>String(h||'').trim());
        const idCol=hdrs.findIndex(h=>/n[uú]mero de documento/i.test(h));
        const nameCol=hdrs.findIndex(h=>/primer nombre/i.test(h));
        const ap1Col=hdrs.findIndex(h=>/primer apellido/i.test(h));

        const ids=new Set();
        const nombres={};
        for(let i=hdrIdx+1;i<rows.length;i++){
          const id=String(rows[i][idCol]||'').replace(/[^0-9A-Za-z]/g,'').trim();
          if(!id)continue;
          ids.add(id);
          const n1=String(rows[i][nameCol]||'').trim();
          const a1=ap1Col>=0?String(rows[i][ap1Col]||'').trim():'';
          nombres[id]=[n1,a1].filter(Boolean).join(' ')||id;
        }
        resolve({ids,nombres,valido:true});
      }catch(err){reject(err);}
    };
    reader.readAsArrayBuffer(file);
  });
}

// ── ETL Principal ─────────────────────────────────────────────────
async function startEscETL(){
  // Maestro comes from shared S.files set by onFile()
  const maestroFile = S.files && S.files['maestro'] ? S.files['maestro'] : ESC_S.files['maestro'];
  if(!maestroFile){
    alert('Carga el archivo maestro Búsqueda de terceros.xlsx');return;
  }
  setStep(3);
  ESC_LOG.length=0;ESC_EXCLUDED.length=0;
  const panel=document.getElementById('logp');
  if(panel)panel.innerHTML='';
  setPStep(0);setPct(0,'Iniciando...');
  const t0=Date.now();

  try{
    // ── Lectura ──────────────────────────────────────────────────
    escLog('📂 Leyendo archivos...','i','Lectura');
    setPStep(1);setPct(10,'Leyendo maestro...');
    await escSleep(30);

    const maestro=await escReadMaestro(maestroFile);
    escLog(`   Maestro: ${maestro.rows.length} registros`,'i','Lectura');

    const emailMapC={},emailMapP={};

    // ── Leer empleados (opcional) ─────────────────────────────────
    let empIds=new Set(), empNombres={};
    const empFile = S.files && S.files['empleados'] ? S.files['empleados'] : null;
    if(empFile){
      const emp=await escReadEmpleados(empFile);
      if(emp.valido){
        empIds=emp.ids; empNombres=emp.nombres;
        escLog(`   Empleados: ${empIds.size} cédulas cargadas — se agregará "Empleado;" a su propiedad`,'i','Lectura');
      } else {
        escLog('   Archivo empleados no válido: no contiene "Número de documento (Obligatorio)" en fila 2 — se ignora','w','Lectura');
      }
    }

    // ── Columnas maestro ─────────────────────────────────────────
    const H=maestro.hdrs;
    const cN  =H.find(h=>/nombre tercero/i.test(h))||'Nombre Tercero';
    const cTI =H.find(h=>/tipo de identificaci/i.test(h))||'Tipo De Identificación';
    const cID =H.find(h=>/^identificaci/i.test(h.trim()))||'Identificación';
    const cReg=H.find(h=>/regimen/i.test(h))||'Tipo De Regimen Iva';
    const cDir=H.find(h=>/^direcci/i.test(h.trim()))||'Dirección';
    const cCiu=H.find(h=>/^ciudad$/i.test(h.trim()))||'Ciudad';
    const cTel=H.find(h=>/^tel/i.test(h.trim()))||'Teléfono.';
    const cEst=H.find(h=>/^estado$/i.test(h.trim()))||'Estado';
    const cCliente=H.find(h=>/^cliente$/i.test(h.trim()))||'Cliente';
    const cProv   =H.find(h=>/^proveedor$/i.test(h.trim()))||'Proveedor';
    const cEmail  =H.find(h=>/correo electr/i.test(h))||null;
    const cSuc    =H.find(h=>/^sucursal$/i.test(h.trim()))||null;

    // ── Consolidar ───────────────────────────────────────────────
    setPStep(2);setPct(30,'Consolidando...');
    await escSleep(30);

    const seen=new Set(),out=[],defaults=[],nitExcluidos=[];
    let totalEntrada=maestro.rows.length;
    let errCount=0,warns=0;

    for(const r of maestro.rows){
      const id=escNormId(r[H.indexOf(cID)]);
      if(!id){ESC_EXCLUDED.push({id:'',nombre:String(r[H.indexOf(cN)]||''),tipo:'sin-id'});continue;}
      if(seen.has(id)){ESC_EXCLUDED.push({id,nombre:String(r[H.indexOf(cN)]||''),tipo:'duplicado'});continue;}
      seen.add(id);

      // Si está en lista de exclusión → hoja aparte
      if(ESC_NITS_EXCLUIR.has(id)){
        nitExcluidos.push({id, nombre:String(r[H.indexOf(cN)]||'').toUpperCase().trim()});
        continue;
      }

      // Si es empleado → no migrar, registrar en defaults como excluido
      if(empIds.has(id)){
        const empNom=empNombres[id]||nombre;
        defaults.push({
          'Tipo Identificación Aplicado':'CC',
          'No. Identificación':id,
          'Nombre':empNom,
          'Concepto':'Tercero no se tiene en cuenta ya que es empleado'
        });
        continue;
      }

      const tipoIdRaw=String(r[H.indexOf(cTI)]||'');
      const tipoId=escMapTipoId(tipoIdRaw, r[H.indexOf(cID)]);
      const fueDefecto=(!tipoIdRaw||!tipoIdRaw.toString().trim()); // tipo estaba vacío
      const _rawCiu=String(r[H.indexOf(cCiu)]||'').trim();
      const _rawDir=String(r[H.indexOf(cDir)]||'').trim();
      const _rawTel=String(r[H.indexOf(cTel)]||'').trim();
      const esNIT=escIsNIT(tipoIdRaw);
      const nombre=String(r[H.indexOf(cN)]||'').trim();
      const regimen=String(r[H.indexOf(cReg)]||'');
      const ciudad=escNormCiudad(_rawCiu);
      const dir=escNormAddr(_rawDir);
      const tel=escNormTel(_rawTel);
      // Track field defaults
      if(!_rawCiu||_rawCiu==='nan') defaults.push({'Tipo Identificación Aplicado':tipoId,'No. Identificación':id,'Nombre':nombre,'Concepto':'Ciudad vacía en origen — se aplica Bogota D.C. por defecto'});
      if(!_rawDir||_rawDir.length<3) defaults.push({'Tipo Identificación Aplicado':tipoId,'No. Identificación':id,'Nombre':nombre,'Concepto':'Dirección vacía en origen — se aplica DIRECCION NO INFORMADA por defecto'});
      if(!_rawTel||_rawTel==='-') defaults.push({'Tipo Identificación Aplicado':tipoId,'No. Identificación':id,'Nombre':nombre,'Concepto':'Teléfono vacío en origen — se aplica 6050000000 por defecto'});
      const activo=escMapActivo(r[H.indexOf(cEst)]);
      const retencion=escMapRegimen(regimen,tipoIdRaw,r[H.indexOf(cID)]);

      // Propiedad activa — siempre termina en ;
      let propActiva='Cliente;Proveedor;';
      if(cCliente&&cProv){
        const esC=String(r[H.indexOf(cCliente)]||'').trim().toLowerCase().startsWith('s');
        const esP=String(r[H.indexOf(cProv)]||'').trim().toLowerCase().startsWith('s');
        if(esC&&esP)propActiva='Cliente;Proveedor;';
        else if(esC)propActiva='Cliente;';
        else if(esP)propActiva='Proveedor;';
        else propActiva='Cliente;Proveedor;';
      }
      // Si el archivo de empleados fue cargado y este ID está en él → agregar Empleado;
      if(empIds.size>0 && empIds.has(id)){
        propActiva = propActiva + 'Empleado;';
      }

      // Email
      let email=cEmail?String(r[H.indexOf(cEmail)]||'').trim():'';
      if(!email)email=emailMapC[id]||emailMapP[id]||'';

      // Nombre split
      // NIT empresa (inicia 8 o 9, 9 dígitos): nombre completo en p1
      // NIT persona (otros): separar nombre como CC
      // CC/otros: separar normalmente
      let p1='',p2='',a1='',a2='';
      const tipoIdRawLow = tipoIdRaw.toLowerCase().trim();
      const isNitRaw = tipoIdRawLow.includes('nit') || tipoIdRawLow==='';
      if(isNitRaw && escNitEsEmpresa(r[H.indexOf(cID)])){
        // NIT empresa → nombre completo en primer campo (mayúsculas)
        p1=String(nombre).toUpperCase();
      } else {
        // NIT persona o CC → separar nombre
        const sn=escSplitName(nombre);p1=sn.p1;p2=sn.p2;a1=sn.a1;a2=sn.a2;
      }

      // Sucursal
      const suc=cSuc?String(r[H.indexOf(cSuc)]||'').trim():'';

      const row={
        'Tipo Identificación':tipoId,
        'No. Identificación':id,
        'Ciudad Identificación':ciudad,
        '1er. Nombre o Razón Social':p1,
        '2do. Nombre':p2||null,
        '1re. Apellido':a1||null,
        '2do.Apellido':a2||null,
        'Propiedad Activa':propActiva,
        'Activo':activo,
        'Propiedad Retención':retencion,
        'Fecha Creación':escFechaHoy(),
        'Plazo':0,
        'Clasificación Dian':'Normal',
        'Actividad Económica':null,
        'Matricula':null,
        'Tipos_Responsabilidades':null,
        'Aplica ReteIca':null,
        '% Ica':null,
        'Maneja Cupo Crédito':null,
        'Cupo Crédito':null,
        'Código':null,
        'Fecha Aniversario':null,
        'Forma de Pago':null,
        'Lista Precios':null,
        'Nota':null,
        '% Descuento':null,
        'Vendedor':null,
        'Clasificación Uno':null,'Clasificación Dos':null,'Clasificación Tres':null,
        'Zona Uno':null,'Zona Dos':null,
        'Personalizado 1':null,'Personalizado 2':null,'Personalizado 3':null,
        'Personalizado 4':null,'Personalizado 5':null,'Personalizado 6':null,
        'Personalizado 7':null,'Personalizado 8':null,'Personalizado 9':null,
        'Personalizado 10':null,'Personalizado 11':null,'Personalizado 12':null,
        'Personalizado 13':null,'Personalizado 14':null,'Personalizado 15':null,
        'Tipo Dirección':(tipoId==='NIT'&&escNitEsEmpresa(r[H.indexOf(cID)]))?'Empresa/Oficina':'Casa',
        'Ciudad Dirección':ciudad,
        'Dirección':dir,
        'Dirección Principal':-1,
        'Teléfonos':tel||null,
        'Código Postal':null,
        'Fax':null,
        'Movil 1':null,
        'Movil 2':null,
        'E_Mail':email||null,
        'E_Mail 2':null,
        'E_Mail 3':null,
        'Página Web':null,
        'Observaciones':null,
        'Sucursal':suc||null
      };
      out.push(row);
      if(fueDefecto){
        defaults.push({
          'Tipo Identificación Aplicado': tipoId,
          'No. Identificación': id,
          'Nombre': nombre,
          'Concepto': 'Dato de origen en blanco — se anexa CC por defecto'
        });
      }
    }

    escLog(`✅ Consolidados: ${out.length} registros`,'o','Consolidación');

    // ── Transformación ───────────────────────────────────────────
    setPStep(3);setPct(60,'Transformando...');
    await escSleep(30);
    escLog(`🔄 Transformados: ${out.length} registros`,'o','Transformación');

    // ── Construcción Excel ────────────────────────────────────────
    setPStep(4);setPct(80,'Generando Excel...');
    await escSleep(30);
    escLog('📊 Construyendo archivo Excel...','i','Escritura');

    if(nitExcluidos.length>0) escLog(`   ${nitExcluidos.length} NITs excluidos (lista predefinida)`,'w','Consolidación');
    ESC_WB = escBuildWB(out, ESC_LOG, {
      archivos_entrada:1,
      registros_entrada:totalEntrada,
      registros_salida:out.length,
      errores:errCount,
      warnings:warns
    }, ESC_EXCLUDED, defaults, nitExcluidos);

    const dur=((Date.now()-t0)/1000).toFixed(1);
    setPct(100,'¡Listo!');
    setPStep(5);
    escLog(`✅ Excel generado en ${dur}s — ${out.length} registros`,'o','Escritura');

    // Mostrar resultado
    const fn=escBuildFN();
    const fnEl=document.getElementById('dl-fn');
    if(fnEl)fnEl.textContent=fn;
    const stIn=document.getElementById('st-in');
    const stOk=document.getElementById('st-ok');
    if(stIn)stIn.textContent=totalEntrada;
    if(stOk)stOk.textContent=out.length;

    // Log backend
    try{
      await api('POST','/migrations',{
        filename_out:fn,orig_soft:'Siigo Nube',dest_soft:'World Office Escritorio',
        module:'Terceros',records_in:totalEntrada,records_out:out.length,
        errors:errCount,warnings:warns,duration_sec:parseFloat(dur),status:'completed'
      },AUTH.token);
    }catch(e){}

    setStep(4);

  }catch(err){
    escLog(`❌ Error: ${err.message}`,'e','Pipeline');
    console.error(err);
  }
}

// ── Build Filename ────────────────────────────────────────────────
function escBuildFN(){
  const now=new Date();
  const ts=now.getFullYear()+'_'+String(now.getMonth()+1).padStart(2,'0')+'_'+String(now.getDate()).padStart(2,'0');
  return `terceros_siigo_nube_wo_escritorio_${ts}.xlsx`;
}

// ── Build Workbook ────────────────────────────────────────────────
function escBuildWB(rows, logEntries, stats, excluded, defaults, nitExcluidos){
  const wb=XLSX.utils.book_new();

  // Hoja 1: migrar clientes proveedores
  const aoa=[ESC_COLS.slice()];
  rows.forEach(r=>{
    aoa.push(ESC_COLS.map(c=>{const v=r[c];return(v===''||v===undefined)?null:v??null;}));
  });
  const ws1=XLSX.utils.aoa_to_sheet(aoa);
  XLSX.utils.book_append_sheet(wb,ws1,'migrar clientes proveedores');

  // Hoja 2: sucursales Clientes
  XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet([['Identificación','Sucursal','']]),'sucursales Clientes');

  // Hoja 3: Logs
  const tsNow=new Date().toISOString();
  const logsAoa=[['timestamp','fase','nivel','mensaje']];
  (logEntries||[]).forEach(e=>logsAoa.push([e.ts||tsNow,e.fase||'',e.lvl==='e'?'ERROR':e.lvl==='w'?'WARN':'INFO',e.msg||'']));
  XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet(logsAoa),'Logs');

  // Hoja 4: Estadísticas
  const s=stats||{};
  const estAoa=[['Métrica','Valor'],
    ['archivos_entrada',s.archivos_entrada||1],
    ['registros_entrada',s.registros_entrada||0],
    ['registros_salida',rows.length],
    ['errores_validacion',s.errores||0],
    ['warnings_validacion',s.warnings||0]];
  XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet(estAoa),'Estadísticas');

  // Hoja 5: IdentificacionesExcluidas
  const exclAoa=[['Identificación','Nombre o Razón Social','Motivo']];
  (excluded||[]).forEach(e=>exclAoa.push([e.id,e.nombre,e.tipo]));
  XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet(exclAoa),'IdentificacionesExcluidas');

  // Hoja 6: Terceros no tenidos en cuenta (NITs excluidos)
  if(nitExcluidos&&nitExcluidos.length>0){
    const nitAoa=[['Identificación','Nombre o Razón Social','Motivo']];
    nitExcluidos.forEach(d=>nitAoa.push([d.id, d.nombre, 'Tercero ya existe en la base de datos — excluido de la migración']));
    XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet(nitAoa),'Terceros no tenidos en cuenta');
  }

  // Hoja 7: Campos aplicados por defecto
  if(defaults&&defaults.length>0){
    const defAoa=[['Tipo Identificación Aplicado','No. Identificación','Nombre','Concepto']];
    defaults.forEach(d=>defAoa.push([d['Tipo Identificación Aplicado'],d['No. Identificación'],d['Nombre'],d['Concepto']]));
    XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet(defAoa),'Campos aplicados por defecto');
  }

  return wb;
}

// ── Descarga ──────────────────────────────────────────────────────
function escDoDownload(){
  if(!ESC_WB){alert('Primero ejecuta el proceso ETL');return;}
  const fn=escBuildFN();
  XLSX.writeFile(ESC_WB,fn);
}
