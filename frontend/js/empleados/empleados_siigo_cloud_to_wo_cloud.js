const EMP_LISTAS = {
  'Ciudad*': ["Abejorral", "Abrego", "Abriaquí", "Acacías", "Acandí", "Acevedo", "Achí", "Agrado", "Agua De Dios", "Aguachica", "Aguada", "Aguadas", "Aguazul", "Agustín Codazzi", "Aipe", "Albán", "Albania", "Alcalá", "Aldana", "Alejandría", "Algarrobo", "Algeciras", "Almaguer", "Almeida", "Alpujarra", "Altamira", "Alto Baudó", "Altos Del Rosario", "Alvarado", "Amagá", "Amalfi", "Ambalema", "Anapoima", "Ancuyá", "Andalucía", "Andes", "Angelópolis", "Angostura", "Anolaima", "Anorí", "Anserma", "Ansermanuevo", "Anzá", "Anzoátegui", "Apartadó", "Apía", "Apulo", "Aquitania", "Aracataca", "Aranzazu", "Aratoca", "Arauca", "Arauquita", "Arbeláez", "Arboleda", "Arboledas", "Arboletes", "Arcabuco", "Arenal", "Argelia", "Ariguaní", "Arjona", "Armenia", "Armero", "Arroyohondo", "Astrea", "Ataco", "Atrato", "Ayapel", "Bagadó", "Bahía Solano", "Bajo Baudó", "Balboa", "Baranoa", "Baraya", "Barbacoas", "Barbosa", "Barichara", "Barranca De Upía", "Barrancabermeja", "Barrancas", "Barranco De Loba", "Barranco Minas", "Barranquilla", "Becerril", "Belalcázar", "Belén", "Belén De Bajirá", "Belén De Los Andaquies", "Belén De Umbría", "Bello", "Belmira", "Beltrán", "Berbeo", "Betania", "Betéitiva", "Betulia", "Bituima", "Boavita", "Bochalema", "Bogotá, D.C.", "Bojacá", "Bojayá", "Bolívar", "Bosconia", "Boyacá", "Briceño", "Bucaramanga", "Bucarasica", "Buenaventura", "Buenavista", "Buenos Aires", "Buesaco", "Bugalagrande", "Buriticá", "Busbanzá", "Cabrera", "Cabuyaro", "Cacahual", "Cáceres", "Cachipay", "Cachirá", "Cácota", "Caicedo", "Caicedonia", "Caimito", "Cajamarca", "Cajibío", "Cajicá", "Calamar", "Calarcá", "Caldas", "Caldono", "Cali", "California", "Calima", "Caloto", "Campamento", "Campo De La Cruz", "Campoalegre", "Campohermoso", "Canalete", "Candelaria", "Cantagallo", "Cañasgordas", "Caparrapí", "Capitanejo", "Cáqueza", "Caracolí", "Caramanta", "Carcasí", "Carepa", "Carmen De Apicalá", "Carmen De Carupa", "Carmen Del Darién", "Carolina", "Cartagena", "Cartagena Del Chairá", "Cartago", "Carurú", "Casabianca", "Castilla La Nueva", "Caucasia", "Cepitá", "Cereté", "Cerinza", "Cerrito", "Cerro San Antonio", "Cértegui", "Chachagüí", "Chaguaní", "Chalán", "Chameza", "Chaparral", "Charalá", "Charta", "Chía", "Chibolo", "Chigorodó", "Chima", "Chimá", "Chimichagua", "Chinácota", "Chinavita", "Chinchiná", "Chinú", "Chipaque", "Chipatá", "Chiquinquirá", "Chíquiza", "Chiriguaná", "Chiscas", "Chita", "Chitagá", "Chitaraque", "Chivatá", "Chivor", "Choachí", "Chocontá", "Cicuco", "Ciénaga", "Ciénaga De Oro", "Ciénega", "Cimitarra", "Circasia", "Cisneros", "Ciudad Bolívar", "Clemencia", "Cocorná", "Coello", "Cogua", "Colombia", "Colón", "Coloso", "Cómbita", "Concepción", "Concordia", "Condoto", "Confines", "Consaca", "Contadero", "Contratación", "Convención", "Copacabana", "Coper", "Córdoba", "Corinto", "Coromoro", "Corozal", "Corrales", "Cota", "Cotorra", "Covarachía", "Coveñas", "Coyaima", "Cravo Norte", "Cuaspud", "Cubará", "Cubarral", "Cucaita", "Cucunubá", "Cúcuta", "Cucutilla", "Cuítiva", "Cumaral", "Cumaribo", "Cumbal", "Cumbitara", "Cunday", "Curillo", "Curití", "Curumaní", "Dabeiba", "Dagua", "Dibulla", "Distracción", "Dolores", "Don Matías", "Dosquebradas", "Duitama", "Durania", "Ebéjico", "El Águila", "El Bagre", "El Banco", "El Cairo", "El Calvario", "El Cantón Del San Pablo", "El Carmen", "El Carmen De Atrato", "El Carmen De Bolívar", "El Carmen De Chucurí", "El Carmen De Viboral", "El Castillo", "El Cerrito", "El Charco", "El Cocuy", "El Colegio", "El Copey", "El Doncello", "El Dorado", "El Dovio", "El Encanto", "El Espino", "El Guacamayo", "El Guamo", "El Litoral Del San Juan", "El Molino", "El Paso", "El Paujil", "El Peñol", "El Peñón", "El Piñon", "El Playón", "El Retén", "El Retorno", "El Roble", "El Rosal", "El Rosario", "El Santuario", "El Tablón De Gómez", "El Tambo", "El Tarra", "El Zulia", "Elías", "Encino", "Enciso", "Entrerríos", "Envigado", "Espinal", "Facatativá", "Falan", "Filadelfia", "Filandia", "Firavitoba", "Flandes", "Florencia", "Floresta", "Florián", "Florida", "Floridablanca", "Fomeque", "Fonseca", "Fortul", "Fosca", "Francisco Pizarro", "Fredonia", "Fresno", "Frontino", "Fuente De Oro", "Fundación", "Funes", "Funza", "Fúquene", "Fusagasugá", "Gachalá", "Gachancipá", "Gachantivá", "Gachetá", "Galán", "Galapa", "Galeras", "Gama", "Gamarra", "Gambita", "Gámeza", "Garagoa", "Garzón", "Génova", "Gigante", "Ginebra", "Giraldo", "Girardot", "Girardota", "Girón", "Gómez Plata", "González", "Gramalote", "Granada", "Guaca", "Guacamayas", "Guacarí", "Guachené", "Guachetá", "Guachucal", "Guadalajara De Buga", "Guadalupe", "Guaduas", "Guaitarilla", "Gualmatán", "Guamal", "Guamo", "Guapi", "Guapotá", "Guaranda", "Guarne", "Guasca", "Guatapé", "Guataquí", "Guatavita", "Guateque", "Guática", "Guavatá", "Guayabal De Siquima", "Guayabetal", "Guayatá", "Güepsa", "Güicán", "Gutiérrez", "Hacarí", "Hatillo De Loba", "Hato", "Hato Corozal", "Hatonuevo", "Heliconia", "Herrán", "Herveo", "Hispania", "Hobo", "Honda", "Ibagué", "Icononzo", "Iles", "Imués", "Inírida", "Inzá", "Ipiales", "Iquira", "Isnos", "Istmina", "Itagüí", "Ituango", "Iza", "Jambaló", "Jamundí", "Jardín", "Jenesano", "Jericó", "Jerusalén", "Jesús María", "Jordán", "Juan De Acosta", "Junín", "Juradó", "La Apartada", "La Argentina", "La Belleza", "La Calera", "La Capilla", "La Ceja", "La Celia", "La Chorrera", "La Cruz", "La Cumbre", "La Dorada", "La Esperanza", "La Estrella", "La Florida", "La Gloria", "La Guadalupe", "La Jagua De Ibirico", "La Jagua Del Pilar", "La Llanada", "La Macarena", "La Merced", "La Mesa", "La Montañita", "La Palma", "La Paz", "La Pedrera", "La Peña", "La Pintada", "La Plata", "La Playa", "La Primavera", "La Salina", "La Sierra", "La Tebaida", "La Tola", "La Unión", "La Uvita", "La Vega", "La Victoria", "La Virginia", "Labateca", "Labranzagrande", "Landázuri", "Lebríja", "Leguízamo", "Leiva", "Lejanías", "Lenguazaque", "Lérida", "Leticia", "Líbano", "Liborina", "Linares", "Lloró", "López", "Lorica", "Los Andes", "Los Córdobas", "Los Palmitos", "Los Patios", "Los Santos", "Lourdes", "Luruaco", "Macanal", "Macaravita", "Maceo", "Macheta", "Madrid", "Magangué", "Magüi", "Mahates", "Maicao", "Majagual", "Málaga", "Malambo", "Mallama", "Manatí", "Manaure", "Maní", "Manizales", "Manta", "Manzanares", "Mapiripán", "Mapiripana", "Margarita", "María La Baja", "Marinilla", "Maripí", "Mariquita", "Marmato", "Marquetalia", "Marsella", "Marulanda", "Matanza", "Medellín", "Medina", "Medio Atrato", "Medio Baudó", "Medio San Juan", "Melgar", "Mercaderes", "Mesetas", "Milán", "Miraflores", "Miranda", "Mirití - Paraná", "Mistrató", "Mitú", "Mocoa", "Mogotes", "Molagavita", "Momil", "Mompós", "Mongua", "Monguí", "Moniquirá", "Montebello", "Montecristo", "Montelíbano", "Montenegro", "Montería", "Monterrey", "Moñitos", "Morales", "Morelia", "Morichal", "Morroa", "Mosquera", "Motavita", "Murillo", "Murindó", "Mutatá", "Mutiscua", "Muzo", "Nariño", "Nátaga", "Natagaima", "Nechí", "Necoclí", "Neira", "Neiva", "Nemocón", "Nilo", "Nimaima", "Nobsa", "Nocaima", "Norcasia", "Norosí", "Nóvita", "Nueva Granada", "Nuevo Colón", "Nunchía", "Nuquí", "Obando", "Ocamonte", "Ocaña", "Oiba", "Oicatá", "Olaya", "Olaya Herrera", "Onzaga", "Oporapa", "Orito", "Orocué", "Ortega", "Ospina", "Otanche", "Ovejas", "Pachavita", "Pacho", "Pacoa", "Pácora", "Padilla", "Páez", "Paicol", "Pailitas", "Paime", "Paipa", "Pajarito", "Palermo", "Palestina", "Palmar", "Palmar De Varela", "Palmas Del Socorro", "Palmira", "Palmito", "Palocabildo", "Pamplona", "Pamplonita", "Pana Pana", "Pandi", "Panqueba", "Papunaua", "Páramo", "Paratebueno", "Pasca", "Pasto", "Patía", "Pauna", "Paya", "Paz De Ariporo", "Paz De Río", "Pedraza", "Pelaya", "Pensilvania", "Peñol", "Peque", "Pereira", "Pesca", "Piamonte", "Piedecuesta", "Piedras", "Piendamó", "Pijao", "Pijiño Del Carmen", "Pinchote", "Pinillos", "Piojó", "Pisba", "Pital", "Pitalito", "Pivijay", "Planadas", "Planeta Rica", "Plato", "Policarpa", "Polonuevo", "Ponedera", "Popayán", "Pore", "Potosí", "Pradera", "Prado", "Providencia", "Pueblo Bello", "Pueblo Nuevo", "Pueblo Rico", "Pueblorrico", "Puebloviejo", "Puente Nacional", "Puerres", "Puerto Alegría", "Puerto Arica", "Puerto Asís", "Puerto Berrío", "Puerto Boyacá", "Puerto Caicedo", "Puerto Carreño", "Puerto Colombia", "Puerto Concordia", "Puerto Escondido", "Puerto Gaitán", "Puerto Guzmán", "Puerto Libertador", "Puerto Lleras", "Puerto López", "Puerto Nare", "Puerto Nariño", "Puerto Parra", "Puerto Rico", "Puerto Rondón", "Puerto Salgar", "Puerto Santander", "Puerto Tejada", "Puerto Triunfo", "Puerto Wilches", "Pulí", "Pupiales", "Puracé", "Purificación", "Purísima", "Quebradanegra", "Quetame", "Quibdó", "Quimbaya", "Quinchía", "Quípama", "Quipile", "Ragonvalia", "Ramiriquí", "Ráquira", "Recetor", "Regidor", "Remedios", "Remolino", "Repelón", "Restrepo", "Retiro", "Ricaurte", "Río De Oro", "Río Iro", "Río Quito", "Río Viejo", "Rioblanco", "Riofrío", "Riohacha", "Rionegro", "Riosucio", "Risaralda", "Rivera", "Roberto Payán", "Roldanillo", "Roncesvalles", "Rondón", "Rosas", "Rovira", "Sabana De Torres", "Sabanagrande", "Sabanalarga", "Sabanas De San Ángel", "Sabaneta", "Saboyá", "Sácama", "Sáchica", "Sahagún", "Saladoblanco", "Salamina", "Salazar", "Saldaña", "Salento", "Salgar", "Samacá", "Samaná", "Samaniego", "Sampués", "San Agustín", "San Alberto", "San Andrés", "San Andrés Sotavento", "San Antero", "San Antonio", "San Antonio Del Tequendama", "San Benito", "San Benito Abad", "San Bernardo", "San Bernardo Del Viento", "San Calixto", "San Carlos", "San Carlos De Guaroa", "San Cayetano", "San Cristóbal", "San Diego", "San Eduardo", "San Estanislao", "San Felipe", "San Fernando", "San Francisco", "San Gil", "San Jacinto", "San Jacinto Del Cauca", "San Jerónimo", "San Joaquín", "San José", "San José De La Montaña", "San José De Miranda", "San José De Pare", "San José de Uré", "San José Del Fragua", "San José Del Guaviare", "San José Del Palmar", "San Juan De Arama", "San Juan De Betulia", "San Juan De Río Seco", "San Juan De Urabá", "San Juan Del Cesar", "San Juan Nepomuceno", "San Juanito", "San Lorenzo", "San Luis", "San Luis De Gaceno", "San Luis De Palenque", "San Marcos", "San Martín", "San Martín De Loba", "San Mateo", "San Miguel", "San Miguel De Sema", "San Onofre", "San Pablo", "San Pablo De Borbur", "San Pedro", "San Pedro De Cartago", "San Pedro De Urabá", "San Pelayo", "San Rafael", "San Roque", "San Sebastián", "San Sebastián De Buenavista", "San Vicente", "San Vicente De Chucurí", "San Vicente Del Caguán", "San Zenón", "Sandoná", "Santa Ana", "Santa Bárbara", "Santa Bárbara De Pinto", "Santa Catalina", "Santa Helena Del Opón", "Santa Isabel", "Santa Lucía", "Santa María", "Santa Marta", "Santa Rosa", "Santa Rosa De Cabal", "Santa Rosa De Osos", "Santa Rosa De Viterbo", "Santa Rosa Del Sur", "Santa Rosalía", "Santa Sofía", "Santacruz", "Santafé De Antioquia", "Santana", "Santander De Quilichao", "Santiago", "Santiago De Tolú", "Santo Domingo", "Santo Tomás", "Santuario", "Sapuyes", "Saravena", "Sardinata", "Sasaima", "Sativanorte", "Sativasur", "Segovia", "Sesquilé", "Sevilla", "Siachoque", "Sibaté", "Sibundoy", "Silos", "Silvania", "Silvia", "Simacota", "Simijaca", "Simití", "Sincé", "Sincelejo", "Sipí", "Sitionuevo", "Soacha", "Soatá", "Socha", "Socorro", "Socotá", "Sogamoso", "Solano", "Soledad", "Solita", "Somondoco", "Sonsón", "Sopetrán", "Soplaviento", "Sopó", "Sora", "Soracá", "Sotaquirá", "Sotará", "Suaita", "Suan", "Suárez", "Suaza", "Subachoque", "Sucre", "Suesca", "Supatá", "Supía", "Suratá", "Susa", "Susacón", "Sutamarchán", "Sutatausa", "Sutatenza", "Tabio", "Tadó", "Talaigua Nuevo", "Tamalameque", "Támara", "Tame", "Támesis", "Taminango", "Tangua", "Taraira", "Tarapacá", "Tarazá", "Tarqui", "Tarso", "Tasco", "Tauramena", "Tausa", "Tello", "Tena", "Tenerife", "Tenjo", "Tenza", "Teorama", "Teruel", "Tesalia", "Tibacuy", "Tibaná", "Tibasosa", "Tibirita", "Tibú", "Tierralta", "Timaná", "Timbío", "Timbiquí", "Tinjacá", "Tipacoque", "Tiquisio", "Titiribí", "Toca", "Tocaima", "Tocancipá", "Togüí", "Toledo", "Tolú Viejo", "Tona", "Tópaga", "Topaipí", "Toribio", "Toro", "Tota", "Totoró", "Trinidad", "Trujillo", "Tubará", "Tuchín", "Tuluá", "Tumaco", "Tunja", "Tununguá", "Túquerres", "Turbaco", "Turbaná", "Turbo", "Turmequé", "Tuta", "Tutazá", "Ubalá", "Ubaque", "Ulloa", "Umbita", "Une", "Unguía", "Unión Panamericana", "Uramita", "Uribe", "Uribia", "Urrao", "Urumita", "Usiacurí", "Útica", "Valdivia", "Valencia", "Valle De San José", "Valle De San Juan", "Valle Del Guamuez", "Valledupar", "Valparaíso", "Vegachí", "Vélez", "Venadillo", "Venecia", "Ventaquemada", "Vergara", "Versalles", "Vetas", "Vianí", "Victoria", "Vigía Del Fuerte", "Vijes", "Villa Caro", "Villa De Leyva", "Villa De San Diego De Ubaté", "Villa Del Rosario", "Villa Rica", "Villagarzón", "Villagómez", "Villahermosa", "Villamaría", "Villanueva", "Villapinzón", "Villarrica", "Villavicencio", "Villavieja", "Villeta", "Viotá", "Viracachá", "Vistahermosa", "Viterbo", "Yacopí", "Yacuanquer", "Yaguará", "Yalí", "Yarumal", "Yavaraté", "Yolombó", "Yondó", "Yopal", "Yotoco", "Yumbo", "Zambrano", "Zapatoca", "Zapayán", "Zaragoza", "Zarzal", "Zetaquira", "Zipacón", "Zipaquirá", "Zona Bananera"],
  'Genero*': ["Femenino", "Masculino"],
  'Estado Civil*': ["Unión ibre", "Casado", "Separado", "Soltero", "Unión Libre", "Viudo"],
  'Tipo de Cuenta': ["Ahorros", "Corriente"],
  'Medio de Pago*': ["Transferencia Debito", "Efectivo", "Cheque", "Transferencia Débito Interbancario", "Transferencia Débito Bancaria", "Instrumento no definido", "Bonos", "Vales", "Cheque certificado", "Tarjeta Crédito", "Tarjeta Débito", "Acuerdo mutuo"],
  'Extranjero- Nacional Residente- Contacto de Emergencia': ["No", "Si"],
  'Tipo de Identificación*': ["Cédula de ciudadania", "Cédula de Extranjería", "Documento de Identificación Extranjero", "NIT", "Pasaporte", "Permiso Especial de Permanencia", "Registro Civil", "Sin ID del exterior o para uso definido por DIAN", "Tarjeta de Extranjería", "Tarjeta de Identidad"],
  'Parentesco*': ["Abuelo", "Amigo", "Cónyuge", "Hermano", "Hijo", "Madre", "Otro", "Padre", "Primo", "Tío"],
  'Dependiente Economico': ["Dependiente Principal", "Dependiente Secundario", "No es Dependiente del Trabajador"],
  'Tipo de Contrato': ["Aprendizaje", "Obra o Labor", "Prácticas", "Término Fijo", "Término Indefinido"],
  'Vinculación Laboral': ["Contrato laboral", "Pensionado", "Aprendices del SENA en etapa lectiva", "Aprendices del SENA en etapa productiva", "Estudiante Pasantía (Decreto 055 de 2015)", "Estudiantes de postgrado en salud (Decreto 190 de 1996)", "Estudiantes (Régimen especial Ley 789/2002)", "Estudiante universitario (Aprendiz)"],
  'Modalidad del Sueldo': ["Fijo", "Variable"],
  'Jornada Laboral*': ["Tiempo Completo", "Medio Tiempo", "Turnos"],
  'Tipo de Cotizante*': ["Dependiente", "Servicio doméstico", "Independiente", "Madre sustituta", "Beneficiario del fondo de solidaridad pensional", "Beneficiario del UPC Adicional", "Cotizante solo salud", "Independiente no obligado a cotizar pensiones con pago por tercero", "Trabajador de tiempo parcial afiliado al Regimén Subsidiado", "Pensionado con aporte voluntario a salud", "Independiente voluntario al Sistema de Riesgos Laborales", "Independiente con contrato de prestación de servicios superior a 1 mes", "Aprendices del Sena en etapa lectiva", "Funcionarios públicos sin tope máximo de ibc", "Aprendices del SENA en etapa productiva", "Estudiantes (régimen especial ley 789 de 2002)", "Profesor de establecimiento particular", "Estudiantes aportes solo riesgos laborales", "Trabajador dependiente de entidad beneficiaria del sistema general de participaciones - aportes patronales", "Beneficiario del mecanismo de protección al cesante", "Estudiantes de prácticas laborales en el sector público", "Beneficiario programa de reincorporación"],
  'Subtipo de Cotizante*': ["Con requisitos cumplidos para Pensión, Indemnización Sustitutiva o Devolución de Saldos", "Cotizante con Devolución de Saldos", "Cotizante no Obligado a cotización a Pensiones por Edad", "Dependiente Pensionado Activo", "Ninguno"],
  'Indicador Tarifa Especial Pensión': ["No aplica", "Actividades de alto riesgo", "Aviadores", "CTI", "Senadores"],
  'Clase de Riesgo': ["Clase I", "Clase II", "Clase III", "Clase IV", "Clase V"],
  '*Procedimiento de Retención': ["Procedimiento Uno", "Procedimiento Dos"],
  'Modalidad de Trabajo*': ["Presencial", "Teletrabajo", "Híbrido"],
};

// ════════════════════════════════════════════════════════════════
// MÓDULO ETL — EMPLEADOS (Siigo Cloud → World Office Cloud)
// Archivos entrada: Modelo_Importacion_Empleados.xlsx + Modelo_Importacion_Contratos.xlsx
// Archivo salida : PlantillaFichaPersonal.xlsx (54 cols + hoja LISTAS)
// ════════════════════════════════════════════════════════════════

// ── Mapeos de valores Siigo → WO ─────────────────────────────────
const EMP_MAP_TIPO_ID = {
  'cédula de ciudadanía':  'Cédula de ciudadania',
  'cedula de ciudadania':  'Cédula de ciudadania',
  'cédula de extranjería': 'Cédula de Extranjería',
  'cedula de extranjeria': 'Cédula de Extranjería',
  'tarjeta de identidad':  'Tarjeta de Identidad',
  'pasaporte':             'Pasaporte',
  'nit':                   'NIT',
  'registro civil':        'Registro Civil',
  'permiso especial de permanencia': 'Permiso Especial de Permanencia',
  'documento de identificación extranjero': 'Documento de Identificación Extranjero',
};

const EMP_MAP_MEDIO_PAGO = {
  'transferencia bancaria':           'Transferencia Debito',
  'transferencia débito':             'Transferencia Debito',
  'transferencia debito':             'Transferencia Debito',
  'efectivo':                         'Efectivo',
  'cheque':                           'Cheque',
  'transferencia débito interbancario':'Transferencia Débito Interbancario',
  'transferencia débito bancaria':    'Transferencia Débito Bancaria',
  'instrumento no definido':          'Instrumento no definido',
  'tpaga':                            'Instrumento no definido',
};

const EMP_MAP_TIPO_CONTRATO = {
  'indefinido':         'Término Indefinido',
  'término indefinido': 'Término Indefinido',
  'fijo':               'Término Fijo',
  'término fijo':       'Término Fijo',
  'obra o labor':       'Obra o Labor',
  'aprendizaje':        'Aprendizaje',
  'prácticas':          'Prácticas',
  'prestación de servicios': 'Prácticas',
};

const EMP_MAP_TIPO_COTIZANTE = {
  'dependiente - contrato laboral': 'Dependiente',
  'dependiente':                    'Dependiente',
  'independiente':                  'Independiente',
  'pensionado':                     'Pensionado',
  'servicio doméstico':             'Servicio doméstico',
  'aprendiz':                       'Aprendices del SENA en etapa productiva',
};

const EMP_MAP_SUBTIPO = {
  'sin subtipo': 'Ninguno',
  'ninguno':     'Ninguno',
  'none':        'Ninguno',
};

const EMP_MAP_CLASE_RIESGO = {
  '0.522': 'Clase I',  '0,522': 'Clase I',  'categoria i':   'Clase I',  'clase i':   'Clase I',  '01': 'Clase I',
  '1.044': 'Clase II', '1,044': 'Clase II', 'categoria ii':  'Clase II', 'clase ii':  'Clase II', '02': 'Clase II',
  '2.436': 'Clase III','2,436': 'Clase III','categoria iii': 'Clase III','clase iii': 'Clase III','03': 'Clase III',
  '4.350': 'Clase IV', '4,350': 'Clase IV', 'categoria iv':  'Clase IV', 'clase iv':  'Clase IV', '04': 'Clase IV',
  '6.960': 'Clase V',  '6,960': 'Clase V',  'categoria v':   'Clase V',  'clase v':   'Clase V',  '05': 'Clase V',
};

function empMapear(mapa, valor) {
  if (!valor) return '';
  const k = String(valor).toLowerCase().trim();
  // Exact match first
  if (mapa[k]) return mapa[k];
  // Partial match
  for (const [key, mapped] of Object.entries(mapa)) {
    if (k.includes(key) || key.includes(k)) return mapped;
  }
  return String(valor).trim();
}

function empNormSueldo(val) {
  if (!val) return '';
  const s = String(val).replace(/\./g, '').replace(',', '.').replace(/[^\d.]/g,'');
  const n = parseFloat(s);
  return isNaN(n) ? '' : Math.round(n);
}

function empNormFecha(val) {
  if (!val) return '';
  // Excel serial date
  if (typeof val === 'number') {
    const d = new Date(Math.round((val - 25569) * 86400 * 1000));
    return d.toLocaleDateString('es-CO', {day:'2-digit',month:'2-digit',year:'numeric'}).replace(/\//g,'/');
  }
  if (val instanceof Date) {
    return val.toLocaleDateString('es-CO', {day:'2-digit',month:'2-digit',year:'numeric'}).replace(/\//g,'/');
  }
  return String(val).trim();
}

// ── Headers de salida PlantillaFichaPersonal ──────────────────────
const EMP_SECCION_HEADERS = {
  1:  'Datos Basicos',
  13: 'Nucleo Familiar',
  23: 'Contratos',
  42: 'Aportes',
  52: 'Rte Fuente',
};

const EMP_COL_HEADERS = [
  'Identificación Empleado*',          // 1
  'Fecha de Nacimiento*',              // 2
  'Lugar de Nacimiento*',              // 3
  'Genero*',                           // 4
  '#Libreta Militar',                  // 5
  'Estado Civil*',                     // 6
  'Medio de Pago*',                    // 7
  'Banco',                             // 8
  'Tipo de Cuenta',                    // 9
  'Número de Cuenta',                  // 10
  'Extranjero',                        // 11
  'Nacional Residente en el Exterior', // 12
  'Primer Nombre*',                    // 13
  'Segundo Nombre',                    // 14
  'Primer Apellido*',                  // 15
  'Segundo Apellido',                  // 16
  'Tipo de Identificación*',           // 17
  'No. Documento Dependiente*',        // 18
  'Parentesco*',                       // 19
  'Genero*',                           // 20
  'Contacto de Emergencia*',           // 21
  'Dependiente Economico*',            // 22
  'Tipo de Contrato*',                 // 23
  'Fecha de Firma del Contrato',       // 24
  'Lugar de Firma Contrato',           // 25
  'Vinculación Laboral*',              // 26
  'Fecha Ingreso',                     // 27
  'Fecha Fin de Contrato',             // 28
  'No de Dias periodo de prueba*',     // 29
  'Fecha de Inicio de Vacaciones*',    // 30
  'Cargo*',                            // 31
  'Centro de Trabajo*',                // 32
  'Centro de Costos*',                 // 33
  'Modalidad del Sueldo*',             // 34
  'Asignación Salarial*',              // 35
  'Fecha Inicio Salario*',             // 36
  'Salario Integral*',                 // 37
  'Auxilio de Transporte y/o Conectividad*', // 38
  'Modalidad de Trabajo',              // 39
  'Jornada Laboral*',                  // 40
  'Dias de Descanso*',                 // 41
  'Tipo de Cotizante*',                // 42
  'Subtipo de Cotizante*',             // 43
  'Fecha Afil. EPS*',                  // 44
  'EPS*',                              // 45
  'Fecha Afil.Fondo*',                 // 46
  'Fondo de Pensiones',                // 47
  'Indicador de Tarifa Especial Pensión*', // 48
  'Fecha Afil. Cesantias*',            // 49
  'Fondo de Cesantías*',               // 50
  'Clase de Riesgo*',                  // 51
  'Empleado Declarante*',              // 52
  'Procedimiento de Retención*',       // 53
  'Porcentaje de Retención',           // 54
];

// ── ETL STATE empleados ───────────────────────────────────────────
const EMP_S = { files: {}, result: null, wbData: null };

function empOnFile(input, slotId, nameId, key) {
  const file = input.files[0];
  if (!file) return;
  document.getElementById(slotId).classList.toggle('ok', true);
  document.getElementById(slotId).classList.toggle('bad', false);
  document.getElementById(nameId).textContent = file.name;
  EMP_S.files[key] = file;
}

function empGoStart() {
  setEmpStep(2);
  document.getElementById('s-emp2').classList.add('vis');
  document.getElementById('s-emp1').classList.remove('vis');
}
function empGoBack() {
  setEmpStep(1);
  document.getElementById('s-emp1').classList.add('vis');
  document.getElementById('s-emp2').classList.remove('vis');
}

function setEmpStep(n) {
  for(let i=1;i<=4;i++){
    const wz=document.getElementById('ne'+i);
    if(wz){wz.className='wz'+(i<n?' done':i===n?' on':'');}
    const sec=document.getElementById('s-emp'+i);
    if(sec)sec.style.display=(i===n)?'':'none';
  }
}

function empLog(msg, tipo='i') {
  const panel = document.getElementById('emp-logp');
  if (!panel) return;
  const now = new Date().toLocaleTimeString('es-CO',{hour12:false});
  const css = {i:'li',w:'lw',o:'lo',e:'le-e'}[tipo]||'li';
  panel.innerHTML += `<div class="le"><span class="lt">${now}</span><span class="${css}">${msg}</span></div>`;
  panel.scrollTop = panel.scrollHeight;
}

function setEmpPStep(n) {
  for(let i=0;i<=5;i++){
    const el=document.getElementById('eps'+i);
    if(!el)continue;
    el.classList.toggle('act',i===n);
    el.classList.toggle('don',i<n);
  }
}
function setEmpPct(p,txt){
  document.getElementById('emp-pbar').style.width=p+'%';
  document.getElementById('emp-ppct').textContent=p+'%';
  if(txt)document.getElementById('emp-pph').textContent=txt;
}

async function startEmpETL() {
  if (!EMP_S.files.empleados) {
    document.getElementById('emp-a2').classList.add('vis');
    document.getElementById('emp-a2m').textContent='El archivo de Empleados es obligatorio.';
    return;
  }
  document.getElementById('emp-a2').classList.remove('vis');

  // Show pipeline
  document.getElementById('s-emp2').classList.remove('vis');
  document.getElementById('s-emp3').classList.add('vis');
  setEmpStep(3);

  const logP = document.getElementById('emp-logp'); if(logP) logP.innerHTML='';
  const t0 = Date.now();
  let warns=0, errs=0;

  try {
    // ── Paso 0: Config ────────────────────────────────────────────
    setEmpPStep(0); setEmpPct(5,'Configurando...');
    empLog('Plantilla: Ficha Personal Empleados (54 columnas)');
    await empSleep(200);

    // ── Paso 1: Lectura ───────────────────────────────────────────
    setEmpPStep(1); setEmpPct(15,'Leyendo archivos...');
    const empData = await empReadEmpleados(EMP_S.files.empleados);
    empLog(`Empleados leídos: ${empData.length} registros`);

    let contData = {};
    if (EMP_S.files.contratos) {
      contData = await empReadContratos(EMP_S.files.contratos);
      empLog(`Contratos leídos: ${Object.keys(contData).length} registros`);
    } else {
      empLog('Sin archivo de contratos — campos laborales quedarán vacíos','w');
      warns++;
    }
    await empSleep(200);

    // ── Paso 2: Join ─────────────────────────────────────────────
    setEmpPStep(2); setEmpPct(35,'Cruzando datos...');
    let joined = 0, sinContrato = 0;
    const rows = empData.map(emp => {
      const id = String(emp['Número de documento'] || '').trim().replace(/\.0$/,'');
      const cont = contData[id] || {};
      if (!cont['Tipo de contrato']) { sinContrato++; warns++; }
      else joined++;
      return { ...emp, ...cont, _id: id };
    });
    empLog(`Cruzados con contrato: ${joined} | Sin contrato: ${sinContrato}`);
    await empSleep(200);

    // ── Paso 3: Transformación ────────────────────────────────────
    setEmpPStep(3); setEmpPct(55,'Transformando campos...');
    const out = [];
    for (const r of rows) {
      if (!r._id) { errs++; continue; }
      const row = new Array(54).fill('');

      // DATOS BÁSICOS (cols 1-12)
      row[0]  = r._id;                                                           // Identificación Empleado*
      row[1]  = '';                                                               // Fecha de Nacimiento* (no en Siigo)
      row[2]  = r['Ciudad de oficina'] || r['Ciudad de residencia'] || '';       // Lugar de Nacimiento*
      row[3]  = '';                                                               // Genero*
      row[4]  = '';                                                               // #Libreta Militar
      row[5]  = '';                                                               // Estado Civil*
      row[6]  = empMapear(EMP_MAP_MEDIO_PAGO, r['Método de pago']);              // Medio de Pago*
      row[7]  = r['Entidad Bancaria'] || '';                                     // Banco
      row[8]  = r['Tipo de cuenta'] || '';                                       // Tipo de Cuenta
      row[9]  = r['Número de cuenta'] || '';                                     // Número de Cuenta
      row[10] = 'No';                                                            // Extranjero
      row[11] = 'No';                                                            // Nacional Residente en el Exterior

      // NÚCLEO FAMILIAR (cols 13-22)
      row[12] = r['Primer nombre'] || '';                                         // Primer Nombre*
      row[13] = r['Segundo nombre'] || '';                                        // Segundo Nombre
      row[14] = r['Primer apellido'] || '';                                       // Primer Apellido*
      row[15] = r['Segundo apellido'] || '';                                      // Segundo Apellido
      row[16] = empMapear(EMP_MAP_TIPO_ID, r['Tipo de documento']);               // Tipo de Identificación*
      row[17] = '';                                                               // No. Documento Dependiente*
      row[18] = '';                                                               // Parentesco*
      row[19] = '';                                                               // Genero* (dependiente)
      row[20] = '';                                                               // Contacto de Emergencia*
      row[21] = '';                                                               // Dependiente Economico*

      // CONTRATOS (cols 23-41)
      row[22] = empMapear(EMP_MAP_TIPO_CONTRATO, r['Tipo de contrato']);          // Tipo de Contrato*
      row[23] = '';                                                               // Fecha de Firma del Contrato
      row[24] = '';                                                               // Lugar de Firma Contrato
      row[25] = 'Contrato laboral';                                              // Vinculación Laboral*
      row[26] = empNormFecha(r['Fecha inicio de contrato']);                      // Fecha Ingreso
      row[27] = empNormFecha(r['Fecha fin de contrato']);                         // Fecha Fin de Contrato
      row[28] = '0';                                                             // No de Dias periodo de prueba*
      row[29] = '';                                                               // Fecha de Inicio de Vacaciones*
      row[30] = r['Cargo'] || '';                                                 // Cargo*
      row[31] = '';                                                               // Centro de Trabajo*
      row[32] = r['Centro de costo'] || '';                                       // Centro de Costos*
      row[33] = 'Fijo';                                                          // Modalidad del Sueldo*
      row[34] = empNormSueldo(r['Sueldo']);                                       // Asignación Salarial*
      row[35] = empNormFecha(r['Fecha inicio de contrato']);                      // Fecha Inicio Salario*
      row[36] = r['Salario integral'] || 'No';                                   // Salario Integral*
      row[37] = 'Si';                                                            // Auxilio de Transporte y/o Conectividad*
      row[38] = 'Presencial';                                                    // Modalidad de Trabajo
      row[39] = 'Tiempo Completo';                                               // Jornada Laboral*
      row[40] = 'Domingo';                                                       // Dias de Descanso*

      // APORTES (cols 42-51)
      row[41] = empMapear(EMP_MAP_TIPO_COTIZANTE, r['Tipo de cotizante']);        // Tipo de Cotizante*
      row[42] = empMapear(EMP_MAP_SUBTIPO, r['Subtipo de Cotizante'] || r['Subtipo de cotizante'] || 'Ninguno'); // Subtipo de Cotizante*
      row[43] = '';                                                               // Fecha Afil. EPS*
      row[44] = r['Fondo de salud'] || '';                                        // EPS*
      row[45] = '';                                                               // Fecha Afil.Fondo*
      row[46] = r['Fondo de pensión'] || '';                                      // Fondo de Pensiones
      row[47] = 'No aplica';                                                     // Indicador de Tarifa Especial Pensión*
      row[48] = '';                                                               // Fecha Afil. Cesantias*
      row[49] = r['Fondo de cesantías'] || '';                                    // Fondo de Cesantías*
      row[50] = empMapear(EMP_MAP_CLASE_RIESGO, r['Clase de riesgo']);            // Clase de Riesgo*

      // RTE FUENTE (cols 52-54)
      row[51] = 'No';                                                            // Empleado Declarante*
      row[52] = 'Procedimiento Uno';                                             // Procedimiento de Retención*
      row[53] = '';                                                               // Porcentaje de Retención

      out.push(row);
    }
    empLog(`Transformados: ${out.length} registros | ${warns} advertencias | ${errs} errores`);
    await empSleep(200);

    // ── Paso 4: Validación ────────────────────────────────────────
    setEmpPStep(4); setEmpPct(75,'Validando...');
    let validOk=0, validFail=0;
    const reqCols = [0,12,14,22,30,34,39,41,44]; // índices obligatorios
    for (const row of out) {
      const missing = reqCols.filter(i => !row[i]);
      if (missing.length) {
        empLog(`ID ${row[0]}: faltan cols ${missing.map(i=>i+1).join(',')}`, 'w');
        warns++;
        validFail++;
      } else validOk++;
    }
    empLog(`Validación: ${validOk} OK | ${validFail} con campos vacíos`);
    await empSleep(200);

    // ── Paso 5: Generar Excel ─────────────────────────────────────
    setEmpPStep(5); setEmpPct(88,'Generando Excel...');
    const wb = empBuildWB(out);
    EMP_S.result = wb;
    EMP_S.wbData = { total: empData.length, ok: out.length, warns, errs, dur: ((Date.now()-t0)/1000).toFixed(1) };
    await empSleep(200);

    // ── Resultado ─────────────────────────────────────────────────
    setEmpPct(100,'¡Completado!');
    setEmpStep(4);
    document.getElementById('s-emp3').classList.remove('vis');
    document.getElementById('s-emp4').classList.add('vis');

    document.getElementById('est-in').textContent = empData.length;
    document.getElementById('est-ok').textContent    = out.length;
    document.getElementById('est-w').textContent     = warns;
    document.getElementById('est-t').textContent     = EMP_S.wbData.dur + 's';

    const fn = `empleados_wo_${new Date().toISOString().slice(0,10)}.xlsx`;
    document.getElementById('edl-fn').textContent = fn;
    EMP_S.filename = fn;

    empLog(`✅ Proceso finalizado en ${EMP_S.wbData.dur}s`,'o');

    // Log to backend
    logMigrationToBackend({
      filename_out: fn, orig_soft: 'Siigo Cloud', dest_soft: 'World Office Cloud',
      module: 'Empleados', records_in: empData.length, records_out: out.length,
      errors: errs, warnings: warns, duration_sec: parseFloat(EMP_S.wbData.dur), status:'completed'
    });

  } catch(e) {
    empLog('ERROR: ' + e.message, 'e');
    setEmpPct(0,'Error');
    console.error(e);
  }
}

// ── Lectura Empleados ─────────────────────────────────────────────
function empReadEmpleados(file) {
  return new Promise((res,rej) => {
    const fr = new FileReader();
    fr.onload = e => {
      try {
        const wb = XLSX.read(new Uint8Array(e.target.result), {type:'array', cellDates:true});
        const ws = wb.Sheets['Plantilla de empleados'] || wb.Sheets[wb.SheetNames[0]];
        // Row 2 = headers, Row 3+ = data
        const raw = XLSX.utils.sheet_to_json(ws, {defval:'', range:1}); // range:1 skips row 1
        res(raw);
      } catch(err){ rej(err); }
    };
    fr.readAsArrayBuffer(file);
  });
}

// ── Lectura Contratos → map by ID ────────────────────────────────
function empReadContratos(file) {
  return new Promise((res,rej) => {
    const fr = new FileReader();
    fr.onload = e => {
      try {
        const wb = XLSX.read(new Uint8Array(e.target.result), {type:'array', cellDates:true});
        const ws = wb.Sheets['Plantilla de contratos'] || wb.Sheets[wb.SheetNames[0]];
        const raw = XLSX.utils.sheet_to_json(ws, {defval:'', range:1});
        const map = {};
        for (const r of raw) {
          const id = String(r['Número de identificación (Obligatorio)'] || r['Número de identificación'] || '').trim().replace(/\.0$/,'');
          if (id) map[id] = {
            'Tipo de contrato':       r['Tipo de contrato (Obligatorio)'] || r['Tipo de contrato'] || '',
            'Fecha inicio de contrato': r['Fecha inicio de contrato (Obligatorio)'] || r['Fecha inicio de contrato'] || '',
            'Fecha fin de contrato':  r['Fecha fin de contrato'] || '',
            'Sueldo':                 r['Sueldo (Obligatorio)'] || r['Sueldo'] || '',
            'Salario integral':       r['Salario integral'] || 'No',
            'Cargo':                  r['Cargo (Obligatorio)'] || r['Cargo'] || '',
            'Centro de costo':        r['Centro de costo'] || '',
            'Tipo de cotizante':      r['Tipo de cotizante (Obligatorio)'] || r['Tipo de cotizante'] || '',
            'Subtipo de Cotizante':   r['Subtipo de Cotizante (Obligatorio)'] || r['Subtipo de Cotizante'] || '',
            'Fondo de salud':         r['Fondo de salud (Obligatorio)'] || r['Fondo de salud'] || '',
            'Fondo de pensión':       r['Fondo de pensión'] || '',
            'Clase de riesgo':        r['Clase de riesgo'] || '',
            'Fondo de cesantías':     r['Fondo de cesantías'] || '',
          };
        }
        res(map);
      } catch(err){ rej(err); }
    };
    fr.readAsArrayBuffer(file);
  });
}

// ── Construir workbook ────────────────────────────────────────────
function empBuildWB(rows) {
  const wb = XLSX.utils.book_new();

  // ─ Hoja 1: Ficha Personal Empleados ─────────────────────────────
  const ws1Data = [];

  // Fila 1: secciones
  const secRow = new Array(54).fill('');
  for (const [col, label] of Object.entries(EMP_SECCION_HEADERS)) {
    secRow[parseInt(col)-1] = label;
  }
  ws1Data.push(secRow);

  // Fila 2: headers
  ws1Data.push([...EMP_COL_HEADERS]);

  // Filas 3+: datos
  for (const r of rows) ws1Data.push(r);

  const ws1 = XLSX.utils.aoa_to_sheet(ws1Data);

  // Anchos de columna
  ws1['!cols'] = EMP_COL_HEADERS.map(h => ({wch: Math.max(h.length+2, 16)}));

  // Merge secciones en fila 1
  ws1['!merges'] = [];
  const secBounds = {1:12, 13:22, 23:41, 42:51, 52:54};
  for (const [start, end] of Object.entries(secBounds)) {
    ws1['!merges'].push({s:{r:0,c:parseInt(start)-1}, e:{r:0,c:end-1}});
  }

  XLSX.utils.book_append_sheet(wb, ws1, 'Ficha Personal Empleados');

  // ─ Hoja 2: LISTAS ────────────────────────────────────────────────
  const listasKeys = Object.keys(EMP_LISTAS);
  const maxRows    = Math.max(...listasKeys.map(k => EMP_LISTAS[k].length));
  const ws2Data    = [listasKeys];
  for (let i=0; i<maxRows; i++) {
    ws2Data.push(listasKeys.map(k => EMP_LISTAS[k][i] || ''));
  }
  const ws2 = XLSX.utils.aoa_to_sheet(ws2Data);
  ws2['!cols'] = listasKeys.map(h => ({wch: Math.max(h.length+2, 18)}));
  XLSX.utils.book_append_sheet(wb, ws2, 'LISTAS');

  return wb;
}

function empDoDownload() {
  if (!EMP_S.result) return;
  XLSX.writeFile(EMP_S.result, EMP_S.filename);
}

function empReset() {
  EMP_S.files = {}; EMP_S.result = null; EMP_S.wbData = null;
  ['sl-emp-e','sl-emp-c'].forEach(id => {
    const el=document.getElementById(id);
    if(el){el.classList.remove('ok','bad');}
  });
  ['nm-emp-e','nm-emp-c'].forEach(id=>{const el=document.getElementById(id);if(el)el.textContent='';});
  ['s-emp1','s-emp2','s-emp3','s-emp4'].forEach((id,i)=>{
    const el=document.getElementById(id);
    if(el)el.classList.toggle('vis',i===0);
  });
  const lp=document.getElementById('emp-logp');if(lp)lp.innerHTML='';
  setEmpStep(1);
}

function empSleep(ms){ return new Promise(r=>setTimeout(r,ms)); }
