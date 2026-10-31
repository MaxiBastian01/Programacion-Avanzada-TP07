
const { Kafka } = require("kafkajs");
const { randomUUID } = require("crypto");
const { fechaSimulada } = require("../reloj");
// CONFIGURACIÓN DE KAFKA
// Nos conectamos al servidor Kafka para enviar los eventos.
const kafka = new Kafka({
  clientId: "rutasur-simulador",
  brokers: (process.env.KAFKA_BROKERS || "localhost:9092").split(","),
});

// Productor: es el encargado de enviar eventos a Kafka.
const producer = kafka.producer();


// CONFIGURACIÓN DEL SIMULADOR
const CONFIG = {
  enviosPorMinuto: 20,       // Cantidad de envíos generados por minuto
  factorAceleracion: 120,    // 30 segundos reales equivalen a 1 hora simulada
  semilla: 12345,            // Permite repetir la misma secuencia aleatoria

  // Probabilidad de que falle una entrega según la zona
  tasaFallo: {
    ZONA_1: 0.05,  // 5%
    ZONA_2: 0.18,  // 18%
    ZONA_3: 0.30,  // 30%
  },

  probabilidadDemora: 0.10,       // 10%
  probabilidadDuplicado: 0.02,    // 2%
  probabilidadFueraOrden: 0.02,   // 2%
  probabilidadMalformado: 0.01,   // 1%
};


// VARIABLES DEL SIMULADOR
let semilla = CONFIG.semilla;
let contadorEnvios = 0;



// GENERAR NÚMEROS ALEATORIOS
// Devuelve un número entre 0 y 1.
// Utiliza una semilla para poder repetir los resultados.
function aleatorio() {
  semilla = (1664525 * semilla + 1013904223) >>> 0;
  return semilla / 4294967296;
}


// ELEGIR UN ELEMENTO ALEATORIO
// Por ejemplo, elegir uno de los tres centros de distribución.
function elegir(lista) {
  return lista[Math.floor(aleatorio() * lista.length)];
}




// GENERAR UN ENVÍO
// Acá creamos los datos que identifican al paquete.
function crearEnvio() {
  contadorEnvios++;

  return {
    // ID único usando fecha y contador
    shipmentId: `ENV-${Date.now()}-${String(contadorEnvios).padStart(6, "0")}`,

    hubId: elegir(["HUB_1", "HUB_2", "HUB_3"]),

    zonaId: elegir(["ZONA_1", "ZONA_2", "ZONA_3"]),

    repartidorId: `REP-${Math.floor(aleatorio() * 120) + 1}`,
  };
}


// GENERAR UN EVENTO
// Un envío puede tener varios eventos a lo largo de su recorrido.
function crearEvento(envio, tipo, secuencia, extra = {}) {
  return {
    // Identificador único de este evento
    eventId: randomUUID(),

    // Identificador del paquete al que pertenece
    shipmentId: envio.shipmentId,

    // Estado del paquete: CREADO, EN_REPARTO, etc.
    tipo,

    // Fecha y hora simulada del evento
    ocurridoEn: fechaSimulada().toISOString(),

    // Número que indica el orden del evento
    secuencia,

    // Centro de distribución del paquete
    hubId: envio.hubId,

    // Zona de entrega
    zonaId: envio.zonaId,

    // Repartidor asignado
    repartidorId: envio.repartidorId,

    // Versión del formato del evento
    version: 1,

    // Datos adicionales, por ejemplo el motivo de un fallo
    ...extra,
  };
}


// ELEGIR EL MOTIVO DE UNA ENTREGA FALLIDA
// Los porcentajes corresponden a los sugeridos por el TP.
function motivoFallo() {
  const valor = aleatorio();

  if (valor < 0.60) return "AUSENTE";                 // 60%
  if (valor < 0.80) return "DIRECCION_INCORRECTA";   // 20%
  if (valor < 0.95) return "RECHAZADO";              // 15%

  return "ZONA_INACCESIBLE";                         // 5%
}


// PUBLICAR UN EVENTO EN KAFKA
async function publicar(topic, evento) {
  await producer.send({
    topic,
    messages: [{
      // Kafka agrupa los eventos del mismo envío por su clave
      key: evento.shipmentId,

      // Convertimos el evento a JSON para enviarlo
      value: JSON.stringify(evento),
    }],
  });

  console.log(`[${topic}] ${evento.shipmentId}: ${evento.tipo}`);

  // En ocasiones enviamos el mismo evento dos veces.
  // Esto permite comprobar que los consumidores
  // no procesen duplicados.
  if (aleatorio() < CONFIG.probabilidadDuplicado) {
    await producer.send({
      topic,
      messages: [{
        key: evento.shipmentId,
        value: JSON.stringify(evento),
      }],
    });
  }
}


// SIMULAR EL RECORRIDO DE UN ENVÍO
async function simularEnvio(escenario = "aleatorio") {

  // 1. Generamos los datos del paquete.
  const envio = crearEnvio();

  // 2. Generamos una promesa de entrega para dentro
  // de 24 horas simuladas.
  const promesa = crearEvento(envio, "PROMESA_ENTREGA", 0, {
    venceEn: new Date(
      fechaSimulada().getTime() + 24 * 60 * 60 * 1000
    ).toISOString(),
  });

  // Enviamos la promesa a su topic de Kafka.
  await publicar("envios.promesas", promesa);

  // 3. Definimos los estados normales del recorrido.
  let estados = [
    "CREADO",
    "INGRESADO_HUB",
    "CLASIFICADO",
    "EN_REPARTO",
  ];

  // 4. Determinamos si la entrega falla.
  // Puede ser por un escenario solicitado o por probabilidad.
  const falla = escenario === "ausente" ||
    escenario === "tercer-fallo" ||
    (escenario === "aleatorio" &&
      aleatorio() < CONFIG.tasaFallo[envio.zonaId]);

  // Si está demorado, el paquete queda en el hub.
  if (escenario === "demorado") {
    estados = ["CREADO", "INGRESADO_HUB"];
  } else {
    // Si no está demorado, termina entregado o fallido.
    estados.push(falla ? "ENTREGA_FALLIDA" : "ENTREGADO");
  }

  // 5. Convertimos cada estado en un evento completo.
  // Cada evento tiene un eventId distinto y una secuencia.
  let eventos = estados.map((tipo, indice) =>
    crearEvento(
      envio,
      tipo,
      indice + 1,
      tipo === "ENTREGA_FALLIDA"
        ? {
            motivo: escenario === "ausente"
              ? "AUSENTE"
              : motivoFallo(),
            intento: 1,
          }
        : {}
    )
  );

  // 6. Escenario de tres intentos fallidos.
  if (escenario === "tercer-fallo") {
    eventos = [
      ...eventos,

      crearEvento(envio, "EN_REPARTO", 6, {
        intento: 2,
      }),

      crearEvento(envio, "ENTREGA_FALLIDA", 7, {
        motivo: "AUSENTE",
        intento: 2,
      }),

      crearEvento(envio, "EN_REPARTO", 8, {
        intento: 3,
      }),

      crearEvento(envio, "ENTREGA_FALLIDA", 9, {
        motivo: "AUSENTE",
        intento: 3,
      }),
    ];
  }

  // 7. En ocasiones intercambiamos dos eventos
  // para simular que llegan fuera de orden.
  if (
    aleatorio() < CONFIG.probabilidadFueraOrden &&
    eventos.length > 2
  ) {
    [eventos[1], eventos[2]] =
      [eventos[2], eventos[1]];
  }

  // 8. Publicamos todos los eventos del envío en Kafka.
  for (const evento of eventos) {
    await publicar("envios.estados", evento);
  }

  // 9. En ocasiones generamos un evento inválido.
  // El consumidor deberá enviarlo a la DLQ.
  if (aleatorio() < CONFIG.probabilidadMalformado) {
    await producer.send({
      topic: "envios.estados",
      messages: [{
        key: envio.shipmentId,
        value: JSON.stringify({
          shipmentId: envio.shipmentId,
          tipo: "EVENTO_INVALIDO",
        }),
      }],
    });
  }

  return envio.shipmentId;
}


// INICIAR EL SIMULADOR
async function iniciar() {

  // Conectamos nuestro productor con Kafka.
  await producer.connect();

  console.log("Simulador RutaSur conectado a Kafka");

  // Leemos el escenario indicado por la terminal.
  const escenario = process.argv[2];

  // Si se solicita un escenario específico, lo ejecutamos.
  if (escenario && escenario !== "continuo") {
    const permitidos = [
      "feliz",
      "ausente",
      "demorado",
      "tercer-fallo",
      "rafaga",
    ];

    if (!permitidos.includes(escenario)) {
      throw new Error(`Escenario desconocido: ${escenario}`);
    }

    // Generamos 500 envíos para el escenario de ráfaga.
    if (escenario === "rafaga") {
      for (let i = 0; i < 500; i++) {
        await simularEnvio();
      }
    } else {
      // Generamos un envío del escenario seleccionado.
      await simularEnvio(escenario);
    }

    await producer.disconnect();
    return;
  }

  // En modo continuo generamos envíos indefinidamente.
  // Con 20 envíos por minuto, generamos uno cada 3 segundos.
  const intervalo = 60000 / CONFIG.enviosPorMinuto;

  while (true) {
    await simularEnvio();

    await new Promise(resolve =>
      setTimeout(resolve, intervalo)
    );
  }
}


// EJECUTAMOS EL PROGRAMA
iniciar().catch(error => {
  console.error("Error en el simulador:", error);
  process.exit(1);
});
