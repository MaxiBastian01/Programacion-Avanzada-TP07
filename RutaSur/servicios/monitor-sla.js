const { fechaSimulada } = require("../reloj");
const { Kafka } = require("kafkajs");
const { MongoClient } = require("mongodb");
const { randomUUID } = require("crypto");

const kafka = new Kafka({
  clientId: "rutasur-monitor-sla",
  brokers: (process.env.KAFKA_BROKERS || "localhost:9092").split(","),
});

const producer = kafka.producer();
const mongo = new MongoClient(
  process.env.MONGODB_URI || "mongodb://localhost:27017"
);


function ahoraSimulado() {
  const transcurrido = Date.now() - INICIO_REAL;

  return new Date(
    INICIO_SIMULADO + transcurrido * FACTOR_ACELERACION
  );
}

function evaluarAlerta(envio, ahora) {
  if (!envio.fechaPrometida || envio.estado === "ENTREGADO") {
    return null;
  }

  const vencimiento = new Date(envio.fechaPrometida);
  const horasRestantes =
    (vencimiento - ahora) / (1000 * 60 * 60);

  if (horasRestantes < 0) {
    return {
      regla: "PLAZO_VENCIDO",
      severidad: "CRITICA",
    };
  }

  if (
    envio.estado === "ENTREGA_FALLIDA" &&
    vencimiento.toISOString().slice(0, 10) ===
      ahora.toISOString().slice(0, 10)
  ) {
    return {
      regla: "FALLO_DIA_VENCIMIENTO",
      severidad: "ALTA",
    };
  }

  if (
    horasRestantes <= 4 &&
    !["EN_REPARTO", "ENTREGA_FALLIDA"].includes(envio.estado)
  ) {
    return {
      regla: "SIN_SALIR_A_REPARTO",
      severidad: "ALTA",
    };
  }

  if (
    horasRestantes <= 10 &&
    envio.estado === "CREADO"
  ) {
    return {
      regla: "SIN_INGRESAR_HUB",
      severidad: "MEDIA",
    };
  }

  return null;
}

async function iniciar() {
  await mongo.connect();
  await producer.connect();

  const db = mongo.db("rutasur");
  const envios = db.collection("envios");
  const alertas = db.collection("alertas");

  // Una alerta activa por envío y regla.
  await alertas.createIndex(
    { shipmentId: 1, regla: 1 },
    { unique: true }
  );

  console.log("Monitor SLA iniciado");

  async function revisarEnvios() {
    const ahora = fechaSimulada();
    const documentos = await envios.find({}).toArray();

    for (const envio of documentos) {
      const alerta = evaluarAlerta(envio, ahora);

      if (!alerta) {
        continue;
      }

      const registro = {
        eventId: randomUUID(),
        shipmentId: envio.shipmentId,
        tipo: "ALERTA_SLA",
        regla: alerta.regla,
        severidad: alerta.severidad,
        ocurridoEn: ahora.toISOString(),
      };

      // Evitamos publicar repetidamente la misma alerta.
      const resultado = await alertas.updateOne(
        {
          shipmentId: envio.shipmentId,
          regla: alerta.regla,
        },
        {
          $setOnInsert: registro,
        },
        { upsert: true }
      );

      if (resultado.upsertedCount === 0) {
        continue;
      }

      await producer.send({
        topic: "alertas.sla",
        messages: [
          {
            key: envio.shipmentId,
            value: JSON.stringify(registro),
          },
        ],
      });

      console.log(
        `Alerta ${alerta.severidad}: ${envio.shipmentId} (${alerta.regla})`
      );
    }
  }

  // Revisamos los envíos cada 5 segundos reales.
  await revisarEnvios();
  setInterval(() => {
    revisarEnvios().catch(console.error);
  }, 5000);
}

iniciar().catch(console.error);
