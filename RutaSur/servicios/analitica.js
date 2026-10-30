
const { Kafka } = require("kafkajs");
const { MongoClient } = require("mongodb");

const kafka = new Kafka({
  clientId: "rutasur-analitica",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({
  groupId: "analitica",
});

const mongo = new MongoClient("mongodb://localhost:27017");

const estadosValidos = [
  "CREADO",
  "INGRESADO_HUB",
  "CLASIFICADO",
  "EN_REPARTO",
  "ENTREGADO",
  "ENTREGA_FALLIDA",
];

async function iniciar() {
  await mongo.connect();

  const db = mongo.db("rutasur");
  const eventos = db.collection("eventos_analitica");
  const metricas = db.collection("metricas_diarias");

  // Evita guardar dos veces el mismo evento.
  await eventos.createIndex(
    { eventId: 1 },
    { unique: true }
  );

  await consumer.connect();

  await consumer.subscribe({
    topic: "envios.estados",
    fromBeginning: true,
  });

  console.log("Servicio de analítica iniciado");

  await consumer.run({
    eachMessage: async ({ topic,         message }) => {
      let evento;

      try {
        evento = JSON.parse(message.value.toString());
      } catch {
        return;
      }

      // Ignoramos eventos incompletos o desconocidos.
      if (
        !evento.eventId ||
        !evento.shipmentId ||
        !estadosValidos.includes(evento.tipo) ||
        !evento.ocurridoEn ||
        Number.isNaN(Date.parse(evento.ocurridoEn))
      ) {
        return;
      }

      // Guardamos el evento solamente si no existe.
      const resultado = await eventos.updateOne(
        { eventId: evento.eventId },
        {
          $setOnInsert: {
            eventId: evento.eventId,
            shipmentId: evento.shipmentId,
            tipo: evento.tipo,
            ocurridoEn: evento.ocurridoEn,
            zonaId: evento.zonaId,
          },
        },
        { upsert: true }
      );

      // Si ya existía, no hace falta recalcular.
      if (resultado.upsertedCount === 0) {
        return;
      }

      // Recalculamos las métricas del día del evento.
      const dia = evento.ocurridoEn.slice(0, 10);

      const eventosDelDia = await eventos.find({
        ocurridoEn: {
          $gte: `${dia}T00:00:00.000Z`,
          $lt: `${dia}T23:59:59.999Z`,
        },
      }).toArray();

      const creados = new Set();
      const entregados = new Set();
      const fallidos = new Set();

      for (const registro of eventosDelDia) {
        if (registro.tipo === "CREADO") {
          creados.add(registro.shipmentId);
        }

        if (registro.tipo === "ENTREGADO") {
          entregados.add(registro.shipmentId);
        }

        if (registro.tipo === "ENTREGA_FALLIDA") {
          fallidos.add(registro.shipmentId);
        }
      }

      const totalCreados = creados.size;
      const totalEntregados = entregados.size;
      const totalFallidos = fallidos.size;

      const tasaEntrega = totalCreados > 0
        ? (totalEntregados / totalCreados) * 100
        : 0;

      await metricas.updateOne(
        { dia },
        {
          $set: {
            dia,
            totalCreados,
            totalEntregados,
            totalFallidos,
            tasaEntrega: Number(tasaEntrega.toFixed(2)),
            actualizadoEn: new Date().toISOString(),
          },
        },
        { upsert: true }
      );

      console.log(
        `Analítica ${dia}: ${totalEntregados}/${totalCreados} entregados`
      );
    },
  });
}

iniciar().catch(console.error);
