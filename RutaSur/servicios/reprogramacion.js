
const { Kafka } = require("kafkajs");
const { randomUUID } = require("crypto");

const kafka = new Kafka({
  clientId: "rutasur-reprogramacion",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({
  groupId: "reprogramacion",
});

const producer = kafka.producer();

// Calcula el próximo día hábil (lunes a viernes).
function siguienteDiaHabil(fecha) {
  const siguiente = new Date(fecha);

  do {
    siguiente.setUTCDate(siguiente.getUTCDate() + 1);
  } while (
    siguiente.getUTCDay() === 0 ||
    siguiente.getUTCDay() === 6
  );

  return siguiente;
}

async function iniciar() {
  await producer.connect();
  await consumer.connect();

  await consumer.subscribe({
    topic: "envios.estados",
    fromBeginning: true,
  });

  console.log("Servicio de reprogramación iniciado");

  await consumer.run({
    eachMessage: async ({ message }) => {
      let evento;

      try {
        evento = JSON.parse(message.value.toString());
      } catch {
        return;
      }

      if (evento.tipo !== "ENTREGA_FALLIDA") {
        return;
      }

      const motivo = evento.motivo;
      const intento = evento.intento ?? 1;

      let decision;
      let nuevaFecha = null;
      let franjaHoraria = null;

      if (intento >= 3 || motivo === "RECHAZADO") {
        decision = "DEVOLUCION";
      } else if (motivo === "DIRECCION_INCORRECTA") {
        decision = "EN_ESPERA";
      } else if (motivo === "AUSENTE") {
        decision = "REPROGRAMADO";

        const fecha = siguienteDiaHabil(
          evento.ocurridoEn || new Date().toISOString()
        );

        fecha.setUTCHours(0, 0, 0, 0);
        nuevaFecha = fecha.toISOString();
        franjaHoraria = "14:00-18:00";
      } else {
        decision = "REVISION_MANUAL";
      }

      const reprogramacion = {
        eventId: randomUUID(),
        shipmentId: evento.shipmentId,
        tipo: decision,
        motivo,
        intento,
        nuevaFecha,
        franjaHoraria,
        ocurridoEn: new Date().toISOString(),
        eventoOrigenId: evento.eventId,
      };

      await producer.send({
        topic: "envios.reprogramaciones",
        messages: [
          {
            key: evento.shipmentId,
            value: JSON.stringify(reprogramacion),
          },
        ],
      });

      console.log(
        `Envío ${evento.shipmentId}: ${decision}`
      );
    },
  });
}

iniciar().catch(console.error);
