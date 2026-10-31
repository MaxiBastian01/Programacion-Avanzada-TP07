const { Kafka } = require("kafkajs");
const { randomUUID } = require("crypto");

const kafka = new Kafka({
    clientId: "rutasur-notificaciones",
    brokers: (process.env.KAFKA_BROKERS || "localhost:9092").split(","),
});

const consumer = kafka.consumer({
    groupId: "notificaciones",
});

const producer = kafka.producer();

async function iniciar() {
    await producer.connect();
    await consumer.connect();

    await consumer.subscribe({
        topic: "envios.estados",
        fromBeginning: true,
    });
    await consumer.subscribe({
        topic: "alertas.sla",
        fromBeginning: true,
    });

    console.log("Servicio de notificaciones iniciado");

    await consumer.run({
        eachMessage: async ({ topic, message }) => {
            let evento;

            try {
                evento = JSON.parse(message.value.toString());
            } catch {
                return;
            }

            // Solo nos interesan las entregas fallidas.
            const entregaFallida =
                topic === "envios.estados" &&
                evento.tipo === "ENTREGA_FALLIDA";

            const alertaSLA =
                topic === "alertas.sla" &&
                evento.tipo === "ALERTA_SLA";

            if (!entregaFallida && !alertaSLA) {
                return;
            }

            const notificacion = {
                eventId: randomUUID(),
                shipmentId: evento.shipmentId,
                tipo: "NOTIFICACION_ENVIADA",

                motivo: entregaFallida
                    ? evento.motivo || "ENTREGA_FALLIDA"
                    : evento.regla,

                severidad: alertaSLA ? evento.severidad : null,

                eventoOrigenId: evento.eventId,
                ocurridoEn: new Date().toISOString(),
            };

            await producer.send({
                topic: "notificaciones.enviadas",
                messages: [
                    {
                        key: evento.shipmentId,
                        value: JSON.stringify(notificacion),
                    },
                ],
            });

            console.log(
                `Notificación generada para ${evento.shipmentId}`
            );
        },
    });
}

iniciar().catch(console.error);