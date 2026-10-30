
const { Kafka } = require("kafkajs");
const { MongoClient } = require("mongodb");

const kafka = new Kafka({
    clientId: "rutasur-proyector",
    brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({
    groupId: "proyector",
});
// Productor para enviar eventos incorrectos a la DLQ.
const producer = kafka.producer();

const mongo = new MongoClient("mongodb://localhost:27017");


const estadosValidos = [
    "CREADO",
    "INGRESADO_HUB",
    "CLASIFICADO",
    "EN_REPARTO",
    "ENTREGADO",
    "ENTREGA_FALLIDA",
];

function validarEvento(evento) {
    if (!evento || typeof evento !== "object" || Array.isArray(evento)) {
        return false;
    }

    return (
        typeof evento.eventId === "string" &&
        evento.eventId.length > 0 &&
        typeof evento.shipmentId === "string" &&
        evento.shipmentId.length > 0 &&
        estadosValidos.includes(evento.tipo) &&
        Number.isInteger(evento.secuencia) &&
        evento.secuencia > 0 &&
        typeof evento.ocurridoEn === "string" &&
        !Number.isNaN(Date.parse(evento.ocurridoEn))
    );
}

async function iniciar() {
    await mongo.connect();

    const db = mongo.db("rutasur");
    const envios = db.collection("envios");

    await envios.createIndex(
        { shipmentId: 1 },
        { unique: true }
    );

    await consumer.connect();
    await producer.connect();
    await consumer.subscribe({
        topic: "envios.estados",
        fromBeginning: true,
    });
    await consumer.subscribe({
        topic: "envios.promesas",
        fromBeginning: true,
    });

    console.log("Proyector conectado a Kafka y MongoDB");

    await consumer.run({
        eachMessage: async ({ topic, message }) => {
            const mensajeOriginal = message.value?.toString() ?? "";

            let evento;
            let motivoError = null;

            // Intentamos convertir el mensaje a JSON.
            try {
                evento = JSON.parse(mensajeOriginal);

                if (topic === "envios.estados" && !validarEvento(evento)) {
                    motivoError = "El evento de estado tiene campos inválidos";
                }
            } catch (error) {
                motivoError = "El mensaje no contiene un JSON válido";
            }

            // Si hay un error, enviamos el mensaje a la DLQ.
            if (motivoError) {
                await producer.send({
                    topic: "envios.estados.dlq",
                    messages: [
                        {
                            value: JSON.stringify({
                                mensajeOriginal,
                                motivoError,
                                detectadoEn: new Date().toISOString(),
                            }),
                        },
                    ],
                });

                console.log(`Evento enviado a DLQ: ${motivoError}`);

                return;
            }
            // Procesamos las promesas de entrega.
            if (topic === "envios.promesas") {

                if (
                    !evento ||
                    typeof evento.shipmentId !== "string" ||
                    !evento.shipmentId ||
                    typeof evento.venceEn !== "string" ||
                    Number.isNaN(Date.parse(evento.venceEn))
                ) {
                    console.log("Promesa inválida ignorada");
                    return;
                }

                await envios.updateOne(
                    { shipmentId: evento.shipmentId },
                    {
                        $set: {
                            fechaPrometida: evento.venceEn,
                        },
                    },
                    { upsert: true }
                );

                console.log(
                    `Promesa guardada: ${evento.shipmentId}`
                );

                return;
            }
            const envio = await envios.findOne({
                shipmentId: evento.shipmentId,
            });

            // Verificamos si el evento ya está en el historial.
            const duplicado = envio?.historial?.some(
                (registro) => registro.eventId === evento.eventId
            );

            if (duplicado) {
                console.log(`Duplicado ignorado: ${evento.eventId}`);
                return;
            }

            // Si no existe, guardamos el evento en el historial.
            await envios.updateOne(
                { shipmentId: evento.shipmentId },
                {
                    $push: {
                        historial: {
                            eventId: evento.eventId,
                            tipo: evento.tipo,
                            secuencia: evento.secuencia,
                            ocurridoEn: evento.ocurridoEn,
                        },
                    },
                    $setOnInsert: {
                        shipmentId: evento.shipmentId,
                    },
                },
                { upsert: true }
            );

            // Solo actualizamos el estado si la secuencia es más reciente.
            if (!envio || evento.secuencia > (envio.secuencia ?? 0)) {
                await envios.updateOne(
                    { shipmentId: evento.shipmentId },
                    {
                        $set: {
                            estado: evento.tipo,
                            secuencia: evento.secuencia,
                            hubId: evento.hubId,
                            zonaId: evento.zonaId,
                            repartidorId: evento.repartidorId,
                            actualizadoEn: evento.ocurridoEn,
                        },
                    }
                );
            }

            console.log(
                `Procesado: ${evento.shipmentId} → ${evento.tipo}`
            );
        },
    });
}

iniciar().catch(console.error);
