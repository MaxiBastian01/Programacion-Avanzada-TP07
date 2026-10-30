// Fecha de referencia compartida por todos los servicios.
const INICIO_REAL = new Date("2026-10-30T00:00:00Z").getTime();

const FACTOR_ACELERACION = 120;

function fechaSimulada() {
    const transcurrido = Date.now() - INICIO_REAL;

    return new Date(
        INICIO_REAL + transcurrido * FACTOR_ACELERACION
    );
}

module.exports = {
    fechaSimulada,
    FACTOR_ACELERACION,
};