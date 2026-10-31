
"use client";

import { Suspense, useEffect, useState } from "react";
import Link from "next/link";
import { useParams } from "next/navigation";

type Evento = {
  eventId: string;
  tipo: string;
  secuencia: number;
  ocurridoEn: string;
};

type Envio = {
  shipmentId: string;
  estado?: string;
  zonaId?: string;
  repartidorId?: string;
  fechaPrometida?: string;
  historial?: Evento[];
};

function DetalleEnvioContenido() {
  const { id } = useParams<{ id: string }>();

  const [envio, setEnvio] = useState<Envio | null>(null);
  const [cargando, setCargando] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    async function cargarEnvio() {
      try {
        const respuesta = await fetch(
          `/api/envios/${encodeURIComponent(id)}`
        );

        if (!respuesta.ok) {
          throw new Error("No se pudo encontrar el envío");
        }

        const datos: Envio = await respuesta.json();
        setEnvio(datos);
      } catch (error) {
        setError(
          error instanceof Error ? error.message : "Error desconocido"
        );
      } finally {
        setCargando(false);
      }
    }

    cargarEnvio();
  }, [id]);

  if (cargando) {
    return (
      <main className="min-h-screen bg-slate-950 p-10 text-slate-400">
        Cargando envío...
      </main>
    );
  }

  if (error || !envio) {
    return (
      <main className="min-h-screen bg-slate-950 p-10 text-red-400">
        {error || "Envío no encontrado"}
      </main>
    );
  }

  const historial = [...(envio.historial || [])].sort(
    (a, b) => a.secuencia - b.secuencia
  );

  return (
    <main className="min-h-screen bg-slate-950 p-6 text-white md:p-10">
      <div className="mx-auto max-w-5xl">
        <Link href="/" className="text-sm text-cyan-400 hover:underline">
          ← Volver a Torre de Control
        </Link>

        <header className="my-8">
          <p className="text-sm font-semibold tracking-widest text-cyan-400">
            SEGUIMIENTO DE ENVÍO
          </p>
          <h1 className="mt-2 break-all text-3xl font-bold">
            {envio.shipmentId}
          </h1>
          <p className="mt-2 text-slate-400">
            Estado actual: {envio.estado || "SIN ESTADO"}
          </p>
        </header>

        <section className="mb-8 grid gap-4 sm:grid-cols-3">
          {[
            { titulo: "Zona", valor: envio.zonaId || "—" },
            { titulo: "Repartidor", valor: envio.repartidorId || "—" },
            {
              titulo: "Fecha prometida",
              valor: envio.fechaPrometida
                ? new Date(envio.fechaPrometida).toLocaleString("es-AR")
                : "—",
            },
          ].map((dato) => (
            <div
              key={dato.titulo}
              className="rounded-xl border border-slate-800 bg-slate-900 p-5"
            >
              <p className="text-sm text-slate-400">{dato.titulo}</p>
              <p className="mt-2 font-semibold">{dato.valor}</p>
            </div>
          ))}
        </section>

        <section className="rounded-xl border border-slate-800 bg-slate-900 p-6">
          <h2 className="mb-6 text-xl font-semibold">
            Historial de eventos
          </h2>

          {historial.length === 0 ? (
            <p className="text-slate-400">
              Todavía no hay eventos registrados.
            </p>
          ) : (
            <div className="space-y-6">
              {historial.map((evento) => (
                <div
                  key={evento.eventId}
                  className="flex gap-4 border-l-2 border-cyan-500 pl-5"
                >
                  <div>
                    <p className="font-semibold">
                      {evento.tipo.replaceAll("_", " ")}
                    </p>
                    <p className="mt-1 text-sm text-slate-400">
                      {new Date(evento.ocurridoEn).toLocaleString("es-AR")}
                    </p>
                    <p className="mt-1 text-xs text-slate-500">
                      Secuencia: {evento.secuencia}
                    </p>
                  </div>
                </div>
              ))}
            </div>
          )}
        </section>
      </div>
    </main>
  );
}
export default function DetalleEnvio() {
  return (
    <Suspense
      fallback={
        <main className="min-h-screen bg-slate-950 p-10 text-slate-400">
          Cargando envío...
        </main>
      }
    >
      <DetalleEnvioContenido />
    </Suspense>
  );
}