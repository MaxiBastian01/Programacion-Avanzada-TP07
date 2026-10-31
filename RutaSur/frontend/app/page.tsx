
"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
type Envio = {
  _id: string;
  shipmentId: string;
  estado?: string;
  zonaId?: string;
  repartidorId?: string;
  fechaPrometida?: string;
  actualizadoEn?: string;
};
type Alerta = {
  _id: string;
  shipmentId: string;
  regla?: string;
  severidad?: string;
  estado?: string;
  creadaEn?: string;
};
export default function Home() {
  const [envios, setEnvios] = useState<Envio[]>([]);
  const [cargando, setCargando] = useState(true);
  const [error, setError] = useState("");
  const [filtro, setFiltro] = useState("TODOS");
  const [alertas, setAlertas] = useState<Alerta[]>([]);
  const [errorAlertas, setErrorAlertas] = useState("");


  useEffect(() => {
    async function cargarEnvios() {
      try {
        const respuesta = await fetch("/api/envios");

        if (!respuesta.ok) {
          throw new Error("No se pudieron cargar los envíos");
        }

        const datos: Envio[] = await respuesta.json();
        setEnvios(datos);
      } catch (error) {
        setError(
          error instanceof Error ? error.message : "Error desconocido"
        );
      } finally {
        setCargando(false);
      }
    }

    cargarEnvios();
  }, []);

  useEffect(() => {
    async function cargarAlertas() {
      try {
        const respuesta = await fetch("/api/alertas");

        if (!respuesta.ok) {
          throw new Error("No se pudieron cargar las alertas");
        }

        const datos: Alerta[] = await respuesta.json();
        setAlertas(datos);
      } catch (error) {
        setErrorAlertas(
          error instanceof Error ? error.message : "Error desconocido"
        );
      }
    }

    cargarAlertas();
  }, []);

  const entregados = envios.filter(
    (envio) => envio.estado === "ENTREGADO"
  ).length;

  const fallidos = envios.filter(
    (envio) => envio.estado === "ENTREGA_FALLIDA"
  ).length;

  const pendientes = envios.length - entregados - fallidos;
  const enviosFiltrados = envios.filter((envio) => {
    if (filtro === "TODOS") return true;

    if (filtro === "PENDIENTES") {
      return (
        envio.estado !== "ENTREGADO" &&
        envio.estado !== "ENTREGA_FALLIDA"
      );
    }

    return envio.estado === filtro;
  });


  return (
    <main className="min-h-screen bg-slate-950 p-6 text-white md:p-10">
      <div className="mx-auto max-w-7xl">
        <header className="mb-10">
          <p className="mb-2 text-sm font-semibold tracking-widest text-cyan-400">
            RUTASUR
          </p>
          <h1 className="text-3xl font-bold md:text-4xl">
            Torre de Control
          </h1>
          <p className="mt-2 text-slate-400">
            Monitoreo de envíos y entregas de última milla
          </p>
        </header>

        <section className="mb-8 grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
          {[
            { titulo: "Total de envíos", valor: envios.length },
            { titulo: "Entregados", valor: entregados },
            { titulo: "Pendientes", valor: pendientes },
            { titulo: "Fallidos", valor: fallidos },
          ].map((tarjeta) => (
            <div
              key={tarjeta.titulo}
              className="rounded-xl border border-slate-800 bg-slate-900 p-6"
            >
              <p className="text-sm text-slate-400">{tarjeta.titulo}</p>
              <p className="mt-3 text-3xl font-bold">{tarjeta.valor}</p>
            </div>
          ))}
        </section>

        <section className="overflow-hidden rounded-xl border border-slate-800 bg-slate-900">
          <div className="flex flex-wrap items-center justify-between gap-4 border-b border-slate-800 p-6">
            <h2 className="text-xl font-semibold">Listado de envíos</h2>

            <select
              value={filtro}
              onChange={(e) => setFiltro(e.target.value)}
              className="rounded-lg border border-slate-700 bg-slate-800 px-4 py-2 text-sm text-white"
            >
              <option value="TODOS">Todos los envíos</option>
              <option value="ENTREGADO">Entregados</option>
              <option value="PENDIENTES">Pendientes</option>
              <option value="ENTREGA_FALLIDA">Fallidos</option>
            </select>
          </div>

          {cargando ? (
            <p className="p-6 text-slate-400">Cargando envíos...</p>
          ) : error ? (
            <p className="p-6 text-red-400">{error}</p>
          ) : enviosFiltrados.length === 0 ? (
            <p className="p-6 text-slate-400">
              Todavía no hay envíos registrados.
            </p>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-left text-sm">
                <thead className="bg-slate-800/60 text-slate-400">
                  <tr>
                    <th className="p-4">Envío</th>
                    <th className="p-4">Estado</th>
                    <th className="p-4">Zona</th>
                    <th className="p-4">Repartidor</th>
                    <th className="p-4">Fecha prometida</th>
                  </tr>
                </thead>
                <tbody>
                  {enviosFiltrados.map((envio) => (
                    <tr
                      key={envio._id}
                      className="border-t border-slate-800 hover:bg-slate-800/40"
                    >
                      <td className="p-4 font-medium text-cyan-400">
                        <Link
                          href={`/envios/${encodeURIComponent(envio.shipmentId)}`}
                          className="hover:underline"
                        >
                          {envio.shipmentId}
                        </Link>
                      </td>
                      <td className="p-4">
                        <span className="rounded-full bg-slate-800 px-3 py-1 text-xs">
                          {envio.estado || "SIN ESTADO"}
                        </span>
                      </td>
                      <td className="p-4">{envio.zonaId || "—"}</td>
                      <td className="p-4">{envio.repartidorId || "—"}</td>
                      <td className="p-4">
                        {envio.fechaPrometida
                          ? new Date(envio.fechaPrometida).toLocaleString(
                            "es-AR"
                          )
                          : "—"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>
        <section className="mt-8 overflow-hidden rounded-xl border border-slate-800 bg-slate-900">
          <div className="border-b border-slate-800 p-6">
            <h2 className="text-xl font-semibold">Alertas SLA</h2>
            <p className="mt-1 text-sm text-slate-400">
              Riesgos e incumplimientos de entrega
            </p>
          </div>

          {errorAlertas ? (
            <p className="p-6 text-red-400">{errorAlertas}</p>
          ) : alertas.length === 0 ? (
            <p className="p-6 text-slate-400">
              No hay alertas SLA registradas.
            </p>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-left text-sm">
                <thead className="bg-slate-800/60 text-slate-400">
                  <tr>
                    <th className="p-4">Envío</th>
                    <th className="p-4">Regla</th>
                    <th className="p-4">Severidad</th>
                    <th className="p-4">Estado</th>
                  </tr>
                </thead>
                <tbody>
                  {alertas.map((alerta) => (
                    <tr key={alerta._id} className="border-t border-slate-800">
                      <td className="p-4">
                        <Link
                          href={`/envios/${encodeURIComponent(alerta.shipmentId)}`}
                          className="text-cyan-400 hover:underline"
                        >
                          {alerta.shipmentId}
                        </Link>
                      </td>
                      <td className="p-4">{alerta.regla || "—"}</td>
                      <td className="p-4">
                        <span
                          className={`rounded-full px-3 py-1 text-xs ${alerta.severidad === "CRITICA"
                            ? "bg-red-500/20 text-red-400"
                            : alerta.severidad === "ALTA"
                              ? "bg-orange-500/20 text-orange-400"
                              : "bg-yellow-500/20 text-yellow-400"
                            }`}
                        >
                          {alerta.severidad || "—"}
                        </span>
                      </td>
                      <td className="p-4">{alerta.estado || "SIN ESTADO"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>
      </div>
    </main>
  );
}
