
import { NextResponse } from "next/server";
import clientPromise from "../../../../lib/mongodb";

export async function GET(
  request: Request,
  context: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await context.params;

    const client = await clientPromise;
    const db = client.db("rutasur");

    const envio = await db.collection("envios").findOne({
      shipmentId: id,
    });

    if (!envio) {
      return NextResponse.json(
        { error: "Envío no encontrado" },
        { status: 404 }
      );
    }

    return NextResponse.json(envio);
  } catch (error) {
    console.error("Error al obtener el envío:", error);

    return NextResponse.json(
      { error: "No se pudo obtener el envío" },
      { status: 500 }
    );
  }
}
