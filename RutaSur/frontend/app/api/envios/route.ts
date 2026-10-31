
import { NextResponse } from "next/server";
import clientPromise from "../../../lib/mongodb";

export async function GET() {
  try {
    const client = await clientPromise;
    const db = client.db("rutasur");

    const envios = await db
      .collection("envios")
      .find({})
      .sort({ actualizadoEn: -1 })
      .limit(100)
      .toArray();

    return NextResponse.json(envios);
  } catch (error) {
    console.error("Error al obtener envíos:", error);

    return NextResponse.json(
      { error: "No se pudieron obtener los envíos" },
      { status: 500 }
    );
  }
}
