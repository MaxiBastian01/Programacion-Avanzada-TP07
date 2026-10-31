
import { NextResponse } from "next/server";
import clientPromise from "../../../lib/mongodb";

export async function GET() {
  try {
    const client = await clientPromise;
    const db = client.db("rutasur");

    const alertas = await db
      .collection("alertas")
      .find({})
      .sort({ _id: -1 })
      .limit(100)
      .toArray();

    return NextResponse.json(alertas);
  } catch (error) {
    console.error("Error al obtener alertas:", error);

    return NextResponse.json(
      { error: "No se pudieron obtener las alertas" },
      { status: 500 }
    );
  }
}
