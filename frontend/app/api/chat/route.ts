import { NextRequest, NextResponse } from "next/server";

const BACKEND_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";

export async function POST(req: NextRequest) {
    try {
        const body = await req.json();

        // Get auth token from request headers (forwarded from browser)
        const authHeader = req.headers.get("Authorization") || "";

        const response = await fetch(`${BACKEND_URL}/chat/`, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                ...(authHeader ? { Authorization: authHeader } : {}),
            },
            body: JSON.stringify(body),
        });

        const data = await response.json();

        if (!response.ok) {
            const errMsg = data.detail || JSON.stringify(data);
            return NextResponse.json({ error: errMsg }, { status: response.status });
        }

        return NextResponse.json(data);
    } catch (error: unknown) {
        const message = error instanceof Error ? error.message : "Unknown error";
        return NextResponse.json({ error: message }, { status: 500 });
    }
}
