import type { RequirementsDoc, BidcheckEnd2endResponse } from "@/lib/bidcheck/types";

const getApiBase = (): string => {
  if (typeof window === "undefined")
    return process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";
  const host = window.location.hostname;
  return `http://${host}:8000/api/v1`;
};

function getToken(): string | null {
  if (typeof window === "undefined") return null;
  return localStorage.getItem("token");
}

async function bidcheckFetch<T>(
  path: string,
  options: RequestInit & { body?: FormData | string } = {}
): Promise<T> {
  const token = getToken();
  const { body, headers: optHeaders, ...rest } = options;
  const isFormData = body instanceof FormData;
  const headers: Record<string, string> = {};
  if (!isFormData) headers["Content-Type"] = "application/json";
  if (token) headers["Authorization"] = `Bearer ${token}`;
  const res = await fetch(`${getApiBase()}${path}`, {
    ...rest,
    body: body as BodyInit,
    headers: { ...headers, ...optHeaders } as HeadersInit,
  });
  if (!res.ok) {
    const error = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(
      typeof error.detail === "string" ? error.detail : JSON.stringify(error.detail ?? "API error")
    );
  }
  return res.json() as Promise<T>;
}

export async function parseBidcheckPdf(file: File): Promise<RequirementsDoc> {
  const form = new FormData();
  form.append("file", file);
  return bidcheckFetch<RequirementsDoc>("/bidcheck/parse", {
    method: "POST",
    body: form,
  });
}

export async function parseAndAnalyzeCompliance(
  tsFile: File,
  supplierFile: File
): Promise<BidcheckEnd2endResponse> {
  const form = new FormData();
  form.append("ts_file", tsFile);
  form.append("supplier_file", supplierFile);
  return bidcheckFetch<BidcheckEnd2endResponse>("/bidcheck/parse-and-analyze-compliance", {
    method: "POST",
    body: form,
  });
}
