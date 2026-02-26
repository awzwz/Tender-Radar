"use client";

import { useEffect, useState } from "react";
import type { TenderFinding, WorkFinding } from "./types";

let _findingsCache: TenderFinding[] | null = null;
let _legacyCache: WorkFinding[] | null = null;

export function useFindingsData() {
  const [data, setData] = useState<TenderFinding[]>(_findingsCache ?? []);
  const [loading, setLoading] = useState(!_findingsCache);

  useEffect(() => {
    if (_findingsCache) return;
    fetch("/demo/specnorm/findings.json")
      .then((r) => r.json())
      .then((d: TenderFinding[]) => {
        _findingsCache = d;
        setData(d);
      })
      .catch(() => setData([]))
      .finally(() => setLoading(false));
  }, []);

  return { data, loading };
}

export function useSpecNormData() {
  const [data, setData] = useState<WorkFinding[]>(_legacyCache ?? []);
  const [loading, setLoading] = useState(!_legacyCache);

  useEffect(() => {
    if (_legacyCache) return;
    fetch("/demo/specnorm/work_findings.json")
      .then((r) => r.json())
      .then((d: WorkFinding[]) => {
        _legacyCache = d;
        setData(d);
      })
      .catch(() => setData([]))
      .finally(() => setLoading(false));
  }, []);

  return { data, loading };
}
