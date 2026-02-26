/* ── Spec vs Norm (existing, rule-based) ─────────────────────────────────── */

export interface ExtractedParams {
  [key: string]: number | undefined;
}

export type SpecFlagCode =
  | "PARAM_INFLATION"
  | "QUALIFICATION_OVERSPEC"
  | "TIMELINE_UNREASONABLE"
  | "MATERIAL_OVERSPEC"
  | "CoatingBuildInflation"
  | "MaterialOverSpec"
  | "AbrasiveOverSpec";

/* ── Supplier Document Check (new, LLM-based) ───────────────────────────── */

export type DocViolationType =
  | "MISSING_DOCUMENT"
  | "QUALIFICATION_MISMATCH"
  | "EXPIRED_LICENSE"
  | "INSUFFICIENT_EXPERIENCE"
  | "EQUIPMENT_MISMATCH"
  | "FALSIFIED_DATA";

export type Severity = "HIGH" | "MEDIUM" | "LOW";

export interface DocViolation {
  type: DocViolationType;
  severity: Severity;
  description: string;
  requirement: string;
  provided: string;
}

export type TenderCategory = "works" | "services" | "goods";

/* ── Combined Finding ────────────────────────────────────────────────────── */

export interface TenderFinding {
  tenderId: string;
  titleRu: string;
  region: string;
  amountKZT: number;
  publishDate: string;
  category: TenderCategory;

  specDeviationScore: number;
  isFlagged: boolean;
  activeFlags: SpecFlagCode[];
  summary: string[];
  extracted: ExtractedParams;
  norm: ExtractedParams;

  supplierName: string;
  docComplianceScore: number;
  docViolations: DocViolation[];
  docSummary: string[];
  llmAnalysis: string;
  overallRiskTier: RiskTier;

  techspecFile: string;
  normFile: string;
  supplierDocFile: string;
}

/* ── Legacy WorkFinding (for backward compat) ────────────────────────────── */

export type FlagCode = SpecFlagCode;

export interface WorkFinding {
  tenderId: string;
  titleRu: string;
  region: string;
  amountKZT: number;
  publishDate?: string;
  specDeviationScore: number;
  isFlagged: boolean;
  activeFlags: FlagCode[];
  summary: string[];
  extracted: ExtractedParams;
  norm: ExtractedParams;
  techspecFile: string;
  normFile: string;
}

/* ── Shared types ────────────────────────────────────────────────────────── */

export type RiskTier = "HIGH" | "MEDIUM" | "LOW";

export const CATEGORY_LABELS: Record<TenderCategory, string> = {
  works: "Работы",
  services: "Услуги",
  goods: "Товары",
};

export const CATEGORY_COLORS: Record<TenderCategory, string> = {
  works: "amber",
  services: "indigo",
  goods: "emerald",
};

export const FLAG_LABELS: Record<string, string> = {
  CoatingBuildInflation: "Завышение толщины покрытия",
  MaterialOverSpec: "Чрезмерные требования к материалам",
  AbrasiveOverSpec: "Чрезмерные требования к абразиву",
  PARAM_INFLATION: "Завышение параметров",
  QUALIFICATION_OVERSPEC: "Чрезмерные требования к квалификации",
  TIMELINE_UNREASONABLE: "Необоснованные сроки",
  MATERIAL_OVERSPEC: "Завышение требований к материалам",
};

export const DOC_VIOLATION_LABELS: Record<DocViolationType, string> = {
  MISSING_DOCUMENT: "Отсутствующий документ",
  QUALIFICATION_MISMATCH: "Несоответствие квалификации",
  EXPIRED_LICENSE: "Истёкшая лицензия",
  INSUFFICIENT_EXPERIENCE: "Недостаточный опыт",
  EQUIPMENT_MISMATCH: "Несоответствие оборудования",
  FALSIFIED_DATA: "Фальсификация данных",
};

export const DOC_VIOLATION_COLORS: Record<DocViolationType, string> = {
  MISSING_DOCUMENT: "rose",
  QUALIFICATION_MISMATCH: "amber",
  EXPIRED_LICENSE: "red",
  INSUFFICIENT_EXPERIENCE: "orange",
  EQUIPMENT_MISMATCH: "violet",
  FALSIFIED_DATA: "rose",
};

export const SEVERITY_COLORS: Record<Severity, string> = {
  HIGH: "rose",
  MEDIUM: "amber",
  LOW: "emerald",
};

export const FLAG_BASE_WEIGHTS: Record<string, number> = {
  CoatingBuildInflation: 45,
  MaterialOverSpec: 35,
  AbrasiveOverSpec: 35,
  PARAM_INFLATION: 40,
  QUALIFICATION_OVERSPEC: 30,
  TIMELINE_UNREASONABLE: 25,
  MATERIAL_OVERSPEC: 35,
};

export const PARAM_LABELS: Record<string, string> = {
  layers: "Кол-во слоёв",
  thickness_um: "Толщина покрытия (мкм)",
  nonvolatile_min_pct: "Нелетучие вещества min (%)",
  pot_life_hours_min: "Жизнеспособность min (ч)",
  dry_degree3_hours_max: "Высыхание ст.3 max (ч)",
  adhesion_points_max: "Адгезия max (балл)",
  abrasive_bulk_density_min: "Насыпная плотность абразива min (г/см³)",
  abrasive_conductivity_max: "Электропроводность абразива max (мСм/м)",
};

export const PARAM_STRICTNESS: Record<string, "higher_is_stricter" | "lower_is_stricter"> = {
  layers: "higher_is_stricter",
  thickness_um: "higher_is_stricter",
  nonvolatile_min_pct: "higher_is_stricter",
  pot_life_hours_min: "higher_is_stricter",
  dry_degree3_hours_max: "lower_is_stricter",
  adhesion_points_max: "lower_is_stricter",
  abrasive_bulk_density_min: "higher_is_stricter",
  abrasive_conductivity_max: "lower_is_stricter",
};

/* ── Utility functions ───────────────────────────────────────────────────── */

export function getRiskTier(score: number): RiskTier {
  if (score >= 75) return "HIGH";
  if (score >= 55) return "MEDIUM";
  return "LOW";
}

export function getDocRiskTier(complianceScore: number): RiskTier {
  if (complianceScore <= 40) return "HIGH";
  if (complianceScore <= 70) return "MEDIUM";
  return "LOW";
}

export function getOverallRisk(specScore: number, docScore: number): RiskTier {
  const combined = (specScore * 0.4) + ((100 - docScore) * 0.6);
  if (combined >= 60) return "HIGH";
  if (combined >= 35) return "MEDIUM";
  return "LOW";
}

export function computeFlagContributions(
  score: number,
  activeFlags: string[],
): { flag: string; contribution: number }[] {
  if (activeFlags.length === 0 || score < 10) return [];
  const sumW = activeFlags.reduce((s, f) => s + (FLAG_BASE_WEIGHTS[f] || 30), 0);
  const comboBonus = activeFlags.length >= 2 ? 10 : 0;
  const baseScore = Math.max(0, score - comboBonus);
  return activeFlags.map((f) => ({
    flag: f,
    contribution: Math.round(baseScore * ((FLAG_BASE_WEIGHTS[f] || 30) / sumW)),
  }));
}

export function isStricter(param: string, extracted: number, norm: number): boolean {
  const dir = PARAM_STRICTNESS[param];
  if (!dir) return false;
  return dir === "higher_is_stricter" ? extracted > norm : extracted < norm;
}
