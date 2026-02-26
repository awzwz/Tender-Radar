/** BidCheck types for PDF parsing and supplier generation */

export interface RequirementItem {
  role: string;
  count?: number;
  notes?: string;
}

export interface RequirementsDoc {
  source_filename: string;
  parsed_at_utc: string;
  labor_requirements: RequirementItem[];
  equipment_requirements?: unknown[];
  other_requirements?: unknown;
}

export interface SupplierCandidate {
  name: string;
  bin_iin?: string;
  contacts?: Record<string, unknown>;
  capabilities: string[];
  confidence?: number;
}

export interface SupplierPacksResponse {
  suppliers: SupplierCandidate[];
  summary?: string;
}

export interface ParseAndGenerateResponse {
  requirements: RequirementsDoc;
  suppliers: SupplierPacksResponse;
  summary?: string;
}

/** End2end (demo_bidcheck-3) response: requirements + suppliers with documents_text + summaries */
export interface LaborRole {
  role: string;
  count: number;
  required_documents?: string[];
  notes?: string[];
  evidence?: string[];
}

export interface End2endSupplier {
  supplier_name: string;
  profile?: "FULL" | "MINOR_MISSING";
  documents_text: string;
}

export interface SummaryCheck {
  role: string;
  required: string;
  status: "OK" | "FAIL" | "UNKNOWN";
  evidence?: string[];
}

export interface SummaryIssue {
  category: string;
  finding: string;
  evidence?: string[];
}

export interface End2endSummary {
  supplier_name: string;
  verdict: "PASS" | "FAIL";
  checks: SummaryCheck[];
  issues?: SummaryIssue[];
}

export interface BidcheckEnd2endResponse {
  source_filename?: string;
  parsed_at_utc?: string;
  requirements: {
    labor_roles: LaborRole[];
    global_notes?: string[];
  };
  suppliers: End2endSupplier[];
  summaries: End2endSummary[];
}
