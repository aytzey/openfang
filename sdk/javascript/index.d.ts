export class PulsivoSalesmanError extends Error {
  status: number;
  body: string;
  constructor(message: string, status: number, body: string);
}

export type SalesSegment = "b2b" | "b2c";

export interface SegmentOpts {
  segment?: SalesSegment;
}

export interface PersistOpts extends SegmentOpts {
  persist?: boolean;
}

export interface SalesListOpts extends SegmentOpts {
  limit?: number;
  runId?: string;
}

export interface RetryJobOpts extends SegmentOpts {
  forceFresh?: boolean;
}

export interface ApprovalListOpts {
  status?: string;
  limit?: number;
}

export class PulsivoSalesman {
  baseUrl: string;
  sales: SalesResource;

  constructor(baseUrl: string, opts?: { headers?: Record<string, string> });

  health(): Promise<unknown>;
  healthDetail(): Promise<unknown>;
  status(): Promise<unknown>;
  version(): Promise<unknown>;
  metrics(): Promise<string>;
}

export class SalesResource {
  getProfile(segment?: SalesSegment): Promise<unknown>;
  updateProfile(profile: Record<string, unknown>, opts?: SegmentOpts): Promise<unknown>;
  autofillProfile(brief: string, opts?: PersistOpts): Promise<unknown>;
  getOnboardingStatus(segment?: SalesSegment): Promise<unknown>;
  updateOnboardingBrief(brief: string, opts?: PersistOpts): Promise<unknown>;
  run(opts?: SegmentOpts): Promise<unknown>;
  getActiveJob(segment?: SalesSegment): Promise<unknown>;
  getJob(jobId: string): Promise<unknown>;
  retryJob(jobId: string, opts?: RetryJobOpts): Promise<unknown>;
  sourceHealth(): Promise<unknown>;
  listRuns(opts?: SalesListOpts): Promise<unknown>;
  listLeads(opts?: SalesListOpts): Promise<unknown>;
  listProspects(opts?: SalesListOpts): Promise<unknown>;
  getAccountDossier(id: string): Promise<unknown>;
  listApprovals(opts?: ApprovalListOpts): Promise<unknown>;
  bulkApprove(ids: string[]): Promise<unknown>;
  editApproval(id: string, editedPayload: Record<string, unknown>): Promise<unknown>;
  approve(id: string): Promise<unknown>;
  reject(id: string): Promise<unknown>;
  listDeliveries(opts?: { limit?: number }): Promise<unknown>;
}
