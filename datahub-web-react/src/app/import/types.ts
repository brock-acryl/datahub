export type ImportEntityStatus = 'READY' | 'CONFLICT' | 'NEW' | 'SKIPPED';

export type ImportEntityAspect = {
    name: string;
    label: string;
    description?: string | null;
    value?: string | null;
    originalValue?: string | null;
    changeType?: string | null;
};

export type ImportEntityRow = {
    urn: string;
    entityType: string;
    name: string;
    description?: string | null;
    originalName: string;
    originalDescription?: string | null;
    status: ImportEntityStatus;
    path: string[];
    parentUrn?: string | null;
    children?: ImportEntityRow[];
    aspects?: ImportEntityAspect[];
};

export type ImportEntityGroup = {
    id: string;
    label: string;
    total: number;
    statusCounts: Record<ImportEntityStatus, number>;
    rows: ImportEntityRow[];
};

export type ImportEntityDraft = {
    name?: string;
    description?: string | null;
    aspects?: Record<string, string | null>;
};

export type ImportEntityDraftUpdate = {
    name?: string;
    description?: string | null;
    aspects?: Record<string, string | null>;
};

export type ImportEntitiesQueryVariables = {
    start: number;
    count: number;
    query?: string;
    group?: string;
};

export type ImportEntitiesQueryResult = {
    groups: ImportEntityGroup[];
    start: number;
    count: number;
    total: number;
};
