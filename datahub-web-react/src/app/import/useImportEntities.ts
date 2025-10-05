import { gql, useQuery } from '@apollo/client';
import { useMemo } from 'react';

import { ENTITY_GROUP_LABELS, GLOSSARY_GROUP_KEY } from '@app/import/constants';
import {
    ImportEntitiesQueryResult,
    ImportEntitiesQueryVariables,
    ImportEntityGroup,
    ImportEntityRow,
    ImportEntityStatus,
} from '@app/import/types';

const IMPORT_ENTITIES_PREVIEW_QUERY = gql`
    query importEntitiesPreview($input: ImportEntitiesPreviewInput!) {
        importEntitiesPreview(input: $input) {
            start
            count
            total
            groups {
                id
                name
                type
                total
                statusCounts {
                    READY
                    CONFLICT
                    NEW
                    SKIPPED
                }
                entities {
                    urn
                    entityType
                    name
                    description
                    originalName
                    originalDescription
                    status
                    path
                    parentUrn
                    aspects {
                        aspectName
                        displayName
                        description
                        newValue
                        previousValue
                        changeType
                    }
                    children {
                        urn
                        entityType
                        name
                        description
                        originalName
                        originalDescription
                        status
                        path
                        parentUrn
                        aspects {
                            aspectName
                            displayName
                            description
                            newValue
                            previousValue
                            changeType
                        }
                    }
                }
                groups {
                    id
                    name
                    type
                    total
                    statusCounts {
                        READY
                        CONFLICT
                        NEW
                        SKIPPED
                    }
                    entities {
                        urn
                        entityType
                        name
                        description
                        originalName
                        originalDescription
                        status
                        path
                        parentUrn
                        aspects {
                            aspectName
                            displayName
                            description
                            newValue
                            previousValue
                            changeType
                        }
                        children {
                            urn
                            entityType
                            name
                            description
                            originalName
                            originalDescription
                            status
                            path
                            parentUrn
                            aspects {
                                aspectName
                                displayName
                                description
                                newValue
                                previousValue
                                changeType
                            }
                        }
                    }
                }
            }
        }
    }
`;

type ImportEntitiesPreviewResponse = {
    importEntitiesPreview?: {
        start?: number;
        count?: number;
        total?: number;
        groups?: RawImportGroup[];
    } | null;
};

type RawImportGroup = {
    id?: string | null;
    name?: string | null;
    type?: string | null;
    total?: number | null;
    statusCounts?: Partial<Record<ImportEntityStatus, number>> | null;
    entities?: RawImportEntity[] | null;
    groups?: RawImportGroup[] | null;
};

type RawImportEntity = {
    urn?: string | null;
    entityType?: string | null;
    name?: string | null;
    description?: string | null;
    originalName?: string | null;
    originalDescription?: string | null;
    status?: ImportEntityStatus | null;
    path?: string[] | null;
    parentUrn?: string | null;
    children?: RawImportEntity[] | null;
    aspects?: RawImportAspect[] | null;
};

type RawImportAspect = {
    aspectName?: string | null;
    displayName?: string | null;
    description?: string | null;
    newValue?: unknown;
    previousValue?: unknown;
    changeType?: string | null;
};

const EMPTY_RESULT: ImportEntitiesQueryResult = {
    start: 0,
    count: 0,
    total: 0,
    groups: [],
};

const DEFAULT_STATUS_COUNTS: Record<ImportEntityStatus, number> = {
    READY: 0,
    CONFLICT: 0,
    NEW: 0,
    SKIPPED: 0,
};

const ensureStatusCounts = (
    counts?: Partial<Record<ImportEntityStatus, number>> | null,
): Record<ImportEntityStatus, number> => {
    const result = { ...DEFAULT_STATUS_COUNTS };
    if (!counts) {
        return result;
    }
    (Object.keys(DEFAULT_STATUS_COUNTS) as ImportEntityStatus[]).forEach((status) => {
        result[status] = counts[status] || 0;
    });
    return result;
};

const normalizeGroupKey = (group?: RawImportGroup) => {
    const key = group?.type || group?.id || 'UNKNOWN';
    if (key === 'GLOSSARY_NODE' || key === 'GLOSSARY_TERM') {
        return GLOSSARY_GROUP_KEY;
    }
    return key;
};

const mergeStatusCounts = (
    target: Record<ImportEntityStatus, number>,
    incoming?: Partial<Record<ImportEntityStatus, number>> | null,
) => {
    const next = { ...target };
    if (!incoming) {
        return next;
    }
    (Object.keys(DEFAULT_STATUS_COUNTS) as ImportEntityStatus[]).forEach((status) => {
        next[status] = (next[status] || 0) + (incoming?.[status] || 0);
    });
    return next;
};

const computeStatusCountsFromRows = (rows: ImportEntityRow[]) => {
    const counts = { ...DEFAULT_STATUS_COUNTS };
    rows.forEach((row) => {
        counts[row.status] = (counts[row.status] || 0) + 1;
        if (row.children?.length) {
            const childCounts = computeStatusCountsFromRows(row.children);
            (Object.keys(DEFAULT_STATUS_COUNTS) as ImportEntityStatus[]).forEach((status) => {
                counts[status] = (counts[status] || 0) + childCounts[status];
            });
        }
    });
    return counts;
};

const stringifyAspectValue = (value: unknown) => {
    if (value === null || value === undefined) {
        return null;
    }
    if (typeof value === 'string') {
        return value;
    }
    try {
        return JSON.stringify(value, null, 2);
    } catch (e) {
        return String(value);
    }
};

const transformAspect = (aspect: RawImportAspect) => {
    const name = aspect.aspectName || 'unknown';
    return {
        name,
        label: aspect.displayName || name,
        description: aspect.description,
        value: stringifyAspectValue(aspect.newValue),
        originalValue: stringifyAspectValue(aspect.previousValue),
        changeType: aspect.changeType,
    };
};

const transformEntity = (entity: RawImportEntity): ImportEntityRow => {
    const children = (entity.children || [])
        .filter((child): child is RawImportEntity => !!child)
        .map((child) => transformEntity(child));
    const status = (entity.status as ImportEntityStatus) || 'READY';
    const aspects = (entity.aspects || [])
        .filter((aspect): aspect is RawImportAspect => !!aspect)
        .map((aspect) => transformAspect(aspect));
    return {
        urn: entity.urn || `pending-${Math.random().toString(36).slice(2)}`,
        entityType: entity.entityType || 'UNKNOWN',
        name: entity.name || '',
        originalName: entity.originalName || entity.name || '',
        description: entity.description,
        originalDescription: entity.originalDescription ?? entity.description,
        status,
        path: entity.path || [],
        parentUrn: entity.parentUrn,
        children,
        aspects,
    };
};

const transformGroup = (group: RawImportGroup): ImportEntityGroup => {
    const rows = (group.entities || [])
        .filter((entity): entity is RawImportEntity => !!entity)
        .map((entity) => transformEntity(entity));

    const nestedGroups = (group.groups || [])
        .filter((child): child is RawImportGroup => !!child)
        .map((child) => transformGroup(child));

    nestedGroups.forEach((nestedGroup) => {
        rows.push(...nestedGroup.rows);
    });

    const computedCounts = computeStatusCountsFromRows(rows);
    const providedCounts = ensureStatusCounts(group.statusCounts as Partial<Record<ImportEntityStatus, number>>);
    const hasProvidedCounts = (Object.keys(DEFAULT_STATUS_COUNTS) as ImportEntityStatus[]).some(
        (status) => providedCounts[status] > 0,
    );
    const statusCounts = hasProvidedCounts ? providedCounts : computedCounts;

    const key = normalizeGroupKey(group);
    const label = group.name || ENTITY_GROUP_LABELS[key] || key;

    return {
        id: key,
        label,
        total: group.total || rows.length,
        statusCounts,
        rows,
    };
};

const flattenRows = (rows: ImportEntityRow[], accumulator: ImportEntityRow[] = []) => {
    rows.forEach((row) => {
        accumulator.push({ ...row, children: undefined });
        if (row.children?.length) {
            flattenRows(row.children, accumulator);
        }
    });
    return accumulator;
};

const buildHierarchyByParent = (rows: ImportEntityRow[]) => {
    const cloneMap = new Map<string, ImportEntityRow>();
    const roots: ImportEntityRow[] = [];

    rows.forEach((row) => {
        cloneMap.set(row.urn, { ...row, children: [] });
    });

    rows.forEach((row) => {
        const clone = cloneMap.get(row.urn);
        if (!clone) {
            return;
        }
        if (row.parentUrn && cloneMap.has(row.parentUrn)) {
            const parent = cloneMap.get(row.parentUrn);
            if (parent) {
                parent.children = parent.children || [];
                parent.children.push(clone);
            }
            return;
        }
        roots.push(clone);
    });

    const finalize = (items: ImportEntityRow[]): ImportEntityRow[] =>
        items.map((item) => ({
            ...item,
            children: item.children && item.children.length ? finalize(item.children) : undefined,
        }));

    return finalize(roots);
};

const groupRowsByEntityType = (groups: ImportEntityGroup[]): ImportEntityGroup[] => {
    const byEntityType = new Map<
        string,
        {
            label: string;
            rows: ImportEntityRow[];
            statusCounts: Record<ImportEntityStatus, number>;
        }
    >();

    groups.forEach((group) => {
        const flattened = flattenRows(group.rows);
        flattened.forEach((row) => {
            const key = normalizeGroupKey({ type: row.entityType });
            const existing = byEntityType.get(key);
            const label = ENTITY_GROUP_LABELS[key] || row.entityType || key;
            const statusCounts = existing?.statusCounts || { ...DEFAULT_STATUS_COUNTS };
            statusCounts[row.status] = (statusCounts[row.status] || 0) + 1;
            const rowsForGroup = existing?.rows || [];
            rowsForGroup.push(row);
            byEntityType.set(key, {
                label,
                rows: rowsForGroup,
                statusCounts,
            });
        });
    });

    return Array.from(byEntityType.entries()).map(([key, value]) => ({
        id: key,
        label: value.label,
        total: value.rows.length,
        statusCounts: value.statusCounts,
        rows: buildHierarchyByParent(value.rows),
    }));
};

const mergeGroupsByKey = (groups: RawImportGroup[]) => {
    const accumulator = new Map<string, RawImportGroup>();
    groups.forEach((group) => {
        const key = normalizeGroupKey(group);
        const existing = accumulator.get(key);
        if (!existing) {
            accumulator.set(key, { ...group, id: key, type: key });
            return;
        }
        const merged: RawImportGroup = {
            ...existing,
            total: (existing.total || 0) + (group.total || 0),
            statusCounts: mergeStatusCounts(
                ensureStatusCounts(existing.statusCounts as Partial<Record<ImportEntityStatus, number>>),
                group.statusCounts as Partial<Record<ImportEntityStatus, number>>,
            ),
            entities: [...(existing.entities || []), ...(group.entities || [])],
            groups: [...(existing.groups || []), ...(group.groups || [])],
        };
        accumulator.set(key, merged);
    });
    return Array.from(accumulator.values());
};

export const buildImportVariables = ({ start, count, query, group }: ImportEntitiesQueryVariables) => ({
    input: {
        start,
        count,
        query: query || undefined,
        group: group || undefined,
    },
});

export function useImportEntities(variables: ImportEntitiesQueryVariables) {
    const { data, loading, error, refetch } = useQuery<ImportEntitiesPreviewResponse>(
        IMPORT_ENTITIES_PREVIEW_QUERY,
        {
            variables: buildImportVariables(variables),
            fetchPolicy: 'cache-and-network',
        },
    );

    const result = useMemo<ImportEntitiesQueryResult>(() => {
        if (!data?.importEntitiesPreview) {
            return EMPTY_RESULT;
        }
        const mergedGroups = mergeGroupsByKey(data.importEntitiesPreview.groups || []);
        const groupsBySource = mergedGroups.map((group) => transformGroup(group));
        const groups = groupRowsByEntityType(groupsBySource);
        return {
            start: data.importEntitiesPreview.start || 0,
            count: data.importEntitiesPreview.count || 0,
            total: data.importEntitiesPreview.total || 0,
            groups,
        };
    }, [data]);

    return {
        data: result,
        loading,
        error,
        refetch,
    };
}
