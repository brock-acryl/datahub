import { gql, useMutation } from '@apollo/client';
import { Button, PageTitle, Pagination, SearchBar, Tabs, Text } from '@components';
import React, { useCallback, useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';

import TabToolbar from '@app/entity/shared/components/styled/TabToolbar';
import { ImportEntitiesTable, StatusSummary } from '@app/import/ImportEntitiesTable';
import { DEFAULT_PAGE_SIZE } from '@app/import/constants';
import {
    ImportEntityAspect,
    ImportEntityDraft,
    ImportEntityDraftUpdate,
    ImportEntityGroup,
    ImportEntityRow,
} from '@app/import/types';
import { useImportEntities } from '@app/import/useImportEntities';
import useShowToast from '@app/homeV3/toast/useShowToast';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';
import { colors } from '@src/alchemy-components';

const PageContainer = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    display: flex;
    flex-direction: column;
    height: 100%;
    padding: 16px;
    background-color: white;
    border-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '8px'};
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        margin: 5px;
        box-shadow: ${props.theme.styles['box-shadow-navbar-redesign']};
    `}
`;

const PageHeaderContainer = styled.div`
    padding: 0 20px 16px 20px;
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

const TitleContainer = styled.div`
    display: flex;
    flex-direction: column;
`;

const HeaderActions = styled.div`
    display: flex;
    gap: 12px;
`;

const StyledTabToolbar = styled(TabToolbar)`
    padding: 8px 20px;
    box-shadow: none;
    flex-shrink: 0;
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

const FiltersContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 12px;
`;

const SummaryContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 20px;
`;

const StyledSearchBar = styled(SearchBar)`
    width: 240px;
`;

const TableContainer = styled.div`
    flex: 1;
    display: flex;
    flex-direction: column;
    padding: 0 20px;
    overflow: hidden;
`;

const EmptyState = styled.div`
    flex: 1;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    gap: 8px;
    border: 1px dashed ${colors.gray[1400]};
    border-radius: 8px;
    margin-top: 16px;
`;

const PaginationContainer = styled.div`
    padding: 12px 20px 16px;
    display: flex;
    justify-content: flex-end;
`;

type PatchOperation = {
    op: 'add' | 'replace' | 'remove';
    path: string;
    value?: unknown;
};

type EntityPatchInput = {
    urn: string;
    entityType: string;
    operations: PatchOperation[];
};

const PATCH_ENTITIES_MUTATION = gql`
    mutation patchEntities($input: PatchEntitiesInput!) {
        patchEntities(input: $input) {
            status
        }
    }
`;

const flattenRows = (rows: ImportEntityRow[], map: Map<string, ImportEntityRow>) => {
    rows.forEach((row) => {
        map.set(row.urn, row);
        if (row.children?.length) {
            flattenRows(row.children, map);
        }
    });
};

const buildOriginalMap = (groups: ImportEntityGroup[]) => {
    const map = new Map<string, ImportEntityRow>();
    groups.forEach((group) => flattenRows(group.rows, map));
    return map;
};

const normalizeAspectString = (value?: string | null) => {
    if (value === null || value === undefined) {
        return null;
    }
    const trimmed = value.trim();
    if (!trimmed) {
        return '';
    }
    try {
        return JSON.stringify(JSON.parse(value));
    } catch (e) {
        return trimmed;
    }
};

const parseAspectValue = (value?: string | null) => {
    if (value === undefined) {
        return undefined;
    }
    if (value === null) {
        return null;
    }
    const trimmed = value.trim();
    if (!trimmed) {
        return '';
    }
    try {
        return JSON.parse(value);
    } catch (e) {
        return value;
    }
};

const buildPatchOperations = (draft: ImportEntityDraft, original: ImportEntityRow): PatchOperation[] => {
    const operations: PatchOperation[] = [];
    const previewName = original.name;
    const targetName = draft.name ?? previewName;
    if (targetName !== original.originalName) {
        operations.push({ op: 'replace', path: '/name', value: targetName });
    }

    const previewDescription = original.description ?? '';
    const targetDescription = draft.description ?? previewDescription ?? '';
    const originalDescription = original.originalDescription ?? '';
    if (targetDescription !== originalDescription) {
        operations.push({ op: 'replace', path: '/description', value: targetDescription });
    }

    const aspectOverrides = draft.aspects || {};
    const aspectMap = new Map<string, ImportEntityAspect>();
    original.aspects?.forEach((aspect) => {
        aspectMap.set(aspect.name, aspect);
    });
    const aspectNames = new Set<string>([
        ...Array.from(aspectMap.keys()),
        ...Object.keys(aspectOverrides),
    ]);

    aspectNames.forEach((aspectName) => {
        const aspect = aspectMap.get(aspectName);
        const previewValue = aspect?.value ?? null;
        const originalValue = aspect?.originalValue ?? null;
        const overrideValue = aspectOverrides[aspectName];
        const targetValue = overrideValue !== undefined ? overrideValue : previewValue;
        if (targetValue === undefined) {
            return;
        }

        const normalizedTarget = normalizeAspectString(targetValue);
        const normalizedOriginal = normalizeAspectString(originalValue);

        if (normalizedTarget === normalizedOriginal) {
            return;
        }

        const parsedValue = parseAspectValue(targetValue);
        if (parsedValue === undefined) {
            return;
        }

        operations.push({
            op: parsedValue === null ? 'remove' : 'replace',
            path: `/aspects/${aspectName}`,
            value: parsedValue,
        });
    });

    return operations;
};

export const ImportEntitiesPage: React.FC = () => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const { showToast } = useShowToast();

    const [page, setPage] = useState(1);
    const [searchValue, setSearchValue] = useState('');
    const [query, setQuery] = useState<string | undefined>();
    const [selectedEntityType, setSelectedEntityType] = useState<string | undefined>();
    const [drafts, setDrafts] = useState<Record<string, ImportEntityDraft>>({});

    const start = (page - 1) * DEFAULT_PAGE_SIZE;

    const { data, loading, error, refetch } = useImportEntities({
        start,
        count: DEFAULT_PAGE_SIZE,
        query,
    });

    const originalByUrn = useMemo(() => buildOriginalMap(data.groups), [data.groups]);

    useEffect(() => {
        setDrafts((prev) => {
            const nextEntries = Object.entries(prev).filter(([urn]) => originalByUrn.has(urn));
            if (nextEntries.length === Object.keys(prev).length) {
                return prev;
            }
            return Object.fromEntries(nextEntries);
        });
    }, [originalByUrn]);

    useEffect(() => {
        if (!data.groups.length) {
            setSelectedEntityType(undefined);
            return;
        }
        if (!selectedEntityType || !data.groups.some((group) => group.id === selectedEntityType)) {
            setSelectedEntityType(data.groups[0].id);
        }
    }, [data.groups, selectedEntityType]);

    useEffect(() => {
        const handler = setTimeout(() => {
            setPage(1);
            setQuery(searchValue ? searchValue : undefined);
        }, 300);
        return () => clearTimeout(handler);
    }, [searchValue]);

    const [patchEntities, { loading: isImporting }] = useMutation(PATCH_ENTITIES_MUTATION);

    const patches: EntityPatchInput[] = useMemo(() => {
        return Array.from(originalByUrn.entries())
            .map(([urn, original]) => {
                const draft = drafts[urn] || {};
                const operations = buildPatchOperations(draft, original);
                if (!operations.length) return null;
                return {
                    urn,
                    entityType: original.entityType,
                    operations,
                };
            })
            .filter((patch): patch is EntityPatchInput => patch !== null);
    }, [drafts, originalByUrn]);

    const pendingPatchCount = patches.length;

    const handleUpdateDraft = useCallback(
        (urn: string, updates: ImportEntityDraftUpdate) => {
            setDrafts((prev) => {
                const original = originalByUrn.get(urn);
                if (!original) {
                    return prev;
                }

                const existing = prev[urn] || {};
                const nextName = updates.name !== undefined ? updates.name : existing.name;
                const nextDescription =
                    updates.description !== undefined ? updates.description : existing.description;
                const mergedAspects = updates.aspects
                    ? { ...(existing.aspects || {}), ...updates.aspects }
                    : existing.aspects;

                const normalized: ImportEntityDraft = {};

                if (nextName !== undefined && nextName !== original.name) {
                    normalized.name = nextName;
                }

                if (nextDescription !== undefined && nextDescription !== (original.description ?? '')) {
                    normalized.description = nextDescription;
                }

                if (mergedAspects) {
                    const normalizedAspects: Record<string, string | null> = {};
                    Object.entries(mergedAspects).forEach(([aspectName, value]) => {
                        const previewAspect = original.aspects?.find((aspect) => aspect.name === aspectName);
                        const previewValue = previewAspect?.value ?? null;
                        const normalizedPreview = normalizeAspectString(previewValue);
                        const normalizedNext = normalizeAspectString(value ?? '');
                        if (normalizedNext !== normalizedPreview) {
                            normalizedAspects[aspectName] = value ?? '';
                        }
                    });
                    if (Object.keys(normalizedAspects).length) {
                        normalized.aspects = normalizedAspects;
                    }
                }

                const hasOverrides =
                    normalized.name !== undefined ||
                    normalized.description !== undefined ||
                    (normalized.aspects && Object.keys(normalized.aspects).length > 0);

                if (!hasOverrides) {
                    if (!prev[urn]) {
                        return prev;
                    }
                    const nextDrafts = { ...prev };
                    delete nextDrafts[urn];
                    return nextDrafts;
                }

                return { ...prev, [urn]: normalized };
            });
        },
        [originalByUrn],
    );

    const selectedGroup = useMemo(() => {
        if (!data.groups.length) {
            return undefined;
        }
        return data.groups.find((group) => group.id === selectedEntityType) || data.groups[0];
    }, [data.groups, selectedEntityType]);

    const tabs = useMemo(
        () =>
            data.groups.map((group) => ({
                key: group.id,
                name: group.label,
                count: group.total,
                component: (
                    <ImportEntitiesTable
                        rows={group.rows}
                        drafts={drafts}
                        loading={loading}
                        onUpdateDraft={handleUpdateDraft}
                    />
                ),
            })),
        [data.groups, drafts, loading, handleUpdateDraft],
    );

    const activeTabKey = selectedEntityType || (tabs[0]?.key ?? '');

    const handlePageChange = (nextPage: number) => {
        setPage(nextPage);
    };

    const handleImport = async () => {
        if (!patches.length) {
            showToast('No changes to import', 'Select or edit rows to import metadata.');
            return;
        }
        try {
            await patchEntities({
                variables: {
                    input: {
                        patches,
                    },
                },
            });
            showToast('Import started', 'We are applying your metadata updates.');
            setDrafts((prev) => {
                const next = { ...prev };
                patches.forEach((patch) => {
                    delete next[patch.urn];
                });
                return next;
            });
            refetch();
        } catch (e: any) {
            showToast('Import failed', e?.message || 'Unable to import entity changes.');
        }
    };

    const pendingCopy = useMemo(() => {
        if (!pendingPatchCount) {
            return 'No pending changes';
        }
        const noun = pendingPatchCount === 1 ? 'change' : 'changes';
        return `${pendingPatchCount} ${noun} ready`;
    }, [pendingPatchCount]);

    const disableImport = !pendingPatchCount || isImporting;

    const totalPages = Math.ceil(data.total / DEFAULT_PAGE_SIZE);

    return (
        <PageContainer $isShowNavBarRedesign={isShowNavBarRedesign}>
            <PageHeaderContainer>
                <TitleContainer>
                    <PageTitle
                        title="Import Entities"
                        subTitle="Review, edit, and apply metadata updates across entity groups."
                    />
                </TitleContainer>
                <HeaderActions>
                    <Button variant="outlined" onClick={() => refetch()} disabled={loading}>
                        Refresh
                    </Button>
                    <Button variant="filled" onClick={handleImport} disabled={disableImport} loading={isImporting}>
                        Import Entities
                    </Button>
                </HeaderActions>
            </PageHeaderContainer>
            <StyledTabToolbar>
                <FiltersContainer>
                    <StyledSearchBar
                        value={searchValue}
                        onChange={(value) => setSearchValue(value)}
                        placeholder="Search entities"
                        allowClear
                    />
                </FiltersContainer>
                <SummaryContainer>
                    <Text color="gray" size="sm">
                        {pendingCopy}
                    </Text>
                    {selectedGroup && <StatusSummary counts={selectedGroup.statusCounts} />}
                </SummaryContainer>
            </StyledTabToolbar>
            <TableContainer>
                {error && (
                    <Text color="red" size="sm" style={{ padding: '12px 0' }}>
                        {error.message}
                    </Text>
                )}
                {!loading && !data.total ? (
                    <EmptyState>
                        <Text weight="bold">No entities ready for import</Text>
                        <Text color="gray" size="sm">
                            Adjust your filters or upload a new import file to review metadata updates.
                        </Text>
                    </EmptyState>
                ) : (
                    tabs.length ? (
                        <Tabs
                            tabs={tabs}
                            selectedTab={activeTabKey}
                            onChange={(key) => setSelectedEntityType(key)}
                            styleOptions={{ containerHeight: 'full' }}
                        />
                    ) : null
                )}
            </TableContainer>
            {totalPages > 1 && (
                <PaginationContainer>
                    <Pagination
                        currentPage={page}
                        itemsPerPage={DEFAULT_PAGE_SIZE}
                        total={data.total}
                        onPageChange={handlePageChange}
                    />
                </PaginationContainer>
            )}
        </PageContainer>
    );
};

export default ImportEntitiesPage;
