import { Input, Pill, Table, Text, TextArea } from '@components';
import { ColorValues, PillVariantValues } from '@components/theme/config';
import React, { useMemo, useState } from 'react';
import styled from 'styled-components';

import { IMPORT_STATUS_COPY } from '@app/import/constants';
import {
    ImportEntityAspect,
    ImportEntityDraft,
    ImportEntityDraftUpdate,
    ImportEntityRow,
    ImportEntityStatus,
} from '@app/import/types';
import { colors } from '@src/alchemy-components';

const TableWrapper = styled.div`
    display: flex;
    flex: 1;
    flex-direction: column;
    overflow: hidden;
`;

const InlineInput = styled(Input)`
    width: 100%;
    && {
        margin-bottom: 0;
    }
    && label {
        display: none;
    }
    && ${'' /* Hide helper text spacing */} div[data-testid='helper-text'] {
        display: none;
    }
`;

const InlineTextArea = styled(TextArea)`
    width: 100%;
    && {
        margin-bottom: 0;
    }
    && label {
        display: none;
    }
`;

const StatusContainer = styled.div`
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
`;

const StatusGroup = styled.div`
    display: flex;
    align-items: center;
    gap: 6px;
`;

const EntityNameCell = styled.div<{ $level: number }>`
    display: flex;
    flex-direction: column;
    gap: 4px;
    padding-left: ${(props) => props.$level * 20}px;
`;

const PathText = styled(Text)`
    color: ${colors.gray[1700]};
`;

const ChangedField = styled.div`
    border-left: 3px solid ${colors.violet[500]};
    padding-left: 8px;
`;

const AspectList = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

const AspectItem = styled.div`
    display: flex;
    flex-direction: column;
    gap: 6px;
`;

const AspectHeader = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const AspectLabel = styled(Text)`
    font-weight: 600;
`;

const AspectOriginalWrapper = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
`;

const AspectOriginal = styled.pre`
    margin: 0;
    padding: 8px;
    border-radius: 4px;
    background-color: ${colors.gray[100]};
    max-height: 160px;
    overflow: auto;
    font-size: 12px;
    font-family: 'Roboto Mono', monospace;
    white-space: pre-wrap;
`;

const NoAspectsText = styled(Text)`
    color: ${colors.gray[1700]};
`;

type EntityRowData = {
    key: string;
    name: string;
    urn: string;
    displayName: string;
    originalName: string;
    description?: string | null;
    originalDescription?: string | null;
    entityType: string;
    status: ImportEntityStatus;
    path: string[];
    level: number;
    aspects?: ImportEntityAspect[];
    children?: EntityRowData[];
};

type ImportEntitiesTableProps = {
    rows: ImportEntityRow[];
    drafts: Record<string, ImportEntityDraft>;
    loading: boolean;
    onUpdateDraft: (urn: string, updates: ImportEntityDraftUpdate) => void;
};

const STATUS_COLOR_MAP: Record<ImportEntityStatus, ColorValues> = {
    READY: ColorValues.green,
    CONFLICT: ColorValues.red,
    NEW: ColorValues.violet,
    SKIPPED: ColorValues.gray,
};

const ASPECT_CHANGE_COLOR_MAP: Record<string, ColorValues> = {
    UPSERT: ColorValues.violet,
    UPDATE: ColorValues.violet,
    DELETE: ColorValues.red,
    REMOVE: ColorValues.red,
};

const buildEntityRows = (rows: ImportEntityRow[], level = 0): EntityRowData[] =>
    rows.map((row) => ({
        key: row.urn,
        name: row.urn,
        urn: row.urn,
        displayName: row.name,
        originalName: row.originalName,
        description: row.description,
        originalDescription: row.originalDescription,
        entityType: row.entityType,
        status: row.status,
        path: row.path,
        level,
        aspects: row.aspects,
        children: row.children?.length ? buildEntityRows(row.children, level + 1) : undefined,
    }));

const EntityStatusPill = ({ status }: { status: ImportEntityStatus }) => (
    <Pill
        label={IMPORT_STATUS_COPY[status]?.label || status}
        color={STATUS_COLOR_MAP[status]}
        variant={PillVariantValues.outline}
        size="sm"
    />
);

type AspectsCellProps = {
    row: EntityRowData;
    draft?: ImportEntityDraft;
    onUpdateDraft: (urn: string, updates: ImportEntityDraftUpdate) => void;
};

const AspectsCell: React.FC<AspectsCellProps> = ({ row, draft, onUpdateDraft }) => {
    if (!row.aspects?.length) {
        return (
            <NoAspectsText color="gray" size="sm">
                No aspect updates
            </NoAspectsText>
        );
    }

    return (
        <AspectList>
            {row.aspects.map((aspect) => {
                const draftValue = draft?.aspects?.[aspect.name];
                const previewValue = aspect.value ?? '';
                const currentValue = draftValue !== undefined ? draftValue ?? '' : previewValue ?? '';
                const originalValue = aspect.originalValue ?? '';
                const hasOriginalValue = (aspect.originalValue ?? '').trim().length > 0;
                const hasChanged = currentValue !== originalValue;
                const changeType = aspect.changeType || '';
                const changeColor = ASPECT_CHANGE_COLOR_MAP[changeType] || ColorValues.gray;

                const editor = (
                    <InlineTextArea
                        value={currentValue}
                        onChange={(event) =>
                            onUpdateDraft(row.urn, { aspects: { [aspect.name]: event.currentTarget.value } })
                        }
                        placeholder="Update aspect payload"
                        label=""
                    />
                );

                return (
                    <AspectItem key={aspect.name}>
                        <AspectHeader>
                            <AspectLabel size="sm">{aspect.label}</AspectLabel>
                            {changeType && (
                                <Pill
                                    label={changeType}
                                    color={changeColor}
                                    variant={PillVariantValues.outline}
                                    size="xs"
                                />
                            )}
                        </AspectHeader>
                        {aspect.description && (
                            <Text size="xs" color="gray">
                                {aspect.description}
                            </Text>
                        )}
                        {hasChanged ? <ChangedField>{editor}</ChangedField> : editor}
                        {hasOriginalValue ? (
                            <AspectOriginalWrapper>
                                <Text size="xs" color="gray">
                                    Original value
                                </Text>
                                <AspectOriginal>{originalValue}</AspectOriginal>
                            </AspectOriginalWrapper>
                        ) : (
                            <Text size="xs" color="gray">
                                No previous value
                            </Text>
                        )}
                    </AspectItem>
                );
            })}
        </AspectList>
    );
};

type EntityHierarchyTableProps = {
    data: EntityRowData[];
    drafts: Record<string, ImportEntityDraft>;
    onUpdateDraft: (urn: string, updates: ImportEntityDraftUpdate) => void;
    isLoading?: boolean;
};

const EntityHierarchyTable = ({ data, drafts, onUpdateDraft, isLoading }: EntityHierarchyTableProps) => {
    const [expandedRowUrns, setExpandedRowUrns] = useState<string[]>([]);

    const toggleRow = (row: EntityRowData) => {
        setExpandedRowUrns((prev) =>
            prev.includes(row.name) ? prev.filter((urn) => urn !== row.name) : [...prev, row.name],
        );
    };

    const columns = useMemo(
        () => [
            {
                title: 'Entity',
                key: 'entity',
                render: (row: EntityRowData) => {
                    const draft = drafts[row.urn];
                    const finalName = draft?.name ?? row.displayName;
                    const content = (
                        <EntityNameCell $level={row.level}>
                            <Text weight="bold">{finalName}</Text>
                            <Text color="gray" size="sm">
                                {row.entityType}
                            </Text>
                            {!!row.path?.length && <PathText size="xs">{row.path.join(' / ')}</PathText>}
                        </EntityNameCell>
                    );
                    return finalName !== row.originalName ? <ChangedField>{content}</ChangedField> : content;
                },
            },
            {
                title: 'Updated Name',
                key: 'name',
                render: (row: EntityRowData) => {
                    const draft = drafts[row.urn];
                    const value = draft?.name ?? row.displayName;
                    const hasChanged = value !== row.originalName;
                    const content = (
                        <InlineInput
                            value={value}
                            setValue={(next) => onUpdateDraft(row.urn, { name: next })}
                            placeholder="Entity name"
                            label=""
                        />
                    );
                    return hasChanged ? <ChangedField>{content}</ChangedField> : content;
                },
            },
            {
                title: 'Updated Description',
                key: 'description',
                render: (row: EntityRowData) => {
                    const draft = drafts[row.urn];
                    const preview = row.description ?? '';
                    const value = draft?.description ?? preview;
                    const original = row.originalDescription ?? '';
                    const hasChanged = value !== original;
                    const content = (
                        <InlineTextArea
                            value={value}
                            onChange={(event) => onUpdateDraft(row.urn, { description: event.currentTarget.value })}
                            placeholder="Describe the entity"
                            label=""
                        />
                    );
                    return hasChanged ? <ChangedField>{content}</ChangedField> : content;
                },
            },
            {
                title: 'Status',
                key: 'status',
                width: '160px',
                render: (row: EntityRowData) => <EntityStatusPill status={row.status} />,
            },
            {
                title: 'Aspect Updates',
                key: 'aspects',
                render: (row: EntityRowData) => (
                    <AspectsCell row={row} draft={drafts[row.urn]} onUpdateDraft={onUpdateDraft} />
                ),
            },
        ],
        [drafts, onUpdateDraft],
    );

    return (
        <Table<EntityRowData>
            columns={columns}
            data={data}
            isLoading={isLoading}
            rowKey={(row: EntityRowData) => row.key}
            onRowClick={toggleRow}
            expandable={{
                rowExpandable: (row: EntityRowData) => !!row.children?.length,
                expandedGroupIds: expandedRowUrns,
                expandedRowRender: (row: EntityRowData) =>
                    row.children ? (
                        <EntityHierarchyTable data={row.children} drafts={drafts} onUpdateDraft={onUpdateDraft} />
                    ) : null,
            }}
            isBorderless
        />
    );
};

export const ImportEntitiesTable = ({ rows, drafts, loading, onUpdateDraft }: ImportEntitiesTableProps) => {
    const entityRows = useMemo(() => buildEntityRows(rows), [rows]);

    return (
        <TableWrapper>
            <EntityHierarchyTable data={entityRows} drafts={drafts} onUpdateDraft={onUpdateDraft} isLoading={loading} />
        </TableWrapper>
    );
};

export const StatusSummary = ({ counts }: { counts: Record<ImportEntityStatus, number> }) => {
    const statuses = (Object.keys(counts) as ImportEntityStatus[]).filter((status) => counts[status] > 0);

    if (!statuses.length) {
        return <Text color="gray">No entities</Text>;
    }

    return (
        <StatusContainer>
            {statuses.map((status) => (
                <StatusGroup key={status}>
                    <Pill
                        label={IMPORT_STATUS_COPY[status]?.label || status}
                        color={STATUS_COLOR_MAP[status]}
                        variant={PillVariantValues.filled}
                        size="sm"
                    />
                    <Text color="gray" size="sm">
                        {counts[status]}
                    </Text>
                </StatusGroup>
            ))}
        </StatusContainer>
    );
};
