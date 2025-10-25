export const DEFAULT_PAGE_SIZE = 25;

export const GLOSSARY_GROUP_KEY = 'GLOSSARY';

export const ENTITY_GROUP_LABELS: Record<string, string> = {
    DATASET: 'Datasets',
    DATAFLOW: 'Pipelines',
    DATAJOB: 'Jobs',
    DATA_PLATFORM: 'Platforms',
    DASHBOARD: 'Dashboards',
    CHART: 'Charts',
    CONTAINER: 'Containers',
    GLOSSARY: 'Glossary',
    GLOSSARY_NODE: 'Glossary',
    GLOSSARY_TERM: 'Glossary',
    DOMAIN: 'Domains',
};

export const IMPORT_STATUS_COPY: Record<string, { label: string; helperText: string }> = {
    READY: {
        label: 'Ready',
        helperText: 'All fields validated',
    },
    CONFLICT: {
        label: 'Needs Review',
        helperText: 'Conflicting metadata detected',
    },
    NEW: {
        label: 'New Entity',
        helperText: 'This entity will be created',
    },
    SKIPPED: {
        label: 'Skipped',
        helperText: 'No changes applied',
    },
};
