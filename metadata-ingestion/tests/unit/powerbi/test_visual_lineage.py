import pathlib

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.powerbi.config import PowerBiDashboardSourceConfig
from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
    create_dataplatform_instance_resolver,
)
from datahub.ingestion.source.powerbi.powerbi import (
    Mapper,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper import data_classes as pbi_dc
from datahub.ingestion.source.powerbi.visual_lineage import (
    extract_visual_upstreams,
    guess_columns_from_dax,
    normalize_query_ref,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import UpstreamLineageClass

FIXTURES_DIR = pathlib.Path(__file__).parent / "fixtures"


def test_normalize_query_ref_handles_table_and_measure():
    assert normalize_query_ref("Sales[Revenue]") == "Sales.Revenue"
    assert normalize_query_ref("[Total Profit]") == "Total Profit"
    assert normalize_query_ref("  'Calendar' [ Date ] ") == "Calendar.Date"


def test_guess_columns_from_dax_finds_references():
    expr = "SUMX('Sales', 'Sales'[Revenue]) + [Total Profit]"
    refs = guess_columns_from_dax(expr)
    assert "Sales.Revenue" in refs
    assert "Total Profit" in refs


def test_extract_visual_upstreams_returns_columns(tmp_path):
    project_root = FIXTURES_DIR / "simple_project"
    dataset_columns = ["Sales.Revenue", "Total Profit", "Calendar.Date"]

    result = extract_visual_upstreams(
        project_root=str(project_root),
        workspace_id="workspace",
        report_id="report",
        dataset_columns=dataset_columns,
    )

    assert "Revenue Visual" in result
    assert sorted(result["Revenue Visual"]) == ["Sales.Revenue", "Total Profit"]
    assert result.get_transform_operation("Revenue Visual") == "DAX_OR_QUERY"
    assert "Calendar Card" in result
    assert result.get_transform_operation("Calendar Card") == "DIRECT"


def test_mapper_emits_fine_grained_lineage(tmp_path):
    project_root = FIXTURES_DIR / "simple_project"
    config = PowerBiDashboardSourceConfig(
        tenant_id="tenant",
        client_id="client",
        client_secret="secret",
        extract_fine_grained_lineage=True,
        pbitools_project_root=str(project_root),
    )
    ctx = PipelineContext(run_id="test")
    resolver = create_dataplatform_instance_resolver(config)
    mapper = Mapper(ctx, config, PowerBiDashboardSourceReport(), resolver)

    dataset = pbi_dc.PowerBIDataset(
        id="dataset1",
        name="Sales Model",
        description="",
        webUrl=None,
        workspace_id="workspace123",
        workspace_name="Workspace",
        parameters={},
        tables=[],
        tags=[],
        configuredBy=None,
    )

    sales_table = pbi_dc.Table(
        name="Sales",
        full_name="SalesModel.Sales",
        columns=[
            pbi_dc.Column(
                name="Revenue",
                dataType="Double",
                isHidden=False,
                datahubDataType=pbi_dc.FIELD_TYPE_MAPPING["Double"],
            )
        ],
        measures=[
            pbi_dc.Measure(
                name="Total Profit",
                expression="SUM('Sales'[Revenue])",
                isHidden=False,
            )
        ],
        dataset=dataset,
    )

    calendar_table = pbi_dc.Table(
        name="Calendar",
        full_name="SalesModel.Calendar",
        columns=[
            pbi_dc.Column(
                name="Date",
                dataType="Datetime",
                isHidden=False,
                datahubDataType=pbi_dc.FIELD_TYPE_MAPPING["Datetime"],
            )
        ],
        measures=None,
        dataset=dataset,
    )

    dataset.tables.extend([sales_table, calendar_table])

    workspace = pbi_dc.Workspace(
        id="workspace123",
        name="Workspace",
        type="Workspace",
        dashboards={},
        reports={},
        datasets={},
        report_endorsements={},
        dashboard_endorsements={},
        scan_result={},
        independent_datasets={},
        app=None,
    )

    report = pbi_dc.Report(
        id="report1",
        name="Sales Overview",
        type=pbi_dc.ReportType.PowerBIReport,
        webUrl=None,
        embedUrl="",
        description="",
        dataset_id=dataset.id,
        dataset=dataset,
        pages=[],
        users=[],
        tags=[],
    )

    mapper.to_datahub_dataset(dataset, workspace)

    lineage_mcps = mapper._visual_lineage_mcps(report, workspace, set())
    assert lineage_mcps, "Expected fine-grained lineage MCPs for report"

    aspect = lineage_mcps[0].aspect
    assert isinstance(aspect, UpstreamLineageClass)

    fine_grained = aspect.fineGrainedLineages
    assert fine_grained, "Expected fine-grained lineage entries"
    assert any(fgl.transformOperation == "DAX_OR_QUERY" for fgl in fine_grained)
    assert any(
        "visuals.Revenue_Visual.data" in downstream
        for fgl in fine_grained
        for downstream in fgl.downstreams or []
    )
