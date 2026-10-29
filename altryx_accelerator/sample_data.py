"""Synthetic smoke-test sources. These are not customer data or parity evidence."""
from datetime import date, datetime
from decimal import Decimal
from .tools.builtin import SPARK_TYPE_MAP

# Semantic aliases for the absent customer Excel lookup, for synthetic tests only.
ALIASES = {
    'txt_income_range': 'household_income', 'id_respondent': 'respondent_id',
    'cde_age_range': 'age_group', 'cde_ethnicity': 'ethnicity',
    'cde_marital_status': 'marital_status', 'cde_termination': 'termination',
    'txt_family_status': 'family_status', 'txt_education': 'education',
    'grp_sub_region': 'region', 'nam_sub_region_alt': 'region_alt',
    'txt_employment_status': 'employment_status', 'sampling_phase': 'sample_phase',
    'flg_survey_version': 'survey_version', 'cde_respondent_status': 'respondent_status',
    'ind_hispanic_latino': 'hispanic/latino', 'txt_military_status': 'military_status',
    'region': 'region2', 'txt_verbatim': 'answer_text', 'val_nps': 'nps',
    'Company_duration': 'company_duration', 'Product_Owner_Combo_Group': 'product_ownership',
    'provider_name': 'provider', 'product_name': 'product', 'Purchase_Channel': 'purchase_channel',
    'Rep+ Filter rNPS': 'rnps_rep_filter', 'Rep+ Filter pNPS': 'pnps_rep_filter',
    'txt_answer': 'answer_text', 'num_answer': 'answer_numeric',
    'switch_value': 'answer_numeric', 'switch_provider': 'answer_text',
    'Episode assigned': 'episode_assigned', 'name': 'episode_name',
    'group': 'episode_group', 'description': 'episode_desc',
    'channel_name_initial': 'initial_channel_name',
    'channel_description_initial': 'initial_channel_description',
    'channel_group_initial': 'initial_channel_group',
    'nam_channel_medium_initial': 'channel_medium_initial',
    'channel_name_final': 'final_channel_name',
    'channel_description_final': 'final_channel_description',
    'channel_group_final': 'final_channel_group',
    'nam_channel_medium_final': 'channel_medium_final',
}


def register_sample_sources(workflow, spark):
    """Register isolated temporary views from the workflow's input field metadata.

    The USI Excel file is unavailable: use an explicitly synthetic rename lookup.
    Every caller receives mappings and must opt in; production conversion never
    substitutes these sources automatically.
    """
    from pyspark.sql.types import StructType, StructField, _parse_datatype_string
    from .parser import ET, SAFE_XML_PARSER
    mapping_rows = {}
    for tool in workflow.all_tools.values():
        if tool.tool_type != 'DynamicRename':
            continue
        xml = ET.fromstring(tool.configuration_xml.encode(), SAFE_XML_PARSER)
        if xml.findtext('RenameMode') != 'RightInputRows':
            continue
        for field in xml.findall('Fields/Field'):
            old = field.get('name')
            if old.startswith('*'):
                continue
            short = old
            for prefix in ('rnps_verbatim_', 'pnps_verbatim_', 'resp_', 'provider_', 'product_', 'channel_', 'episode_', 'firms_', 'likelihood_', 'considered_'):
                if short.startswith(prefix):
                    short = short[len(prefix):]
                    break
            # Already-common fields retain their names.
            new = old if old.startswith('misc_') else 'misc_' + ALIASES.get(old, ALIASES.get(short, short))
            mapping_rows[old] = new
    mappings = {}
    for tool in workflow.all_tools.values():
        if tool.tool_type != 'InputData':
            continue
        fields = tool.parsed_config.get('fields', [])
        names = [f['name'] for f in fields]
        if 'Consumption Field Name' in names:
            frame = spark.createDataFrame(sorted(mapping_rows.items()), ['Consumption Field Name', 'New Field Name'])
        elif 'transactions' in tool.parsed_config.get('table_name', '').lower():
            frame = spark.createDataFrame([(1, 10, 'East', 100.0), (2, 20, 'West', 200.0)], ['TxnID','CustomerID','Region','Revenue'])
        else:
            if not fields:
                raise ValueError(f'Input {tool.tool_id} has no field metadata for synthetic validation')
            schema = StructType([StructField(f['name'], _parse_datatype_string(SPARK_TYPE_MAP.get(f['type'], 'string')), True) for f in fields])
            values = []
            for f in fields:
                typ = SPARK_TYPE_MAP.get(f['type'], 'string')
                value = (1 if typ in ('short','int','long','byte') else 1.0 if typ in ('float','double')
                         else True if typ == 'boolean' else date(2024,1,1) if typ == 'date'
                         else datetime(2024,1,1) if typ == 'timestamp' else Decimal('1') if typ.startswith('decimal') else '1')
                if f["name"] == "gid_wave":
                    value = workflow.metadata.get("constants", {}).get("User.gid_wave", 1)
                values.append(value)
            frame = spark.createDataFrame([tuple(values)], schema)
        view = f'altryx_sample_{tool.tool_id}'
        frame.createOrReplaceTempView(view)
        mappings[str(tool.tool_id)] = view
    return mappings
