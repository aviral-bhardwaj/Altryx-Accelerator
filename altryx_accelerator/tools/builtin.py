"""
Built-in Tool Converters
=========================
One plugin class per Alteryx tool family. Each converter emits PySpark
DataFrame code lines for its tool and registers itself with the tool
registry via ``@register("ToolType", ...)``.

To add support for a new Alteryx tool:

1. Subclass ``ToolConverter`` here (or in a new module under ``altryx_accelerator/tools/``).
2. Decorate it with ``@register("YourToolType")``.
3. Optionally declare it in ``config/tool_mapping.yaml`` to override or
   extend the built-in mapping without touching this file.
"""

import re

from ..expression_parser import convert_expression, convert_filter_expression
from ..models import Tool
from .base import GeneratorContext, ToolConverter
from .registry import register


def pystr(value: str) -> str:
    """Emit a safe double-quoted Python string literal (escapes \\ and \")."""
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _identifier(value: str) -> str:
    """Sanitize an arbitrary string into a snake_case identifier fragment."""
    cleaned = re.sub(r"[^a-z0-9_]+", "_", value.lower()).strip("_")
    return cleaned or "unknown"


FILE_EXTENSIONS = {"csv", "xlsx", "xls", "parquet", "yxdb", "txt", "json", "avro", "orc"}


def _table_leaf_name(table_name: str) -> str:
    """
    Extract the logical table/stem name from an output target:
    ``catalog.schema.table`` → ``table``; ``path/output.csv`` → ``output``.
    """
    leaf = table_name.replace("\\", "/").rsplit("/", 1)[-1]
    if "." in leaf:
        stem, ext = leaf.rsplit(".", 1)
        if ext.lower() in FILE_EXTENSIONS:
            # File target: use the stem's last dotted segment (drop the extension).
            return stem.rsplit(".", 1)[-1]
        # Catalog reference: the last dotted segment IS the table name.
        return ext
    return leaf

# Alteryx field type -> Spark SQL type used for casts.
SPARK_TYPE_MAP = {
    "Int16": "short", "Int32": "int", "Int64": "long",
    "Byte": "byte", "Float": "float", "Double": "double",
    "FixedDecimal": "decimal(18,2)", "String": "string",
    "V_String": "string", "V_WString": "string", "WString": "string",
    "Bool": "boolean", "Date": "date", "DateTime": "timestamp",
}


@register("InputData", "LockInStreamIn", "DynamicInput")
class InputDataConverter(ToolConverter):
    """Convert InputData tools to spark.read / spark.table calls."""

    def convert(self, tool: Tool, ctx: GeneratorContext) -> list:
        pc = tool.parsed_config or {}
        table_name = pc.get("table_name", "")
        var = ctx.make_var_name(tool)
        ctx.set_output_var(tool.tool_id, var)

        if table_name and "\\" not in table_name:
            fmt = "csv" if table_name.lower().endswith(".csv") else "parquet" if table_name.lower().endswith(".parquet") else None
            if fmt:
                options = ", header=True, inferSchema=True" if fmt == "csv" else ""
                return [f"_source_{tool.tool_id} = _resolve_source({str(tool.tool_id)!r})",
                        f"if _source_{tool.tool_id}.lower().endswith('.{fmt}'):",
                        f"    {var} = spark.read.{fmt}(_source_{tool.tool_id}{options})  # Tool {tool.tool_id}",
                        "else:", f"    {var} = spark.table(_source_{tool.tool_id})"]
        # All sources are editable in the generated notebook's first config cell.
        return [f'{var} = spark.table(_resolve_source({str(tool.tool_id)!r}))  # Tool {tool.tool_id}: InputData']


_INT_RE = re.compile(r"^[+-]?(0|[1-9][0-9]*)$")
_DEC_RE = re.compile(r"^[+-]?(0|[1-9][0-9]*)?\.[0-9]+$|^[+-]?(0|[1-9][0-9]*)\.?$")


def _infer_text_input_type(values) -> str:
    """
    Mimic Alteryx Text Input auto-typing: a column whose non-empty values are
    all integers becomes ``long``, all numeric becomes ``double``, otherwise it
    stays ``string``. Values with leading zeros (zip codes, IDs) stay strings.
    Columns containing empty values stay strings to avoid ANSI cast errors.
    """
    vals = [("" if v is None else str(v)).strip() for v in values]
    if not vals or any(v == "" for v in vals):
        return "string"
    if all(_INT_RE.match(v) for v in vals):
        return "long"
    if all(_INT_RE.match(v) or _DEC_RE.match(v) for v in vals):
        return "double"
    return "string"


@register("TextInput")
class TextInputConverter(ToolConverter):
    """Convert TextInput tools to spark.createDataFrame()."""

    def convert(self, tool: Tool, ctx: GeneratorContext) -> list:
        var = ctx.make_var_name(tool)
        ctx.set_output_var(tool.tool_id, var)

        text_data = ctx.workflow.text_inputs.get(tool.tool_id, [])
        if not text_data:
            return [f'{var} = spark.createDataFrame([], schema="")  # Tool {tool.tool_id}: TextInput (empty)']

        headers = list(text_data[0].keys())
        lines = []
        lines.append(f"# Tool {tool.tool_id}: TextInput")
        lines.append(f"_data_{tool.tool_id} = [")
        for row in text_data:
            values = ", ".join(repr(row.get(h, "")) for h in headers)
            lines.append(f"    ({values}),")
        lines.append("]")
        col_list = ", ".join(repr(h) for h in headers)
        lines.append(f"{var} = spark.createDataFrame(_data_{tool.tool_id}, [{col_list}])")
        # Alteryx Text Input auto-detects numeric field types; replicate so that
        # numeric filters/sorts behave correctly (and don't fail under ANSI mode).
        for h in headers:
            spark_type = _infer_text_input_type([row.get(h, "") for row in text_data])
            if spark_type != "string":
                lines.append(f'{var} = {var}.withColumn({pystr(h)}, F.col({pystr(h)}).cast("{spark_type}"))')
        return lines


@register("OutputData", "LockInStreamOut")
class OutputDataConverter(ToolConverter):
    """Convert OutputData tools to df.write statements."""

    def convert(self, tool: Tool, ctx: GeneratorContext) -> list:
        pc = tool.parsed_config or {}
        table_name = pc.get("table_name", "output_table")
        input_var = ctx.get_input_var(tool.tool_id)
        var = f"df_{tool.tool_id}"
        ctx.set_output_var(tool.tool_id, var)

        lower_table = table_name.lower()
        is_file_target = ("/" in table_name or "\\" in table_name
                          or lower_table.rsplit(".", 1)[-1] in FILE_EXTENSIONS)

        # Qualify bare table names with the configured Unity Catalog target
        # (catalog.schema.table) so generated writes are workspace-portable.
        if (not is_file_target and table_name.count(".") < 2
                and ctx.target_catalog and ctx.target_schema):
            table_name = (f"{ctx.target_catalog}.{ctx.target_schema}."
                          f"{_identifier(table_name)}")

        lines = [f"# Tool {tool.tool_id}: OutputData -> {table_name}"]
        lines.append(f"{var} = {input_var}")

        if lower_table.endswith(".csv"):
            lines.append(f'# {var}.write.csv({pystr(table_name)}, header=True, mode="overwrite")')
        else:
            lines.append(f'# {var}.write.format("delta").mode("overwrite").saveAsTable({pystr(table_name)})')
        view_name = _identifier(_table_leaf_name(table_name))
        lines.append(f'{var}.createOrReplaceTempView("{view_name}")')
        return lines


@register("Filter", "LockInFilter")
class FilterConverter(ToolConverter):
    """Convert Filter tools to df.filter()."""

    def convert(self, tool: Tool, ctx: GeneratorContext) -> list:
        pc = tool.parsed_config or {}
        expression = pc.get("expression", "")
        input_var = ctx.get_input_var(tool.tool_id)

        lines = []
        pyspark_expr = convert_filter_expression(expression) if expression else "F.lit(True)"

        # True output
        true_connected = ctx.is_port_connected(tool.tool_id, "True")
        false_connected = ctx.is_port_connected(tool.tool_id, "False")
        output_connected = ctx.is_port_connected(tool.tool_id, "Output")

        if true_connected or output_connected:
            true_var = f"df_{tool.tool_id}_true"
            lines.append(f"{true_var} = {input_var}.filter({pyspark_expr})  # Tool {tool.tool_id}: Filter (True)")
            ctx.set_output_var(tool.tool_id, true_var, "True")
            ctx.set_output_var(tool.tool_id, true_var, "Output")

        if false_connected:
            false_var = f"df_{tool.tool_id}_false"
            lines.append(f"{false_var} = {input_var}.filter(~({pyspark_expr}))  # Tool {tool.tool_id}: Filter (False)")
            ctx.set_output_var(tool.tool_id, false_var, "False")

        if not lines:
            # Neither port connected - emit True output by default
            var = f"df_{tool.tool_id}"
            lines.append(f"{var} = {input_var}.filter({pyspark_expr})  # Tool {tool.tool_id}: Filter")
            ctx.set_output_var(tool.tool_id, var)

        return lines


@register("Formula", "LockInFormula")
class FormulaConverter(ToolConverter):
    """Convert Formula tools to df.withColumn() chains."""

    def convert(self, tool: Tool, ctx: GeneratorContext) -> list:
        pc = tool.parsed_config or {}
        formulas = pc.get("formulas", [])
        input_var = ctx.get_input_var(tool.tool_id)
        var = ctx.make_var_name(tool)
        ctx.set_output_var(tool.tool_id, var)

        if not formulas:
            return [f"{var} = {input_var}  # Tool {tool.tool_id}: Formula (no formulas parsed)"]

        lines = [f"# Tool {tool.tool_id}: Formula"]
        lines.append(f"{var} = {input_var}")
        for f in formulas:
            field = f.get("field", "unknown")
            expr = f.get("expression", "")
            ftype = f.get("type", "")
            try:
                pyspark_expr = convert_expression(expr) if expr else 'F.lit(None)'
            except Exception:
                pyspark_expr = f'F.expr("{expr}")  # TODO: manual conversion needed'

            if ftype == "Bool":
                # Alteryx permits 0/1 branches with Boolean fields; Spark requires
                # branch types to agree before the final withColumn cast.
                import ast
                class BooleanBranches(ast.NodeTransformer):
                    def visit_Call(self, node):
                        self.generic_visit(node)
                        if isinstance(node.func, ast.Attribute):
                            index = 1 if node.func.attr == "when" else 0 if node.func.attr == "otherwise" else None
                            if index is not None and len(node.args) > index:
                                node.args[index] = ast.Call(func=ast.Attribute(value=node.args[index], attr="cast", ctx=ast.Load()), args=[ast.Constant("boolean")], keywords=[])
                        return node
                pyspark_expr = ast.unparse(BooleanBranches().visit(ast.parse(pyspark_expr, mode="eval")))

            lines.append(f'{var} = {var}.withColumn("{field}", {pyspark_expr})')
            if ftype:
                spark_type = SPARK_TYPE_MAP.get(ftype, "")
                if spark_type:
                    lines.append(f'{var} = {var}.withColumn("{field}", F.col("{field}").cast("{spark_type}"))')
        return lines


@register("Select", "LockInSelect")
class SelectConverter(ToolConverter):
    """Convert Select tools to df.select/drop/rename/cast operations."""

    def convert(self, tool: Tool, ctx: GeneratorContext) -> list:
        pc = tool.parsed_config or {}
        fields = pc.get("select_fields", [])
        input_var = ctx.get_input_var(tool.tool_id)
        var = ctx.make_var_name(tool)
        ctx.set_output_var(tool.tool_id, var)

        if not fields:
            return [f"{var} = {input_var}  # Tool {tool.tool_id}: Select (passthrough)"]

        lines = [f"# Tool {tool.tool_id}: Select"]
        lines.append(f"{var} = {input_var}")

        drops = []
        for sf in fields:
            field = sf.get("field", "")
            selected = sf.get("selected", "True")
            rename = sf.get("rename", "")
            ftype = sf.get("type", "")

            if field.startswith("*"):
                continue  # Wildcard, skip

            if selected == "False":
                drops.append(field)
                continue

            if rename:
                lines.append(f'{var} = {var}.withColumnRenamed("{field}", "{rename}")')

            if ftype and ftype in SPARK_TYPE_MAP:
                col_name = rename or field
                lines.append(f'{var} = {var}.withColumn("{col_name}", F.col("{col_name}").cast("{SPARK_TYPE_MAP[ftype]}"))')

        if drops:
            drop_str = ", ".join(f'"{d}"' for d in drops)
            lines.append(f"{var} = {var}.drop({drop_str})")

        return lines


@register("Join", "LockInJoin")
class JoinConverter(ToolConverter):
    """Convert Join tools to df.join()."""

    def convert(self, tool: Tool, ctx: GeneratorContext) -> list:
        pc = tool.parsed_config or {}
        left_keys = pc.get("left_keys", [])
        right_keys = pc.get("right_keys", [])
        input_left = ctx.get_input_var_for_port(tool.tool_id, "Left")
        input_right = ctx.get_input_var_for_port(tool.tool_id, "Right")

        lines = [f"# Tool {tool.tool_id}: Join"]

        # Determine which output ports are connected
        join_connected = ctx.is_port_connected(tool.tool_id, "Join")
        left_anti_connected = ctx.is_port_connected(tool.tool_id, "Left")
        right_anti_connected = ctx.is_port_connected(tool.tool_id, "Right")

        # Build join condition
        if left_keys == right_keys:
            # Same key names - use on=[] syntax to auto-deduplicate
            key_list = ", ".join(f'"{k}"' for k in left_keys)
            join_cond = f"[{key_list}]"
        else:
            # Different key names - use explicit condition
            conditions = []
            for lk, rk in zip(left_keys, right_keys):
                conditions.append(f'{input_left}["{lk}"] == {input_right}["{rk}"]')
            join_cond = " & ".join(f"({c})" for c in conditions) if len(conditions) > 1 else conditions[0] if conditions else "F.lit(True)"

        if join_connected:
            join_var = f"df_{tool.tool_id}_joined"
            select_config = pc.get("select_config", [])
            if select_config:
                # Prefix both schemas before joining so duplicate names remain addressable.
                lp, rp = f"_left_{tool.tool_id}", f"_right_{tool.tool_id}"
                lines += [f"{lp} = {input_left}.select(*[F.col('`' + c.replace('`', '``') + '`').alias('Left_' + c) for c in {input_left}.columns])",
                          f"{rp} = {input_right}.select(*[F.col('`' + c.replace('`', '``') + '`').alias('Right_' + c) for c in {input_right}.columns])"]
                conditions = [f"{lp}[{('Left_' + lk)!r}] == {rp}[{('Right_' + rk)!r}]" for lk,rk in zip(left_keys,right_keys)]
                cond = " & ".join(f"({c})" for c in conditions) or "F.lit(True)"
                right_ref = f"F.broadcast({rp})" if any(ctx.tools[c.origin_tool_id].tool_type == "TextInput" for c in ctx.get_incoming(tool.tool_id) if c.dest_connection == "Right") else rp
                lines += [f'{join_var} = {lp}.join({right_ref}, {cond}, "inner")',
                          f"_spec_{tool.tool_id} = {{s['field']: s for s in {select_config!r}}}",
                          f"_projection_{tool.tool_id}, _used_{tool.tool_id} = [], set()",
                          f"for _c in {join_var}.columns:",
                          f"    _raw = _c.split('_', 1)[1]",
                          f"    _s = _spec_{tool.tool_id}.get(_c, _spec_{tool.tool_id}.get(_raw, {{}}))",
                          f"    if _s.get('selected', 'True') == 'False':",
                          f"        continue",
                          f"    _name = _s.get('rename') or _raw",
                          f"    if _name in _used_{tool.tool_id}:",
                          f"        _name = _c",
                          f"    _used_{tool.tool_id}.add(_name)",
                          f"    _projection_{tool.tool_id}.append(F.col('`' + _c.replace('`', '``') + '`').alias(_name))",
                          f"{join_var} = {join_var}.select(*_projection_{tool.tool_id})"]
            else:
                lines.append(f'{join_var} = {input_left}.join({input_right}, {join_cond}, "inner")  # Join output')

            ctx.set_output_var(tool.tool_id, join_var, "Join")
            ctx.set_output_var(tool.tool_id, join_var, "Output")

        if left_anti_connected:
            left_var = f"df_{tool.tool_id}_left_unmatched"
            lines.append(f'{left_var} = {input_left}.join({input_right}, {join_cond}, "left_anti")  # Left unmatched')
            ctx.set_output_var(tool.tool_id, left_var, "Left")

        if right_anti_connected:
            right_var = f"df_{tool.tool_id}_right_unmatched"
            lines.append(f'{right_var} = {input_right}.join({input_left}, {join_cond}, "left_anti")  # Right unmatched')
            ctx.set_output_var(tool.tool_id, right_var, "Right")

        if not lines[1:]:
            # No ports connected - default to inner join
            join_var = f"df_{tool.tool_id}"
            lines.append(f'{join_var} = {input_left}.join({input_right}, {join_cond}, "inner")')
            ctx.set_output_var(tool.tool_id, join_var)

        return lines


@register("Union")
class UnionConverter(ToolConverter):
    """Convert Union tools to unionByName."""

    def convert(self, tool: Tool, ctx: GeneratorContext) -> list:
        var = ctx.make_var_name(tool)
        ctx.set_output_var(tool.tool_id, var)

        # Collect all input variables
        incoming = ctx.get_incoming(tool.tool_id)
        input_vars = []
        for conn in incoming:
            origin_port = conn.origin_connection
            vars_map = ctx.df_vars.get(conn.origin_tool_id, {})
            v = vars_map.get(origin_port, vars_map.get("Output", f"df_{conn.origin_tool_id}"))
            input_vars.append(v)

        if len(input_vars) < 2:
            if input_vars:
                return [f"{var} = {input_vars[0]}  # Tool {tool.tool_id}: Union (single input)"]
            return [f"{var} = spark.createDataFrame([], schema='')  # Tool {tool.tool_id}: Union (no inputs)"]

        lines = [f"# Tool {tool.tool_id}: Union"]
        stream_list = ", ".join(input_vars)
        lines.append(f"_union_streams_{tool.tool_id} = [{stream_list}]")
        lines.append("from functools import reduce")
        lines.append(
            f"{var} = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), "
            f"_union_streams_{tool.tool_id})"
        )
        return lines


@register("Summarize")
class SummarizeConverter(ToolConverter):
    """Convert Summarize tools to groupBy().agg()."""

    def convert(self, tool: Tool, ctx: GeneratorContext) -> list:
        pc = tool.parsed_config or {}
        fields = pc.get("summarize_fields", [])
        input_var = ctx.get_input_var(tool.tool_id)
        var = ctx.make_var_name(tool)
        ctx.set_output_var(tool.tool_id, var)

        if not fields:
            return [f"{var} = {input_var}  # Tool {tool.tool_id}: Summarize (no config)"]

        group_cols = []
        agg_exprs = []

        action_map = {
            "GroupBy": None,
            "Sum": "F.sum",
            "Count": "F.count",
            "CountDistinct": "F.countDistinct",
            "Avg": "F.avg",
            "Min": "F.min",
            "Max": "F.max",
            "First": "F.first",
            "Last": "F.last",
            "CountNonNull": "F.count",
        }

        order_sensitive = False
        for sf in fields:
            field = sf.get("field", "")
            action = sf.get("action", "")
            rename = sf.get("rename", "")
            alias = rename or f"{action.lower()}_{field}"

            if action == "GroupBy":
                group_cols.append(f'"{field}"')
            elif action == "Concatenate":
                agg_exprs.append(f'F.concat_ws(", ", F.collect_list("{field}")).alias("{alias}")')
            elif action in action_map and action_map[action]:
                func = action_map[action]
                agg_exprs.append(f'{func}("{field}").alias("{alias}")')
                if action in ("First", "Last"):
                    order_sensitive = True
            else:
                agg_exprs.append(f'F.first("{field}").alias("{alias}")')
                order_sensitive = True

        lines = [f"# Tool {tool.tool_id}: Summarize"]
        if order_sensitive:
            # Unlike Alteryx's in-order desktop engine, Spark gives no row-order
            # guarantee after a shuffle, so First/Last can vary between runs.
            lines.append("# TODO: First/Last aggregations are order-dependent — Spark does not")
            lines.append("#       guarantee row order after a shuffle. Add an explicit ordering")
            lines.append("#       (e.g. Window + row_number over a sort key) for deterministic results.")
        if group_cols:
            group_str = ", ".join(group_cols)
            agg_str = ",\n    ".join(agg_exprs)
            lines.append(f"{var} = {input_var}.groupBy({group_str}).agg(")
            lines.append(f"    {agg_str}")
            lines.append(")")
        elif agg_exprs:
            agg_str = ",\n    ".join(agg_exprs)
            lines.append(f"{var} = {input_var}.agg(")
            lines.append(f"    {agg_str}")
            lines.append(")")
        else:
            lines.append(f"{var} = {input_var}")

        return lines


@register("Sort")
class SortConverter(ToolConverter):
    """Convert Sort tools to df.orderBy()."""

    def convert(self, tool: Tool, ctx: GeneratorContext) -> list:
        pc = tool.parsed_config or {}
        sort_fields = pc.get("sort_fields", [])
        input_var = ctx.get_input_var(tool.tool_id)
        var = ctx.make_var_name(tool)
        ctx.set_output_var(tool.tool_id, var)

        if not sort_fields:
            return [f"{var} = {input_var}  # Tool {tool.tool_id}: Sort (no sort config)"]

        order_exprs = []
        for sf in sort_fields:
            field = sf.get("field", "")
            order = sf.get("order", "Ascending")
            if order.lower() == "descending":
                order_exprs.append(f'F.col("{field}").desc()')
            else:
                order_exprs.append(f'F.col("{field}").asc()')

        order_str = ", ".join(order_exprs)
        return [f"{var} = {input_var}.orderBy({order_str})  # Tool {tool.tool_id}: Sort"]


@register("Unique")
class UniqueConverter(ToolConverter):
    """Convert Unique tools to df.dropDuplicates()."""

    def convert(self, tool: Tool, ctx: GeneratorContext) -> list:
        pc = tool.parsed_config or {}
        unique_fields = pc.get("unique_fields", [])
        input_var = ctx.get_input_var(tool.tool_id)

        lines = [f"# Tool {tool.tool_id}: Unique"]

        unique_connected = ctx.is_port_connected(tool.tool_id, "Unique") or ctx.is_port_connected(tool.tool_id, "Output")
        dupes_connected = ctx.is_port_connected(tool.tool_id, "Dupes")

        if unique_fields:
            field_list = ", ".join(f'"{f}"' for f in unique_fields)
            subset = f"[{field_list}]"
        else:
            subset = ""

        if unique_connected:
            unique_var = f"df_{tool.tool_id}_unique"
            if subset:
                lines.append(f"{unique_var} = {input_var}.dropDuplicates({subset})")
            else:
                lines.append(f"{unique_var} = {input_var}.dropDuplicates()")
            ctx.set_output_var(tool.tool_id, unique_var, "Unique")
            ctx.set_output_var(tool.tool_id, unique_var, "Output")

        if dupes_connected:
            dupes_var = f"df_{tool.tool_id}_dupes"
            unique_ref = f"df_{tool.tool_id}_unique"
            lines.append(f"{dupes_var} = {input_var}.exceptAll({unique_ref})")
            ctx.set_output_var(tool.tool_id, dupes_var, "Dupes")

        if not unique_connected and not dupes_connected:
            var = f"df_{tool.tool_id}"
            if subset:
                lines.append(f"{var} = {input_var}.dropDuplicates({subset})")
            else:
                lines.append(f"{var} = {input_var}.dropDuplicates()")
            ctx.set_output_var(tool.tool_id, var)

        return lines


@register("Sample")
class SampleConverter(ToolConverter):
    """Convert Sample tools to df.limit()."""

    def convert(self, tool: Tool, ctx: GeneratorContext) -> list:
        pc = tool.parsed_config or {}
        n_records = pc.get("n_records", "100")
        input_var = ctx.get_input_var(tool.tool_id)
        var = ctx.make_var_name(tool)
        ctx.set_output_var(tool.tool_id, var)

        try:
            n = int(n_records)
        except (ValueError, TypeError):
            n = 100

        return [f"{var} = {input_var}.limit({n})  # Tool {tool.tool_id}: Sample"]


@register("CrossTab")
class CrossTabConverter(ToolConverter):
    """Convert CrossTab tools to groupBy().pivot().agg()."""

    def convert(self, tool: Tool, ctx: GeneratorContext) -> list:
        pc = tool.parsed_config or {}
        group_fields = pc.get("group_fields", "")
        header_field = pc.get("header_field", "")
        data_field = pc.get("data_field", "")
        method = pc.get("method", "Sum")
        input_var = ctx.get_input_var(tool.tool_id)
        var = ctx.make_var_name(tool)
        ctx.set_output_var(tool.tool_id, var)

        method_map = {
            "Sum": "F.sum", "Count": "F.count", "Avg": "F.avg",
            "Min": "F.min", "Max": "F.max",
        }
        agg_func = method_map.get(method, "F.sum")

        group_list = ", ".join(f'"{g.strip()}"' for g in group_fields.split(",") if g.strip())

        lines = [f"# Tool {tool.tool_id}: CrossTab"]
        lines.append(
            f'{var} = {input_var}.groupBy({group_list})'
            f'.pivot("{header_field}")'
            f'.agg({agg_func}("{data_field}"))'
        )
        return lines


@register("Transpose")
class TransposeConverter(ToolConverter):
    """Convert Transpose tools to stack() / unpivot."""

    def convert(self, tool: Tool, ctx: GeneratorContext) -> list:
        pc = tool.parsed_config or {}
        key_fields = pc.get("key_fields", [])
        data_fields = pc.get("data_fields", [])
        input_var = ctx.get_input_var(tool.tool_id)
        var = ctx.make_var_name(tool)
        ctx.set_output_var(tool.tool_id, var)

        if not data_fields:
            return [f"{var} = {input_var}  # Tool {tool.tool_id}: Transpose (no data fields)"]

        key_cols = ", ".join(f'"{k}"' for k in key_fields)
        n = len(data_fields)
        col_pairs = ", ".join(
            f"'{f}', cast(`{f}` as string)" for f in data_fields
        )
        stack_expr = f"stack({n}, {col_pairs}) as (Name, Value)"

        lines = [f"# Tool {tool.tool_id}: Transpose"]
        lines.append(f'{var} = {input_var}.select({key_cols}, F.expr("{stack_expr}"))')
        return lines


@register("MultiRowFormula")
class MultiRowFormulaConverter(ToolConverter):
    """Convert MultiRowFormula tools to Window functions."""

    def convert(self, tool: Tool, ctx: GeneratorContext) -> list:
        pc = tool.parsed_config or {}
        formulas = pc.get("formulas", [])
        num_rows = pc.get("num_rows", "1")
        input_var = ctx.get_input_var(tool.tool_id)
        var = ctx.make_var_name(tool)
        ctx.set_output_var(tool.tool_id, var)

        lines = [f"# Tool {tool.tool_id}: MultiRowFormula"]
        lines.append("# TODO: monotonically_increasing_id() only approximates the original row")
        lines.append("#       order and is unreliable after shuffles — replace with a real sort")
        lines.append("#       key (e.g. an upstream RecordID column) for order-dependent logic.")
        lines.append(f"_w_{tool.tool_id} = Window.orderBy(F.monotonically_increasing_id())")
        lines.append(f"{var} = {input_var}")

        try:
            offset = int(num_rows)
        except (ValueError, TypeError):
            offset = 1

        for f in formulas:
            field = f.get("field", "unknown")
            expr = f.get("expression", "")
            # Detect common patterns
            if "Row-1" in expr or "row-1" in expr.lower():
                lines.append(f'{var} = {var}.withColumn("{field}", F.lag("{field}", {offset}).over(_w_{tool.tool_id}))')
            elif "Row+1" in expr or "row+1" in expr.lower():
                lines.append(f'{var} = {var}.withColumn("{field}", F.lead("{field}", {offset}).over(_w_{tool.tool_id}))')
            else:
                try:
                    pyspark_expr = convert_expression(expr) if expr else 'F.lit(None)'
                except Exception:
                    pyspark_expr = 'F.lit(None)'
                lines.append(f'{var} = {var}.withColumn("{field}", {pyspark_expr})')

        return lines


@register("RegEx")
class RegExConverter(ToolConverter):
    """Convert RegEx tools to regexp_extract/replace/rlike."""

    def convert(self, tool: Tool, ctx: GeneratorContext) -> list:
        pc = tool.parsed_config or {}
        expression = pc.get("expression", "")
        field = pc.get("field", "")
        output_method = pc.get("output_method", "")
        input_var = ctx.get_input_var(tool.tool_id)
        var = ctx.make_var_name(tool)
        ctx.set_output_var(tool.tool_id, var)

        lines = [f"# Tool {tool.tool_id}: RegEx"]

        if "replace" in output_method.lower():
            lines.append(f'{var} = {input_var}.withColumn("{field}", F.regexp_replace(F.col("{field}"), r"{expression}", ""))')
        elif "match" in output_method.lower():
            lines.append(f'{var} = {input_var}.filter(F.col("{field}").rlike(r"{expression}"))')
        elif "parse" in output_method.lower() or "tokenize" in output_method.lower():
            lines.append(f'{var} = {input_var}.withColumn("{field}_parsed", F.regexp_extract(F.col("{field}"), r"{expression}", 1))')
        else:
            lines.append(f'{var} = {input_var}.withColumn("{field}_regex", F.regexp_extract(F.col("{field}"), r"{expression}", 0))')

        return lines


@register("RecordID")
class RecordIDConverter(ToolConverter):
    """Convert RecordID tools to monotonically_increasing_id()."""

    def convert(self, tool: Tool, ctx: GeneratorContext) -> list:
        pc = tool.parsed_config or {}
        field_name = pc.get("field_name", "RecordID")
        start_value = pc.get("start_value", "1")
        input_var = ctx.get_input_var(tool.tool_id)
        var = ctx.make_var_name(tool)
        ctx.set_output_var(tool.tool_id, var)

        try:
            start = int(start_value)
        except (ValueError, TypeError):
            start = 1

        return [
            f'{var} = {input_var}.withColumn("{field_name}", '
            f'F.monotonically_increasing_id() + {start})  # Tool {tool.tool_id}: RecordID'
        ]


@register("AppendFields")
class AppendFieldsConverter(ToolConverter):
    """Convert AppendFields tools to crossJoin."""

    def convert(self, tool: Tool, ctx: GeneratorContext) -> list:
        input_target = ctx.get_input_var_for_port(tool.tool_id, "Target")
        input_source = ctx.get_input_var_for_port(tool.tool_id, "Source")

        # If we can't distinguish Target/Source, use Left/Right or generic
        if input_target == f"df_{tool.tool_id}_target_input":
            input_target = ctx.get_input_var_for_port(tool.tool_id, "Input")
        if input_source == f"df_{tool.tool_id}_source_input":
            # Get second incoming connection
            incoming = ctx.get_incoming(tool.tool_id)
            if len(incoming) >= 2:
                origin_id = incoming[1].origin_tool_id
                vars_map = ctx.df_vars.get(origin_id, {})
                input_source = vars_map.get("Output", f"df_{origin_id}")

        var = ctx.make_var_name(tool)
        ctx.set_output_var(tool.tool_id, var)

        return [
            f"# Tool {tool.tool_id}: AppendFields (CrossJoin)",
            f"{var} = {input_target}.crossJoin({input_source})"
        ]


@register("RunningTotal")
class RunningTotalConverter(ToolConverter):
    """Convert RunningTotal tools to window-based running sum."""

    def convert(self, tool: Tool, ctx: GeneratorContext) -> list:
        input_var = ctx.get_input_var(tool.tool_id)
        var = ctx.make_var_name(tool)
        ctx.set_output_var(tool.tool_id, var)

        lines = [f"# Tool {tool.tool_id}: RunningTotal"]
        lines.append(f"_w_rt_{tool.tool_id} = Window.orderBy(F.monotonically_increasing_id()).rowsBetween(Window.unboundedPreceding, Window.currentRow)")
        lines.append(f"{var} = {input_var}")
        lines.append(f'# REVIEW: Add running total columns: {var} = {var}.withColumn("running_total", F.sum("value_col").over(_w_rt_{tool.tool_id}))')
        return lines


@register("Browse", "BrowseV2")
class BrowseConverter(ToolConverter):
    """Browse tools are inspection-only; pass through."""

    def convert(self, tool: Tool, ctx: GeneratorContext) -> list:
        input_var = ctx.get_input_var(tool.tool_id)
        var = f"df_{tool.tool_id}"
        ctx.set_output_var(tool.tool_id, var)
        return [f"{var} = {input_var}  # Tool {tool.tool_id}: Browse (passthrough)"]


@register("TextToColumns")
class TextToColumnsConverter(ToolConverter):
    """Convert TextToColumns to F.split / explode."""

    def convert(self, tool: Tool, ctx: GeneratorContext) -> list:
        input_var = ctx.get_input_var(tool.tool_id)
        var = ctx.make_var_name(tool)
        ctx.set_output_var(tool.tool_id, var)

        lines = [f"# Tool {tool.tool_id}: TextToColumns"]
        lines.append(f'{var} = {input_var}  # REVIEW: configure split field and delimiter')
        return lines


@register("DateTime")
class DateTimeConverter(ToolConverter):
    """Convert DateTime tools."""

    def convert(self, tool: Tool, ctx: GeneratorContext) -> list:
        input_var = ctx.get_input_var(tool.tool_id)
        var = ctx.make_var_name(tool)
        ctx.set_output_var(tool.tool_id, var)
        return [f"{var} = {input_var}  # Tool {tool.tool_id}: DateTime - REVIEW: configure format conversion"]


@register("DynamicRename")
class DynamicRenameConverter(ToolConverter):
    """Translate actual Designer XML, including L/R lookup connections."""

    def convert(self, tool: Tool, ctx: GeneratorContext) -> list:
        from ..parser import ET, SAFE_XML_PARSER
        root = ET.fromstring((tool.configuration_xml or "<Configuration/>").encode(), SAFE_XML_PARSER)
        mode = root.findtext("RenameMode", "Formula")
        fields = {f.get("name"): f.get("selected", "True").lower() != "false"
                  for f in root.findall("Fields/Field")}
        input_var = ctx.get_input_var_for_port(tool.tool_id, "Left")
        incoming = ctx.get_incoming(tool.tool_id)
        left = next((c for c in incoming if c.dest_connection in ("Left", "L", "Input", "Targets")), None)
        right = next((c for c in incoming if c.dest_connection in ("Right", "R", "Source")), None)
        if left:
            input_var = ctx.df_vars[left.origin_tool_id].get(left.origin_connection, ctx.df_vars[left.origin_tool_id].get("Output"))
        else:
            input_var = ctx.get_input_var(tool.tool_id)
        var = ctx.make_var_name(tool)
        ctx.set_output_var(tool.tool_id, var)
        prefix = f"_rename_{tool.tool_id}"
        lines = [f"# Tool {tool.tool_id}: DynamicRename ({mode})",
                 f"{prefix}_fields = {fields!r}",
                 f"{prefix}_cols = [c for c in {input_var}.columns if {prefix}_fields.get(c, {prefix}_fields.get('*Unknown', not {prefix}_fields))]",
                 f"{prefix}_map = {{}}"]
        norm = mode.lower().replace(" ", "")
        if norm in ("firstrow", "firstrowofdata"):
            lines += [f"{prefix}_data = {input_var}", f"{prefix}_rows = {input_var}.take(1)",
                      f"if {prefix}_rows:",
                      f"    {prefix}_map = {{c: str({prefix}_rows[0][c]) for c in {prefix}_cols if {prefix}_rows[0][c] is not None}}",
                      # exceptAll removes one matching row, preserving duplicate data rows.
                      f"    {prefix}_data = {input_var}.exceptAll(spark.createDataFrame({prefix}_rows, {input_var}.schema))"]
        elif norm in ("rightinput", "rightinputrows", "rightinputfieldnames"):
            if right is None:
                raise ValueError("DynamicRename lookup requires an R/Right input")
            rv = ctx.df_vars[right.origin_tool_id].get(right.origin_connection, ctx.df_vars[right.origin_tool_id].get("Output"))
            old = root.findtext("NamesFromRows/OldName", "OldName")
            new = root.findtext("NamesFromRows/NewName", "NewName")
            if norm == "rightinputfieldnames":
                lines += [f"{prefix}_map = dict(zip({prefix}_cols, {rv}.columns))"]
            elif root.findtext("NamesFromRows/InputMode", "Mapped") == "Position":
                lines += [f"{prefix}_map = dict(zip({prefix}_cols, [str(r[{new!r}]) for r in {rv}.select({new!r}).collect()]))"]
            else:
                lines += [f"{prefix}_map = {{str(r[0]): str(r[1]) for r in {rv}.select({old!r}, {new!r}).collect() if r[0] is not None and r[1] is not None}}"]
        elif norm == "formula":
            expr = convert_expression(root.findtext("Expression", "[_CurrentField_]"), string_fields={"_CurrentField_"})
            lines += [f"if {prefix}_cols:",
                      f"    {prefix}_names = spark.createDataFrame([(c,) for c in {prefix}_cols], ['_CurrentField_'])",
                      f"    {prefix}_map = {{r[0]: str(r[1]) for r in {prefix}_names.select(F.col('_CurrentField_'), ({expr}).alias('_new')).collect() if r[1] is not None}}"]
        else:
            kind = 'RemovePrefixSuffix' if norm.startswith('remove') else 'AddPrefixSuffix'
            side = root.findtext(f"{kind}/Type", 'Suffix' if 'suffix' in norm else 'Prefix').lower()
            text = root.findtext(f"{kind}/Text", root.findtext('Text', ''))
            if norm.startswith('add'):
                expression = f"{text!r} + c" if side == 'prefix' else f"c + {text!r}"
            elif norm.startswith('remove'):
                expression = (f"c[len({text!r}):] if {text!r} and c.startswith({text!r}) else c" if side == 'prefix'
                              else f"c[:-len({text!r})] if {text!r} and c.endswith({text!r}) else c")
            else:
                case = root.findtext('ChangeCase/Type', root.findtext('Case', mode)).lower()
                operation = 'upper' if 'upper' in case else 'lower' if 'lower' in case else 'title'
                expression = f"c.{operation}()"
            lines += [f"{prefix}_map = {{c: ({expression}) for c in {prefix}_cols}}"]
        data_var = f"{prefix}_data" if norm in ("firstrow", "firstrowofdata") else input_var
        lines += [f"{var} = {data_var}.select(*[F.col('`' + c.replace('`', '``') + '`').alias({prefix}_map.get(c, c) if c in {prefix}_cols else c) for c in {input_var}.columns])"]
        return lines


@register("GenerateRows")
class GenerateRowsConverter(ToolConverter):
    """Convert GenerateRows tools."""

    def convert(self, tool: Tool, ctx: GeneratorContext) -> list:
        var = ctx.make_var_name(tool)
        ctx.set_output_var(tool.tool_id, var)
        return [
            f"# Tool {tool.tool_id}: GenerateRows",
            "# REVIEW: GenerateRows expressions are not translated; empty output until implemented.",
            f'{var} = spark.range(0).toDF("RowCount")',
        ]


@register("MultiFieldFormula")
class MultiFieldFormulaConverter(ToolConverter):
    """Convert MultiFieldFormula tools."""

    def convert(self, tool: Tool, ctx: GeneratorContext) -> list:
        input_var = ctx.get_input_var(tool.tool_id)
        var = ctx.make_var_name(tool)
        ctx.set_output_var(tool.tool_id, var)
        return [
            f"# Tool {tool.tool_id}: MultiFieldFormula",
            f"{var} = {input_var}  # REVIEW: formula across multiple fields requires implementation",
        ]


@register("FindReplace")
class FindReplaceConverter(ToolConverter):
    """Convert FindReplace tools."""

    def convert(self, tool: Tool, ctx: GeneratorContext) -> list:
        input_var = ctx.get_input_var(tool.tool_id)
        var = ctx.make_var_name(tool)
        ctx.set_output_var(tool.tool_id, var)
        return [
            f"# Tool {tool.tool_id}: FindReplace",
            f"{var} = {input_var}  # REVIEW: find/replace pairs require implementation",
        ]


@register("Comment", "BlockUntilDone", "RunCommand")
class PassthroughConverter(ToolConverter):
    """Fallback converter for unknown or no-op tools."""

    def convert(self, tool: Tool, ctx: GeneratorContext) -> list:
        input_var = ctx.get_input_var(tool.tool_id)
        var = f"df_{tool.tool_id}"
        ctx.set_output_var(tool.tool_id, var)
        return [f"{var} = {input_var}  # Tool {tool.tool_id}: {tool.tool_type} (passthrough)"]
