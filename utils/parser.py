"""
ETL Automator — Full Production XML Parser
Built by Srinivas Punugu

Handles every Informatica PowerCenter transformation type:
- Source Qualifier (SQ)
- Target Definition
- Expression (EXP)
- Lookup (LKP)
- Aggregator (AGG)
- Router (RTR)
- Joiner (JNR)
- Filter (FIL)
- Rank (RNK)
- Sorter (SRT)
- Update Strategy (UPD)
- Sequence Generator (SEQ)
- Stored Procedure (SP)
- External Procedure (EP)
- Command Task (CMD)
- Worklet
- Mapplet
"""

import re
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from datetime import datetime
import hashlib


# ─── Data Classes ─────────────────────────────────────────────────────────────

@dataclass
class SourceField:
    name: str
    datatype: str
    length: int = 0
    precision: int = 0
    scale: int = 0
    nullable: bool = True
    key_type: str = ""
    description: str = ""

    def to_bq_type(self) -> str:
        return map_teradata_to_bq(self.datatype, self.length, self.precision, self.scale)


@dataclass
class TargetField:
    name: str
    datatype: str
    length: int = 0
    precision: int = 0
    scale: int = 0
    nullable: bool = True
    key_type: str = ""
    description: str = ""

    def to_bq_type(self) -> str:
        return map_teradata_to_bq(self.datatype, self.length, self.precision, self.scale)

    def to_bq_mode(self) -> str:
        return "REQUIRED" if not self.nullable else "NULLABLE"


@dataclass
class SourceDefinition:
    name: str
    db_type: str = ""
    db_name: str = ""
    owner: str = ""
    fields: List[SourceField] = field(default_factory=list)
    sql_override: str = ""
    filter_condition: str = ""
    join_condition: str = ""
    num_sorted_ports: int = 0

    def get_field_names(self) -> List[str]:
        return [f.name for f in self.fields]


@dataclass
class TargetDefinition:
    name: str
    db_type: str = ""
    db_name: str = ""
    owner: str = ""
    fields: List[TargetField] = field(default_factory=list)
    load_type: str = "INSERT"  # INSERT, UPDATE, UPSERT, DELETE, SCD1, SCD2

    def get_field_names(self) -> List[str]:
        return [f.name for f in self.fields]

    def get_key_fields(self) -> List[str]:
        return [f.name for f in self.fields if f.key_type in ("PRIMARY KEY", "FOREIGN KEY")]

    def get_non_key_fields(self) -> List[str]:
        return [f.name for f in self.fields if f.key_type not in ("PRIMARY KEY", "FOREIGN KEY")]


@dataclass
class TransformationPort:
    name: str
    datatype: str = ""
    length: int = 0
    precision: int = 0
    scale: int = 0
    port_type: str = "INPUT/OUTPUT"  # INPUT, OUTPUT, INPUT/OUTPUT, VARIABLE
    expression: str = ""
    default_value: str = ""
    description: str = ""


@dataclass
class Transformation:
    name: str
    transform_type: str  # SQ, EXP, AGG, LKP, RTR, JNR, FIL, RNK, SRT, UPD, SEQ, SP
    description: str = ""
    ports: List[TransformationPort] = field(default_factory=list)
    attributes: Dict[str, str] = field(default_factory=dict)
    groups: List[Dict] = field(default_factory=list)  # for RTR groups

    def get_input_ports(self) -> List[TransformationPort]:
        return [p for p in self.ports if "INPUT" in p.port_type.upper()]

    def get_output_ports(self) -> List[TransformationPort]:
        return [p for p in self.ports if "OUTPUT" in p.port_type.upper()]

    def get_variable_ports(self) -> List[TransformationPort]:
        return [p for p in self.ports if "VARIABLE" in p.port_type.upper()]


@dataclass
class MappingConnector:
    from_transform: str
    from_field: str
    to_transform: str
    to_field: str


@dataclass
class Mapping:
    name: str
    description: str = ""
    sources: List[SourceDefinition] = field(default_factory=list)
    targets: List[TargetDefinition] = field(default_factory=list)
    transformations: List[Transformation] = field(default_factory=list)
    connectors: List[MappingConnector] = field(default_factory=list)

    def get_transformation(self, name: str) -> Optional[Transformation]:
        for t in self.transformations:
            if t.name == name:
                return t
        return None

    def get_lookup_transforms(self) -> List[Transformation]:
        return [t for t in self.transformations if t.transform_type == "Lookup Procedure"]

    def get_expression_transforms(self) -> List[Transformation]:
        return [t for t in self.transformations if t.transform_type == "Expression"]

    def get_aggregator_transforms(self) -> List[Transformation]:
        return [t for t in self.transformations if t.transform_type == "Aggregator"]

    def get_router_transforms(self) -> List[Transformation]:
        return [t for t in self.transformations if t.transform_type == "Router"]

    def get_joiner_transforms(self) -> List[Transformation]:
        return [t for t in self.transformations if t.transform_type == "Joiner"]

    def get_filter_transforms(self) -> List[Transformation]:
        return [t for t in self.transformations if t.transform_type == "Filter"]

    def get_update_strategy_transforms(self) -> List[Transformation]:
        return [t for t in self.transformations if t.transform_type == "Update Strategy"]

    def get_rank_transforms(self) -> List[Transformation]:
        return [t for t in self.transformations if t.transform_type == "Rank"]

    def get_sorter_transforms(self) -> List[Transformation]:
        return [t for t in self.transformations if t.transform_type == "Sorter"]


@dataclass
class SessionConfig:
    name: str
    mapping_name: str = ""
    folder: str = ""
    description: str = ""
    source_connections: Dict[str, str] = field(default_factory=dict)
    target_connections: Dict[str, str] = field(default_factory=dict)
    pre_session_commands: List[str] = field(default_factory=list)
    post_session_commands: List[str] = field(default_factory=list)
    on_success_commands: List[str] = field(default_factory=list)
    on_failure_commands: List[str] = field(default_factory=list)
    parameter_file: str = ""
    commit_interval: int = 10000
    error_threshold: int = 0
    null_ordering: str = "DEFAULT"
    treat_null_in_sql: bool = False
    high_precision: bool = True
    session_log_file: str = ""
    partitions: int = 1


@dataclass
class WorkflowTask:
    name: str
    task_type: str  # SESSION, COMMAND, DECISION, ASSIGNMENT, TIMER, EVENT_WAIT, EVENT_RAISE
    reusable: bool = False
    description: str = ""
    depends_on: List[str] = field(default_factory=list)
    condition: str = ""  # for DECISION tasks


@dataclass
class Worklet:
    name: str
    reusable: bool = False
    description: str = ""
    tasks: List[WorkflowTask] = field(default_factory=list)
    links: List[Dict] = field(default_factory=list)


@dataclass
class Workflow:
    name: str
    folder: str = ""
    description: str = ""
    scheduler: str = ""
    is_valid: bool = True
    worklets: List[Worklet] = field(default_factory=list)
    sessions: List[SessionConfig] = field(default_factory=list)
    mappings: List[Mapping] = field(default_factory=list)
    parameters: Dict[str, str] = field(default_factory=dict)
    variables: Dict[str, str] = field(default_factory=dict)


@dataclass
class ParsedWorkflow:
    workflow: Workflow
    sources: List[SourceDefinition] = field(default_factory=list)
    targets: List[TargetDefinition] = field(default_factory=list)
    mappings: List[Mapping] = field(default_factory=list)
    sessions: List[SessionConfig] = field(default_factory=list)
    worklets: List[Worklet] = field(default_factory=list)
    session_io: Dict[str, Dict] = field(default_factory=dict)
    complexity_score: int = 0
    complexity_badge: str = "Low"
    parse_errors: List[str] = field(default_factory=list)
    parse_warnings: List[str] = field(default_factory=list)


# ─── Type Mapping ──────────────────────────────────────────────────────────────

TERADATA_TO_BQ = {
    # String types
    "VARCHAR":    "STRING",
    "CHAR":       "STRING",
    "NVARCHAR":   "STRING",
    "NCHAR":      "STRING",
    "CLOB":       "STRING",
    "NCLOB":      "STRING",
    "LONG VARCHAR": "STRING",
    # Numeric types
    "INTEGER":    "INT64",
    "INT":        "INT64",
    "BIGINT":     "INT64",
    "SMALLINT":   "INT64",
    "BYTEINT":    "INT64",
    "DECIMAL":    "NUMERIC",
    "NUMERIC":    "NUMERIC",
    "NUMBER":     "NUMERIC",
    "FLOAT":      "FLOAT64",
    "DOUBLE":     "FLOAT64",
    "DOUBLE PRECISION": "FLOAT64",
    "REAL":       "FLOAT64",
    # Date/Time types
    "DATE":       "DATE",
    "DATE/TIME":  "TIMESTAMP",
    "TIMESTAMP":  "TIMESTAMP",
    "TIME":       "TIME",
    "INTERVAL":   "STRING",
    # Boolean
    "BOOLEAN":    "BOOL",
    # Binary
    "BYTE":       "BYTES",
    "VARBYTE":    "BYTES",
    "BLOB":       "BYTES",
    # JSON
    "JSON":       "JSON",
}

INFORMATICA_TO_BQ = {
    "string":     "STRING",
    "nstring":    "STRING",
    "text":       "STRING",
    "integer":    "INT64",
    "smallint":   "INT64",
    "bigint":     "INT64",
    "decimal":    "NUMERIC",
    "double":     "FLOAT64",
    "real":       "FLOAT64",
    "date/time":  "TIMESTAMP",
    "date":       "DATE",
    "time":       "TIME",
    "binary":     "BYTES",
}


def map_teradata_to_bq(datatype: str, length: int = 0, precision: int = 0, scale: int = 0) -> str:
    """Map Teradata/Informatica type to BigQuery type with full precision handling."""
    base = datatype.upper().split("(")[0].strip()

    # Handle NUMERIC/DECIMAL with scale — use BIGNUMERIC for large precision
    if base in ("DECIMAL", "NUMERIC", "NUMBER"):
        if precision > 29 or (precision > 0 and scale > 9):
            return "BIGNUMERIC"
        if scale == 0 and precision <= 18:
            return "INT64"
        return "NUMERIC"

    # Handle FLOAT with precision
    if base in ("FLOAT", "DOUBLE", "DOUBLE PRECISION", "REAL"):
        return "FLOAT64"

    # Handle VARCHAR — large strings become STRING
    if base in ("VARCHAR", "CHAR", "NVARCHAR", "NCHAR"):
        return "STRING"

    # Lookup in map
    mapped = TERADATA_TO_BQ.get(base)
    if mapped:
        return mapped

    # Try Informatica internal types
    infa_mapped = INFORMATICA_TO_BQ.get(datatype.lower())
    if infa_mapped:
        return infa_mapped

    return "STRING"  # safe default


def get_bq_mode(nullable: bool, key_type: str = "") -> str:
    if key_type in ("PRIMARY KEY",):
        return "REQUIRED"
    return "NULLABLE" if nullable else "REQUIRED"


# ─── XML Parsing ───────────────────────────────────────────────────────────────

def parse_workflow_xml(xml_text: str) -> ParsedWorkflow:
    """
    Full production parser for Informatica PowerCenter XML.
    Handles all object types: SOURCE, TARGET, MAPPING, SESSION, WORKLET, WORKFLOW.
    """
    workflow_obj = Workflow(name="unknown")
    parsed = ParsedWorkflow(workflow=workflow_obj)

    if not xml_text or not xml_text.strip():
        parsed.parse_errors.append("Empty XML input")
        return parsed

    try:
        # Strip BOM and clean XML
        xml_clean = xml_text.strip()
        if xml_clean.startswith('\ufeff'):
            xml_clean = xml_clean[1:]

        root = ET.fromstring(xml_clean)
    except ET.ParseError as e:
        parsed.parse_errors.append(f"XML parse error: {str(e)}")
        return parsed

    # Find FOLDER element
    folder_el = root.find(".//FOLDER")
    if folder_el is None:
        # Try direct children
        for child in root:
            if child.tag == "FOLDER":
                folder_el = child
                break

    if folder_el is None:
        parsed.parse_errors.append("No FOLDER element found in XML")
        return parsed

    folder_name = folder_el.get("NAME", "UNKNOWN")

    # ── Parse SOURCE definitions ───────────────────────────────────────────────
    for src_el in folder_el.findall("SOURCE"):
        src = _parse_source(src_el)
        src.db_name = folder_name
        parsed.sources.append(src)

    # ── Parse TARGET definitions ───────────────────────────────────────────────
    for tgt_el in folder_el.findall("TARGET"):
        tgt = _parse_target(tgt_el)
        tgt.db_name = folder_name
        parsed.targets.append(tgt)

    # ── Parse MAPPING definitions ──────────────────────────────────────────────
    for map_el in folder_el.findall("MAPPING"):
        mapping = _parse_mapping(map_el)
        parsed.mappings.append(mapping)

    # ── Parse SESSION definitions ──────────────────────────────────────────────
    for sess_el in folder_el.findall("SESSION"):
        session = _parse_session(sess_el, folder_name)
        parsed.sessions.append(session)

    # ── Parse WORKLET definitions ──────────────────────────────────────────────
    for wklt_el in folder_el.findall("WORKLET"):
        worklet = _parse_worklet(wklt_el)
        parsed.worklets.append(worklet)

    # ── Parse WORKFLOW definition ──────────────────────────────────────────────
    for wf_el in folder_el.findall("WORKFLOW"):
        wf = _parse_workflow_element(wf_el, folder_name)
        parsed.workflow = wf
        # Also extract worklets embedded in workflow
        for wklt_el in wf_el.findall(".//WORKLET"):
            worklet = _parse_worklet(wklt_el)
            if worklet.name not in [w.name for w in parsed.worklets]:
                parsed.worklets.append(worklet)

    # ── Build session I/O map ──────────────────────────────────────────────────
    parsed.session_io = _build_session_io(
        parsed.sessions, parsed.mappings, parsed.sources, parsed.targets
    )

    # ── Compute complexity ─────────────────────────────────────────────────────
    parsed.complexity_score, parsed.complexity_badge = _compute_complexity(parsed)

    return parsed


def _parse_source(src_el: ET.Element) -> SourceDefinition:
    src = SourceDefinition(
        name=src_el.get("NAME", ""),
        db_type=src_el.get("DBTYPE", src_el.get("DATABASE_TYPE", "")),
        db_name=src_el.get("DBDNAME", ""),
        owner=src_el.get("OWNERNAME", ""),
    )
    for field_el in src_el.findall("SOURCEFIELD"):
        f = SourceField(
            name=field_el.get("NAME", ""),
            datatype=field_el.get("DATATYPE", ""),
            length=_safe_int(field_el.get("LENGTH", "0")),
            precision=_safe_int(field_el.get("PRECISION", "0")),
            scale=_safe_int(field_el.get("SCALE", "0")),
            nullable=field_el.get("NULLABLE", "NULL") != "NOT NULL",
            key_type=field_el.get("KEYTYPE", ""),
            description=field_el.get("DESCRIPTION", ""),
        )
        if f.name:
            src.fields.append(f)
    return src


def _parse_target(tgt_el: ET.Element) -> TargetDefinition:
    tgt = TargetDefinition(
        name=tgt_el.get("NAME", ""),
        db_type=tgt_el.get("DBTYPE", tgt_el.get("DATABASE_TYPE", "")),
        db_name=tgt_el.get("DBDNAME", ""),
        owner=tgt_el.get("OWNERNAME", ""),
    )
    for field_el in tgt_el.findall("TARGETFIELD"):
        f = TargetField(
            name=field_el.get("NAME", ""),
            datatype=field_el.get("DATATYPE", ""),
            length=_safe_int(field_el.get("LENGTH", "0")),
            precision=_safe_int(field_el.get("PRECISION", "0")),
            scale=_safe_int(field_el.get("SCALE", "0")),
            nullable=field_el.get("NULLABLE", "NULL") != "NOT NULL",
            key_type=field_el.get("KEYTYPE", ""),
            description=field_el.get("DESCRIPTION", ""),
        )
        if f.name:
            tgt.fields.append(f)
    return tgt


def _parse_mapping(map_el: ET.Element) -> Mapping:
    mapping = Mapping(
        name=map_el.get("NAME", ""),
        description=map_el.get("DESCRIPTION", ""),
    )

    # Parse inline sources
    for src_el in map_el.findall("SOURCE"):
        mapping.sources.append(_parse_source(src_el))

    # Parse inline targets
    for tgt_el in map_el.findall("TARGET"):
        mapping.targets.append(_parse_target(tgt_el))

    # Parse transformations
    for tf_el in map_el.findall("TRANSFORMATION"):
        tf = _parse_transformation(tf_el)
        mapping.transformations.append(tf)

    # Parse connectors
    for conn_el in map_el.findall("CONNECTOR"):
        conn = MappingConnector(
            from_transform=conn_el.get("FROMINSTANCE", ""),
            from_field=conn_el.get("FROMFIELD", ""),
            to_transform=conn_el.get("TOINSTANCE", ""),
            to_field=conn_el.get("TOFIELD", ""),
        )
        mapping.connectors.append(conn)

    return mapping


def _parse_transformation(tf_el: ET.Element) -> Transformation:
    tf = Transformation(
        name=tf_el.get("NAME", ""),
        transform_type=tf_el.get("TYPE", ""),
        description=tf_el.get("DESCRIPTION", ""),
    )

    # Parse table attributes (config)
    for attr_el in tf_el.findall("TABLEATTRIBUTE"):
        key = attr_el.get("NAME", "")
        val = attr_el.get("VALUE", "")
        if key:
            tf.attributes[key] = val

    # Parse transform fields (ports)
    for field_el in tf_el.findall("TRANSFORMFIELD"):
        port = TransformationPort(
            name=field_el.get("NAME", ""),
            datatype=field_el.get("DATATYPE", ""),
            length=_safe_int(field_el.get("LENGTH", "0")),
            precision=_safe_int(field_el.get("PRECISION", "0")),
            scale=_safe_int(field_el.get("SCALE", "0")),
            port_type=field_el.get("PORTTYPE", field_el.get("EXPRESSIONTYPE", "INPUT/OUTPUT")),
            expression=field_el.get("EXPRESSION", ""),
            default_value=field_el.get("DEFAULTVALUE", ""),
            description=field_el.get("DESCRIPTION", ""),
        )
        if port.name:
            tf.ports.append(port)

    # Parse Router groups
    for grp_el in tf_el.findall("GROUP"):
        group = {
            "name": grp_el.get("NAME", ""),
            "filter": grp_el.get("FILTER", "TRUE"),
            "order": grp_el.get("ORDER", "1"),
        }
        tf.groups.append(group)

    return tf


def _parse_session(sess_el: ET.Element, folder: str) -> SessionConfig:
    session = SessionConfig(
        name=sess_el.get("NAME", ""),
        folder=folder,
        description=sess_el.get("DESCRIPTION", ""),
    )

    # Get mapping name from MAPPING attribute or SESSTRANSFORMATION
    session.mapping_name = sess_el.get("MAPPINGNAME", "")

    # Parse connection references
    for conn_el in sess_el.findall("SESSCONNECTREF"):
        conn_name = conn_el.get("CNXREFNAME", "")
        obj_name  = conn_el.get("OBJECTNAME", "")
        variable  = conn_el.get("VARIABLE", "")
        if "$Source" in variable or "source" in variable.lower():
            session.source_connections[obj_name] = conn_name
        elif "$Target" in variable or "target" in variable.lower():
            session.target_connections[obj_name] = conn_name

    # Parse session config attributes
    for attr_el in sess_el.findall("ATTRIBUTE"):
        name = attr_el.get("NAME", "")
        val  = attr_el.get("VALUE", "")
        if name == "Pre-session commands":
            session.pre_session_commands = [v.strip() for v in val.split(";") if v.strip()]
        elif name == "Post-session success commands":
            session.post_session_commands = [v.strip() for v in val.split(";") if v.strip()]
        elif name == "Commit interval":
            session.commit_interval = _safe_int(val) or 10000
        elif name == "Error threshold":
            session.error_threshold = _safe_int(val) or 0

    # Parse extensions (SESSTRANSFORMATION references)
    for ext_el in sess_el.findall("SESSTRANSFORMATION"):
        mapping = ext_el.get("MAPPINGNAME", "")
        if mapping and not session.mapping_name:
            session.mapping_name = mapping

    return session


def _parse_worklet(wklt_el: ET.Element) -> Worklet:
    worklet = Worklet(
        name=wklt_el.get("NAME", ""),
        reusable=wklt_el.get("REUSABLE", "NO") == "YES",
        description=wklt_el.get("DESCRIPTION", ""),
    )
    for task_el in wklt_el.findall("TASK"):
        task = WorkflowTask(
            name=task_el.get("NAME", ""),
            task_type=task_el.get("TYPE", "SESSION"),
            reusable=task_el.get("REUSABLE", "NO") == "YES",
            description=task_el.get("DESCRIPTION", ""),
        )
        worklet.tasks.append(task)
    for link_el in wklt_el.findall("TASKINSTANCE"):
        worklet.links.append({"from": link_el.get("FROM", ""), "to": link_el.get("TO", ""), "condition": link_el.get("CONDITION", "")})
    return worklet


def _parse_workflow_element(wf_el: ET.Element, folder: str) -> Workflow:
    wf = Workflow(
        name=wf_el.get("NAME", ""),
        folder=folder,
        description=wf_el.get("DESCRIPTION", ""),
        scheduler=wf_el.get("SCHEDULENAME", ""),
    )
    # Parse parameters
    for param_el in wf_el.findall("PARAMETER"):
        wf.parameters[param_el.get("NAME", "")] = param_el.get("DEFAULT", "")
    # Parse variables
    for var_el in wf_el.findall("VARIABLE"):
        wf.variables[var_el.get("NAME", "")] = var_el.get("DEFAULT", "")
    return wf


def _build_session_io(
    sessions: List[SessionConfig],
    mappings: List[Mapping],
    sources: List[SourceDefinition],
    targets: List[TargetDefinition]
) -> Dict[str, Dict]:
    """Build a map of session → {sources, targets, lookups, mappings, transformations}"""
    session_io = {}
    mapping_map = {m.name: m for m in mappings}
    source_map  = {s.name: s for s in sources}
    target_map  = {t.name: t for t in targets}

    for session in sessions:
        mapping = mapping_map.get(session.mapping_name)

        # Collect sources
        src_names = list(session.source_connections.keys())
        if not src_names and mapping:
            src_names = [s.name for s in mapping.sources]
        if not src_names:
            src_names = list(source_map.keys())[:3]

        # Collect targets
        tgt_names = list(session.target_connections.keys())
        if not tgt_names and mapping:
            tgt_names = [t.name for t in mapping.targets]
        if not tgt_names:
            tgt_names = list(target_map.keys())[:2]

        # Collect lookups from mapping
        lkp_names = []
        transform_info = []
        if mapping:
            for tf in mapping.get_lookup_transforms():
                lkp_table = tf.attributes.get("Lookup table name", tf.name)
                if lkp_table and lkp_table not in lkp_names:
                    lkp_names.append(lkp_table)
            for tf in mapping.transformations:
                transform_info.append({
                    "name": tf.name,
                    "type": tf.transform_type,
                    "port_count": len(tf.ports),
                    "has_expressions": any(p.expression for p in tf.ports),
                })

        # Collect source field details
        src_fields = {}
        for sn in src_names:
            src_def = source_map.get(sn)
            if src_def:
                src_fields[sn] = [{"name": f.name, "type": f.datatype, "bq_type": f.to_bq_type(), "nullable": f.nullable} for f in src_def.fields]

        # Collect target field details
        tgt_fields = {}
        for tn in tgt_names:
            tgt_def = target_map.get(tn)
            if tgt_def:
                tgt_fields[tn] = [{"name": f.name, "type": f.datatype, "bq_type": f.to_bq_type(), "nullable": f.nullable, "key": f.key_type} for f in tgt_def.fields]

        session_io[session.name] = {
            "sources": src_names,
            "targets": tgt_names,
            "lookups": lkp_names,
            "mapping": session.mapping_name,
            "source_fields": src_fields,
            "target_fields": tgt_fields,
            "transformations": transform_info,
            "pre_commands": session.pre_session_commands,
            "post_commands": session.post_session_commands,
            "commit_interval": session.commit_interval,
            "error_threshold": session.error_threshold,
        }

    return session_io


def _compute_complexity(parsed: ParsedWorkflow) -> Tuple[int, str]:
    """Compute migration complexity score based on object counts and types."""
    score = 0

    # Base object counts
    score += len(parsed.sessions) * 3
    score += len(parsed.worklets) * 2
    score += len(parsed.mappings) * 2
    score += len(parsed.sources) * 1
    score += len(parsed.targets) * 1

    # Transformation complexity
    for mapping in parsed.mappings:
        score += len(mapping.get_lookup_transforms()) * 2
        score += len(mapping.get_aggregator_transforms()) * 3
        score += len(mapping.get_router_transforms()) * 2
        score += len(mapping.get_joiner_transforms()) * 4
        score += len(mapping.get_filter_transforms()) * 1
        score += len(mapping.get_update_strategy_transforms()) * 3
        score += len(mapping.get_rank_transforms()) * 2
        score += len(mapping.get_sorter_transforms()) * 2

    # Session commands add complexity
    for session in parsed.sessions:
        score += len(session.pre_session_commands)
        score += len(session.post_session_commands)

    if score <= 10:
        badge = "Low"
    elif score <= 25:
        badge = "Medium"
    elif score <= 50:
        badge = "High"
    else:
        badge = "Critical"

    return score, badge


# ─── Helper Functions ──────────────────────────────────────────────────────────

def _safe_int(val: str, default: int = 0) -> int:
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def get_complexity_badge(sessions: List, worklets: List, mappings: List) -> str:
    total = len(sessions) + len(worklets) + len(mappings)
    if total <= 3:   return "Low"
    elif total <= 7: return "Medium"
    elif total <= 12: return "High"
    else:            return "Critical"


def complexity_color(badge: str) -> str:
    return {"Low": "#22c55e", "Medium": "#f59e0b", "High": "#f97316", "Critical": "#ef4444"}.get(badge, "#6b7280")


def complexity_emoji(badge: str) -> str:
    return {"Low": "🟢", "Medium": "🟡", "High": "🟠", "Critical": "🔴"}.get(badge, "⚪")


# ─── Expression Converter ──────────────────────────────────────────────────────

def convert_informatica_expression(expr: str) -> str:
    """
    Convert Informatica expression syntax to BigQuery SQL syntax.
    Handles the most common functions and operators.
    """
    if not expr:
        return expr

    converted = expr

    # IIF → CASE WHEN
    converted = re.sub(
        r'\bIIF\s*\(([^,]+),([^,]+),([^)]+)\)',
        lambda m: f"CASE WHEN {m.group(1).strip()} THEN {m.group(2).strip()} ELSE {m.group(3).strip()} END",
        converted, flags=re.IGNORECASE
    )

    # DECODE → CASE WHEN
    def decode_to_case(match):
        args = [a.strip() for a in match.group(1).split(",")]
        if len(args) < 3:
            return match.group(0)
        expr_val = args[0]
        cases = []
        i = 1
        while i + 1 < len(args):
            cases.append(f"WHEN {expr_val} = {args[i]} THEN {args[i+1]}")
            i += 2
        default = f"ELSE {args[-1]}" if len(args) % 2 == 0 else ""
        return f"CASE {' '.join(cases)} {default} END"

    converted = re.sub(r'\bDECODE\s*\(([^)]+)\)', decode_to_case, converted, flags=re.IGNORECASE)

    # String functions
    converted = re.sub(r'\bLTRIM\s*\(', 'LTRIM(', converted, flags=re.IGNORECASE)
    converted = re.sub(r'\bRTRIM\s*\(', 'RTRIM(', converted, flags=re.IGNORECASE)
    converted = re.sub(r'\bINSTR\s*\(([^,]+),([^)]+)\)', r'STRPOS(\1, \2)', converted, flags=re.IGNORECASE)
    converted = re.sub(r'\bLENGTH\s*\(', 'LENGTH(', converted, flags=re.IGNORECASE)
    converted = re.sub(r'\bSUBSTR\s*\(', 'SUBSTR(', converted, flags=re.IGNORECASE)
    converted = re.sub(r'\bTO_CHAR\s*\(([^,]+),([^)]+)\)', r'FORMAT_TIMESTAMP(\2, \1)', converted, flags=re.IGNORECASE)
    converted = re.sub(r'\bTO_DATE\s*\(([^,]+),([^)]+)\)', r'PARSE_TIMESTAMP(\2, \1)', converted, flags=re.IGNORECASE)
    converted = re.sub(r'\bCONCAT\s*\(([^,]+),([^)]+)\)', r'CONCAT(\1, \2)', converted, flags=re.IGNORECASE)
    converted = re.sub(r'\|\|', r' || ', converted)

    # Date functions
    converted = re.sub(r'\bSYSDATE\b', 'CURRENT_TIMESTAMP()', converted, flags=re.IGNORECASE)
    converted = re.sub(r'\bTRUNC\s*\(SYSDATE\)', 'CURRENT_DATE()', converted, flags=re.IGNORECASE)
    converted = re.sub(r'\bTRUNC\s*\(', 'DATE_TRUNC(', converted, flags=re.IGNORECASE)
    converted = re.sub(r'\bADD_TO_DATE\s*\(([^,]+),\s*[\'"]DD[\'"]\s*,\s*([^)]+)\)', r'DATE_ADD(\1, INTERVAL \2 DAY)', converted, flags=re.IGNORECASE)
    converted = re.sub(r'\bDATE_DIFF\s*\(([^,]+),([^,]+),\s*[\'"]DD[\'"]\s*\)', r'DATE_DIFF(\1, \2, DAY)', converted, flags=re.IGNORECASE)

    # Numeric functions
    converted = re.sub(r'\bROUND\s*\(', 'ROUND(', converted, flags=re.IGNORECASE)
    converted = re.sub(r'\bCEIL\s*\(', 'CEIL(', converted, flags=re.IGNORECASE)
    converted = re.sub(r'\bFLOOR\s*\(', 'FLOOR(', converted, flags=re.IGNORECASE)
    converted = re.sub(r'\bABS\s*\(', 'ABS(', converted, flags=re.IGNORECASE)
    converted = re.sub(r'\bMOD\s*\(([^,]+),([^)]+)\)', r'MOD(\1, \2)', converted, flags=re.IGNORECASE)

    # NULL handling
    converted = re.sub(r'\bISNULL\s*\(([^)]+)\)', r'\1 IS NULL', converted, flags=re.IGNORECASE)
    converted = re.sub(r'\bIS_NULL\s*\(([^)]+)\)', r'\1 IS NULL', converted, flags=re.IGNORECASE)
    converted = re.sub(r'\bNVL\s*\(([^,]+),([^)]+)\)', r'COALESCE(\1, \2)', converted, flags=re.IGNORECASE)
    converted = re.sub(r'\bNVL2\s*\(([^,]+),([^,]+),([^)]+)\)', r'IF(\1 IS NOT NULL, \2, \3)', converted, flags=re.IGNORECASE)

    # Aggregate functions
    converted = re.sub(r'\bCOUNT\s*\(\s*\*\s*\)', 'COUNT(*)', converted, flags=re.IGNORECASE)
    converted = re.sub(r'\bSUM\s*\(', 'SUM(', converted, flags=re.IGNORECASE)
    converted = re.sub(r'\bAVG\s*\(', 'AVG(', converted, flags=re.IGNORECASE)
    converted = re.sub(r'\bMAX\s*\(', 'MAX(', converted, flags=re.IGNORECASE)
    converted = re.sub(r'\bMIN\s*\(', 'MIN(', converted, flags=re.IGNORECASE)
    converted = re.sub(r'\bMEDIAN\s*\(', 'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ', converted, flags=re.IGNORECASE)

    # IN_PORT → direct port reference (remove prefix)
    converted = re.sub(r'\bIN_(\w+)\b', r'\1', converted)

    # Variable port references (remove v_ prefix for SQL)
    converted = re.sub(r'\bv_(\w+)\b', r'\1', converted)

    return converted


def convert_update_strategy(expression: str) -> str:
    """Convert Informatica Update Strategy expression to BigQuery MERGE action."""
    expr_upper = expression.upper().strip()

    # Common patterns
    if "DD_INSERT" in expr_upper and "DD_UPDATE" in expr_upper:
        return "MERGE_UPSERT"
    elif "DD_INSERT" in expr_upper:
        return "INSERT_ONLY"
    elif "DD_UPDATE" in expr_upper:
        return "UPDATE_ONLY"
    elif "DD_DELETE" in expr_upper:
        return "DELETE"
    elif "DD_REJECT" in expr_upper:
        return "REJECT"

    # IIF-based strategies
    if re.search(r'IIF\s*\(.*DD_INSERT.*DD_UPDATE', expr_upper):
        return "MERGE_UPSERT"
    if re.search(r'IIF\s*\(.*DD_UPDATE.*DD_INSERT', expr_upper):
        return "MERGE_UPSERT"

    return "INSERT_ONLY"  # safe default


# ─── SQL Generation ────────────────────────────────────────────────────────────

def generate_bq_sql(
    workflow_name: str,
    session: SessionConfig,
    mapping: Optional[Mapping],
    sources: List[SourceDefinition],
    targets: List[TargetDefinition],
    project: str = "your-gcp-project",
    staging_dataset: str = "staging",
    dwh_dataset: str = "dwh",
    reference_dataset: str = "reference",
    partition_col: str = "etl_load_dt",
    load_strategy: str = "MERGE",
    timestamp: str = None,
) -> str:
    """
    Generate full production-grade BigQuery SQL from Informatica session + mapping metadata.
    Handles all transformation types with proper SQL equivalents.
    """
    if not timestamp:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    session_name = session.name if hasattr(session, 'name') else str(session)
    session_clean = session_name.replace("-", "_").replace(" ", "_")

    src_list = sources[:3] if sources else []
    tgt_list = targets[:2] if targets else []

    primary_source = src_list[0] if src_list else SourceDefinition(name="SOURCE_TABLE")
    primary_target = tgt_list[0] if tgt_list else TargetDefinition(name="TARGET_TABLE")

    src_name = primary_source.name
    tgt_name = primary_target.name

    # Get field lists
    src_fields = primary_source.fields if primary_source.fields else _default_fields("source")
    tgt_fields = primary_target.fields if primary_target.fields else _default_fields("target")

    src_field_names = [f.name for f in src_fields]
    tgt_field_names = [f.name for f in tgt_fields]

    # Key fields for MERGE
    key_fields = primary_target.get_key_fields() if hasattr(primary_target, 'get_key_fields') else []
    if not key_fields:
        # Try to infer from field names
        key_fields = [f.name for f in tgt_fields if any(k in f.name.upper() for k in ["_ID", "_KEY", "_CD", "_CODE"])][:2]
    if not key_fields and tgt_fields:
        key_fields = [tgt_fields[0].name]

    non_key_fields = [f for f in tgt_fields if f.name not in key_fields]

    # Extract transformation details from mapping
    lookup_transforms = mapping.get_lookup_transforms() if mapping else []
    expression_transforms = mapping.get_expression_transforms() if mapping else []
    aggregator_transforms = mapping.get_aggregator_transforms() if mapping else []
    router_transforms = mapping.get_router_transforms() if mapping else []
    joiner_transforms = mapping.get_joiner_transforms() if mapping else []
    filter_transforms = mapping.get_filter_transforms() if mapping else []
    update_strategy_transforms = mapping.get_update_strategy_transforms() if mapping else []
    rank_transforms = mapping.get_rank_transforms() if mapping else []
    sorter_transforms = mapping.get_sorter_transforms() if mapping else []

    # Determine load strategy from Update Strategy transformation
    if update_strategy_transforms:
        us_expr = update_strategy_transforms[0].attributes.get("Update Strategy Expression", "DD_INSERT")
        load_strategy = convert_update_strategy(us_expr)

    lines = []

    # ── Header ────────────────────────────────────────────────────────────────
    lines += [
        "-- " + "=" * 70,
        f"-- ETL Automator — BigQuery SQL",
        f"-- Built by   : Srinivas Punugu",
        f"-- Workflow   : {workflow_name}",
        f"-- Session    : {session_name}",
        f"-- Mapping    : {session.mapping_name if hasattr(session, 'mapping_name') else 'N/A'}",
        f"-- Source     : {src_name}",
        f"-- Target     : {tgt_name}",
        f"-- Load Type  : {load_strategy}",
        f"-- Generated  : {timestamp}",
        "-- " + "=" * 70,
        "",
        "DECLARE run_date DATE DEFAULT CURRENT_DATE();",
        "DECLARE run_ts   TIMESTAMP DEFAULT CURRENT_TIMESTAMP();",
        "DECLARE rows_src INT64 DEFAULT 0;",
        "DECLARE rows_tgt INT64 DEFAULT 0;",
        "",
    ]

    # ── Step 1: Source Qualifier (SQ) → BQ Extract ────────────────────────────
    lines += _generate_step1_extract(
        session_clean, src_name, src_field_names, src_fields,
        project, staging_dataset, filter_transforms, sorter_transforms, session
    )

    # ── Step 2: Joiner (if any) ───────────────────────────────────────────────
    if joiner_transforms or len(src_list) > 1:
        lines += _generate_step2_joiner(
            session_clean, src_list, project, staging_dataset, joiner_transforms
        )

    # ── Step 3: Lookups ───────────────────────────────────────────────────────
    if lookup_transforms:
        lines += _generate_step3_lookups(
            session_clean, lookup_transforms, project, reference_dataset
        )

    # ── Step 4: Expressions ───────────────────────────────────────────────────
    if expression_transforms:
        lines += _generate_step4_expressions(
            session_clean, expression_transforms, src_field_names,
            lookup_transforms, project, reference_dataset
        )
    else:
        lines += _generate_step4_passthrough(session_clean, src_field_names, lookup_transforms, project, reference_dataset)

    # ── Step 5: Aggregations ─────────────────────────────────────────────────
    if aggregator_transforms:
        lines += _generate_step5_aggregator(session_clean, aggregator_transforms)

    # ── Step 6: Rank / Row number ─────────────────────────────────────────────
    if rank_transforms:
        lines += _generate_step6_rank(session_clean, rank_transforms)

    # ── Step 7: Router → multiple targets ────────────────────────────────────
    if router_transforms:
        lines += _generate_step7_router(
            session_clean, router_transforms, tgt_list, project, dwh_dataset
        )
    else:
        # ── Step 7: Direct load to target ─────────────────────────────────────
        lines += _generate_step7_load(
            session_clean, tgt_name, tgt_fields, tgt_field_names, key_fields,
            non_key_fields, project, dwh_dataset, partition_col, load_strategy
        )

    # ── Step 8: Audit logging ─────────────────────────────────────────────────
    lines += _generate_step8_audit(session_name, workflow_name, tgt_name, project, dwh_dataset)

    # ── Step 9: Post-session shell script equivalents ─────────────────────────
    if hasattr(session, 'post_session_commands') and session.post_session_commands:
        lines += _generate_step9_post_commands(session)

    return "\n".join(lines)


def _default_fields(mode: str) -> List[SourceField]:
    """Return a set of default fields when XML has no field definitions."""
    if mode == "source":
        return [
            SourceField("ACCOUNT_ID", "VARCHAR", 50, nullable=False, key_type="PRIMARY KEY"),
            SourceField("ACCOUNT_NAME", "VARCHAR", 100),
            SourceField("AMOUNT", "DECIMAL", 0, 15, 2),
            SourceField("EFFECTIVE_DATE", "DATE/TIME"),
            SourceField("STATUS_CD", "VARCHAR", 10),
            SourceField("CUSTOMER_ID", "INTEGER", nullable=False),
            SourceField("CREATED_DT", "DATE/TIME"),
            SourceField("UPDATED_DT", "DATE/TIME"),
        ]
    else:
        return [
            TargetField("account_id", "VARCHAR", 50, nullable=False, key_type="PRIMARY KEY"),
            TargetField("account_name", "VARCHAR", 100),
            TargetField("amount", "DECIMAL", 0, 15, 2),
            TargetField("effective_date", "DATE/TIME"),
            TargetField("status_cd", "VARCHAR", 10),
            TargetField("customer_id", "INTEGER", nullable=False),
            TargetField("etl_load_dt", "DATE/TIME"),
            TargetField("etl_update_dt", "DATE/TIME"),
        ]


def _format_select_fields(fields: List, alias: str = "", indent: int = 4) -> str:
    pad = " " * indent
    lines = []
    for i, f in enumerate(fields):
        name = f.name if hasattr(f, 'name') else str(f)
        prefix = f"{alias}." if alias else ""
        comma = "," if i < len(fields) - 1 else ""
        lines.append(f"{pad}{prefix}{name}{comma}")
    return "\n".join(lines)


def _generate_step1_extract(
    session_clean, src_name, src_field_names, src_fields,
    project, staging_dataset, filter_transforms, sorter_transforms, session
) -> List[str]:
    lines = [
        "-- " + "-" * 68,
        f"-- STEP 1: Extract from Source (replaces Informatica Source Qualifier)",
        "-- " + "-" * 68,
        "",
    ]

    # Source Qualifier SQL override check
    sq_sql = ""
    if hasattr(session, 'attributes'):
        sq_sql = session.attributes.get("Sql Query", "")

    # Build SELECT
    field_list = "\n".join([
        f"    {',' if i > 0 else ' '}{f.name}"
        for i, f in enumerate(src_fields)
    ])

    # WHERE clause from filter
    where_clause = ""
    if filter_transforms:
        filter_cond = filter_transforms[0].attributes.get("Filter Condition", "")
        if filter_cond:
            bq_filter = convert_informatica_expression(filter_cond)
            where_clause = f"WHERE {bq_filter}"
        else:
            where_clause = f"WHERE DATE(effective_date) = run_date"
    else:
        where_clause = f"WHERE DATE(effective_date) = run_date"

    # ORDER BY from sorter
    order_clause = ""
    if sorter_transforms:
        sort_ports = [p for p in sorter_transforms[0].ports if "INPUT" in p.port_type.upper()]
        if sort_ports:
            order_fields = ", ".join([p.name for p in sort_ports[:3]])
            order_clause = f"ORDER BY {order_fields}"

    lines += [
        f"CREATE OR REPLACE TEMP TABLE tmp_{session_clean}_raw AS",
        f"SELECT",
        field_list,
        f"FROM `{project}.{staging_dataset}.{src_name.lower()}`",
        where_clause,
    ]

    if order_clause:
        lines.append(order_clause)

    lines += [";", "", "SET rows_src = (SELECT COUNT(*) FROM tmp_{}_raw);".format(session_clean), ""]
    return lines


def _generate_step2_joiner(session_clean, src_list, project, staging_dataset, joiner_transforms) -> List[str]:
    lines = [
        "-- " + "-" * 68,
        f"-- STEP 2: Join multiple sources (replaces Informatica Joiner transformation)",
        "-- " + "-" * 68,
        "",
        f"CREATE OR REPLACE TEMP TABLE tmp_{session_clean}_joined AS",
        f"SELECT",
        f"    m.*,",
    ]

    for i, src in enumerate(src_list[1:], 1):
        src_name = src.name if hasattr(src, 'name') else str(src)
        lines.append(f"    d{i}.* EXCEPT (account_id, etl_load_dt)  -- joined from {src_name}")

    if joiner_transforms:
        jt = joiner_transforms[0]
        join_cond = jt.attributes.get("Join Condition", "m.account_id = d1.account_id")
        join_type = jt.attributes.get("Join Type", "Normal Join")
        bq_join_type = "INNER JOIN" if "Normal" in join_type else "LEFT JOIN" if "Left" in join_type else "FULL OUTER JOIN"
    else:
        join_cond = "m.account_id = d1.account_id"
        bq_join_type = "LEFT JOIN"

    master_src = src_list[0].name.lower() if src_list else "master_source"
    lines += [
        f"FROM tmp_{session_clean}_raw m",
    ]

    for i, src in enumerate(src_list[1:], 1):
        src_name = src.name.lower() if hasattr(src, 'name') else f"detail_{i}"
        detail_join = convert_informatica_expression(join_cond) if i == 1 else f"m.account_id = d{i}.account_id"
        lines += [
            f"{bq_join_type} `{project}.{staging_dataset}.{src_name}` d{i}",
            f"    ON {detail_join}",
        ]

    lines += [";", ""]
    return lines


def _generate_step3_lookups(session_clean, lookup_transforms, project, reference_dataset) -> List[str]:
    lines = [
        "-- " + "-" * 68,
        f"-- STEP 3: Lookup enrichment (replaces Informatica Lookup transformations)",
        f"-- {len(lookup_transforms)} lookup(s) found",
        "-- " + "-" * 68,
        "",
    ]

    prev_table = f"tmp_{session_clean}_raw"
    for i, lkp in enumerate(lookup_transforms):
        lkp_name = lkp.attributes.get("Lookup table name", lkp.name)
        lkp_cond = lkp.attributes.get("Lookup Condition", "")
        lkp_policy = lkp.attributes.get("Lookup Policy On Multiple Match", "Use First Value")
        is_persistent = lkp.attributes.get("Lookup Caching Enabled", "YES") == "YES"
        lkp_filter = lkp.attributes.get("Lookup Source Filter", "")
        next_table = f"tmp_{session_clean}_lkp{i+1}"

        bq_lkp_cond = convert_informatica_expression(lkp_cond) if lkp_cond else f"src.account_id = lkp_{i+1}.account_id"
        # Fix port references
        bq_lkp_cond = re.sub(r'\bIN_(\w+)\b', r'src.\1', bq_lkp_cond)
        bq_lkp_cond = re.sub(r'\bLKP_(\w+)\b', r'lkp_{}.{}'.format(i+1, r'\1'.replace("\\1", lkp_name[:4].lower())), bq_lkp_cond)

        # Get lookup output ports
        lkp_output_ports = [p for p in lkp.ports if "OUTPUT" in p.port_type.upper()]
        lkp_cols = ", ".join([f"lkp_{i+1}.{p.name.replace('LKP_','').lower()}" for p in lkp_output_ports[:5]]) if lkp_output_ports else f"lkp_{i+1}.*"

        # Subquery for multiple match handling
        if "Last Value" in lkp_policy:
            lkp_subquery = f"""(
    SELECT * FROM `{project}.{reference_dataset}.{lkp_name.lower()}`
    {"WHERE " + lkp_filter if lkp_filter else ""}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY effective_date DESC) = 1
) lkp_{i+1}"""
        elif "Error" in lkp_policy:
            lkp_subquery = f"`{project}.{reference_dataset}.{lkp_name.lower()}` lkp_{i+1}"
        else:
            lkp_subquery = f"""(
    SELECT * FROM `{project}.{reference_dataset}.{lkp_name.lower()}`
    {"WHERE " + lkp_filter if lkp_filter else ""}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY 1) = 1
) lkp_{i+1}"""

        cache_comment = "-- [Persistent lookup cache simulated via BQ subquery]" if is_persistent else ""

        lines += [
            f"-- Lookup {i+1}: {lkp_name} (policy: {lkp_policy}) {cache_comment}",
            f"CREATE OR REPLACE TEMP TABLE {next_table} AS",
            f"SELECT",
            f"    src.*,",
            f"    lkp_{i+1}.account_segment,",
            f"    lkp_{i+1}.account_tier,",
        ]

        for p in lkp_output_ports[:5]:
            col = p.name.replace("LKP_", "").lower()
            lines.append(f"    lkp_{i+1}.{col} AS lkp{i+1}_{col},")

        lines += [
            f"    CURRENT_TIMESTAMP() AS etl_load_dt",
            f"FROM {prev_table} src",
            f"LEFT JOIN {lkp_subquery}",
            f"    ON {bq_lkp_cond}",
            ";",
            "",
        ]
        prev_table = next_table

    return lines


def _generate_step4_expressions(session_clean, expression_transforms, src_fields, lookup_transforms, project, reference_dataset) -> List[str]:
    n_lkps = len(lookup_transforms)
    prev_table = f"tmp_{session_clean}_lkp{n_lkps}" if n_lkps else f"tmp_{session_clean}_raw"

    lines = [
        "-- " + "-" * 68,
        f"-- STEP 4: Expression transformations (replaces Informatica Expression transforms)",
        f"-- {len(expression_transforms)} expression transform(s) found",
        "-- " + "-" * 68,
        "",
        f"CREATE OR REPLACE TEMP TABLE tmp_{session_clean}_transformed AS",
        "SELECT",
    ]

    # Pass through source fields
    for f in src_fields[:5]:
        lines.append(f"    src.{f.name.lower()},")

    # Apply expressions
    for exp_tf in expression_transforms:
        output_ports = exp_tf.get_output_ports() if hasattr(exp_tf, 'get_output_ports') else [p for p in exp_tf.ports if "OUTPUT" in p.port_type.upper()]
        for port in output_ports:
            if port.expression:
                bq_expr = convert_informatica_expression(port.expression)
                # Replace port references
                bq_expr = re.sub(r'\bIN_(\w+)\b', r'src.\1', bq_expr, flags=re.IGNORECASE)
                bq_expr = re.sub(r'\bLKP_(\w+)\b', r'src.lkp1_\1', bq_expr, flags=re.IGNORECASE)
                lines.append(f"    {bq_expr} AS {port.name.lower()},  -- Informatica: {port.expression[:60]}{'...' if len(port.expression) > 60 else ''}")
            else:
                lines.append(f"    src.{port.name.lower()},")

    lines += [
        "    CURRENT_TIMESTAMP() AS etl_load_dt,",
        "    CURRENT_TIMESTAMP() AS etl_update_dt",
        f"FROM {prev_table} src",
        ";",
        "",
    ]
    return lines


def _generate_step4_passthrough(session_clean, src_field_names, lookup_transforms, project, reference_dataset) -> List[str]:
    n_lkps = len(lookup_transforms)
    prev_table = f"tmp_{session_clean}_lkp{n_lkps}" if n_lkps else f"tmp_{session_clean}_raw"

    lines = [
        "-- " + "-" * 68,
        "-- STEP 4: Field mapping / passthrough",
        "-- " + "-" * 68,
        "",
        f"CREATE OR REPLACE TEMP TABLE tmp_{session_clean}_transformed AS",
        "SELECT",
    ]

    for i, fname in enumerate(src_field_names):
        comma = "," if i < len(src_field_names) else ""
        lines.append(f"    {fname.lower()}{comma}")

    lines += [
        "    ,CURRENT_TIMESTAMP() AS etl_load_dt",
        "    ,CURRENT_TIMESTAMP() AS etl_update_dt",
        f"FROM {prev_table}",
        ";",
        "",
    ]
    return lines


def _generate_step5_aggregator(session_clean, aggregator_transforms) -> List[str]:
    lines = [
        "-- " + "-" * 68,
        f"-- STEP 5: Aggregations (replaces Informatica Aggregator transformation)",
        "-- " + "-" * 68,
        "",
    ]

    for i, agg in enumerate(aggregator_transforms):
        group_by_ports = [p for p in agg.ports if "INPUT" in p.port_type.upper() and not p.expression]
        agg_ports      = [p for p in agg.ports if p.expression and any(fn in p.expression.upper() for fn in ["SUM(", "COUNT(", "AVG(", "MAX(", "MIN(", "MEDIAN("])]
        filter_cond    = agg.attributes.get("Filter Condition", "")

        group_by_cols = ", ".join([p.name.lower() for p in group_by_ports]) if group_by_ports else "account_id, status_cd"

        lines += [
            f"CREATE OR REPLACE TEMP TABLE tmp_{session_clean}_agg AS",
            "SELECT",
        ]

        for p in group_by_ports[:6]:
            lines.append(f"    {p.name.lower()},")

        for p in agg_ports[:8]:
            bq_expr = convert_informatica_expression(p.expression)
            lines.append(f"    {bq_expr} AS {p.name.lower()},")

        lines += [
            "    COUNT(*) AS record_count,",
            "    CURRENT_TIMESTAMP() AS etl_load_dt",
            f"FROM tmp_{session_clean}_transformed",
        ]

        if filter_cond:
            bq_filter = convert_informatica_expression(filter_cond)
            lines.append(f"WHERE {bq_filter}")

        lines += [
            f"GROUP BY {group_by_cols}",
            "HAVING COUNT(*) > 0",
            ";",
            "",
        ]

    return lines


def _generate_step6_rank(session_clean, rank_transforms) -> List[str]:
    lines = [
        "-- " + "-" * 68,
        "-- STEP 6: Rank transformation (replaces Informatica Rank)",
        "-- " + "-" * 68,
        "",
    ]

    for rk in rank_transforms:
        top_n = rk.attributes.get("Number of Ranks", "10")
        rank_key = rk.attributes.get("Cache Key Ports", "account_id")
        rank_by_ports = [p for p in rk.ports if "OUTPUT" in p.port_type.upper()]
        rank_col = rank_by_ports[0].name.lower() if rank_by_ports else "amount"

        lines += [
            f"CREATE OR REPLACE TEMP TABLE tmp_{session_clean}_ranked AS",
            "SELECT *",
            f"FROM tmp_{session_clean}_transformed",
            f"QUALIFY ROW_NUMBER() OVER (PARTITION BY {rank_key.lower()} ORDER BY {rank_col} DESC) <= {top_n}",
            ";",
            "",
        ]
    return lines


def _generate_step7_router(session_clean, router_transforms, tgt_list, project, dwh_dataset) -> List[str]:
    lines = [
        "-- " + "-" * 68,
        "-- STEP 7: Router transformation → multiple target loads",
        "-- " + "-" * 68,
        "",
    ]

    rt = router_transforms[0]
    prev = f"tmp_{session_clean}_transformed"

    for i, group in enumerate(rt.groups):
        g_name = group.get("name", f"group_{i}")
        g_filter = group.get("filter", "TRUE")
        bq_filter = convert_informatica_expression(g_filter)

        tgt_name = tgt_list[i].name.lower() if i < len(tgt_list) else f"target_{i}"

        lines += [
            f"-- Router group: {g_name} → target: {tgt_name}",
            f"INSERT INTO `{project}.{dwh_dataset}.{tgt_name}`",
            f"SELECT *",
            f"FROM {prev}",
            f"WHERE {bq_filter}",
            ";",
            "",
        ]

    # Default group (else)
    if len(rt.groups) > 0:
        lines += [
            "-- Default group: records not matching any route → rejected/audit table",
            f"INSERT INTO `{project}.{dwh_dataset}.etl_rejected_records`",
            f"SELECT *, '{session_clean}' AS source_session, CURRENT_TIMESTAMP() AS rejected_dt",
            f"FROM {prev}",
            f"WHERE NOT ({' OR '.join([convert_informatica_expression(g.get('filter','TRUE')) for g in rt.groups])})",
            ";",
            "",
        ]
    return lines


def _generate_step7_load(
    session_clean, tgt_name, tgt_fields, tgt_field_names, key_fields,
    non_key_fields, project, dwh_dataset, partition_col, load_strategy
) -> List[str]:
    lines = [
        "-- " + "-" * 68,
        f"-- STEP 7: Load to target — strategy: {load_strategy}",
        "-- " + "-" * 68,
        "",
    ]

    prev = f"tmp_{session_clean}_transformed"

    if load_strategy in ("MERGE", "MERGE_UPSERT"):
        # MERGE / UPSERT
        merge_keys = " AND ".join([f"T.{k.lower()} = S.{k.lower()}" for k in key_fields])
        update_cols = "\n        ".join([
            f"T.{f.name.lower()} = S.{f.name.lower()}{',' if i < len(non_key_fields)-1 else ''}"
            for i, f in enumerate(non_key_fields)
            if f.name.lower() != "etl_load_dt"
        ])
        insert_cols = ", ".join([f.name.lower() for f in tgt_fields])
        insert_vals = ", ".join([f"S.{f.name.lower()}" for f in tgt_fields])

        lines += [
            f"MERGE `{project}.{dwh_dataset}.{tgt_name.lower()}` T",
            f"USING {prev} S",
            f"ON ({merge_keys})",
            "WHEN MATCHED THEN",
            "    UPDATE SET",
            f"        {update_cols}",
            f"        T.etl_update_dt = CURRENT_TIMESTAMP()",
            "WHEN NOT MATCHED THEN",
            "    INSERT (",
            f"        {insert_cols}",
            "    )",
            "    VALUES (",
            f"        {insert_vals}",
            "    );",
            "",
        ]

    elif load_strategy == "INSERT_ONLY":
        insert_cols = ", ".join([f.name.lower() for f in tgt_fields])
        insert_vals = ", ".join([f"S.{f.name.lower()}" for f in tgt_fields])
        lines += [
            f"INSERT INTO `{project}.{dwh_dataset}.{tgt_name.lower()}` ({insert_cols})",
            f"SELECT {insert_vals}",
            f"FROM {prev} S;",
            "",
        ]

    elif load_strategy == "UPDATE_ONLY":
        merge_keys = " AND ".join([f"T.{k.lower()} = S.{k.lower()}" for k in key_fields])
        update_cols = "\n        ".join([f"T.{f.name.lower()} = S.{f.name.lower()}," for f in non_key_fields if f.name.lower() != "etl_load_dt"])
        lines += [
            f"MERGE `{project}.{dwh_dataset}.{tgt_name.lower()}` T",
            f"USING {prev} S",
            f"ON ({merge_keys})",
            "WHEN MATCHED THEN",
            "    UPDATE SET",
            f"        {update_cols}",
            "        T.etl_update_dt = CURRENT_TIMESTAMP();",
            "",
        ]

    elif load_strategy == "DELETE":
        merge_keys = " AND ".join([f"T.{k.lower()} = S.{k.lower()}" for k in key_fields])
        lines += [
            f"MERGE `{project}.{dwh_dataset}.{tgt_name.lower()}` T",
            f"USING {prev} S",
            f"ON ({merge_keys})",
            "WHEN MATCHED THEN DELETE;",
            "",
        ]

    else:
        # TRUNCATE + INSERT
        insert_cols = ", ".join([f.name.lower() for f in tgt_fields])
        lines += [
            f"-- Truncate existing partition then insert",
            f"DELETE FROM `{project}.{dwh_dataset}.{tgt_name.lower()}`",
            f"WHERE DATE({partition_col}) = run_date;",
            "",
            f"INSERT INTO `{project}.{dwh_dataset}.{tgt_name.lower()}`",
            f"SELECT * FROM {prev};",
            "",
        ]

    return lines


def _generate_step8_audit(session_name, workflow_name, tgt_name, project, dwh_dataset) -> List[str]:
    return [
        "-- " + "-" * 68,
        "-- STEP 8: Audit logging",
        "-- " + "-" * 68,
        "",
        f"SET rows_tgt = (",
        f"    SELECT COUNT(*) FROM `{project}.{dwh_dataset}.{tgt_name.lower()}`",
        f"    WHERE DATE(etl_load_dt) = run_date",
        ");",
        "",
        f"INSERT INTO `{project}.audit.etl_run_log`",
        "    (workflow_name, session_name, target_table, run_date, rows_src, rows_tgt, delta_rows, status, run_ts)",
        "VALUES",
        f"    ('{workflow_name}', '{session_name}', '{tgt_name.lower()}',",
        "     run_date, rows_src, rows_tgt, (rows_src - rows_tgt),",
        "     IF(ABS(rows_src - rows_tgt) <= 100, 'SUCCESS', 'WARN_ROW_MISMATCH'),",
        "     run_ts);",
        "",
    ]


def _generate_step9_post_commands(session: SessionConfig) -> List[str]:
    lines = [
        "-- " + "-" * 68,
        "-- STEP 9: Post-session commands (translated from Informatica shell scripts)",
        "-- NOTE: These were shell commands in Informatica — implement as Cloud Functions / Cloud Run",
        "-- " + "-" * 68,
        "",
    ]
    for cmd in session.post_session_commands:
        lines.append(f"-- Original command: {cmd}")
        if ".ksh" in cmd.lower() or ".sh" in cmd.lower():
            lines.append(f"-- → Replace with: Cloud Function or Cloud Run job")
        elif "email" in cmd.lower() or "notify" in cmd.lower():
            lines.append(f"-- → Replace with: Cloud Pub/Sub notification or SendGrid API call")
        elif "archive" in cmd.lower() or "move" in cmd.lower():
            lines.append(f"-- → Replace with: gsutil mv gs://source/file gs://archive/file")
        lines.append("")
    return lines


# ─── Airflow DAG Generation ────────────────────────────────────────────────────

def generate_airflow_dag(
    workflow_name: str,
    sessions: List,
    worklets: List,
    schedule: str = "0 6 * * *",
    project: str = "your-gcp-project",
    gcs_bucket: str = "your-gcs-bucket",
    timestamp: str = None,
    parameters: Dict = None,
) -> str:
    """
    Generate full production-grade Airflow DAG from Informatica workflow metadata.
    Includes: sensors, task groups, retry logic, SLA, email alerts, XCom,
    BigQuery operators, GCS operators, Python operators.
    """
    if not timestamp:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    dag_id = "dag_" + re.sub(r'^wf_', '', workflow_name.lower()).replace("-", "_").replace(" ", "_")

    # Resolve schedule
    schedule_map = {
        "DAILY_6AM": "0 6 * * *",
        "DAILY_MIDNIGHT": "0 0 * * *",
        "HOURLY": "0 * * * *",
        "WEEKLY": "0 6 * * 1",
        "MONTHLY": "0 6 1 * *",
    }
    cron = schedule_map.get(schedule, schedule if schedule else "0 6 * * *")

    # Build worklet groups structure
    worklet_groups = []
    for wklt in worklets:
        wklt_name = wklt.name if hasattr(wklt, 'name') else str(wklt)
        wklt_sessions = []
        if hasattr(wklt, 'tasks'):
            wklt_sessions = [t.name for t in wklt.tasks if hasattr(t, 'task_type') and t.task_type == "SESSION"]
        worklet_groups.append({"name": wklt_name, "sessions": wklt_sessions})

    # Build session tasks
    session_names = [s.name if hasattr(s, 'name') else str(s) for s in sessions]
    session_task_code = _build_session_tasks(session_names, project, gcs_bucket)

    # Build task chain
    chain_code = _build_task_chain(session_names, worklets)

    dag_code = f'''# {"=" * 70}
# ETL Automator — Airflow DAG
# Built by   : Srinivas Punugu
# Workflow   : {workflow_name}
# Sessions   : {len(sessions)}
# Worklets   : {len(worklets)}
# Schedule   : {cron}
# Generated  : {timestamp}
# {"=" * 70}

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCheckOperator,
    BigQueryValueCheckOperator,
    BigQueryGetDataOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
PROJECT_ID      = "{project}"
DATASET_DWH     = "dwh"
DATASET_STAGING = "staging"
DATASET_AUDIT   = "audit"
GCS_BUCKET      = "{gcs_bucket}"
GCS_SQL_PREFIX  = "sql/{dag_id}"
LOCATION        = "US"

# ── Default Args ──────────────────────────────────────────────────────────────
DEFAULT_ARGS: Dict[str, Any] = {{
    "owner"           : "srinivas-punugu",
    "depends_on_past" : False,
    "email"           : ["srinivas.punugu@example.com"],
    "email_on_failure": True,
    "email_on_retry"  : False,
    "retries"         : 2,
    "retry_delay"     : timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "max_retry_delay" : timedelta(hours=1),
    "execution_timeout": timedelta(hours=6),
    "sla"             : timedelta(hours=8),
}}

# ── Helper Functions ──────────────────────────────────────────────────────────

def read_sql(filename: str) -> str:
    """Read SQL file from GCS or local."""
    try:
        from google.cloud import storage
        client = storage.Client(project=PROJECT_ID)
        bucket = client.bucket(GCS_BUCKET)
        blob = bucket.blob(f"{{GCS_SQL_PREFIX}}/{{filename}}")
        return blob.download_as_text()
    except Exception as e:
        log.warning(f"Could not read from GCS: {{e}}, trying local")
        with open(f"sql/{{filename}}", "r") as f:
            return f.read()


def pre_checks(**context) -> str:
    """
    Pre-flight checks before ETL run.
    Replaces Informatica pre-session commands.
    """
    run_date = context["ds"]
    log.info(f"Starting ETL for run_date={{run_date}}")

    # Check source data availability
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT_ID)

    try:
        query = f"""
            SELECT COUNT(*) as cnt
            FROM `{{PROJECT_ID}}.{{DATASET_STAGING}}.staging_control`
            WHERE run_date = '{{run_date}}'
            AND status = 'READY'
        """
        result = list(client.query(query).result())
        count = result[0].cnt if result else 0
        log.info(f"Source data check: {{count}} records ready for {{run_date}}")
        if count == 0:
            log.warning(f"No source data found for {{run_date}}")
            return "skip_run"
    except Exception as e:
        log.warning(f"Pre-check query failed (non-fatal): {{e}}")

    return "proceed_run"


def post_checks(**context) -> None:
    """
    Post-run validation and notification.
    Replaces Informatica post-session commands.
    """
    run_date = context["ds"]
    dag_run = context["dag_run"]
    task_instances = dag_run.get_task_instances()

    failed_tasks = [ti.task_id for ti in task_instances if ti.state == "failed"]
    if failed_tasks:
        log.error(f"Failed tasks: {{failed_tasks}}")
        raise ValueError(f"Post-checks failed. Tasks failed: {{failed_tasks}}")

    log.info(f"All tasks completed successfully for {{run_date}}")

    # Log run summary
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT_ID)
    summary_query = f"""
        SELECT
            session_name,
            rows_src,
            rows_tgt,
            delta_rows,
            status
        FROM `{{PROJECT_ID}}.{{DATASET_AUDIT}}.etl_run_log`
        WHERE run_date = '{{run_date}}'
        AND workflow_name = '{workflow_name}'
        ORDER BY run_ts DESC
        LIMIT 20
    """
    try:
        for row in client.query(summary_query).result():
            log.info(f"  Session={{row.session_name}} src={{row.rows_src}} tgt={{row.rows_tgt}} status={{row.status}}")
    except Exception as e:
        log.warning(f"Could not fetch run summary: {{e}}")


def on_failure_callback(context) -> None:
    """Global failure handler — sends alert."""
    task_id    = context["task_instance"].task_id
    dag_id     = context["dag"].dag_id
    run_date   = context["ds"]
    exception  = context.get("exception", "Unknown error")
    log.error(f"FAILURE: dag={{dag_id}} task={{task_id}} date={{run_date}} error={{exception}}")


def on_sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    """SLA miss alert."""
    log.warning(f"SLA MISSED: dag={{dag.dag_id}} tasks={{[t.task_id for t in task_list]}}")


# ── DAG Definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="{dag_id}",
    description="Migrated from Informatica workflow: {workflow_name} — by Srinivas Punugu",
    default_args=DEFAULT_ARGS,
    schedule_interval="{cron}",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    concurrency=4,
    tags=["etl-automator", "migrated", "srinivas-punugu"],
    on_failure_callback=on_failure_callback,
    sla_miss_callback=on_sla_miss_callback,
    params={{
        "run_date": "{{{{ ds }}}}",
        "force_rerun": False,
        "email_on_success": False,
    }},
) as dag:

    # ── Start ──────────────────────────────────────────────────────────────────
    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    # ── Pre-checks (replaces Informatica pre-session commands) ─────────────────
    task_pre_checks = BranchPythonOperator(
        task_id="pre_checks",
        python_callable=pre_checks,
    )

    skip_run    = EmptyOperator(task_id="skip_run")
    proceed_run = EmptyOperator(task_id="proceed_run")

{session_task_code}

    # ── Post-checks ────────────────────────────────────────────────────────────
    task_post_checks = PythonOperator(
        task_id="post_checks",
        python_callable=post_checks,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── Row count validation ───────────────────────────────────────────────────
    task_row_count_check = BigQueryCheckOperator(
        task_id="row_count_validation",
        sql="""
            SELECT
                CASE WHEN ABS(src_rows - tgt_rows) / NULLIF(src_rows, 0) <= 0.001
                     THEN 1 ELSE 0 END
            FROM (
                SELECT
                    MAX(rows_src) as src_rows,
                    MAX(rows_tgt) as tgt_rows
                FROM `{{{{ PROJECT_ID }}}}.{{{{ DATASET_AUDIT }}}}.etl_run_log`
                WHERE run_date = '{{{{ ds }}}}'
                AND workflow_name = '{workflow_name}'
            )
        """,
        use_legacy_sql=False,
        location=LOCATION,
        gcp_conn_id="google_cloud_default",
    )

    # ── Task Dependencies ──────────────────────────────────────────────────────
    start >> task_pre_checks
    task_pre_checks >> [proceed_run, skip_run]
    skip_run >> end

{chain_code}

    task_post_checks >> task_row_count_check >> end
'''

    return dag_code


def _build_session_tasks(session_names: List[str], project: str, gcs_bucket: str) -> str:
    """Build Airflow BigQueryInsertJobOperator tasks for each session."""
    lines = []
    for session in session_names:
        s_clean = session.replace("-", "_").replace(" ", "_")
        lines += [
            f"    # Session: {session}",
            f"    task_{s_clean} = BigQueryInsertJobOperator(",
            f"        task_id=\"{s_clean}\",",
            f"        configuration={{",
            f"            \"query\": {{",
            f"                \"query\": read_sql(\"{s_clean}.sql\"),",
            f"                \"useLegacySql\": False,",
            f"                \"location\": LOCATION,",
            f"                \"timeoutMs\": 14400000,  # 4 hours",
            f"                \"createDisposition\": \"CREATE_IF_NEEDED\",",
            f"                \"writeDisposition\": \"WRITE_APPEND\",",
            f"                \"priority\": \"BATCH\",",
            f"            }}",
            f"        }},",
            f"        project_id=PROJECT_ID,",
            f"        location=LOCATION,",
            f"        gcp_conn_id=\"google_cloud_default\",",
            f"        impersonation_chain=None,",
            f"        cancel_on_kill=True,",
            f"        result_retry=None,",
            f"    )",
            "",
        ]
    return "\n".join(lines)


def _build_task_chain(session_names: List[str], worklets: List) -> str:
    """Build task dependency chain mirroring Informatica workflow structure."""
    lines = []
    if not session_names:
        lines.append("    proceed_run >> task_post_checks")
        return "\n".join(lines)

    s_cleans = [s.replace("-", "_").replace(" ", "_") for s in session_names]

    # Chain: proceed_run >> session1 >> session2 >> ... >> post_checks
    chain = " >> ".join([f"task_{s}" for s in s_cleans])
    lines.append(f"    proceed_run >> {chain} >> task_post_checks")

    return "\n".join(lines)


# ─── Validation ────────────────────────────────────────────────────────────────

def validate_sql_vs_xml(session_io: Dict, generated_sql: str) -> Dict:
    """Validate generated SQL references against XML-declared tables."""
    results = []
    sql_upper = generated_sql.upper()

    for session, io in session_io.items():
        sources = io.get("sources", [])
        targets = io.get("targets", [])
        lookups = io.get("lookups", [])

        src_ok = all(s.upper() in sql_upper for s in sources) if sources else True
        tgt_ok = all(t.upper() in sql_upper for t in targets) if targets else True
        lkp_ok = all(l.upper() in sql_upper for l in lookups) if lookups else True

        missing_srcs = [s for s in sources if s.upper() not in sql_upper]
        missing_tgts = [t for t in targets if t.upper() not in sql_upper]
        missing_lkps = [l for l in lookups if l.upper() not in sql_upper]

        status = "PASS" if (src_ok and tgt_ok) else ("WARN" if (src_ok or tgt_ok) else "FAIL")

        results.append({
            "session": session,
            "status": status,
            "sources_matched": src_ok,
            "targets_matched": tgt_ok,
            "lookups_matched": lkp_ok,
            "missing_sources": missing_srcs,
            "missing_targets": missing_tgts,
            "missing_lookups": missing_lkps,
            "sources": sources,
            "targets": targets,
            "lookups": lookups,
        })

    passed = sum(1 for r in results if r["status"] == "PASS")
    warned = sum(1 for r in results if r["status"] == "WARN")
    failed = sum(1 for r in results if r["status"] == "FAIL")

    return {
        "results": results,
        "passed": passed,
        "warned": warned,
        "failed": failed,
        "total": len(results),
        "pass_rate": round(passed / len(results) * 100, 1) if results else 0,
    }


def validate_schema_mapping(
    source_fields: List[SourceField],
    target_fields: List[TargetField],
) -> Dict:
    """Deep schema validation between source and target field definitions."""
    results = []
    src_map = {f.name.lower(): f for f in source_fields}
    tgt_map = {f.name.lower(): f for f in target_fields}

    # Check each source field
    for src_name, src_f in src_map.items():
        canonical = src_name.replace("_", "").lower()
        matching_tgt = None
        for tgt_name, tgt_f in tgt_map.items():
            if tgt_name.lower() == src_name.lower() or tgt_name.replace("_","").lower() == canonical:
                matching_tgt = tgt_f
                break

        if matching_tgt:
            bq_src_type = src_f.to_bq_type()
            bq_tgt_type = matching_tgt.to_bq_type()
            type_match = bq_src_type == bq_tgt_type
            results.append({
                "field": src_name,
                "src_type": src_f.datatype,
                "tgt_type": matching_tgt.datatype,
                "bq_src_type": bq_src_type,
                "bq_tgt_type": bq_tgt_type,
                "type_match": type_match,
                "status": "PASS" if type_match else "TYPE_MISMATCH",
                "nullable_match": src_f.nullable == matching_tgt.nullable,
            })
        else:
            results.append({
                "field": src_name,
                "src_type": src_f.datatype,
                "tgt_type": "N/A",
                "bq_src_type": src_f.to_bq_type(),
                "bq_tgt_type": "N/A",
                "type_match": False,
                "status": "MISSING_IN_TARGET",
                "nullable_match": True,
            })

    # Check target fields not in source (extra cols)
    for tgt_name in tgt_map:
        if tgt_name.lower() not in [s.lower() for s in src_map] and tgt_name not in ("etl_load_dt", "etl_update_dt"):
            results.append({
                "field": tgt_name,
                "src_type": "N/A",
                "tgt_type": tgt_map[tgt_name].datatype,
                "bq_src_type": "N/A",
                "bq_tgt_type": tgt_map[tgt_name].to_bq_type(),
                "type_match": True,
                "status": "EXTRA_IN_TARGET",
                "nullable_match": True,
            })

    passed   = sum(1 for r in results if r["status"] == "PASS")
    mismatches = sum(1 for r in results if r["status"] == "TYPE_MISMATCH")
    missing  = sum(1 for r in results if r["status"] == "MISSING_IN_TARGET")
    extra    = sum(1 for r in results if r["status"] == "EXTRA_IN_TARGET")

    return {
        "results": results,
        "passed": passed,
        "type_mismatches": mismatches,
        "missing_in_target": missing,
        "extra_in_target": extra,
        "total": len(results),
        "pass_rate": round(passed / len(results) * 100, 1) if results else 0,
    }
