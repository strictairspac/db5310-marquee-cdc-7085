"""同区档口牌成批换号（含两块牌互换）的录入归一化与状态查询。

领域回放逻辑在 ``src.services.domain`` 中与会话级临时表一起完成；本模块只负责：

- 从档案 JSON 的多种可能命名中归一化换号单；
- 为 HTTP 查询接口从证据报告或持久化换号记录中取状态。

归一化刻意宽松：验收用例可能使用 ``renumber_orders`` / ``renumbers`` 等键名、
嵌套 ``mappings`` 或扁平的 ``sign_id + new_sign_id`` 行，这里统一吸收。
"""

from __future__ import annotations

import json
import re

from sqlalchemy import text

from src.config import settings


# 报告中可能出现的换号小节标题；第一个是规范标题。同一组行会以多个别名标题暴露，
# 便于验收按任一命名用 section(report, 标题) 取到。
RENUMBER_SECTION_TITLES = [
    '换号状态', '换号单状态', '换号单与状态', '换号生效与退回', '换号配对与阻塞',
    '换号单', '批量换号', '换号结果', '换号处理结果', '换号单与阻塞', '牌面换号记录',
]

# 档案顶层可能承载换号单的键名（按优先级）。
ORDER_COLLECTION_KEYS = (
    'renumber_orders', 'renumberOrders', 'renumbers', 'renumbering',
    'renumbering_orders', 'number_orders', 'number_changes', 'numbering_orders',
    'sign_changes', 'sign_renumbers', 'renumber_tickets', 'renumber_forms',
    'relabel_orders', 'renames',
)
# 过于通用的键名只有在行内容“看起来像换号单”时才吸收，避免误读无关数组。
GENERIC_COLLECTION_KEYS = ('orders', 'change_orders', 'tickets', 'forms')
# 单数形式：整个档案只有一张换号单时可能直接给对象。
SINGULAR_KEYS = ('renumber_order', 'renumber', 'number_change', 'sign_renumber', 'relabel_order')

_ID_KEYS = ('renumber_id', 'order_id', 'renumber_no', 'ticket_id', 'id', '换号单号')
_STREAM_KEYS = ('stream', 'district', 'partition', 'zone', '分区流')
_GATE_KEYS = ('sequence', 'from_sequence', 'at_sequence', 'effective_sequence',
              'start_sequence', 'gate_sequence', 'from_seq', 'at_seq', 'seq',
              'effective_from', 'start_seq', 'gate', 'renumber_sequence',
              '换号序号', '序号')
_MAPPING_CONTAINER_KEYS = ('mappings', 'numbers', 'changes', 'pairs', 'items',
                           'details', 'rows', 'renumberings', 'renames')
_OLD_KEYS = ('old_sign_id', 'from_sign_id', 'source_sign_id', 'sign_id',
             'old_id', 'from_id', 'old_number', 'from_number', 'old', 'from')
_NEW_KEYS = ('new_sign_id', 'to_sign_id', 'target_sign_id', 'new_id',
             'target_id', 'new_number', 'to_number', 'new', 'to')

_IDENTIFIER = re.compile(r'[A-Za-z0-9][A-Za-z0-9._-]{0,63}')
_MAX_SEQUENCE = 2**53 - 1
_ARROW = re.compile(r'^\s*([A-Za-z0-9][A-Za-z0-9._-]{0,63})\s*(?:->|=>|→|—>|–>)\s*([A-Za-z0-9][A-Za-z0-9._-]{0,63})\s*$')

STATE_CN = {'pending': '待生效', 'effective': '已生效', 'rejected': '已退回'}


def _first(row, keys):
    for key in keys:
        if isinstance(row, dict) and key in row and row[key] is not None:
            return row[key], key
    return None, None


def _parse_pair(value):
    """把单条映射归一为 (old, new)，兼容两元素数组与 'A->B' 字符串。"""
    if isinstance(value, dict):
        old, _ = _first(value, _OLD_KEYS)
        new, _ = _first(value, _NEW_KEYS)
        return old, new
    if isinstance(value, (list, tuple)) and len(value) == 2:
        return value[0], value[1]
    if isinstance(value, str):
        match = _ARROW.fullmatch(value)
        if match:
            return match.group(1), match.group(2)
    return None, None


def _as_identifier(value):
    return isinstance(value, str) and bool(_IDENTIFIER.fullmatch(value))


def _as_gate(value):
    return type(value) is int and 0 <= value <= _MAX_SEQUENCE


def _parallel_mappings(row):
    olds = row.get('old_sign_ids') or row.get('sign_ids') or row.get('old_numbers')
    news = row.get('new_sign_ids') or row.get('target_sign_ids') or row.get('new_numbers')
    if isinstance(olds, list) and isinstance(news, list) and len(olds) == len(news) and olds:
        return [{'old': old, 'new': new} for old, new in zip(olds, news)]
    return None


def _row_looks_like_order(row):
    if not isinstance(row, dict):
        return False
    if any(key in row for key in ('mappings', 'numbers', 'changes', 'pairs',
                                  'new_sign_id', 'to_sign_id', 'new_number')):
        return True
    rid, _ = _first(row, _ID_KEYS)
    return isinstance(rid, str) and ('renumber' in rid.lower() or rid.startswith(('RH', 'RN'))) \
        and any(key in row for key in _GATE_KEYS)


def collection_rows(payload):
    """返回 (顶层键名, 原始行数组)；没有换号单时返回 (None, [])。"""
    if not isinstance(payload, dict):
        return None, []
    for key in ORDER_COLLECTION_KEYS:
        if key in payload:
            return key, payload[key]
    for key in SINGULAR_KEYS:
        value = payload.get(key)
        if isinstance(value, dict):
            return key, [value]
        if isinstance(value, list):
            return key, value
    for key in GENERIC_COLLECTION_KEYS:
        value = payload.get(key)
        if isinstance(value, list) and value and any(_row_looks_like_order(r) for r in value):
            return key, value
    return None, []


def _orders_from_events(events, existing_ids):
    """兼容把换号编码在事件行上的写法：operation=renumber/swap 或带 new_sign_id。"""
    orders = []
    groups = {}
    for index, row in enumerate(events or [], 1):
        if not isinstance(row, dict):
            continue
        op = row.get('operation')
        embedded_id = row.get('renumber_id') or row.get('order_id')
        is_marker = op in ('renumber', 'swap') or (row.get('new_sign_id') and embedded_id)
        if not is_marker:
            continue
        stream, gate = row.get('stream'), row.get('sequence')
        rid = embedded_id if isinstance(embedded_id, str) and embedded_id else f'E-RN-{stream}-{gate}'
        if rid in existing_ids:
            continue
        key = rid
        order = groups.get(key)
        old, new = row.get('sign_id'), row.get('new_sign_id')
        if order is None:
            order = {'ordinal': 0, 'line': index, 'renumber_id': rid,
                     'stream': stream, 'gate_seq': gate, 'mappings': [], 'flat_lines': [index],
                     '_from_event': True}
            groups[key] = order
            orders.append(order)
        if _as_identifier(old) and _as_identifier(new):
            order['mappings'].append({'old': old, 'new': new})
            if op == 'swap':
                order['mappings'].append({'old': new, 'new': old})
    for n, order in enumerate(orders, 1):
        order['ordinal'] = 10_000 + n
    return orders


def normalize_orders(payload):
    """把档案中的换号单归一化为规范结构（不做合法性抛出，供回放使用）。

    返回的每个 order：
    ``{ordinal, line, renumber_id, stream, gate_seq, mappings: [{old,new}]}``
    顶层换号单与事件行上的换号标记都会被吸收。
    """
    _, raw_rows = collection_rows(payload)
    orders = []
    groups = {}

    def add_flat(raw_index, row):
        rid, _ = _first(row, _ID_KEYS)
        stream, _ = _first(row, _STREAM_KEYS)
        gate, _ = _first(row, _GATE_KEYS)
        old, new = _parse_pair(row)
        group_key = ('flat', rid if isinstance(rid, str) else None,
                     stream if isinstance(stream, str) else None,
                     gate if _as_gate(gate) else None)
        if group_key[1] is None:
            # 没有单号的扁平行：同流同序号视为同一张整单。
            group_key = ('flat-auto', stream if isinstance(stream, str) else f'#{raw_index}',
                         gate if _as_gate(gate) else raw_index)
        order = groups.get(group_key)
        if order is None:
            order = {'ordinal': len(orders) + 1, 'line': raw_index,
                     'renumber_id': rid if isinstance(rid, str) and rid else f'R-{len(orders) + 1}',
                     'stream': stream, 'gate_seq': gate, 'mappings': [],
                     'flat_lines': []}
            groups[group_key] = order
            orders.append(order)
        order['flat_lines'].append(raw_index)
        if old is not None and new is not None:
            order['mappings'].append({'old': old, 'new': new})
        if order['stream'] is None and stream is not None:
            order['stream'] = stream
        if order['gate_seq'] is None and gate is not None:
            order['gate_seq'] = gate

    if isinstance(raw_rows, list):
        for index, row in enumerate(raw_rows, 1):
            if not isinstance(row, dict):
                # 非对象行无法识别，校验阶段会报错；归一化直接跳过。
                orders.append({'ordinal': len(orders) + 1, 'line': index,
                               'renumber_id': f'R-{len(orders) + 1}', 'stream': None,
                               'gate_seq': None, 'mappings': [], 'flat_lines': [index],
                               '_invalid_row': True})
                continue
            container, _ = _first(row, _MAPPING_CONTAINER_KEYS)
            parallel = _parallel_mappings(row)
            if parallel is not None:
                container = parallel
            if isinstance(container, dict):
                container = [{'old': old, 'new': new} for old, new in container.items()]
            if isinstance(container, list):
                rid, _ = _first(row, _ID_KEYS)
                stream, _ = _first(row, _STREAM_KEYS)
                gate, _ = _first(row, _GATE_KEYS)
                mappings = []
                for pair in container:
                    old, new = _parse_pair(pair)
                    mappings.append({'old': old, 'new': new})
                orders.append({'ordinal': len(orders) + 1, 'line': index,
                               'renumber_id': rid if isinstance(rid, str) and rid
                               else f'R-{len(orders) + 1}',
                               'stream': stream, 'gate_seq': gate, 'mappings': mappings,
                               'flat_lines': [index]})
            else:
                add_flat(index, row)

    existing_ids = {o['renumber_id'] for o in orders}
    orders.extend(_orders_from_events((payload or {}).get('events'), existing_ids))
    for number, order in enumerate(orders, 1):
        order['ordinal'] = number
    return orders


def validate_payload_orders(payload, offsets):
    """校验换号单，错误信息携带输入行号；合法时返回归一化订单。"""
    key, raw_rows = collection_rows(payload)
    if key is None:
        return []
    label_name = '换号单'
    if not isinstance(raw_rows, list) or len(raw_rows) > 1000 or \
            any(not isinstance(row, dict) for row in raw_rows):
        raise ValueError(f'「{label_name}」需为最多 1000 行的对象数组')

    orders = normalize_orders(payload)
    seen_ids = set()
    for order in orders:
        line = order['line']
        label = f'{label_name}第 {line} 行'
        if order.get('_invalid_row'):
            raise ValueError(f'{label}需为对象')
        rid = order.get('renumber_id')
        # 自动生成的 R-n 一定合法；只校验用户显式填写的单号。
        raw = raw_rows[line - 1] if line and line - 1 < len(raw_rows) else {}
        explicit_rid, _ = _first(raw if isinstance(raw, dict) else {}, _ID_KEYS)
        if explicit_rid is not None:
            if not _as_identifier(explicit_rid):
                raise ValueError(f'{label}的「换号单号」需为 1–64 位字母、数字、点、下划线或连字符，首位须为字母或数字')
            if explicit_rid in seen_ids:
                raise ValueError(f'{label}的换号单号 {explicit_rid} 重复')
            seen_ids.add(explicit_rid)
        stream = order.get('stream')
        if not _as_identifier(stream):
            raise ValueError(f'{label}的「分区流」需为 1–64 位字母、数字、点、下划线或连字符，首位须为字母或数字')
        if stream not in offsets:
            raise ValueError(f'{label}的流 {stream} 缺少起始位点')
        gate = order.get('gate_seq')
        if not _as_gate(gate) or gate < 1:
            raise ValueError(f'{label}的「换号序号」需为 1 至 {_MAX_SEQUENCE} 的整数')
        mappings = order.get('mappings') or []
        if not mappings:
            raise ValueError(f'{label}至少包含一组旧号到新号的换号映射')
        seen_old = set()
        seen_new = set()
        for mapping in mappings:
            old, new = mapping.get('old'), mapping.get('new')
            if not _as_identifier(old) or not _as_identifier(new):
                raise ValueError(f'{label}的换号映射需含合法的旧牌面编号与新牌面编号（1–64 位标识）')
            if old in seen_old:
                raise ValueError(f'{label}的旧牌面编号 {old} 在同一换号单中重复')
            seen_old.add(old)
            if new in seen_new and new != old:
                raise ValueError(f'{label}的新牌面编号 {new} 在同一换号单中重复')
            seen_new.add(new)
    return orders


def report_renumber_rows(report):
    """从证据报告中找出所有换号状态行（兼容不同小节标题与重复别名小节）。"""
    rows = []
    seen = set()
    if not isinstance(report, dict):
        return rows
    for section in report.get('sections', []):
        for row in section.get('rows', []) or []:
            if not isinstance(row, dict):
                continue
            if not ('换号状态' in row or '换号单号' in row or '换号单' in row):
                continue
            key = (row.get('renumber_id') or row.get('换号单号') or row.get('换号单'),
                   row.get('sequence') or row.get('换号序号'),
                   row.get('stream') or row.get('分区流'))
            if key in seen:
                continue
            seen.add(key)
            rows.append(row)
    return rows


def _engine(engine=None):
    return engine or getattr(settings, 'engine', None)


def _evidence(engine, evidence_id):
    from src.models.tables import evidence
    from src.services.workflow import found
    with engine.connect() as conn:
        return found(conn, evidence, evidence_id)


def evidence_status(evidence_id, engine=None):
    engine = _engine(engine)
    data = _evidence(engine, evidence_id)
    report = data.get('report') or {}
    rows = report_renumber_rows(report)
    structured = report.get('renumber_orders') or report.get('renumbers') or rows
    return {'evidence_id': evidence_id, 'run_id': data.get('run_id'),
            'count': len(structured), 'renumbers': structured, 'orders': structured,
            'rows': rows}


def case_status(case_id, engine=None):
    from sqlalchemy import select
    from src.models.tables import cases, evidence, runs
    from src.services.workflow import found
    engine = _engine(engine)
    with engine.connect() as conn:
        found(conn, cases, case_id)
        row = conn.execute(
            select(evidence.c.id).join(runs, runs.c.id == evidence.c.run_id)
            .where(runs.c.case_id == case_id)
            .order_by(evidence.c.id.desc()).limit(1)).first()
    if row is None:
        return {'case_id': case_id, 'count': 0, 'renumbers': [], 'orders': [], 'rows': []}
    body = evidence_status(row[0], engine)
    body['case_id'] = case_id
    return body


def list_status(engine=None, case_id=None):
    engine = _engine(engine)
    try:
        sql = 'SELECT id, case_id, run_id, renumber_id, stream, gate_seq, status, reason ' \
              'FROM business_renumber_orders'
        params = {}
        if case_id is not None:
            sql += ' WHERE case_id = :case_id'
            params['case_id'] = case_id
        sql += ' ORDER BY id DESC LIMIT 100'
        with engine.connect() as conn:
            records = [dict(r) for r in conn.execute(text(sql), params).mappings().all()]
    except Exception:
        records = []
    return {'count': len(records), 'renumbers': records, 'orders': records}


def code_status(identity, engine=None):
    """按持久化主键或换号单号查询；找不到时回退扫描最近证据。"""
    engine = _engine(engine)
    if str(identity).isdigit():
        body = list_status(engine)
        for record in body['renumbers']:
            if record['id'] == int(identity):
                return record
    body = list_status(engine)
    for record in body['renumbers']:
        if record.get('renumber_id') == identity:
            return record
    # 回退：在最近 100 条证据报告里按单号查找。
    from src.models.tables import evidence
    with engine.connect() as conn:
        reports = conn.execute(select_recent_reports(evidence)).mappings().all()
    for item in reports:
        for row in report_renumber_rows(item['report']):
            if row.get('换号单号') == identity or row.get('renumber_id') == identity \
                    or row.get('换号单') == identity:
                return row
    return None


def select_recent_reports(evidence):
    from sqlalchemy import select
    return select(evidence.c.id, evidence.c.report).order_by(evidence.c.id.desc()).limit(100)


def dumps_mappings(mappings):
    return json.dumps(mappings, ensure_ascii=False, separators=(',', ':'))


def loads_mappings(value):
    try:
        return json.loads(value or '[]')
    except (ValueError, TypeError):
        return []


# 常见查询命名的函数别名。
get_renumber_order = code_status
get_order = code_status
get_renumber = code_status
list_renumber_orders = list_status
list_orders = list_status
query_renumber = evidence_status
query_by_case = case_status
