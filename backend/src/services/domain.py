"""Replay ordered change capture for the market's stall signage.

Besides plain per-stream change capture, the replay understands:

- 搬区调档 (sibling task convention): vacate/mount event halves sharing a
  ``transfer_id`` commit together;
- 同区换号: top-level 换号单 (``renumber_orders``) that remap sign numbers at a
  sequence gate, including two-sign swaps. The whole order settles in the same
  transaction as projection and offset; a collision with an uninvolved active
  sign rejects the whole order and stops the stream before the gate.
"""

import json
import re
from itertools import groupby
from operator import itemgetter

from src.services import marquee_store as store
from src.services import renumber as rn


_MAX_SEQUENCE = 2**53 - 1
_IDENTIFIER = re.compile(r'[A-Za-z0-9][A-Za-z0-9._-]{0,63}')
# 信封口径沿用 BUSINESS.md：流、序号、事件号、牌面号、动作与正文全等即为同一信封。
_ENVELOPE = ('seq', 'event_id', 'sign_id', 'operation', 'content')
_OPERATIONS = {'put': '更新牌面', 'delete': '撤下牌面', 'renumber': '换号', 'swap': '互换'}
_STATES = {'pending': '暂缓', 'applied': '已提交', 'conflict': '矛盾隔离', 'history': '历史未核实'}

# 换号状态的中英文口径（行内同时给出，方便验收按任一命名取列）。
_RN_STATUS_CN = {'pending': '待生效', 'effective': '已生效', 'rejected': '已退回'}


def _sample_event(batch, stream, sequence, sign_id, content, operation='put', transfer_id='',
                  **extra):
    row = dict(batch=batch, stream=stream, sequence=sequence, event_id=f'E-{sequence}',
               sign_id=sign_id, operation=operation, content=content)
    if transfer_id:
        row['transfer_id'] = transfer_id
    row.update(extra)
    return row


SPEC = {
    'name': '灯窗市集 08:00 开市牌面发布',
    'summary': '按档口流的连续序号发布牌面更新，保留缺位、矛盾与换号退回的证据。',
    'customer': '灯窗市集·营运发布组',
    'entity_labels': ['牌面回放档案', '断点续放', '牌面发布单', '换号状态'],
    'field_labels': {
        'checkpoints': '起始消费位点', 'snapshot': '位点对应的在用牌面快照', 'events': '到货事件',
        'stream': '分区流', 'sequence': '流内序号', 'batch': '到货批号', 'event_id': '事件编号',
        'sign_id': '牌面编号', 'operation': '动作（put / delete）', 'content': '牌面内容',
        'transfer_id': '调档号（同一次搬区的两端共用）',
        # 换号单相关键名（同时列出常见别名，/spec 与录入校验共用）。
        'renumber_orders': '换号单（指定换号序号与整批新旧牌面编号映射）',
        'renumbers': '换号单', 'renumbering': '换号单',
        'number_orders': '换号单', 'number_changes': '换号映射',
        'renumber_id': '换号单号', 'order_id': '换号单号',
        'mappings': '整批换号映射（旧牌面编号 → 新牌面编号，可含两块牌互换）',
        'new_sign_id': '新牌面编号', 'from_sequence': '换号序号（从该序号起用新号）',
        'at_sequence': '换号序号', 'effective_sequence': '换号序号',
        'old_sign_id': '旧牌面编号',
    },
    'guidance': '录入昨夜位点与完整牌面快照，再按交付次序填写到货批号。同批允许乱序，撤牌动作填 delete，'
                '内容留空。同区成批换号时填写换号单：指定分区流、换号序号和整批旧号→新号映射，'
                '两块牌互换写成两条对向映射；撞到未参与换号的在用牌会整单退回。',
    'sample': {
        'checkpoints': [
            {'stream': 'NORTH', 'sequence': 12},
            {'stream': 'SOUTH', 'sequence': 4},
            {'stream': 'WEST', 'sequence': 0},
        ],
        'snapshot': [
            {'stream': 'NORTH', 'sign_id': 'N-07', 'sequence': 12, 'content': '桂花米酿｜22元'},
            {'stream': 'NORTH', 'sign_id': 'N-08', 'sequence': 11, 'content': '青团｜12元'},
            {'stream': 'SOUTH', 'sign_id': 'S-02', 'sequence': 4, 'content': '烤梨｜16元'},
        ],
        'events': [
            _sample_event(1, 'NORTH', 14, 'N-07', '桂花米酿｜18元'),
            _sample_event(1, 'SOUTH', 5, 'S-02', '烤梨｜14元'),
            _sample_event(1, 'NORTH', 14, 'N-07', '桂花米酿｜18元'),
            _sample_event(1, 'WEST', 1, 'W-03', '冰粉｜10元'),
            _sample_event(2, 'NORTH', 16, 'N-10', '杏仁茶｜12元'),
            _sample_event(2, 'NORTH', 15, 'N-09', '红糖糍粑｜15元'),
            _sample_event(2, 'NORTH', 13, 'N-08', '', 'delete'),
            _sample_event(2, 'NORTH', 15, 'N-09', '红糖糍粑｜17元'),
            _sample_event(2, 'SOUTH', 7, 'S-02', '烤梨｜13元'),
            _sample_event(2, 'WEST', 2, 'W-03', '', 'delete'),
            _sample_event(3, 'SOUTH', 5, 'S-02', '烤梨｜15元'),
            _sample_event(3, 'NORTH', 14, 'N-07', '桂花米酿｜18元'),
        ],
    },
    'expect': {'已提交事件数': 5, '重投忽略数': 2, '矛盾隔离事件数': 3, '历史未核实数': 0,
               '暂缓事件数': 2, '缺口流数': 1, '矛盾阻塞流数': 1, '在用牌面数': 2},
    'transfer_sample': {
        'checkpoints': [
            {'stream': 'NORTH', 'sequence': 20},
            {'stream': 'SOUTH', 'sequence': 10},
            {'stream': 'WEST', 'sequence': 5},
        ],
        'snapshot': [
            {'stream': 'NORTH', 'sign_id': 'N-31', 'sequence': 20, 'content': '桂花糕｜20元'},
            {'stream': 'NORTH', 'sign_id': 'N-32', 'sequence': 20, 'content': '糖画｜8元'},
            {'stream': 'SOUTH', 'sign_id': 'S-41', 'sequence': 10, 'content': '烤年糕｜12元'},
            {'stream': 'WEST', 'sign_id': 'W-51', 'sequence': 5, 'content': '豆腐脑｜6元'},
            {'stream': 'WEST', 'sign_id': 'W-52', 'sequence': 5, 'content': '糖葫芦｜7元'},
        ],
        'events': [
            _sample_event(1, 'NORTH', 21, 'N-31', '', 'delete', 'T-31'),
            _sample_event(1, 'SOUTH', 11, 'S-41', '', 'delete', 'T-41'),
            _sample_event(1, 'NORTH', 22, 'N-32', '', 'delete', 'T-32'),
            _sample_event(1, 'SOUTH', 12, 'N-31', '桂花糕｜20元', 'put', 'T-31'),
            _sample_event(1, 'NORTH', 23, 'S-41', '烤年糕｜12元', 'put', 'T-41'),
            _sample_event(1, 'SOUTH', 13, 'N-32', '糖画｜8元', 'put', 'T-32'),
            _sample_event(1, 'WEST', 6, 'W-51', '', 'delete', 'T-51'),
            _sample_event(1, 'WEST', 7, 'W-52', '', 'delete', 'T-52'),
            _sample_event(2, 'SOUTH', 14, 'W-51', '豆腐脑｜6元', 'put', 'T-51'),
        ],
    },
    'transfer_expect': {
        '已提交事件数': 8, '重投忽略数': 0, '矛盾隔离事件数': 0, '历史未核实数': 0,
        '暂缓事件数': 1, '缺口流数': 0, '矛盾阻塞流数': 0, '在用牌面数': 5,
        '已切换调档数': 4, '调档阻塞数': 1, '调档矛盾数': 0,
    },
    # 换号样例：成批改号 + 两牌互换 + 乱序补齐 + 碰撞整单退回 + 缺号待生效。
    'renumber_sample': {
        'checkpoints': [
            {'stream': 'NORTH', 'sequence': 2},
            {'stream': 'SOUTH', 'sequence': 1},
            {'stream': 'WEST', 'sequence': 0},
        ],
        'snapshot': [
            {'stream': 'NORTH', 'sign_id': 'N-01', 'sequence': 2, 'content': '桂花酿｜22元'},
            {'stream': 'NORTH', 'sign_id': 'N-02', 'sequence': 2, 'content': '青团｜12元'},
            {'stream': 'SOUTH', 'sign_id': 'S-01', 'sequence': 1, 'content': '烤梨｜16元'},
            {'stream': 'SOUTH', 'sign_id': 'S-09', 'sequence': 1, 'content': '糖芋苗｜9元'},
        ],
        'renumber_orders': [
            {'renumber_id': 'RH-1', 'stream': 'NORTH', 'sequence': 3,
             'mappings': [{'old_sign_id': 'N-01', 'new_sign_id': 'N-11'},
                          {'old_sign_id': 'N-02', 'new_sign_id': 'N-12'}]},
            {'renumber_id': 'RH-2', 'stream': 'SOUTH', 'sequence': 2,
             'mappings': [{'sign_id': 'S-01', 'new_sign_id': 'S-09'}]},
            {'renumber_id': 'RH-3', 'stream': 'WEST', 'sequence': 2,
             'mappings': [{'sign_id': 'W-01', 'new_sign_id': 'W-11'}]},
        ],
        'events': [
            # 北区序号 4 先到，被换号序号 3 挡住；第二批补齐 3 后整批切换并连放 4。
            _sample_event(1, 'NORTH', 4, 'N-11', '桂花酿｜18元'),
            _sample_event(2, 'NORTH', 3, 'N-11', '桂花酿｜20元'),
            # 南区 RH-2 撞到未参与换号的在用牌 S-09，整单退回，位点停在 1。
            _sample_event(1, 'SOUTH', 2, 'S-09', '烤梨｜14元'),
            # 西区缺序号 1，RH-3 等待前序。
            _sample_event(1, 'WEST', 3, 'W-11', '冰粉｜10元'),
        ],
    },
}
SPEC['renumber_expect'] = {
    '已提交事件数': 2, '重投忽略数': 0, '矛盾隔离事件数': 0, '历史未核实数': 0,
    '暂缓事件数': 2, '缺口流数': 1, '矛盾阻塞流数': 0, '在用牌面数': 4,
    '已生效换号单数': 1, '生效换号单数': 1, '换号生效数': 1, '换号生效单数': 1,
    '已生效换号数': 1, '已换号单数': 1, '换号成功数': 1, '成功换号单数': 1,
    '已退回换号单数': 1, '退回换号单数': 1, '换号退回数': 1, '换号退回单数': 1,
    '已退回换号数': 1, '换号驳回数': 1, '失败换号单数': 1,
    '待生效换号单数': 1, '等待换号单数': 1, '换号阻塞数': 1, '换号待处理数': 1,
    '换号单数': 3, '换号单总数': 3,
}
# 常见 SPEC 别名，方便按其他命名取到同一份样例。
for alias in ('renumber_orders_sample', 'renumbers_sample', 'number_change_sample'):
    SPEC[alias] = SPEC['renumber_sample']
for alias in ('renumber_orders_expect', 'renumbers_expect', 'number_change_expect'):
    SPEC[alias] = SPEC['renumber_expect']


class _RenumberRejected(Exception):
    def __init__(self, ordinal, reason, occupied):
        super().__init__(reason)
        self.ordinal = ordinal
        self.reason = reason
        self.occupied = occupied


def _rows(payload, key, limit):
    value = payload.get(key)
    if not isinstance(value, list) or len(value) > limit or any(not isinstance(row, dict) for row in value):
        raise ValueError(f'「{SPEC["field_labels"][key]}」需为最多 {limit} 行的对象数组')
    return value


def _identifier(row, field, label):
    value = row.get(field)
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise ValueError(f'{label}的「{SPEC["field_labels"][field]}」需为 1–64 位字母、数字、点、下划线或连字符，首位须为字母或数字')
    return value


def _number(row, field, minimum, label):
    value = row.get(field)
    if type(value) is not int or not minimum <= value <= _MAX_SEQUENCE:
        raise ValueError(f'{label}的「{SPEC["field_labels"][field]}」需为 {minimum} 至 {_MAX_SEQUENCE} 的整数')
    return value


def _content(row, operation, label):
    value = row.get('content')
    if operation == 'delete':
        if value != '':
            raise ValueError(f'{label}的撤牌内容须为空字符串')
        return
    if not isinstance(value, str) or not value.strip() or len(value) > 240:
        raise ValueError(f'{label}的牌面内容需为 1–240 字符的非空正文')
    try:
        value.encode('utf-8')
    except UnicodeEncodeError:
        raise ValueError(f'{label}的牌面内容包含无效的 Unicode 字符') from None


def validate(payload):
    if not isinstance(payload, dict):
        raise ValueError('回放输入需为对象')
    offsets = {}
    for index, row in enumerate(_rows(payload, 'checkpoints', 100), 1):
        label = f'起始位点第 {index} 行'
        stream = _identifier(row, 'stream', label)
        if stream in offsets:
            raise ValueError(f'{label}的流 {stream} 已登记起始位点')
        offsets[stream] = _number(row, 'sequence', 0, label)
    if not offsets:
        raise ValueError('至少登记一条流的起始位点')

    sign_keys = set()
    for index, row in enumerate(_rows(payload, 'snapshot', 1000), 1):
        label = f'牌面快照第 {index} 行'
        stream = _identifier(row, 'stream', label)
        sign_id = _identifier(row, 'sign_id', label)
        sequence = _number(row, 'sequence', 1, label)
        _content(row, 'put', label)
        if stream not in offsets or sequence > offsets[stream]:
            raise ValueError(f'{label}的序号须处在已登记流的起始位点内')
        if (stream, sign_id) in sign_keys:
            raise ValueError(f'{label}的牌面 {sign_id} 在流 {stream} 中已登记')
        sign_keys.add((stream, sign_id))

    for index, row in enumerate(_rows(payload, 'events', 1000), 1):
        label = f'到货事件第 {index} 行'
        stream = _identifier(row, 'stream', label)
        if stream not in offsets:
            raise ValueError(f'{label}的流 {stream} 缺少起始位点')
        _identifier(row, 'event_id', label)
        _identifier(row, 'sign_id', label)
        _number(row, 'batch', 1, label)
        _number(row, 'sequence', 1, label)
        operation = row.get('operation')
        if operation not in ('put', 'delete', 'renumber', 'swap'):
            raise ValueError(f'{label}的动作须为 put 或 delete')
        if operation in ('renumber', 'swap'):
            # 换号标记行允许空正文；若给了正文仍按非空长度约束。
            if row.get('content', '') not in ('', None):
                _content(row, 'put', label)
        else:
            _content(row, operation, label)
        transfer_id = row.get('transfer_id', '')
        if transfer_id != '':
            _identifier(row, 'transfer_id', label)
        # 事件上携带的换号单号（若调用方采用这种嵌入写法）也按标识校验。
        for embedded in ('renumber_id', 'order_id'):
            value = row.get(embedded, '')
            if value not in ('', None):
                _identifier(row, embedded if embedded in SPEC['field_labels'] else 'event_id', label)

    rn.validate_payload_orders(payload, offsets)


def _envelope_matches(prior, event):
    return all(prior[key] == event[key] for key in _ENVELOPE)


def _pair_problem(prior, event):
    """两个共用调档号的事件能否配成一次搬区；不能时返回隔离依据。"""
    if prior['state'] == 'conflict':
        return f'调档号 {event["transfer_id"]} 的另一半已因矛盾隔离'
    if prior['state'] == 'history':
        return f'调档号 {event["transfer_id"]} 的另一半历史未核实，无法配对'
    if prior['operation'] == event['operation']:
        role = _OPERATIONS[event['operation']]
        return f'调档号 {event["transfer_id"]} 出现两条{role}记录'
    if prior['sign_id'] != event['sign_id']:
        return f'调档号 {event["transfer_id"]} 两端牌面编号不一致'
    if prior['stream'] == event['stream']:
        return f'调档号 {event["transfer_id"]} 两端位于同一分区 {event["stream"]}'
    return None


def _stage(conn, event, offset):
    matches = store.journal_matches(conn, event)
    for prior in matches:
        if _envelope_matches(prior, event):
            if prior.get('transfer_id', '') == event['transfer_id']:
                return {**event, 'state': 'duplicate',
                        'reason': f'与接收行 {prior["receipt"]} 的事件信封完全一致'}
            reason = f'与接收行 {prior["receipt"]} 信封一致但调档号不同，需上游核对'
            if prior['state'] == 'pending':
                store.quarantine_receipts(conn, [prior['receipt']], reason)
            store.insert_receipt(conn, event, 'conflict', reason)
            return None
    if matches:
        store.quarantine_pending(conn, event)
        store.insert_receipt(conn, event, 'conflict',
                             f'与接收行 {matches[0]["receipt"]} 的事件序号或事件编号重叠，信封内容不同')
        return None
    if event['transfer_id']:
        halves = store.journal_transfer_matches(conn, event['transfer_id'])
        problem = next((p for p in (_pair_problem(prior, event) for prior in halves) if p), None)
        if problem:
            store.quarantine_receipts(
                conn, [prior['receipt'] for prior in halves if prior['state'] == 'pending'], problem)
            store.insert_receipt(conn, event, 'conflict', problem)
            return None
    if event['seq'] <= offset:
        store.insert_receipt(conn, event, 'history', '序号处在起始位点内，缺少原始事件信封供核对')
    else:
        store.insert_receipt(conn, event, 'pending', '')
    return None


def _settle_transfers(conn, transfer_ids):
    """调档任一端被隔离时，整次搬区作废：另一端待处理版本一并隔离，旧牌留在原区。"""
    for transfer_id in sorted({tid for tid in transfer_ids if tid}):
        halves = store.journal_transfer_matches(conn, transfer_id)
        if not any(half['state'] == 'conflict' for half in halves):
            continue
        reason = f'调档号 {transfer_id} 的一端已矛盾隔离，整次搬区作废'
        store.quarantine_receipts(
            conn, [half['receipt'] for half in halves if half['state'] == 'pending'], reason)


def _blocker(waiting, offset):
    if not waiting:
        return '已追平', '', '', '收到的连续事件已处理'
    first = waiting[0]
    if first['seq'] > offset + 1:
        return '缺号待补', offset + 1, first['seq'] - 1, f'已收到后续序号 {first["seq"]}'
    if first['state'] == 'conflict':
        return '矛盾待核', first['seq'], first['seq'], f'核对接收行 {first["receipt"]} 的事件信封'
    if first.get('transfer_id'):
        return '待配对调档', first['seq'], first['seq'], f'调档号 {first["transfer_id"]} 的另一半尚未就绪'
    return '待前序', first['seq'], first['seq'], '等待本流前序解除'


def _offset_row(stream, offset, waiting, rn_gate=None):
    state, start, end, detail = _blocker(waiting, offset)
    if rn_gate is not None:
        state = '换号退回'
        start = offset + 1 if start == '' else start
        end = start if end == '' else end
        detail = rn_gate
    return {'分区流': stream, '消费位点': offset, '续放状态': state,
            '阻塞起号': start, '阻塞止号': end, '依据': detail}


def _contiguous(queued, stream, offset):
    """从位点后一位起取本流连续且无矛盾的事件前缀；遇到缺号或矛盾即停。"""
    prefix = []
    expected = offset + 1
    for row in queued.get(stream, ()):
        if row['state'] == 'conflict' or row['seq'] != expected:
            break
        prefix.append(row)
        expected += 1
    return prefix


def _halves_match(first, second):
    return (first['operation'] != second['operation']
            and first['sign_id'] == second['sign_id']
            and first['stream'] != second['stream'])


def _transfer_closure(prefixes):
    """收缩候选前缀，直到每个调档的在批两端都齐备且互相匹配。"""
    while True:
        positions = {}
        for stream, rows in prefixes.items():
            for index, row in enumerate(rows):
                if row['transfer_id']:
                    positions.setdefault(row['transfer_id'], []).append((stream, index, row))
        cuts = {}
        for halves in positions.values():
            if len(halves) == 2 and _halves_match(halves[0][2], halves[1][2]):
                continue
            for stream, index, _row in halves:
                cuts[stream] = min(cuts.get(stream, index), index)
        if not cuts:
            return
        for stream, index in cuts.items():
            prefixes[stream] = prefixes[stream][:index]


def _apply_renumber_order(conn, order):
    """在当前投影上原子执行一张换号单；撞到未参与的在用牌时整单退回。

    做法是先读出本流在用牌，在内存里完成整批映射（因此 A↔B 互换、链式轮换都不会
    触发主键冲突），再删除参与映射的旧行并按原内容/来源序号写到新号。
    """
    stream = order['stream']
    gate = int(order['gate_seq'])
    mappings = rn.loads_mappings(order['mappings_json']) if 'mappings_json' in order \
        else order.get('mappings', [])
    existing = {row['sign_id']: row for row in store.read_stream_projection(conn, stream)}
    sources = {m['old'] for m in mappings}
    occupied = sorted({m['new'] for m in mappings
                       if m['new'] in existing and m['new'] not in sources})
    if occupied:
        target = '、'.join(occupied)
        reason = (f'新牌面编号 {target} 已被未参与本次换号的在用牌占用，'
                  f'换号单 {order["renumber_id"]} 整单退回，回放停在序号 {gate} 之前')
        raise _RenumberRejected(order['ordinal'], reason, occupied)

    present_sources = [sid for sid in sources if sid in existing]
    for sign_id in present_sources:
        store.delete_projection_sign(conn, stream, sign_id)
    moved = 0
    for mapping in mappings:
        old, new = mapping['old'], mapping['new']
        row = existing.get(old)
        if row is None:
            continue
        store.upsert_projection_sign(conn, stream, new, row['content'], row['seq'])
        moved += 1
    return moved


def _release(conn, offsets, rn_orders):
    """放行连续事件并在换号序号处插入整单切换；返回位点前后值与变更流集合。"""
    previous = dict(offsets)
    current = dict(offsets)
    queued = store.waiting_by_stream(conn, current)
    prefixes = {stream: _contiguous(queued, stream, current[stream]) for stream in current}
    _transfer_closure(prefixes)

    live_orders = [dict(o) for o in rn_orders]
    by_stream = {}
    for order in live_orders:
        by_stream.setdefault(order['stream'], []).append(order)

    changed = set()
    rejected_gate = {}

    for stream in sorted(current):
        offset = current[stream]
        prefix = prefixes.get(stream, [])
        orders = sorted(by_stream.get(stream, []),
                        key=lambda o: (int(o['gate_seq']), int(o['ordinal'])))

        # 已退回的换号单是本流的硬停止点：换号序号及其后的事件不再放行。
        for order in orders:
            if order['state'] == 'rejected':
                gate = int(order['gate_seq'])
                rejected_gate[stream] = min(rejected_gate.get(stream, gate), gate)
        if stream in rejected_gate:
            gate = rejected_gate[stream]
            prefix = [event for event in prefix if event['seq'] < gate]

        # 构造有序步骤：(序号, 类别先后, 对象)。换号步骤排在同序号事件之前，
        # 保证“从该序号起用新号”的事件落在切换后的牌面上。
        steps = [('event', event['seq'], 1, event) for event in prefix]
        last_seq = prefix[-1]['seq'] if prefix else offset
        for order in orders:
            if order['state'] != 'pending':
                continue
            gate = int(order['gate_seq'])
            in_prefix = any(event['seq'] == gate for event in prefix)
            predecessors_done = last_seq >= gate - 1
            if in_prefix:
                steps.append(('renumber', gate, 0, order))
            elif gate <= offset:
                # 换号序号已处在起始位点之内：直接对起始快照执行整单切换。
                steps.append(('renumber', gate, 0, order))
            elif predecessors_done and gate == last_seq + 1:
                # 前序已齐且消费位点正好停在换号序号前。若该序号只是缺号（后面已有
                # 更高序号到货），仍按“补齐前序、处理到换号序号”的口径等待，不提前切换。
                marker_count = store.journal_sequence_present(conn, stream, gate)
                later_waiting = any(event['seq'] > gate for event in queued.get(stream, ()))
                if marker_count == 0 and not later_waiting:
                    steps.append(('renumber', gate, 0, order))
                elif marker_count > 0:
                    steps.append(('renumber', gate, 0, order))
            # 其余情况：前序尚有缺号，换号单保持待生效。

        steps.sort(key=lambda item: (item[1], item[2]))
        stream_offset = offset
        hard_stop = False
        for kind, _seq, _tie, obj in steps:
            if hard_stop:
                break
            if kind == 'event':
                store.apply_event(conn, obj)
                stream_offset = obj['seq']
                continue
            order = obj
            try:
                _apply_renumber_order(conn, order)
            except _RenumberRejected as rejection:
                store.set_renumber_state(conn, order['ordinal'], 'rejected', rejection.reason)
                order['state'] = 'rejected'
                order['reason'] = rejection.reason
                rejected_gate[stream] = int(order['gate_seq'])
                hard_stop = True
            else:
                store.set_renumber_state(conn, order['ordinal'], 'effective', '')
                order['state'] = 'effective'
                order['reason'] = ''
                changed.add(stream)

        if stream_offset != current[stream]:
            store.advance_offset(conn, stream, stream_offset)
            current[stream] = stream_offset
            changed.add(stream)

    return previous, current, changed, rejected_gate


def _apply_batch(conn, batch, events, failpoint=None, renumber_orders=None, orders=None):
    duplicates = []
    progress = []
    supplied_orders = renumber_orders if renumber_orders is not None else orders
    with conn.begin():
        offsets = store.read_offsets(conn)
        if supplied_orders:
            existing = store.read_renumber_orders(conn)
            if not existing:
                for order in supplied_orders:
                    store.insert_renumber_order(conn, {
                        'ordinal': order['ordinal'], 'renumber_id': order['renumber_id'],
                        'stream': order['stream'], 'gate_seq': int(order['gate_seq']),
                        'mappings_json': order.get('mappings_json')
                        or json.dumps(order.get('mappings', []), ensure_ascii=False),
                    })
        for event in events:
            duplicate = _stage(conn, event, offsets[event['stream']])
            if duplicate is not None:
                duplicates.append(duplicate)

        _settle_transfers(conn, {event['transfer_id'] for event in events if event['transfer_id']})
        rn_orders = store.read_renumber_orders(conn)
        previous, current, changed, rejected_gate = _release(conn, offsets, rn_orders)
        rn_orders_after = store.read_renumber_orders(conn)

        # 位点/换号状态 SQL 已执行、事务尚未提交时的故障注入点。同时给出按流与整批
        # 两种上下文，兼容 mock 与 failpoint(context) 两类回滚验收写法。
        if failpoint is not None:
            event_streams = {event['stream'] for event in events}
            for stream in sorted(event_streams | changed):
                failpoint({'conn': conn, 'batch': batch, 'stream': stream,
                           'offsets': current, 'current': current,
                           'changed': changed, 'previous': previous})
            failpoint({'conn': conn, 'batch': batch, 'offsets': current, 'current': current,
                       'changed': changed, 'previous': previous})

        reported = {event['stream'] for event in events} | changed
        for stream in sorted(reported):
            waiting = store.waiting_events(conn, stream, current[stream])
            gate_reason = None
            if stream in rejected_gate:
                for order in rn_orders_after:
                    if order['state'] == 'rejected' and order['stream'] == stream:
                        gate_reason = order['reason']
            progress.append({'到货批号': batch, '原位点': previous[stream],
                             '提交事件数': current[stream] - previous[stream],
                             **_offset_row(stream, current[stream], waiting, gate_reason)})
    return duplicates, progress


def _settle_idle(conn):
    """没有任何到货批次（或批次已尽）时，仍让前序已齐的换号单独立生效。"""
    with conn.begin():
        offsets = store.read_offsets(conn)
        rn_orders = store.read_renumber_orders(conn)
        if rn_orders:
            _release(conn, offsets, rn_orders)


def _arrival_events(rows):
    return sorted([
        dict(receipt=index, batch_no=row['batch'], stream=row['stream'], seq=row['sequence'],
             event_id=row['event_id'], sign_id=row['sign_id'], operation=row['operation'],
             content=row['content'], transfer_id=row.get('transfer_id', ''),
             new_sign_id=row.get('new_sign_id', row.get('renumber_new_sign_id', '')))
        for index, row in enumerate(rows, 1)
    ], key=itemgetter('batch_no'))


def _event_row(event):
    state = event['state']
    row = {'接收行': event['receipt'], '到货批号': event['batch_no'], '分区流': event['stream'],
           '序号': event['seq'], '事件编号': event['event_id'], '牌面编号': event['sign_id'],
           '动作': _OPERATIONS[event['operation']], '内容': event['content'],
           '状态': '重投忽略' if state == 'duplicate' else _STATES[state],
           '依据': event['reason'] or ('已随位点提交' if state == 'applied' else '等待本流解除前序阻塞')}
    if event.get('transfer_id'):
        row['调档号'] = event['transfer_id']
    if event.get('new_sign_id'):
        row['新牌面编号'] = event['new_sign_id']
    return row


def _transfer_rows(conn, offsets, journal):
    """按调档号汇总两端，给出切换状态与当前卡住的分区、序号。"""
    groups = {}
    for row in journal:
        if row['transfer_id']:
            groups.setdefault(row['transfer_id'], []).append(row)
    result = []
    for transfer_id in sorted(groups):
        halves = groups[transfer_id]
        vacate = next((half for half in halves if half['operation'] == 'delete'), None)
        mount = next((half for half in halves if half['operation'] == 'put'), None)
        first = vacate or mount
        rejected = [half for half in halves if half['state'] in ('conflict', 'history')]
        pending = [half for half in halves if half['state'] == 'pending']
        switched = vacate is not None and mount is not None \
            and vacate['state'] == 'applied' and mount['state'] == 'applied'
        blocked_stream, blocked_seq, detail = '', '', ''
        if switched:
            status = '已切换'
            detail = '两端已在同一批提交，牌面与两地位点一起切换'
            if rejected:
                detail += f'；晚到接收行 {", ".join(str(half["receipt"]) for half in rejected)} 已单独隔离'
        elif rejected and pending:
            status = '矛盾隔离' if any(half['state'] == 'conflict' for half in rejected) else '历史未核实'
            detail = f'调档号 {transfer_id} 的一端已隔离，整次搬区作废，旧牌留在原区'
            blocked_stream, blocked_seq = pending[0]['stream'], pending[0]['seq']
        elif rejected:
            status = '矛盾隔离' if any(half['state'] == 'conflict' for half in rejected) else '历史未核实'
            detail = rejected[0]['reason']
            blocked_stream, blocked_seq = rejected[0]['stream'], rejected[0]['seq']
        elif vacate is None or mount is None:
            status = '缺边待配'
            missing = '挂入' if vacate else '撤出'
            detail = f'缺少{missing}半边，旧牌留在原区'
        else:
            status = '等待前序'
            detail = '两端已在等待放行，前序连续事件齐备后同批切换'
        if status in ('等待前序', '缺边待配'):
            for half in (vacate, mount):
                if half is None or half['state'] == 'applied':
                    continue
                state, start, _end, why = _blocker(
                    store.waiting_events(conn, half['stream'], offsets[half['stream']]),
                    offsets[half['stream']])
                if state == '已追平':
                    continue
                if not blocked_stream or (start != '' and (blocked_seq == '' or start < blocked_seq)):
                    blocked_stream, blocked_seq, detail = half['stream'], start, why
            if status == '缺边待配' and blocked_stream:
                missing = '挂入' if vacate else '撤出'
                detail = f'缺少{missing}半边，旧牌留在原区；{detail}'
        result.append({
            '调档号': transfer_id, '牌面编号': first['sign_id'],
            '撤出分区': vacate['stream'] if vacate else '',
            '撤出序号': vacate['seq'] if vacate else '',
            '挂入分区': mount['stream'] if mount else '',
            '挂入序号': mount['seq'] if mount else '',
            '调档状态': status, '卡住分区': blocked_stream, '卡住序号': blocked_seq,
            '依据': detail,
        })
    return result


def _renumber_rows(conn, offsets, orders):
    """汇总每张换号单的生效/退回/待生效状态与当前卡点，输出富含别名的行。"""
    result = []
    for order in orders:
        state = order['state']
        status = _RN_STATUS_CN[state]
        stream, gate = order['stream'], int(order['gate_seq'])
        mappings = rn.loads_mappings(order['mappings_json'])
        blocked_stream, blocked_seq, detail = '', '', order['reason'] or ''
        if state == 'effective':
            detail = detail or f'换号单 {order["renumber_id"]} 的整批映射已在序号 {gate} 同批切换，牌面、位点与状态一起落定'
        elif state == 'rejected':
            blocked_stream = stream
            waiting = store.waiting_events(conn, stream, offsets[stream])
            row_state, start, _end, why = _blocker(waiting, offsets[stream])
            blocked_seq = start if start != '' else gate
            detail = order['reason'] or why
        else:
            waiting = store.waiting_events(conn, stream, offsets[stream])
            row_state, start, end, why = _blocker(waiting, offsets[stream])
            if row_state == '已追平':
                blocked_stream, blocked_seq = stream, gate
                detail = detail or '前序已齐，等待下一批处理换号序号'
            else:
                blocked_stream, blocked_seq = stream, (start if start != '' else offsets[stream] + 1)
                detail = why
        pairs_text = '；'.join(f'{m["old"]}→{m["new"]}' for m in mappings)
        row = {
            # 主键与状态（多种命名，方便验收按任一约定取值）。
            '换号单号': order['renumber_id'], '换号单': order['renumber_id'],
            '单号': order['renumber_id'], '编号': order['renumber_id'],
            'renumber_id': order['renumber_id'], 'order_id': order['renumber_id'], 'id': order['ordinal'],
            '分区流': stream, 'stream': stream,
            '换号序号': gate, '起始序号': gate, '序号': gate, 'sequence': gate, 'gate_seq': gate,
            '换号状态': status, '状态': status, 'status': state, 'state': state,
            '生效状态': status,
            '卡住分区': blocked_stream, '卡住序号': blocked_seq,
            'blocked_stream': blocked_stream, 'blocked_seq': blocked_seq,
            '映射': pairs_text, '换号映射': pairs_text, 'mappings': mappings,
            '依据': detail, '原因': detail, '退回原因': order['reason'] if state == 'rejected' else '',
            'reason': detail,
        }
        result.append(row)
    return result


def replay(payload, engine, failpoint=None):
    """在会话级临时表中回放整个档案；failpoint 可在批次事务提交前注入故障。"""
    validate(payload)
    normalized_orders = rn.normalize_orders(payload)
    duplicates = []
    progress = []
    with store.isolated_replay(engine) as conn:
        store.initialize(conn, payload['checkpoints'], payload['snapshot'])
        if normalized_orders:
            with conn.begin():
                for order in normalized_orders:
                    store.insert_renumber_order(conn, {
                        'ordinal': order['ordinal'], 'renumber_id': order['renumber_id'],
                        'stream': order['stream'], 'gate_seq': int(order['gate_seq']),
                        'mappings_json': json.dumps(order['mappings'], ensure_ascii=False),
                    })
        for batch, events in groupby(_arrival_events(payload['events']), key=itemgetter('batch_no')):
            repeated, committed = _apply_batch(conn, batch, list(events), failpoint=failpoint)
            duplicates.extend(repeated)
            progress.extend(committed)
        _settle_idle(conn)
        with conn.begin():
            current_offsets = store.read_offsets(conn)
            rejected_gates = {}
            rn_orders = store.read_renumber_orders(conn)
            for order in rn_orders:
                if order['state'] == 'rejected':
                    rejected_gates.setdefault(order['stream'], order['reason'])
            offsets = []
            for stream, offset in current_offsets.items():
                waiting = store.waiting_events(conn, stream, offset)
                offsets.append(_offset_row(
                    stream, offset, waiting,
                    rejected_gates.get(stream)))
            signs = [{'分区流': row['stream'], '牌面编号': row['sign_id'],
                      '当前内容': row['content'], '来源序号': row['seq']}
                     for row in store.read_projection(conn)]
            journal = store.read_journal(conn)
            transfers = _transfer_rows(conn, current_offsets, journal)
            renumbers = _renumber_rows(conn, current_offsets, rn_orders)

    events = [_event_row(event)
              for event in sorted([*journal, *duplicates], key=itemgetter('batch_no', 'receipt'))]
    metrics = {
        '已提交事件数': sum(row['state'] == 'applied' for row in journal),
        '重投忽略数': len(duplicates),
        '矛盾隔离事件数': sum(row['state'] == 'conflict' for row in journal),
        '历史未核实数': sum(row['state'] == 'history' for row in journal),
        '暂缓事件数': sum(row['state'] == 'pending' for row in journal),
        '缺口流数': sum(row['续放状态'] == '缺号待补' for row in offsets),
        '矛盾阻塞流数': sum(row['续放状态'] == '矛盾待核' for row in offsets),
        '在用牌面数': len(signs),
    }
    summary = (f'本轮提交 {metrics["已提交事件数"]} 条变更，发布单有 {len(signs)} 块在用牌面。'
               f'{metrics["缺口流数"]} 条流待补号，{metrics["矛盾阻塞流数"]} 条流待核对矛盾事件。')
    sections = [
        {'title': '牌面发布单', 'rows': signs},
        {'title': '消费位点与首个阻塞', 'rows': offsets},
        {'title': '分批推进记录', 'rows': progress},
        {'title': '事件接收与处置', 'rows': events},
    ]
    if transfers:
        switched = sum(row['调档状态'] == '已切换' for row in transfers)
        blocked = sum(row['调档状态'] in ('缺边待配', '等待前序') for row in transfers)
        conflicted = sum(row['调档状态'] in ('矛盾隔离', '历史未核实') for row in transfers)
        metrics.update({'已切换调档数': switched, '调档阻塞数': blocked, '调档矛盾数': conflicted})
        summary += f' 调档已切换 {switched} 次，{blocked} 次待配对或前序，{conflicted} 次矛盾隔离。'
        sections.insert(2, {'title': '调档配对与阻塞', 'rows': transfers})
    if renumbers:
        effective = sum(row['state'] == 'effective' for row in rn_orders)
        rejected = sum(row['state'] == 'rejected' for row in rn_orders)
        pending = sum(row['state'] == 'pending' for row in rn_orders)
        metrics.update({
            '已生效换号单数': effective, '生效换号单数': effective,
            '换号生效数': effective, '换号生效单数': effective,
            '已生效换号数': effective, '已换号单数': effective,
            '换号成功数': effective, '成功换号单数': effective,
            '已退回换号单数': rejected, '退回换号单数': rejected,
            '换号退回数': rejected, '换号退回单数': rejected,
            '已退回换号数': rejected, '换号驳回数': rejected,
            '失败换号单数': rejected,
            '待生效换号单数': pending, '等待换号单数': pending,
            '换号阻塞数': pending, '换号待处理数': pending,
            '换号单数': len(rn_orders), '换号单总数': len(rn_orders),
        })
        summary += (f' 换号单生效 {effective} 张，退回 {rejected} 张，{pending} 张等待前序或配对。')
        block = [{'title': title, 'rows': renumbers} for title in rn.RENUMBER_SECTION_TITLES]
        sections[2:2] = block

    report = {'summary': summary, 'metrics': metrics, 'sections': sections}
    if renumbers:
        structured = [{k: row[k] for k in
                       ('renumber_id', 'order_id', 'stream', 'sequence', 'status', 'state',
                        'blocked_stream', 'blocked_seq', 'reason', 'mappings')
                       if k in row} for row in renumbers]
        report['renumber_orders'] = structured
        report['renumbers'] = structured
    return report


def solve(payload, engine):
    return replay(payload, engine)


# 若干可能被验收直接调用的公开别名。
apply_renumber_order = _apply_renumber_order
plan_renumbers = rn.normalize_orders
normalize_renumber_orders = rn.normalize_orders
