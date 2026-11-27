"""Connection-scoped InnoDB workspace for one replay."""

from contextlib import contextmanager

from sqlalchemy import bindparam, text


_TABLES = {
    'marquee_cdc_offsets': '''
        stream VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
        seq BIGINT UNSIGNED NOT NULL
    ''',
    'marquee_cdc_projection': '''
        stream VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
        sign_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
        content VARCHAR(240) NOT NULL,
        seq BIGINT UNSIGNED NOT NULL,
        PRIMARY KEY (stream, sign_id)
    ''',
    'marquee_cdc_journal': '''
        receipt INT UNSIGNED PRIMARY KEY,
        batch_no BIGINT UNSIGNED NOT NULL,
        stream VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
        seq BIGINT UNSIGNED NOT NULL,
        event_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
        sign_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
        operation ENUM('put', 'delete', 'renumber', 'swap') NOT NULL,
        content VARCHAR(240) NOT NULL,
        transfer_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
        new_sign_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
        state ENUM('pending', 'applied', 'conflict', 'history') NOT NULL,
        reason VARCHAR(240) NOT NULL,
        KEY stream_sequence (stream, seq),
        KEY stream_event (stream, event_id),
        KEY transfer_lookup (transfer_id)
    ''',
    'marquee_cdc_renumber_orders': '''
        ordinal INT PRIMARY KEY,
        renumber_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
        stream VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
        gate_seq BIGINT UNSIGNED NOT NULL,
        mappings_json TEXT NOT NULL,
        state ENUM('pending', 'effective', 'rejected') NOT NULL DEFAULT 'pending',
        reason VARCHAR(480) NOT NULL DEFAULT '',
        KEY stream_gate (stream, gate_seq)
    ''',
}


@contextmanager
def isolated_replay(engine):
    if engine.dialect.name != 'mysql':
        raise ValueError('牌面回放需要 MySQL 8.0')
    with engine.connect().execution_options(isolation_level='READ COMMITTED') as conn:
        try:
            with conn.begin():
                for table, columns in _TABLES.items():
                    conn.exec_driver_sql(
                        f'CREATE TEMPORARY TABLE {table} ({columns}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4'
                    )
            yield conn
        finally:
            if not conn.invalidated:
                try:
                    conn.rollback()
                    with conn.begin():
                        conn.exec_driver_sql(f'DROP TEMPORARY TABLE IF EXISTS {", ".join(_TABLES)}')
                except Exception:
                    conn.invalidate()
                    raise


def initialize(conn, checkpoints, snapshot):
    with conn.begin():
        conn.execute(text('INSERT INTO marquee_cdc_offsets (stream, seq) VALUES (:stream, :sequence)'), checkpoints)
        if snapshot:
            conn.execute(text('''
                INSERT INTO marquee_cdc_projection (stream, sign_id, content, seq)
                VALUES (:stream, :sign_id, :content, :sequence)
            '''), snapshot)


def journal_matches(conn, event):
    return conn.execute(text('''
        SELECT receipt, seq, event_id, sign_id, operation, content, transfer_id, new_sign_id, state
        FROM marquee_cdc_journal
        WHERE stream = :stream AND (seq = :seq OR event_id = :event_id)
        ORDER BY receipt
    '''), event).mappings().all()


def insert_receipt(conn, event, state, reason):
    conn.execute(text('''
        INSERT INTO marquee_cdc_journal
            (receipt, batch_no, stream, seq, event_id, sign_id, operation, content,
             transfer_id, new_sign_id, state, reason)
        VALUES (:receipt, :batch_no, :stream, :seq, :event_id, :sign_id, :operation, :content,
                :transfer_id, :new_sign_id, :state, :reason)
    '''), {
        'transfer_id': '', 'new_sign_id': '', **event,
        'state': state, 'reason': reason,
    })


def quarantine_pending(conn, event):
    conn.execute(text('''
        UPDATE marquee_cdc_journal SET state = 'conflict', reason = :reason
        WHERE stream = :stream AND state = 'pending' AND (seq = :seq OR event_id = :event_id)
    '''), {**event, 'reason': f'与接收行 {event["receipt"]} 的事件序号或事件编号重叠，信封内容不同'})


def quarantine_receipts(conn, receipts, reason):
    if not receipts:
        return
    conn.execute(text('''
        UPDATE marquee_cdc_journal SET state = 'conflict', reason = :reason
        WHERE state = 'pending' AND receipt IN :receipts
    ''').bindparams(bindparam('receipts', expanding=True)),
        {'receipts': list(receipts), 'reason': reason})


def waiting_events(conn, stream, offset):
    return conn.execute(text('''
        SELECT receipt, stream, seq, sign_id, operation, content, transfer_id, new_sign_id, state
        FROM marquee_cdc_journal
        WHERE stream = :stream AND seq > :offset AND state IN ('pending', 'conflict')
        ORDER BY seq, receipt
    '''), {'stream': stream, 'offset': offset}).mappings().all()


def waiting_by_stream(conn, offsets):
    """一次读出全部流的待处理与矛盾事件，按流分组，保持序号与接收行顺序。"""
    rows = conn.execute(text('''
        SELECT receipt, stream, seq, sign_id, operation, content, transfer_id, new_sign_id, state
        FROM marquee_cdc_journal
        WHERE state IN ('pending', 'conflict')
        ORDER BY stream, seq, receipt
    ''')).mappings().all()
    grouped = {stream: [] for stream in offsets}
    for row in rows:
        if row['seq'] > offsets[row['stream']]:
            grouped[row['stream']].append(row)
    return grouped


def journal_transfer_matches(conn, transfer_id):
    return conn.execute(text('''
        SELECT receipt, stream, seq, sign_id, operation, content, transfer_id, state, reason
        FROM marquee_cdc_journal
        WHERE transfer_id = :transfer_id
        ORDER BY receipt
    '''), {'transfer_id': transfer_id}).mappings().all()


def journal_sequence_present(conn, stream, seq):
    """该流该序号是否已登记任意版本的事件信封。"""
    return conn.execute(text('''
        SELECT COUNT(*) FROM marquee_cdc_journal WHERE stream = :stream AND seq = :seq
    '''), {'stream': stream, 'seq': seq}).scalar_one()


def apply_event(conn, event):
    if event['operation'] == 'delete':
        conn.execute(text('''
            DELETE FROM marquee_cdc_projection WHERE stream = :stream AND sign_id = :sign_id
        '''), event)
    elif event['operation'] in ('renumber', 'swap'):
        # 换号标记行本身不写牌面：整批映射由换号单步骤在同一事务内完成。
        pass
    else:
        conn.execute(text('''
            INSERT INTO marquee_cdc_projection (stream, sign_id, content, seq)
            VALUES (:stream, :sign_id, :content, :seq)
            ON DUPLICATE KEY UPDATE content = :content, seq = :seq
        '''), event)
    conn.execute(text('''
        UPDATE marquee_cdc_journal SET state = 'applied' WHERE receipt = :receipt
    '''), event)


def read_offsets(conn):
    return dict(conn.execute(text('SELECT stream, seq FROM marquee_cdc_offsets ORDER BY stream')).all())


def advance_offset(conn, stream, seq):
    conn.execute(text('UPDATE marquee_cdc_offsets SET seq = :seq WHERE stream = :stream'),
                 {'stream': stream, 'seq': seq})


def read_projection(conn):
    return conn.execute(text('''
        SELECT stream, sign_id, content, seq FROM marquee_cdc_projection ORDER BY stream, sign_id
    ''')).mappings().all()


def read_stream_projection(conn, stream):
    return conn.execute(text('''
        SELECT stream, sign_id, content, seq FROM marquee_cdc_projection
        WHERE stream = :stream ORDER BY sign_id
    '''), {'stream': stream}).mappings().all()


def delete_projection_sign(conn, stream, sign_id):
    conn.execute(text('DELETE FROM marquee_cdc_projection WHERE stream = :stream AND sign_id = :sign_id'),
                 {'stream': stream, 'sign_id': sign_id})


def upsert_projection_sign(conn, stream, sign_id, content, seq):
    conn.execute(text('''
        INSERT INTO marquee_cdc_projection (stream, sign_id, content, seq)
        VALUES (:stream, :sign_id, :content, :seq)
        ON DUPLICATE KEY UPDATE content = :content, seq = :seq
    '''), {'stream': stream, 'sign_id': sign_id, 'content': content, 'seq': seq})


def read_journal(conn):
    return conn.execute(text('SELECT * FROM marquee_cdc_journal ORDER BY receipt')).mappings().all()


# ---------------------------------------------------------------------------
# 换号单（同区成批换号 / 两块牌互换）的会话级状态
# ---------------------------------------------------------------------------

def insert_renumber_order(conn, order):
    conn.execute(text('''
        INSERT INTO marquee_cdc_renumber_orders
            (ordinal, renumber_id, stream, gate_seq, mappings_json, state, reason)
        VALUES (:ordinal, :renumber_id, :stream, :gate_seq, :mappings_json, 'pending', '')
    '''), {**order})


def read_renumber_orders(conn):
    return conn.execute(text('''
        SELECT ordinal, renumber_id, stream, gate_seq, mappings_json, state, reason
        FROM marquee_cdc_renumber_orders ORDER BY ordinal
    ''')).mappings().all()


def set_renumber_state(conn, ordinal, state, reason=''):
    conn.execute(text('''
        UPDATE marquee_cdc_renumber_orders SET state = :state, reason = :reason
        WHERE ordinal = :ordinal
    '''), {'ordinal': ordinal, 'state': state, 'reason': reason[:480]})
