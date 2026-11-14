"""Frozen supervision observations and the latched, durable stop intent."""
from datetime import datetime
from sqlalchemy import Boolean, DateTime, ForeignKey, JSON, String, select
from sqlalchemy.orm import Mapped, mapped_column
from .db import Base
from .models import new_id


class RuntimeSupervision(Base):
    __tablename__ = 'runtime_supervisions'
    command_id: Mapped[str] = mapped_column(ForeignKey('managed_commands.id'), primary_key=True)
    contract: Mapped[dict] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(String(32), default='awaiting_metrics')
    publication_veto: Mapped[bool] = mapped_column(Boolean, default=True)
    reasons: Mapped[list] = mapped_column(JSON, default=list)
    current_metrics: Mapped[dict] = mapped_column(JSON, default=dict)
    last_trusted: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    stop_request: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class RuntimeObservation(Base):
    __tablename__ = 'runtime_observations'
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    command_id: Mapped[str] = mapped_column(ForeignKey('managed_commands.id'), index=True)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    source: Mapped[str | None] = mapped_column(String(128), nullable=True)
    sample: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    decision: Mapped[dict] = mapped_column(JSON)


def supervision_view(db, command_id):
    row = db.get(RuntimeSupervision, command_id)
    if row is None:
        return None
    observation = db.scalar(select(RuntimeObservation).where(RuntimeObservation.command_id == command_id)
                            .order_by(RuntimeObservation.received_at.desc()).limit(1))
    return {**{key: getattr(row, key) for key in ('contract', 'status', 'publication_veto', 'reasons',
             'current_metrics', 'stop_request', 'updated_at')},
            'latest_observation': {'id': observation.id, 'received_at': observation.received_at,
                'source': observation.source, 'sample': observation.sample, 'decision': observation.decision} if observation else None,
            'termination': '终止事实见独立控制记录；停止 ACK 仅表示接收，目标只在独立清理确认后释放'}


def request_stop(db, run, row, reasons, now):
    """Latch a stop under the tenant lock; dispatch happens after commit."""
    from .execution_runtime import record
    row.publication_veto = True
    row.reasons = list(dict.fromkeys([*row.reasons, *reasons]))
    row.status = 'stop_requested'
    run.status = 'stop_requested'
    if row.stop_request is None:
        row.stop_request = {'id': new_id(), 'detected_at': now.isoformat(), 'requested_at': now.isoformat(),
                            'reasons': row.reasons, 'detection_metrics': dict(row.current_metrics or {}),
                            'delivery_status': 'pending', 'termination_confirmed': False}
        record(db, run, 'supervision.stop_requested', {'command_id': row.command_id, **row.stop_request,
            'publication_veto': True, 'ownership_retained': True})
