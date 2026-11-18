"""窗口配置领域服务：定义流式计算的时间窗口策略。

数据流角色: stream_source → window_config（消费者+生产者）→ stream_job（消费者）
增强能力: 窗口参数校验、重叠检测、策略绑定、水位推算、消费上游事件批次

序号32修复：窗口指标数据必须绑定当前生效策略版本。
- 同名策略按版本共存，当前生效版本 = 已生效记录中版本号最大的一版（冲突时高版本优先，
  并发操作只留一个当前版本）。
- 消费上游批次时读取当前生效版本的水位延迟生成窗口，窗口指标记录写入 policy_version
  绑定；策略变更前已发布的指标保持旧口径可追溯，不按新口径静默重写。
- 水位只依据当前版本窗口边界计算，不再遍历旧记录凑结果。
- 消费写入按 (策略, 版本, 批次) 幂等：并发重试只保留一版；写入失败事务回滚保留原状态。
- 状态按 草稿→已生效→已停用 单向生命周期流转，非法流转拒绝且保留原状态。
"""
from __future__ import annotations

import copy
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class WindowConfigService(BaseModuleService):
    MODULE = "window_config"
    ENTITY = "窗口策略"
    LIST_FIELDS = ["策略名称", "窗口类型", "窗口大小", "滑动步长", "水位延迟", "触发策略", "延迟处理"]
    REQUIRED_FIELDS = ["策略名称", "窗口类型", "窗口大小"]
    STATUS_ORDER = ["草稿", "已生效", "已停用"]
    ACTION_RULES = {"启用策略": "草稿", "停用策略": "已生效"}
    NEGATIVE_ACTIONS = ["停用策略"]

    VALID_WINDOW_TYPES = {"tumbling", "sliding", "session", "global"}

    def __init__(self) -> None:
        self._normalize_policy_versions()

    # ---- 策略版本管理（序号32） ----

    @staticmethod
    def _is_metric_row(row: dict[str, Any]) -> bool:
        """窗口指标数据行（消费产物）与策略定义行同表存储，靠 window_definitions 区分。"""
        return isinstance(row.get("window_definitions"), list)

    def _policy_rows(self, policy_name: str) -> list[dict[str, Any]]:
        """同名策略定义行（不含窗口指标数据行）。"""
        return [
            r for r in store.rows(self.MODULE)
            if not self._is_metric_row(r) and r.get("策略名称") == policy_name
        ]

    @staticmethod
    def policy_version_of(row: dict[str, Any]) -> int:
        """读取记录绑定的策略版本；历史缺省记录按 v1 处理。"""
        try:
            return int(row.get("policy_version", 1) or 1)
        except (ValueError, TypeError):
            return 1

    def _normalize_policy_versions(self) -> None:
        """为历史记录补齐版本号：同名策略按出现顺序从 1 递增，窗口指标缺省绑定 v1。"""
        seen: dict[str, int] = {}
        for row in store.rows(self.MODULE):
            if self._is_metric_row(row):
                row.setdefault("policy_version", 1)
                continue
            if "policy_version" not in row:
                name = row.get("策略名称", "")
                seen[name] = seen.get(name, 0) + 1
                row["policy_version"] = seen[name]

    def current_effective_policy(self, policy_name: str) -> dict[str, Any] | None:
        """当前生效策略版本：已生效记录中版本号最大的一版。

        策略切换与窗口发布同时发生时，以该版本为唯一当前结果；
        并发启用/重试天然收敛到同一版本，只留一版。
        """
        candidates = [
            row for row in self._policy_rows(policy_name)
            if row.get("status") == "已生效"
        ]
        if not candidates:
            return None
        return max(candidates, key=self.policy_version_of)

    def window_metrics_for(
        self, policy_name: str, policy_version: int | None = None,
    ) -> list[dict[str, Any]]:
        """读取指定策略的窗口指标数据；给定版本时只取绑定该版本的记录。"""
        rows = [
            r for r in store.rows(self.MODULE)
            if self._is_metric_row(r) and r.get("策略名称") == policy_name
        ]
        if policy_version is not None:
            rows = [r for r in rows if self.policy_version_of(r) == policy_version]
        return rows

    @staticmethod
    def _allowed_lateness(policy: dict[str, Any]) -> float:
        try:
            return float(policy.get("水位延迟", "10"))
        except (ValueError, TypeError):
            return 10.0

    def _metric_allowed_lateness(
        self, metric_row: dict[str, Any], policy: dict[str, Any] | None = None,
    ) -> float:
        """窗口指标按生成时绑定版本的口径计；快照缺失时回退到策略当前值。"""
        raw = metric_row.get("allowed_lateness", metric_row.get("水位延迟"))
        try:
            return float(raw)
        except (ValueError, TypeError):
            return self._allowed_lateness(policy or {})

    # ---- 登记与单向生命周期 ----

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """登记策略：同名策略自动递增版本号，版本由系统分配而非用户输入。"""
        entry, missing = super().create_entry(values)
        if entry is not None:
            siblings = [
                self.policy_version_of(r)
                for r in self._policy_rows(entry.get("策略名称", ""))
                if r is not entry
            ]
            entry["policy_version"] = (max(siblings) + 1) if siblings else 1
        return entry, missing

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """单向生命周期流转：草稿→已生效→已停用。

        非法流转直接拒绝且不改动任何状态（事务回滚保留原状态）；
        策略停用即归档，归档和恢复条件同步写入绑定的窗口指标数据。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        source = self.ACTION_RULES.get(action)
        if source is None:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"
        current = entry.get("status")
        if current != source:
            return None, (
                f"{self.ENTITY}当前状态「{current}」不允许执行「{action}」，"
                f"需处于「{source}」状态"
            )
        idx = self.STATUS_ORDER.index(source)
        target = self.STATUS_ORDER[min(idx + 1, len(self.STATUS_ORDER) - 1)]
        entry["status"] = target
        entry["pending"] = target != self.STATUS_ORDER[-1]
        entry["abnormal"] = action in self.NEGATIVE_ACTIONS
        if target == self.STATUS_ORDER[-1] and not self._is_metric_row(entry):
            # 32号归档和恢复条件写入窗口指标数据，旧口径窗口保持可追溯
            version = self.policy_version_of(entry)
            for metric in self.window_metrics_for(entry.get("策略名称", ""), version):
                metric["archived"] = True
                metric["archive_condition"] = "策略停用即归档，窗口指标保持旧口径可追溯"
                metric["recovery_condition"] = "恢复需登记同名新版本策略并启用，归档指标不按新口径回写"
        return entry, f"{self.ENTITY}已{action}"

    # ---- 跨模块数据消费与生产 ----

    def validate_window_params(
        self, window_type: str, window_size: str, slide_step: str | None = None,
    ) -> dict[str, Any]:
        """校验窗口参数合法性：类型、大小范围、步长≤窗口大小。"""
        issues = []
        wtype = window_type.lower()
        if wtype not in self.VALID_WINDOW_TYPES:
            issues.append(f"窗口类型「{window_type}」无效，支持: {self.VALID_WINDOW_TYPES}")
        try:
            wsize_sec = float(window_size)
            if wsize_sec <= 0:
                issues.append("窗口大小必须 > 0")
            if wsize_sec > 86400:
                issues.append("窗口大小不能超过 24 小时")
        except (ValueError, TypeError):
            issues.append("窗口大小必须为数字")
        if slide_step is not None and wtype in ("sliding",):
            try:
                step_sec = float(slide_step)
                if step_sec <= 0:
                    issues.append("滑动步长必须 > 0")
                elif step_sec > float(window_size):
                    issues.append("滑动步长不能超过窗口大小")
            except (ValueError, TypeError):
                issues.append("滑动步长必须为数字")
        return {"ok": len(issues) == 0, "type": window_type, "size": window_size, "issues": issues}

    def detect_overlapping_policies(
        self, new_policy: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """检测新窗口策略是否与已有策略重叠或冲突（只比对策略定义行）。"""
        new_type = new_policy.get("窗口类型", "")
        new_size = new_policy.get("窗口大小", "")
        overlaps = []
        for r in store.rows(self.MODULE):
            if self._is_metric_row(r):
                continue
            if r.get("id", 0) == new_policy.get("id"):
                continue
            if r.get("status") == "已停用":
                continue
            if r.get("窗口类型") == new_type and r.get("窗口大小") == new_size:
                overlaps.append({
                    "conflicting_policy": r.get("策略名称", ""),
                    "conflict_id": r.get("id"),
                    "policy_version": self.policy_version_of(r),
                    "reason": f"相同类型({new_type})和大{new_size}",
                })
        return overlaps

    def consume_source_batch(
        self, source_name: str, policy_name: str,
    ) -> dict[str, Any]:
        """消费上游 stream_source 产生的数据批次，按当前生效策略版本生成窗口定义。

        序号32修复要点：
        - 读取当前生效策略版本的水位延迟，不再沿用策略最初绑定的旧值；
        - 窗口指标数据写入 policy_version 绑定，供水位计算和迟到补算按版本读取；
        - 按 (策略, 版本, 批次) 幂等写入，并发重试只保留一版；
        - 任何异常触发事务回滚，保留原状态。
        """
        table = store.rows(self.MODULE)
        snapshot = copy.deepcopy(table)  # 事务快照：失败时回滚保留原状态
        try:
            # 1. 确认当前生效策略版本
            policy = self.current_effective_policy(policy_name)
            if policy is None:
                if not self._policy_rows(policy_name):
                    return {"ok": False, "error": f"窗口策略「{policy_name}」不存在"}
                return {"ok": False, "error": f"策略「{policy_name}」未生效"}
            version = self.policy_version_of(policy)

            # 2. 查找上游源端批次
            batches = [
                r for r in store.rows("stream_source")
                if isinstance(r.get("batch_id"), str) and r.get("source") == source_name
            ]
            if not batches:
                return {"ok": False, "error": f"未找到源端「{source_name}」的生产批次"}

            latest = max(batches, key=lambda b: b.get("produced_at", 0))
            events = latest.get("events", [])
            batch_id = latest.get("batch_id", "")

            # 3. 按当前生效版本参数计算窗口（含水位延迟）
            try:
                wsize = float(policy.get("窗口大小", "60"))
            except (ValueError, TypeError):
                wsize = 60.0
            try:
                slide = float(policy.get("滑动步长", str(wsize)))
            except (ValueError, TypeError):
                slide = wsize
            allowed_late = self._allowed_lateness(policy)

            wtype = policy.get("窗口类型", "sliding")
            windows = []
            if events:
                min_ts = min(e.get("event_time", time.time()) for e in events)
                if wtype == "tumbling":
                    step = wsize
                else:
                    step = min(slide, wsize)
                ws = min_ts
                while ws <= min_ts + max(wsize * 2, 60):
                    we = ws + wsize
                    win_events = [e for e in events if ws <= e.get("event_time", 0) < we]
                    windows.append({
                        "window_start": ws,
                        "window_end": we,
                        "event_count": len(win_events),
                        "policy": policy_name,
                        "policy_version": version,
                        "source": source_name,
                    })
                    ws += step

            # 4. 幂等写入窗口指标数据：同一 (策略, 版本, 批次) 只保留一版
            metric_key = f"{policy_name}#v{version}#{batch_id}"
            window_def = {
                "策略名称": policy_name,
                "窗口类型": wtype,
                "窗口大小": str(wsize),
                "滑动步长": str(slide),
                "水位延迟": policy.get("水位延迟", "10"),
                "触发策略": policy.get("触发策略", "watermark"),
                "延迟处理": policy.get("延迟处理", "bypass"),
                "status": "已生效",
                "pending": True,
                "abnormal": False,
                "batch_id": batch_id,
                "policy_version": version,
                "metric_key": metric_key,
                "allowed_lateness": allowed_late,
                "window_definitions": windows,
                "produced_at": time.time(),
            }
            existing = [r for r in table if r.get("metric_key") == metric_key]
            replayed = bool(existing)
            if existing:
                # 并发重试收敛：保留首条并刷新内容，清除重复版本
                window_def["id"] = existing[0].get("id", 0)
                table[table.index(existing[0])] = window_def
                for dup in existing[1:]:
                    table.remove(dup)
            else:
                window_def["id"] = max(
                    (int(r.get("id", 0)) for r in table), default=0
                ) + 1
                table.append(window_def)
            return {
                "ok": True,
                "policy": policy_name,
                "policy_version": version,
                "allowed_lateness": allowed_late,
                "source": source_name,
                "events_consumed": len(events),
                "windows_generated": len(windows),
                "batch_id": batch_id,
                "metric_key": metric_key,
                "idempotent_replay": replayed,
            }
        except Exception as exc:  # noqa: BLE001 - 领域服务兜底回滚
            table[:] = snapshot
            store.register_failure(
                self.MODULE, "consume_source_batch", str(exc), "retry",
                {"source": source_name, "policy": policy_name},
            )
            return {"ok": False, "error": f"消费批次失败，已回滚保留原状态：{exc}"}

    def calculate_watermark(
        self, policy_name: str, events_processed: int = 0, event_time_max: float = 0,
    ) -> dict[str, Any]:
        """根据当前生效策略版本计算水位线。

        水位只依据当前版本窗口边界计算；旧版本窗口指标的水位单独列入
        history 保持可追溯，不并入当前结果。
        """
        policy = self.current_effective_policy(policy_name)
        if policy is None:
            return {
                "ok": False,
                "error": f"窗口策略「{policy_name}」不存在或未生效",
                "policy": policy_name,
                "watermarks": [],
            }
        version = self.policy_version_of(policy)
        allowed_late = self._allowed_lateness(policy)

        def _marks_for(metrics: list[dict[str, Any]]) -> list[dict[str, Any]]:
            marks = []
            for row in metrics:
                windows = row.get("window_definitions", [])
                if not windows:
                    continue
                last_end = max(w.get("window_end", 0) for w in windows)
                row_allowed = self._metric_allowed_lateness(row, policy)
                marks.append({
                    "policy": policy_name,
                    "policy_version": self.policy_version_of(row),
                    "metric_id": row.get("id"),
                    "metric_key": row.get("metric_key", ""),
                    "watermark": last_end - row_allowed,
                    "window_end": last_end,
                    "allowed_lateness": row_allowed,
                })
            return marks

        # 当前结果：只取绑定当前生效版本的窗口指标，最终限定一个标明版本的结果
        current_marks = _marks_for(self.window_metrics_for(policy_name, version))
        current = None
        if current_marks:
            latest = max(current_marks, key=lambda w: w["window_end"])
            current = {
                "policy": policy_name,
                "policy_version": version,
                "watermark": latest["watermark"],
                "window_end": latest["window_end"],
                "allowed_lateness": allowed_late,
            }

        # 旧口径窗口保持可追溯：历史版本水位单独列出，不按新口径静默重写
        history = _marks_for([
            row for row in self.window_metrics_for(policy_name)
            if self.policy_version_of(row) != version
        ])
        for mark in history:
            mark["traceable"] = True

        return {
            "ok": True,
            "policy": policy_name,
            "policy_version": version,
            "allowed_lateness": allowed_late,
            "current": current,
            "watermarks": current_marks,
            "history": history,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["active_policies"] = sum(
            1 for r in rows
            if not self._is_metric_row(r) and r.get("status") == "已生效"
        )
        base["total_window_defs"] = sum(1 for r in rows if self._is_metric_row(r))
        base["policy_versions"] = sum(1 for r in rows if not self._is_metric_row(r))
        return base
